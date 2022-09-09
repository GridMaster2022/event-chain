import pandas as pd
from io import BytesIO
import tarfile
import boto3

from networktools.postprocessing import make_MCA_input_frame
from config import *

s3_client = boto3.client('s3')


def mca_post_processing(network, body, essim_methane, etm_mcurve, essim_hydrogen, etm_hcurve):
    # load sites and curves
    methane_sites, hydrogen_sites = get_essim_sites(body, network)

    # REMOVE LATER
    methane_sites = methane_sites.dropna()
    methane_sites = methane_sites[methane_sites.index.notnull()]
    hydrogen_sites = hydrogen_sites.dropna()
    hydrogen_sites = hydrogen_sites[hydrogen_sites.index.notnull()]

    # Tijdstappen uit ESSIM niet altijd consistent, drop ESSIM als die te veel zijn. of ETM als ESSIM te weinig heeft.
    essim_methane = essim_methane[:8760]
    essim_hydrogen = essim_hydrogen[:8760]

    old_messim_len = len(essim_methane)
    if old_messim_len != len(etm_mcurve):
        if old_messim_len < len(etm_mcurve):
            for i in range(1, 8760-old_messim_len+1):
                essim_methane.loc[old_messim_len+i-1] = essim_methane.iloc[-1]

    old_hessim_len = len(essim_hydrogen)
    if old_hessim_len != len(etm_hcurve):
        if old_hessim_len < len(etm_hcurve):
            for i in range(1, 8760-old_hessim_len+1):
                essim_hydrogen.loc[old_hessim_len+i-1] = essim_hydrogen.iloc[-1]

    # Filter essim columns on balance nodes, set them on respective ETM frames
    # CH4
    bal_m_list = [index for index, row in methane_sites.iterrows() if row['substation'] == 'BAL_M']
    bal_m_list = [entry for entry in bal_m_list if entry in essim_methane.columns]
    essim_methane['BAL_M'] = essim_methane[bal_m_list].sum(axis=1)
    essim_methane = essim_methane.drop(bal_m_list, axis=1)
    # Add row to sites to allow Post Processing to add BAL_M/H to the MCA frame.
    methane_sites_temp = pd.DataFrame([['BAL_M']], columns=['substation'], index=['BAL_M'])
    methane_sites = methane_sites.append(methane_sites_temp)

    # Hydrogen
    bal_h_list = [index for index, row in hydrogen_sites.iterrows() if row['substation'] == 'BAL_H']
    bal_h_list = [entry for entry in bal_h_list if entry in essim_hydrogen.columns]
    essim_hydrogen['BAL_H'] = essim_hydrogen[bal_h_list].sum(axis=1)
    essim_hydrogen = essim_hydrogen.drop(bal_h_list, axis=1)
    # Add row to sites to allow Post Processing to add BAL_M/H to the MCA frame.
    hydro_sites_temp = pd.DataFrame([['BAL_H']], columns=['substation'], index=['BAL_H'])
    hydrogen_sites = hydrogen_sites.append(hydro_sites_temp)

    mca = make_MCA_input_frame(essim_methane, methane_sites, essim_hydrogen, hydrogen_sites)
    if 0 in mca.index.levels[1]:
        # Fix discrepancy between post processing and loadflow (1-24 hours instead of 0-23)
        mca.index = mca.index.set_levels(mca.index.levels[1] + 1, level=1)
    s3_gasunie_mca_key = network + 'postProcessedGasunie.csv.gz'
    assignment_csv = loadflow_assigment_contructor(network)
    s3_gasunie_assignment_key = network + 'assignment.csv.gz'

    return assignment_csv, s3_gasunie_assignment_key, mca, s3_gasunie_mca_key


def get_tar_gz_files(key, data_type):
    input_tar_file = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    input_tar_content = input_tar_file['Body'].read()
    file_dict = {}
    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if tar_resource.isfile():
                inner_file_bytes = tar.extractfile(tar_resource).read()
                if data_type == 'etm':
                    file_dict[tar_resource.name] = pd.read_csv(BytesIO(inner_file_bytes),
                                                               index_col=0).reset_index(drop=True)
                else:
                    file_dict[tar_resource.name] = pd.read_csv(BytesIO(inner_file_bytes),
                                                               index_col=0, sep=';', decimal='.'
                                                               ).reset_index(drop=True)
    return file_dict


def loadflow_assigment_contructor(network):
    invest_plan = 'NETWERK' + network[:-1].split('_')[1]
    assignment = pd.DataFrame(data={'investeringsplan': [invest_plan], 'etmreeks': ['postProcessedGasunie']})
    return assignment


def get_essim_sites(body, network):
    s3_essim_msites_key = 's3://' + body['bucketName'] + '/' + network + 'essim_msites.csv.gz'
    methane_sites = pd.read_csv(s3_essim_msites_key, index_col=0, decimal='.', sep=';')
    if "substation" not in methane_sites.columns:
        methane_sites = methane_sites.rename(columns={methane_sites.columns[0]: "substation"})
    s3_essim_hsites_key = 's3://' + body['bucketName'] + '/' + network + 'essim_hsites.csv.gz'
    hydrogen_sites = pd.read_csv(s3_essim_hsites_key, index_col=0, decimal='.', sep=';')
    if "substation" not in hydrogen_sites.columns:
        hydrogen_sites = hydrogen_sites.rename(columns={hydrogen_sites.columns[0]: "substation"})
    return methane_sites, hydrogen_sites


def df_to_buffer(df, compression='infer', index=True):
    buffer = BytesIO()
    df.to_csv(buffer, compression=compression, sep=';', index=index, decimal='.')
    buffer.seek(0)
    return buffer


def pandasify_s3_key(s3_key):
    return 's3://' + BUCKET_NAME + '/' + s3_key


def fix_essim_df(essim_gas):
    # Temporary fix for the gasunie output of essim. To make sure the demand/prod is balanced, due to missing nodes in essim.
    essim_methane = essim_gas['methane.csv']
    rowSumMethane = essim_methane.sum(axis=1)
    overDemand = rowSumMethane.where(rowSumMethane > 0).fillna(0)
    overProduction = rowSumMethane.where(rowSumMethane < 0).fillna(0)
    essim_methane.CH4Export_Per = essim_methane.CH4Export_Per.sub(overProduction)
    essim_methane.CH4Import_Wijngaarden = essim_methane.CH4Import_Wijngaarden.sub(overDemand)
    essim_gas['methane.csv'] = essim_methane
    essim_hydrogen = essim_gas['hydrogen.csv']
    rowSumHydrogen = essim_hydrogen.sum(axis=1)
    overDemand = rowSumHydrogen.where(rowSumHydrogen > 0).fillna(0)
    overProduction = rowSumHydrogen.where(rowSumHydrogen < 0).fillna(0)
    essim_hydrogen.ExportH2_Per = essim_hydrogen.ExportH2_Per.sub(overProduction)
    essim_hydrogen.ImportH2_MV = essim_hydrogen.ImportH2_MV.sub(overDemand)
    essim_gas['hydrogen.csv'] = essim_hydrogen
    return essim_gas
