import pandas as pd
import pandapower as pp
from io import BytesIO
import tarfile
import logging
import boto3

from networktools.postprocessing import make_nodal_ecurves
from config import *

s3_client = boto3.client('s3')


def electricity_post_processing(sites, etm_curve, essim_df, cat, reg, network):
    # load sites and curves:
    if 'Rijnmond_Per' in essim_df.columns:
        essim_df = essim_df.drop('Rijnmond_Per', axis=1)
    header_list = [column for column in essim_df.columns if column not in sites.index]
    for header in header_list:
        if essim_df[header].sum() == 0:
            essim_df = essim_df.drop(header, axis=1)
    logging.info('Missing headers in essim df: ' + ' '.join(header_list))

    # Tijdstappen uit ESSIM niet altijd consistent, drop ESSIM als die te veel zijn. of ETM als ESSIM te weinig heeft.
    essim_df = essim_df[:8760]
    if len(essim_df) != len(etm_curve):
        if len(essim_df) < len(etm_curve):
            etm_curve = etm_curve[:len(essim_df)]
        elif len(etm_curve) < len(essim_df):
            essim_df = essim_df[:len(etm_curve)]
    etm_curve = etm_curve.drop('Time', axis=1)
    power = make_nodal_ecurves(etm_curve, essim_df, cat, reg, sites, network, size=750)
    return power


def save_power_to_s3(body, power, network_name):
    s3_tennet_key = body['bucketFolder'] + 'tennetLoadFlow/' + network_name + '/postProcessedTennet.csv.gz'
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_tennet_key,
        Body=df_to_buffer(power)
    )
    return s3_tennet_key


def get_static_data():
    investment_model_map = pd.read_csv('data/investments_investments_model_mapping.csv', sep=';', index_col='index', dtype=str)
    cat = pd.read_csv('data/etm_curves_categorization.csv', sep=';', decimal=',', index_col=0)
    reg = pd.read_csv('data/etm_ecurves_regionalization.csv', sep=';', decimal=',', index_col=0)
    return investment_model_map, cat, reg


def get_network_database(network_name):
    response = s3_client.get_object(
        Bucket=NETWORK_BUCKET_NAME,
        Key=network_name + '.sqlite'
    )
    # If script is running in AWS lambda use /tmp storage folder, max 500 MB ephemeral storage
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        write_path = '/tmp/network.sqlite'
    else:
        write_path = 'tmp/network.sqlite'
    with open(write_path, 'wb') as f:
        f.write(response['Body'].read())
    net = pp.from_sqlite(write_path)
    return net


def df_to_buffer(df):
    buffer = BytesIO()
    df.to_csv(buffer, compression='gzip', sep=';', decimal='.')
    buffer.seek(0)
    return buffer.getvalue()


def get_tar_gz_files(key):
    input_tar_file = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    input_tar_content = input_tar_file['Body'].read()
    etm_dict = {}
    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if tar_resource.isfile():
                inner_file_bytes = tar.extractfile(tar_resource).read()
                etm_dict[tar_resource.name] = pd.read_csv(BytesIO(inner_file_bytes))
    return etm_dict


def get_essim_sites(body):
    s3_essim_sites_key = 's3://'+body['bucketName']+'/'+body['bucketFolder'] + 'tennetInvestmentModels/'\
                         + body['tennetInvestmentPath'] + '/essim_sites.csv.gz'
    sites = pd.read_csv(s3_essim_sites_key, index_col=0, decimal='.', sep=';')
    if "substation [t-1]" in sites.columns:
        sites = sites.rename(columns={"substation [t-1]": "substation"})
    sites['sector'] = sites['sector'].apply(lambda x: x.replace('_', ' '))
    return sites


def pandasify_s3_key(s3_key):
    return 's3://' + BUCKET_NAME + '/' + s3_key


def determine_investment_paths(network_name):
    investment_model_map = pd.read_csv('data/investments_investments_model_mapping.csv', sep=';', index_col='index', dtype=str, decimal='.')
    investment_list = investment_model_map[investment_model_map.apply(lambda row: row.str.contains(network_name).any(), axis=1)].index.tolist()
    return investment_list
