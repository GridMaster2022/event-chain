import pandas as pd
from io import BytesIO
import boto3
import tarfile

from config import *

s3_client = boto3.client('s3')


def save_to_s3(key, bytestream):
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=bytestream.getvalue()
    )


def get_tar_gz_files(key):
    input_tar_file = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    input_tar_content = input_tar_file['Body'].read()
    file_dict = {}
    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if tar_resource.isfile():
                inner_file_bytes = tar.extractfile(tar_resource).read()
                file_dict[tar_resource.name] = pd.read_csv(BytesIO(inner_file_bytes), index_col=0,
                                                           sep=',', decimal='.', on_bad_lines='warn'
                                                           ).reset_index(drop=True)
    return file_dict


def build_export_lists(df_dict):
    elec_df_list = []
    ch4_df_list = []
    h2_df_list = []
    co2_df_list = []
    for key in df_dict:
        if 'assetClass' in df_dict[key].columns and 'assetName' in df_dict[key].columns:
            unique_carrier_ids = df_dict[key]['carrierId'].unique()
            unique_asset_names = df_dict[key]['assetName'].unique()
            if ELEC_CLASS in df_dict[key]['assetClass'].iloc[0]:
                df_dict[key].loc[:, 'allocationPower'] /= 1000000
                elec_df_list.append(df_dict[key][['allocationPower', 'sector', 'assetName']])
            if CH4_CLASS in df_dict[key]['assetClass'].iloc[0]:
                if matched_carriers := [unique_carrier for unique_carrier in unique_carrier_ids if any(carrier in unique_carrier for carrier in CH4_CARRIER_IDS)]:
                    for matched_carrier in matched_carriers:
                        # There is a CH4 carrier present in this df
                        temp_df = df_dict[key][df_dict[key]['carrierId'] == matched_carrier]
                        temp_df.loc[:, 'allocationPower'] /= 1000000
                        ch4_df_list.append(temp_df[['allocationPower', 'sector', 'assetName']])
                if matched_carriers := [unique_carrier for unique_carrier in unique_carrier_ids if any(carrier in unique_carrier for carrier in H2_CARRIER_IDS)]:
                    # There is a H2 carrier present in this df
                    for matched_carrier in matched_carriers:
                        temp_df = df_dict[key][df_dict[key]['carrierId'] == matched_carrier]
                        temp_df.loc[:, 'allocationPower'] /= 1000000
                        h2_df_list.append(temp_df[['allocationPower', 'sector', 'assetName']])
                if matched_carriers := [unique_carrier for unique_carrier in unique_carrier_ids if any(carrier in unique_carrier for carrier in CO2_CARRIER_IDS)]:
                    # There is a H2 carrier present in this df
                    for matched_carrier in matched_carriers:
                        temp_df = df_dict[key][df_dict[key]['carrierId'] == matched_carrier]
                        temp_df.loc[:, 'allocationPower'] /= 1000000
                        co2_df_list.append(temp_df[['allocationPower', 'sector', 'assetName']])
            elif matched_assets := [unique_asset for unique_asset in unique_asset_names if
                                  any(asset in unique_asset for asset in CH4_ASSET_NAMES)]:
                for matched_asset in matched_assets:
                    temp_df = df_dict[key][df_dict[key]['assetName'] == matched_asset]
                    temp_df.loc[:, 'allocationPower'] /= 1000000
                    ch4_df_list.append(temp_df[['allocationPower', 'sector', 'assetName']])
            elif matched_assets := [unique_asset for unique_asset in unique_asset_names if
                                  any(asset in unique_asset for asset in H2_ASSET_NAMES)]:
                for matched_asset in matched_assets:
                    temp_df = df_dict[key][df_dict[key]['assetName'] == matched_asset]
                    temp_df.loc[:, 'allocationPower'] /= 1000000
                    h2_df_list.append(temp_df[['allocationPower', 'sector', 'assetName']])
    return pd.concat(elec_df_list), pd.concat(ch4_df_list), pd.concat(h2_df_list), pd.concat(co2_df_list)


def site_mapping(dataframe):
    if ELECTRICITY_MAPPING == METHANE_MAPPING:
        changeto_CH4 = lambda x: x.replace('RTLH_ODO', 'CH4') if 'RTLH_ODO' in x else \
            (x.replace('RTLG_ODO', 'CH4') if 'RTLG_ODO' in x else
                 (x.replace('RTLH_NODO', 'CH4') if 'RTLH_NODO' in x else
                      (x.replace('RTLG_NODO', 'CH4') if 'RTLG_NODO' in x else
                           (x.replace('HTLH', 'CH4') if 'HTLH' in x else
                                (x.replace('HTLG', 'CH4') if 'HTLG' in x else
                                    (x.replace('GM', 'CH4') if 'GM' in x else
                                     x))))))

        dataframe = dataframe.rename(columns=changeto_CH4)
        dataframe = dataframe.groupby(by=dataframe.columns, axis=1).sum()

    dataframe.columns = dataframe.columns.map(ELECTRICITY_MAPPING)

    return dataframe


def structure_table(original_influxdb_dataframe):
    df = pd.DataFrame()

    for assetName in original_influxdb_dataframe['assetName'].unique():
        boolean_client = original_influxdb_dataframe['assetName'] == assetName
        allocationPower_client = original_influxdb_dataframe[boolean_client].allocationPower
        df[assetName] = allocationPower_client

    df = site_mapping(df)
    df = df.reset_index(drop=True)
    return df


def df_to_buffer(df, compression='infer'):
    buffer = BytesIO()
    df.to_csv(buffer, compression=compression, sep=';', decimal='.')
    buffer.seek(0)
    return buffer


def pandasify_s3_key(s3_key):
    return 's3://' + BUCKET_NAME + '/' + s3_key
