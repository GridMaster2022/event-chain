import json
import logging
import pandas as pd
import boto3
from io import BytesIO
import tarfile

from config import *
from helper import *
from credentials import get_secret
from rds_handler import SqlHandler

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

secret = json.loads(get_secret(DATABASE_SECRET_NAME))
if ENVIRONMENT == 'local':
    secret["host"] = 'host.docker.internal'

sns_client = boto3.client('sns')


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        SQS Input Format, a maximum total of 10 records in 1 event
        Event doc: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """
    logging.info(json.dumps(event))
    sql_handler = SqlHandler(secret)
    for record in event['Records']:
        body = json.loads(record['body'])
        logging.info('starting ESSIM export with scenarioId: {}'.format(body['scenarioId']))

        essim_file_dict = get_tar_gz_files(body['essimResultLocation'])
        elec_df, ch4_df, h2_df, co2_df = build_export_lists(essim_file_dict)

        # Electriciteit export (tennet & stedin)
        essim_electricity = structure_table(elec_df)
        essim_electricity['hour'] = essim_electricity.index
        tennet_s3_key = body['bucketFolder'] + 'essimResultTennet.csv.gz'
        pandas_tennet_s3_key = pandasify_s3_key(tennet_s3_key)
        essim_electricity.to_csv(pandas_tennet_s3_key, compression='gzip', index=False, sep=';', decimal='.')
        del essim_electricity, elec_df

        # GasUnie Export
        essim_methane = structure_table(ch4_df)
        essim_methane = essim_methane.loc[:, essim_methane.columns.notnull()]

        essim_hydrogen = structure_table(h2_df)
        essim_hydrogen = essim_hydrogen.loc[:, essim_hydrogen.columns.notnull()]

        essim_hydrogen_buffer = df_to_buffer(essim_hydrogen)
        essim_methane_buffer = df_to_buffer(essim_methane)
        # Package results
        tar_file = BytesIO()
        with tarfile.open(fileobj=tar_file, mode='w:gz') as tar:
            info = tarfile.TarInfo('methane.csv')
            info.size = essim_methane_buffer.getbuffer().nbytes  # this is crucial
            tar.addfile(info, essim_methane_buffer)
            info = tarfile.TarInfo('hydrogen.csv')
            info.size = essim_hydrogen_buffer.getbuffer().nbytes
            tar.addfile(info, fileobj=essim_hydrogen_buffer)

        gasunie_s3_key = body['bucketFolder'] + 'essimResultGasunie.tar.gz'
        save_to_s3(gasunie_s3_key, tar_file)

        # Export additional CO2 data from InfluxDB
        co2_df_f = co2_df[co2_df['assetName'].str.contains('CO2_F')]
        s3_key = body['bucketFolder'] + 'co2Results/co2_f_export.csv.gz'
        pandas_s3_key = pandasify_s3_key(s3_key)
        co2_df_f.to_csv(pandas_s3_key, compression='gzip', index=False, sep=';', decimal='.')

        co2_df_b = co2_df[co2_df['assetName'].str.contains('CO2_B')]
        s3_key = body['bucketFolder'] + 'co2Results/co2_b_export.csv.gz'
        pandas_s3_key = pandasify_s3_key(s3_key)
        co2_df_b.to_csv(pandas_s3_key, compression='gzip', index=False, sep=';', decimal='.')

        co2_df_p = co2_df[co2_df['assetName'].str.contains('CO2_P')]
        s3_key = body['bucketFolder'] + 'co2Results/co2_p_export.csv.gz'
        pandas_s3_key = pandasify_s3_key(s3_key)
        co2_df_p.to_csv(pandas_s3_key, compression='gzip', index=False, sep=';', decimal='.')

        # Push message to next queue
        body['calculationState'] = 'essimExported'
        body['essimExportTennetLocation'] = tennet_s3_key
        body['essimExportGasunieLocation'] = gasunie_s3_key

        logging.info('Succesfully exported and deleted with scenarioId: {} and UUID: {}'.format(
            body['scenarioId'],
            body['scenarioUuid']
        ))
        with open('sql/update_scenario.sql', 'r') as f:
            sql_stmt = f.read()
        sql_handler.update_scenario_state(sql_stmt, [body])

        response = sns_client.publish(
            TopicArn=POST_PROCESSING_FANOUT_ARN,
            Message=json.dumps(body),
        )
