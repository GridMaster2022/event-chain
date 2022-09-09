import json
import logging
import boto3
from copy import deepcopy

from helper import *
from config import *
from credentials import get_secret
from rds_handler import SqlHandler

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

secret = json.loads(get_secret(DATABASE_SECRET_NAME))
if ENVIRONMENT == 'local':
    secret["host"] = 'host.docker.internal'


def lambda_handler(event, context):
    """

    Parameters
    ----------
    event: dict, required
        SNS Input Format
        Event doc: https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """
    sql_handler = SqlHandler(secret)
    for record in event['Records']:
        sns_body = json.loads(record['body'])
        body = json.loads(sns_body['Message'])
        logging.info(json.dumps(event))
        logging.info('starting GasUnie post processing with scenarioId: {}'.format(body['scenarioId']))

        etm_dict = get_tar_gz_files(body['etmResultLocation'], data_type='etm')
        # Get ESSIM export for Methane/Hydrogen
        essim_gas = get_tar_gz_files(body['essimExportGasunieLocation'], data_type='normal')  # [24:]
        # Process Gasunie loadflow stuff
        essim_gas = fix_essim_df(essim_gas)

        # Find which networks have been initialized in the bucket and calculate them
        paginator = s3_client.get_paginator('list_objects_v2')
        result = paginator.paginate(Bucket=BUCKET_NAME, Prefix=body['bucketFolder'] +'gasunieInvestmentModels/', Delimiter='/')
        network_list = []
        for prefix in result.search('CommonPrefixes'):
            network_list.append(prefix.get('Prefix'))
        update_list = []
        for network in network_list:
            temp_body = deepcopy(body)
            try:
                assignment_csv, s3_gasunie_assignment_key, mca, s3_gasunie_mca_key = mca_post_processing(network, body,
                    essim_gas['methane.csv'], etm_dict['network_gas.csv'], essim_gas['hydrogen.csv'], etm_dict['hydrogen.csv'])
            except FileNotFoundError:
                continue
            mca.to_csv(pandasify_s3_key(s3_gasunie_mca_key), compression='gzip', sep=';', decimal='.')
            assignment_csv.to_csv(pandasify_s3_key(s3_gasunie_assignment_key), compression='gzip',
                                  index=False, sep=';', decimal='.')
            network_id = network.split('/')[-2]
            investment_model, network_id = network_id.split('_')
            temp_body['networkId'] = network_id
            temp_body['gasunieInvestmentModel'] = investment_model
            temp_body['calculationState'] = 'postProcessingDone'
            temp_body['postProcessingGasunieLocation'] = s3_gasunie_mca_key
            temp_body['postProcessingGasunieAssignmentLocation'] = s3_gasunie_assignment_key
            update_list.append(temp_body)
            response = sqs_client.send_message(
                QueueUrl=GASUNIE_LOADFLOW_QUEUE_URL,
                MessageBody=json.dumps(temp_body, default=str),
            )

        logging.info('Successfully calculated GasUnie post processing with scenarioId: {}'.format(body['scenarioId']))
        with open('sql/update_scenario.sql', 'r') as f:
            sql_stmt = f.read()
        sql_handler.update_scenario_state(sql_stmt, update_list)
