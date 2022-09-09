import json
import logging
import boto3
import pandas as pd

from helper import *
from credentials import get_secret
from rds_handler import SqlHandler
from config import *


if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

sqs_client = boto3.client('sqs')

secret = json.loads(get_secret(DATABASE_SECRET_NAME))
if ENVIRONMENT == 'local':
    secret["host"] = 'host.docker.internal'


def lambda_handler(event, context):
    """
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
        logging.debug(json.dumps(body))
        logging.info('starting post processing with scenarioId: {}'.format(body['scenarioId']))

        # Fetch ETM data
        etm_dict = get_tar_gz_files(body['etmResultLocation'])
        logging.info('Retrieved ETM curves, found {}'.format(len(etm_dict)))
        # Fetch electricity ESSIM data
        essim_s3_key = pandasify_s3_key(body['essimExportTennetLocation'])
        essim_df = pd.read_csv(essim_s3_key, compression='gzip', sep=';', decimal='.', index_col='hour')
        # Process Electricity Load flow stuff
        investment_model_map, cat, reg = get_static_data()

        sites = get_essim_sites(body)
        network = get_network_database(body['networkId'])
        try:
            power = electricity_post_processing(sites, etm_dict['merit_order.csv'], essim_df, cat, reg, network)
        except ValueError as ex:
            logging.error(ex)
            logging.error('Post processing failed for scenarioId {}'.format(body['scenarioId']))
            return
        s3_key = body['bucketFolder'] + 'tennetInvestmentModels/' + body['tennetInvestmentPath'] + '/postProcessedTennet.csv.gz'
        pandas_s3_key = pandasify_s3_key(s3_key)
        power.to_csv(pandas_s3_key, compression='gzip', sep=';', decimal='.')

        update_list = []
        body['calculationState'] = 'postProcessingDone'
        body['postProcessingTennetLocation'] = s3_key

        response = sqs_client.send_message(
            QueueUrl=TENNET_LOADFLOW_QUEUE_URL,
            MessageBody=json.dumps(body, default=str),
        )
        logging.info(
            'Successfully calculated TenneT post processing with scenarioId: {} and network name {}'.format(
                body['scenarioId'], body['networkId']))

        with open('sql/update_scenario.sql', 'r') as f:
            sql_stmt = f.read()
        sql_handler.update_scenario_state(sql_stmt, update_list)
