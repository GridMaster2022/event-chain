import json
import logging
import boto3

from helper import get_tar_gz_files, pandasify_s3_key, calculate_metrics
from credentials import get_secret
from rds_handler import SqlHandler
from config import *

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
    sql_handler = SqlHandler(secret)
    logging.info(json.dumps(event))
    for record in event['Records']:
        body = json.loads(record['body'])
        logging.info('starting GasUnie post processing with scenarioId: {}'.format(body['scenarioId']))

        gasunie_loadflow_tar = get_tar_gz_files(body['gasunieLoadFlowLocation'], data_type='normal')
        csv_file = [key for key in gasunie_loadflow_tar if key.startswith('faal')][0]
        performance = calculate_metrics(gasunie_loadflow_tar[csv_file])

        perf_s3_key = body['gasunieLoadFlowLocation'].rsplit('/', 1)[0] + '/Metrics/loadflowGasunieMetrics.csv.gz'
        performance.to_csv(pandasify_s3_key(perf_s3_key), compression='gzip', sep=';', decimal='.')
        investment_model, network_id = body['gasunieLoadFlowLocation'].rsplit('/', 2)[1].split('_')
        body['gasunieMetricsLocationCH4'] = perf_s3_key
        body['gasunieMetricsLocationH2'] = ''
        body['networkId'] = network_id
        body['gasunieInvestmentModel'] = investment_model
        body['calculationState'] = 'metricsCalculated'

        logging.info('Successfully calculated GasUnie post processing with scenarioId: {}'.format(body['scenarioId']))
        with open('sql/update_scenario.sql', 'r') as f:
            sql_stmt = f.read()
        sql_handler.update_scenario_state(sql_stmt, [body])
