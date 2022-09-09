import json
import logging
import boto3

from helper import get_loadflow_input, tennet_loadflow, upload_loadflow_to_s3
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
        logging.info('starting Tennet Loadflow with scenarioId: {}'.format(body['scenarioId']))

        # The required PandaPower network will not be shared, user will have to generate their own, ref:
        # https://pandapower.readthedocs.io/en/v2.9.0/elements.html
        power, network = get_loadflow_input(s3_client, body)
        load, performance = tennet_loadflow(network, power)

        load_s3_key, metrics_s3_key = upload_loadflow_to_s3(body, load, performance)
        body['calculationState'] = 'TennetLoadFlowProcessed'
        body['tennetLoadFlowLocation'] = load_s3_key
        body['investmentPlan'] = body['tennetInvestmentPath']
        body['tennetMetricslocation'] = metrics_s3_key

        with open('sql/update_scenario.sql', 'r') as f:
            sql_stmt = f.read()
        sql_handler.update_scenario_state(sql_stmt, [body])
