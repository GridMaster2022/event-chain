import json
import logging
import boto3
import os
from io import StringIO

from config import *
from esdl_updater import update_esdl, update_profiles
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

sqs_client = boto3.client('sqs')


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
    logging.debug(json.dumps(event))
    sql_handler = SqlHandler(secret)
    for record in event['Records']:
        body = json.loads(record['body'])
        logging.info('starting esdl update for scenarioId: {}'.format(body['scenarioId']))
        logging.info(json.dumps(body))

        esdl_string = get_esdl_from_s3(body['baseEsdlLocation'])
        context_scenario = get_json_from_s3(body['contextScenarioLocation'])
        etm_dict = get_tar_gz_files(body['etmResultLocation'])

        updated_esdl = update_esdl(context_scenario['contextScenario'], esdl_string)
        updated_esdl_with_profiles = update_profiles(updated_esdl, etm_dict['merit_order.csv'])

        # write updated esdl to s3
        s3_key = body['bucketFolder'] + 'updatedEsdl.esdl'
        save_to_s3(s3_key, updated_esdl_with_profiles.getvalue())

        # Push message to next queue
        body['calculationState'] = 'esdlUpdated'
        body['updatedEsdlLocation'] = s3_key

        logging.info('Successfully updated the esdl with scenarioId: {}'.format(body['scenarioId']))
        with open('sql/update_scenario.sql', 'r') as f:
            sql_stmt = f.read()
        sql_handler.update_scenario_state(sql_stmt, [body])
        response = sqs_client.send_message(
            QueueUrl=ESSIM_QUEUE_URL,
            MessageBody=json.dumps(body, default=str),
        )
