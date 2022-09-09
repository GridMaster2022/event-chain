import json
import logging
import boto3
from copy import deepcopy

from config import *

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        SNS Input Format
        Event doc: https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """
    logging.info(json.dumps(event))
    for record in event['Records']:
        sns_body = json.loads(record['body'])
        body = json.loads(sns_body['Message'])
        logging.debug(json.dumps(body))
        logging.info('Starting TenneT fanout for scenarioId: {}'.format(body['scenarioId']))

        for investment_path, network_id in INVESTMENT_MODEL_MAP[str(body['scenarioYear'])].iteritems():
            temp_body = deepcopy(body)
            temp_body['networkId'] = network_id
            temp_body['tennetInvestmentPath'] = investment_path
            response = sqs_client.send_message(
                QueueUrl=TENNET_POST_PROCESSING_QUEUE_URL,
                MessageBody=json.dumps(temp_body, default=str),
            )
