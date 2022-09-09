import boto3
import logging
import json

from config import *
from rds_handler import SqlHandler
from credentials import get_secret

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
    """
    Grid master Kick Off lambda to initialize calculation of a scenario, limits based on ETM request limit
    Parameters
    ----------
    event: dict, required
        Eventbridge input format
        Event doc: https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-run-lambda-schedule.html

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """
    # check messages in following queue to confirm no pile-up

    etm_queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=ETM_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    essim_queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=ESSIM_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    if int(etm_queue_attributes['Attributes']['ApproximateNumberOfMessages']) > 20:
        logging.info('Too many messages in ETM queue, skipping kick-off. Number of messages: ' +
                     str(etm_queue_attributes['Attributes']['ApproximateNumberOfMessages'])
                     )
        return
    elif int(essim_queue_attributes['Attributes']['ApproximateNumberOfMessages']) > ETM_REQUEST_LIMIT_PER_MINUTE:
        logging.info('Too many messages in ESSIM queue, skipping kick-off. Number of messages: ' +
                     str(etm_queue_attributes['Attributes']['ApproximateNumberOfMessages'])
                     )
        return

    # retrieve a scenarios from db where state = free
    sql_handler = SqlHandler(secret)
    with open('sql/get_new_scenarios.sql', 'r') as f:
        sql_stmt = f.read()
    sql_stmt = sql_stmt.format('essimExported', round(ETM_REQUEST_LIMIT_PER_MINUTE/6))
    scenarios = sql_handler.generic_fetchall(sql_stmt)
    # send every new scenario to the ETM queue

    # Update scenario state in DB
    with open('sql/update_scenario.sql', 'r') as f:
        sql_stmt = f.read()
    sql_handler.update_scenario_state(sql_stmt, scenarios)

    for scenario in scenarios:
        response = sqs_client.send_message(
            QueueUrl=ESDL_QUEUE_URL,
            MessageBody=json.dumps(scenario, default=str),
        )
