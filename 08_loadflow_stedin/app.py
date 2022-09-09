import json
import logging
import boto3
import pandas as pd
from copy import deepcopy

from helper import stedin_loadflow, get_data_from_s3, upload_result_to_s3, pandasify_s3_key
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
        SNS Input Format, a maximum total of 10 records in 1 event
        Event doc: https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """
    logging.info(json.dumps(event))
    sql_handler = SqlHandler(secret)
    for record in event['Records']:
        sns_body = json.loads(record['body'])
        body = json.loads(sns_body['Message'])
        logging.info('starting Stedin loadflow with scenarioId: {}'.format(body['scenarioId']))

        # Find Stedin Station Designs which are to be calculated, based on unique folders
        paginator = s3_client.get_paginator('list_objects')
        result = paginator.paginate(Bucket=BUCKET_NAME, Prefix=body['bucketFolder']+'stedinDesigns/', Delimiter='/')
        stedin_design_list = []
        for prefix in result.search('CommonPrefixes'):
            stedin_design_list.append(prefix.get('Prefix'))
        # fetch the essim curves from the top level folder (uuid/year)
        essim_s3_key_pandas = pandasify_s3_key(body['essimExportTennetLocation'])
        essim_curves = pd.read_csv(essim_s3_key_pandas, compression='gzip', sep=';', decimal='.', index_col='hour')
        update_list = []
        for stedin_design in stedin_design_list:
            temp_body = deepcopy(body)
            essim_substations, essim_sites = get_data_from_s3(stedin_design)
            header_list = [column for column in essim_curves.columns if column not in essim_sites.index]
            logging.info('The following sites were not found in essim curves: {}'.format(' '.join(header_list)))
            for header in header_list:
                if essim_curves[header].sum() == 0:
                    essim_curves = essim_curves.drop(header, axis=1)
            flow, overload = stedin_loadflow(essim_curves, essim_sites, essim_substations)
            upload_result_to_s3(stedin_design, flow, overload)
            temp_body['stedinDesign'] = stedin_design.split('/')[-2]
            temp_body['calculationState'] = 'stedinLoadFlowDone'
            temp_body['stedinLoadFlowLocation'] = stedin_design + 'flow.csv.gz'
            temp_body['stedinOverloadLocation'] = stedin_design + 'overload.csv.gz'
            update_list.append(temp_body)

        with open('sql/update_scenario.sql', 'r') as f:
            sql_stmt = f.read()
        sql_handler.update_scenario_state(sql_stmt, update_list)

