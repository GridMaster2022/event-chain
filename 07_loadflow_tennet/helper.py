import pandas as pd
import pandapower as pp
import os

from networktools.loadflow import run_loadflow
from networktools.loadflow import evaluate_network_overload
from config import *


def tennet_loadflow(net, power):
    flows, subnet = run_loadflow(net, power)
    performance = evaluate_network_overload(subnet, flows)
    return flows, performance


def get_loadflow_input(s3_client, body):
    power = pd.read_csv(pandasify_s3_key(body['postProcessingTennetLocation']), compression='gzip', sep=';', decimal='.')
    power = power.drop('hour', axis=1)
    network = get_network_database(s3_client, body)
    return power, network


def get_network_database(s3_client, body):
    response = s3_client.get_object(
        Bucket='gridmaster-networks',
        Key=body['networkId'] + '.sqlite'
    )
    # If script is running in AWS lambda use /tmp storage folder, max 500 MB ephemeral storage
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        write_path = '/tmp/network.sqlite'
    else:
        write_path = 'tmp/network.sqlite'
    with open(write_path, 'wb') as f:
        f.write(response['Body'].read())
    net = pp.from_sqlite(write_path)
    return net


def pandasify_s3_key(s3_key):
    return 's3://' + BUCKET_NAME + '/' + s3_key


def determine_investment_paths(network_name):
    investment_model_map = pd.read_csv('data/investments_investments_model_mapping.csv', sep=';', index_col='index', dtype=str)
    investment_list = investment_model_map[investment_model_map.apply(lambda row: row.str.contains(network_name).any(), axis=1)].index.tolist()
    return investment_list


def upload_loadflow_to_s3(body, load, performance):
    load_s3_key = BUCKET_NAME + 'tennetInvestmentModels/' + body['tennetInvestmentPath'] + '/loadFlow/' + 'loadFlowTennet.csv.gz'
    load_pandas_s3_key = pandasify_s3_key(load_s3_key)
    load.to_csv(load_pandas_s3_key, compression='gzip', sep=';', decimal='.')
    metrics_s3_key = BUCKET_NAME + 'tennetInvestmentModels/' + body['tennetInvestmentPath'] + '/metrics/' + 'loadFlowTennetMetrics.csv.gz'
    metrics_pandas_s3_key = pandasify_s3_key(metrics_s3_key)
    performance.to_csv(metrics_pandas_s3_key, compression='gzip', sep=';', decimal='.')
    return load_s3_key, metrics_s3_key

