import pandas as pd

from networktools.substations import evaluate_mv_substations, evaluate_substation_overload
from config import *


def stedin_loadflow(essim_curves, essim_sites, essim_substations):
    flow = evaluate_mv_substations(essim_curves, essim_sites)
    overload = evaluate_substation_overload(essim_substations, flow, 0, 13.3)
    return flow, overload


def get_data_from_s3(s3_prefix):
    pandas_s3_key_base = 's3://' + BUCKET_NAME + '/' + s3_prefix
    essim_substations = pd.read_csv(pandas_s3_key_base + 'essim_substations.csv.gz', compression='gzip', index_col=1, decimal='.', sep=';')
    essim_sites = pd.read_csv(pandas_s3_key_base + 'essim_sites.csv.gz', compression='gzip', index_col=0, decimal='.', sep=';')
    if 'substation [t-1]' in essim_sites.columns:
        essim_sites = essim_sites.rename(columns={'substation [t-1]': 'substation'})
    return essim_substations, essim_sites


def upload_result_to_s3(s3_prefix, flow, overload):
    pandas_s3_key_base = 's3://' + BUCKET_NAME + '/' + s3_prefix
    flow_s3_key = pandas_s3_key_base + 'flow/flow.csv.gz'
    overload_s3_key = pandas_s3_key_base + 'overload/overload.csv.gz'

    flow.to_csv(flow_s3_key, compression='gzip', sep=';', decimal='.')
    overload.to_csv(overload_s3_key, compression='gzip', sep=';', decimal='.')
    return flow_s3_key, overload_s3_key


def pandasify_s3_key(s3_key):
    return 's3://' + BUCKET_NAME + '/' + s3_key