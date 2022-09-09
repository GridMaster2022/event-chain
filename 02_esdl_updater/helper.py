import tarfile
from io import BytesIO
import pandas as pd
import json
import boto3

from config import *

s3_client = boto3.client('s3')


def get_tar_gz_files(key):
    input_tar_file = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    input_tar_content = input_tar_file['Body'].read()
    etm_dict = {}
    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if tar_resource.isfile():
                inner_file_bytes = tar.extractfile(tar_resource).read()
                etm_dict[tar_resource.name] = pd.read_csv(BytesIO(inner_file_bytes), index_col=0)
    return etm_dict


def get_json_from_s3(key):
    response = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=key
    )
    return json.loads(response['Body'].read().decode('utf-8'))


def get_esdl_from_s3(key):
    response = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=key
    )
    esdl_string = response['Body'].read().decode('utf-8')
    if 'host="http://influxdb"' in esdl_string:
        esdl_string = esdl_string.replace('host="http://influxdb"', 'host="http://{}"'.format(INFLUX_DB_IP))
    return esdl_string


def save_to_s3(s3_key, body):
    response = s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=body
    )
