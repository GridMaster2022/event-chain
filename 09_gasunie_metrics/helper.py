import pandas as pd
from io import BytesIO
import tarfile
import boto3

from config import *


s3_client = boto3.client('s3')

# Constant
factors = pd.read_csv('data/gasunie_factors.csv', sep=';', index_col=0, usecols=['name', 'length_km', 'from', 'to'])
h2_nodes = ["W1_001", "W1_002", "W1_003", "W1_004", "HW_001", "HW_002", "HW_003", "HW_004",
            "HW_005", "HW_006", "HW_007", "BAL_H"]

# Split H2 and CH4 [constant]
h2_links = factors.loc[factors['from'].isin(h2_nodes) | factors['to'].isin(h2_nodes)]
h2_links = h2_links.index.to_list()

ch4_links = factors.loc[-factors['from'].isin(h2_nodes) | -factors['to'].isin(h2_nodes)]
ch4_links = ch4_links.index.to_list()


def calculate_metrics(flow):
    # Evaluate overload values
    flow['overload'] = abs(flow.berekening) - abs(flow.voorwaarde)
    flow = flow.reset_index(drop=True)

    # Pivot table with elements on columns
    df = flow.pivot(columns='naam', values='overload')

    # evaluate overload stats
    median = df.median()
    frequency = df.count()
    volume = df.sum()

    # determine median * frequency and isolate element_ids
    scores = median * frequency / 8760

    # evaluate overload weighted scores
    weighted = scores.mul(factors.length_km, axis=0)

    # assign names to recognize
    volume.name = 'overloadVolume'
    median.name = 'overloadMedian'
    frequency.name = 'overloadFreq'
    scores.name = 'overloadScore'
    weighted.name = 'overloadCalculated'

    # make results dataframe
    frames = [volume, median, frequency, scores, weighted]
    performance = pd.concat(frames, axis=1)

    # add factors
    performance.insert(loc=0, column='weighFactor', value=factors.length_km)

    performance = performance.fillna(0)
    performance.index.name = 'elementName'
    return performance


def get_tar_gz_files(key, data_type):
    input_tar_file = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    input_tar_content = input_tar_file['Body'].read()
    file_dict = {}
    with tarfile.open(fileobj=BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if tar_resource.isfile():
                inner_file_bytes = tar.extractfile(tar_resource).read()
                if data_type == 'etm':
                    file_dict[tar_resource.name] = pd.read_csv(BytesIO(inner_file_bytes), index_col=0).reset_index(drop=True)
                else:
                    file_dict[tar_resource.name] = pd.read_csv(BytesIO(inner_file_bytes), index_col=0, sep=';', decimal='.').reset_index(
                        drop=True)

    return file_dict


def df_to_buffer(df, compression='infer', index=True):
    buffer = BytesIO()
    df.to_csv(buffer, compression=compression, sep=';', index=index)
    buffer.seek(0)
    return buffer


def pandasify_s3_key(s3_key):
    return 's3://' + BUCKET_NAME + '/' + s3_key