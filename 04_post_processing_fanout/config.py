import os
import pandas as pd

ENVIRONMENT = os.environ['ENVIRONMENT']
TENNET_POST_PROCESSING_QUEUE_URL = os.environ['TENNET_POST_PROCESSING_QUEUE_URL']
BUCKET_NAME = os.environ['BUCKET_NAME']
DATABASE_SECRET_NAME = os.environ['DATABASE_SECRET_NAME']

INVESTMENT_MODEL_MAP = pd.read_csv('data/investments_investments_model_mapping.csv', sep=';', index_col='index',
                                   dtype=str)
