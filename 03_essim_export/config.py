import os
from pandas import read_excel

ENVIRONMENT = os.environ['ENVIRONMENT']
POST_PROCESSING_FANOUT_ARN = os.environ['POST_PROCESSING_FANOUT_ARN']
BUCKET_NAME = os.environ['BUCKET_NAME']
INFLUX_DB_IP = os.environ['INFLUX_DB_IP']
DATABASE_SECRET_NAME = os.environ['DATABASE_SECRET_NAME']
DATABASE_SCHEMA_NAME = os.environ['DATABASE_SCHEMA_NAME']


MAPPING_DATA = read_excel('hic_description_final.xlsx', sheet_name='Site_mapping', header=0, index_col=None,
                          engine='openpyxl')

ELECTRICITY_MAPPING = dict(MAPPING_DATA[['EInfluxName', 'E_GridmasterName']].values)
METHANE_MAPPING = dict(MAPPING_DATA[['GInfluxName', 'G_GridmasterName']].values)
HYDROGEN_MAPPING = dict(MAPPING_DATA[['H2InfluxName', 'G_GridmasterName']].values)

EXPORT_LIST = ['CH4Export_Per', 'ExportH2_Per', 'ExportH2_Hinterland']
IMPORT_LIST = ['LNGImport_MV', 'CH4Import_Wijngaarden', 'ImportH2_MV',
               'Enecogen_MVB', 'UniperMPP3_MVB', 'AVR_BotA', 'Cabot_BotA', 'Engie_MVB',
               'MaasStroom_Per', 'Rijnmond_Per']


ELEC_CLASS = 'EConnection'
CH4_CLASS = 'GConnection'
CH4_CARRIER_IDS = ['RTLH_NODO', 'RTLG_NODO', 'HTLH', 'HTLG', 'RTLH_ODO', 'RTLG_ODO', 'GM', 'CH4']
CH4_ASSET_NAMES = ['LNGImport_MV', 'CH4Import_Wijngaarden']
H2_CLASS = 'GConnection'
H2_CARRIER_IDS = ['H2_new']
H2_ASSET_NAMES = ['ImportH2_MV', 'ExportH2_Per', 'ExportH2_new_Hinterland']
CO2_CLASS = 'GConnection'
CO2_CARRIER_IDS = ['CO2_F', 'CO2_B', 'CO2_P']
