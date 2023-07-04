import os
import sys
import pandas as pd
from pandas import json_normalize
from datetime import datetime

# OPC #1#
# OBTENEMOS LA RUTA RELATIVA DE LOS SCRIPT
# PATH_RELATIVE_ROOT = '../'
# sys.path.append(PATH_RELATIVE_ROOT)

# OPC #2#
# OBTENEMOS LA RUTA ABSOLUTA DE LOS SCRIPT
PATH_BASENAME = os.path.dirname(os.path.abspath(__file__))
PATH_RELATIVE_ROOT = '../'
PATH_ROOT = os.path.realpath(os.path.join(PATH_BASENAME, PATH_RELATIVE_ROOT))
sys.path.append(PATH_ROOT)

print('---------')
print(f'PATH_ROOT: {PATH_ROOT}')
print('---------')

from helpers.connections import Postgresql

# CONNECTION
# ##########
database = Postgresql(host='localhost', port='5433', db_name='postgres_db', user_name='postgres', password='postgres')

# CREATE DATABASE
# ###############
# initialize destination tables for data ingestion
database.create_schema('stock')
database.create_table(
    table_schema='stock', table_name='stock_symbols',
    columns={
        'ticker_symbol': 'varchar',
        'stock_name': 'varchar'
    })

database.truncate_table(
    table_schema='stock', 
    table_name='stock_symbols')

database.create_table(
    table_schema='stock', table_name='stock_values',
    columns={
        'ticker_symbol': 'varchar',
        'day_date': 'timestamp',
        'close_value': 'numeric',
        'volume': 'bigint',
        'open_value': 'float',
        'high_value': 'float',
        'low_value': 'float'
    })

database.truncate_table(
    table_schema='stock', 
    table_name='stock_values')

# CREATE DATABASE MANAGER
# #######################
# initialize reference table
database.create_schema('etl_manager')
database.create_table(
    table_schema='etl_manager', table_name='database_flow_reference_table',
    columns={
        'insert_date': 'timestamp',
        'source_connection': 'varchar',
        'source_schema': 'varchar',
        'source_table': 'varchar',
        'key_fields': 'varchar',
        'extraction_method': 'varchar',
        'extraction_type': 'varchar',
        'destination_connection': 'varchar',
        'destination_schema': 'varchar',
        'destination_table': 'varchar',
        'target_fields': 'varchar'
    })

database.truncate_table(
    table_schema='etl_manager', 
    table_name='database_flow_reference_table')

# INSERT ROWS MANAGER
# ###################
stock_symbol_dict = {
    'insert_date': str(datetime.now()), 
    'source_connection': 'mysql',
    'source_schema': 'stock',
    'source_table': 'stock_symbols', 
    'key_fields': 'ticker_symbol, stock_name',
    'extraction_method': 'jdbc',
    'extraction_type': 'full', 
    'destination_connection': 'postgresql',
    'destination_schema': 'stock',
    'destination_table': 'stock_symbols', 
    'target_fields': 'ticker_symbol, stock_name'
}

stock_values_dict = {
    'insert_date': str(datetime.now()), 
    'source_connection': 'mysql',
    'source_schema': 'stock',
    'source_table': 'stock_values',
    'key_fields': 'ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value',
    'extraction_method': 'jdbc',
    'extraction_type': 'full', 
    'destination_connection': 'postgresql',
    'destination_schema': 'stock',
    'destination_table': 'stock_values',
    'target_fields': 'ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value'
}

rows = [stock_symbol_dict, stock_values_dict]
df_rows = json_normalize(rows)
database.insert_values(
    data=df_rows, 
    table_schema='etl_manager', 
    table_name='database_flow_reference_table',
    columns=', '.join(df_rows.columns.tolist()) )

database.close_connection()