import os
import sys
import pandas as pd

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

from helpers.connections import Mysql

PATH_DATA_COMPANY = os.path.join(PATH_ROOT, 'data', 'Company.csv')
PATH_DATA_COMPANY_VALUES = os.path.join(PATH_ROOT, 'data', 'CompanyValues.csv')

# CONNECTION
# ##########
database = Mysql(host='localhost', port='3306', db_name='mysql_db', user_name='root', password='root_mysql')

# CLEAN DATABASE
# ##############
database.drop_table(table_schema='stock', table_name='stock_symbols')
database.drop_table(table_schema='stock', table_name='stock_values')
database.drop_schema(table_schema='stock')
database.create_schema('stock')

# CREATE TABLE "stock_symbols"
# ############################
# Reads the data, creates the table and inserts values to stock_symbols
stock_symbols_df = pd.read_csv(PATH_DATA_COMPANY)
database.create_table(
    table_schema='stock', table_name='stock_symbols',
    columns={
        'ticker_symbol': 'varchar(20)',
        'stock_name': 'varchar(20)'
    })

database.insert_values(
    data=stock_symbols_df, table_schema='stock', table_name='stock_symbols',
    columns='ticker_symbol, stock_name')

# CREATE TABLE "stock_values"
# ###########################
# Reads the data, creates the table and inserts values to stock_values
stock_values_df = pd.read_csv(PATH_DATA_COMPANY_VALUES)
database.create_table(
    table_schema='stock', table_name='stock_values',
    columns={
        'ticker_symbol': 'varchar(20)',
        'day_date': 'timestamp',
        'close_value': 'float',
        'volume': 'bigint',
        'open_value': 'float',
        'high_value': 'float',
        'low_value': 'float'
    })

database.insert_values(
    data=stock_values_df, table_schema='stock', table_name='stock_values',
    columns='ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value')

print("Data is ready!")
database.close_connection()
