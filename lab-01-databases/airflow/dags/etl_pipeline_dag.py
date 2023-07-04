# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
import os
import sys
import pandas as pd
from pandas import DataFrame

import airflow
from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

PATH_BASENAME = os.path.dirname(os.path.abspath(__file__))
PATH_RELATIVE_ROOT = '../'
PATH_ROOT = os.path.realpath(os.path.join(PATH_BASENAME, PATH_RELATIVE_ROOT))
sys.path.append(PATH_ROOT)

from helpers.connections import Mysql, Postgresql


DAG_NAME = 'ETL pipeline from MySQL to PostgreSQL'
DAG_ID = 'mysql_to_postgresql'
DIR_ARTIFACT = 'artifacts'

default_args = {
    'owner': 'datapath',
    'start_date': airflow.utils.dates.days_ago(7),
    'depends_on_past': False,
    'retries': 1,
    'provide_context': True
}

#### Other Variables ####
POSTGRE_HOST, POSTGRE_PORT, POSTGRE_DB_NAME, POSTGRE_USER, POSTGRE_PASSWORD = 'host.docker.internal', '5433', 'postgres_db', 'postgres', 'postgres'
MYSQL_HOST, MYSQL_PORT, MYSQL_DB_NAME, MYSQL_USER, MYSQL_PASSWORD = 'host.docker.internal', '3306', 'mysql_db', 'root', 'root_mysql'


def download_reference_table() -> DataFrame:
    """
    Description: Downloads reference table from PostgreSQL database.
    """
    conn_obj = Postgresql(
        host=POSTGRE_HOST, port=POSTGRE_PORT, 
        db_name=POSTGRE_DB_NAME, user_name=POSTGRE_USER, 
        password=POSTGRE_PASSWORD)
    query = """SELECT * FROM etl_manager.database_flow_reference_table"""
    ref_table = conn_obj.execute_query(query=query, return_data=True)
    conn_obj.close_connection()  # close connection to greenplum db
    return ref_table


@task
def extract(
    source_connection_name: str, 
    schema_name: str, 
    table_name: str, 
    key_fields: str) -> str:
    """
    Description: Extracts given table from the development database. Saves it as csv file.

    Arguments:
        :param source_connection_name: specifies which database is going to be used for extraction.
        :param schema_name: source schema name
        :param table_name: source table name
        :param key_fields: column names to be extract from the database.
    :return:
        Outputs csv file, no return from the function.
    """
    # Connection to source database based on given parameter in the reference table
    # if you want multi-directional etl pipeline inside a single DAG, then you only have to create
    # class for that particular database and add if condition to this function.
    context = get_current_context()
    dag_id = context['dag_run'].dag_id
    run_id = context['run_id']
    
    print("dag_id:", dag_id)
    print("run_id:", run_id)

    if source_connection_name == 'mysql':
        conn_obj = Mysql(host=MYSQL_HOST, port=MYSQL_PORT, db_name=MYSQL_DB_NAME, user_name=MYSQL_USER, password=MYSQL_PASSWORD)

    query = f"SELECT {key_fields} FROM {schema_name}.{table_name}"
    data = conn_obj.execute_query(query, return_data=True)
    print('total_rows:', len(data))

    file_name = f"{schema_name}_{table_name}.csv"

    file_dir = os.path.join(PATH_ROOT, DIR_ARTIFACT, run_id)
    os.makedirs(file_dir, exist_ok=True)
    file_path = os.path.join(file_dir, file_name)

    data.to_csv(file_path, index=False)
    print("file_path:", file_path)
    
    conn_obj.close_connection()

    return file_path

@task
def load_to_target(
    output_path: str,
    target_connection_name: str,
    target_schema: str,
    target_table: str,
    target_fields: str ) -> None:
    """
    Description: For csv extraction method, this function is used to read the data which is extracted from the source
    database and executes ingestion process to the target database

    Arguments:
        :param output_path: csv output path from the extraction process
        :param target_connection_name: target database name
        :param target_schema: target schema name
        :param target_table_name: target table name
        :param target_fields: columns of the target table
    Returns:
        None
    """
    if target_connection_name == 'postgresql':
        conn_obj = Postgresql(host=POSTGRE_HOST, port=POSTGRE_PORT, db_name=POSTGRE_DB_NAME, user_name=POSTGRE_USER, password=POSTGRE_PASSWORD)

    # reading data from extract_node
    print("output_path:", output_path)
    data = pd.read_csv(output_path)

    # TRUNCATE TABLE
    print(f'TRUNCATING {target_schema}.{target_table}')
    conn_obj.truncate_table(table_schema=target_schema, table_name=target_table)

    print(f'Insertion started for {target_schema}.{target_table}!')
    # data insertion for each value
    conn_obj.insert_values(data=data, table_schema=target_schema, table_name=target_table, columns=target_fields)

    print(f'Inserting to {target_schema}.{target_table} is successfully completed!')
    conn_obj.close_connection()


@dag(
    dag_id=DAG_ID,
    description=DAG_NAME,
    start_date=datetime(2023, 7, 1),
    default_args=default_args,
    schedule_interval=None,
    schedule=None
)
def mydag():

    start_node = EmptyOperator(task_id="start", trigger_rule="all_done")
    end_node = EmptyOperator(task_id="end", trigger_rule="all_done")

    # GET ETL MANAGER TABLE
    ref_table = download_reference_table()
    schemas = ref_table.source_schema.unique().tolist()

    # LIST DATABASE
    for source_schema in schemas:
        schema_node = EmptyOperator(task_id='source_schema' + "_" + source_schema)
        
        start_node >> schema_node

        # LIST DATABASE TABLES 
        schema_tables = ref_table[ref_table.source_schema == source_schema].copy()

        nodes = []
        for idx, row in schema_tables.iterrows():
            source_connection = row['source_connection']
            source_table = row['source_table']
            key_fields = row['key_fields']

            destination_connection = row['destination_connection']
            destination_schema = row['destination_schema']
            destination_table = row['destination_table']
            target_fields = row['target_fields']

            extract_node = extract.override(task_id='source_table' + '_' + source_schema + '_' + source_table)(
                source_connection_name=source_connection,
                schema_name=source_schema,
                table_name=source_table,
                key_fields=key_fields
            )

            insert_node = load_to_target.override(task_id='destination_' + destination_schema + '_' + destination_table)(
                extract_node, 
                target_connection_name=destination_connection,
                target_schema=destination_schema,
                target_table=destination_table,
                target_fields=target_fields
            )

            nodes.append(insert_node)

            schema_node >> extract_node >> insert_node
    
    nodes >> end_node

etl_dag = mydag()