import os
import sys
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

PATH_BASENAME = os.path.dirname(os.path.abspath(__file__))
PATH_RELATIVE_ROOT = '../'
PATH_ROOT = os.path.realpath(os.path.join(PATH_BASENAME, PATH_RELATIVE_ROOT))
sys.path.append(PATH_ROOT)

from helpers.functions import (
    download_dataset_task,
    data_processing_task,
    ml_training_RandomForest_task,
    ml_training_Logisitic_task,
    identify_best_model_task
)

args={
    'owner' : 'datapath',
    'retries': 1,
    'start_date':days_ago(1)  #1 means yesterday
}

@dag(
    dag_id='airflow_ml_pipeline', ## Name of DAG run
    default_args=args,
    description='ML pipeline',
    schedule = None 
)
def mydag():

    start_node = EmptyOperator(task_id='Starting_the_process', retries=2)  

    end_node = EmptyOperator(task_id="end", trigger_rule="all_done")

    download_node = download_dataset_task()

    processing_node = data_processing_task(download_node)

    ml_rf_node = ml_training_RandomForest_task(processing_node)

    ml_log_node = ml_training_Logisitic_task(processing_node)

    best_model = identify_best_model_task(
        ml_rf_node['model_accuracy'],
        ml_log_node['model_accuracy']
    )

    start_node >> download_node
    best_model >> end_node
    # Define the workflow process
    # dummy_task >> task_extract_data >> task_process_data >> [task_train_RF_model,task_train_LR_model] >> task_identify_best_model

etl_dag = mydag()
