import argparse
from file_utils.utils import load_yaml_config
from data_preparation.data_prep import pre_process_data, process_data
from data_preparation.data_validation import data_validate
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def data_validation():
    base_path=os.getcwd()
    data_validate(base_path,'input')

def data_pre_process(**kwargs):
    base_path = os.getcwd()
    dag_run_dict = kwargs.get('dag_run')
    if dag_run_dict.conf:
        input_folder = str(dag_run_dict.conf["input_folder"])
        config_filename_pre_process = str(dag_run_dict.conf["config_filename"])
    else:
        input_folder = 'input'
        config_filename_pre_process = 'data_pre_process_config.yaml'
    config_pre_process = load_yaml_config(
        f'{base_path}/config/{config_filename_pre_process}')
    pre_process_data(base_path, input_folder, config_pre_process)


def data_process(**kwargs):
    base_path = os.getcwd()
    dag_run_dict = kwargs.get('dag_run')
    if dag_run_dict.conf:
        input_folder = str(dag_run_dict.conf["input_folder"])
        config_filename_process = str(dag_run_dict.conf["config_filename"])
    else:
        intermediate_folder = 'intermediate_parquet'
        config_filename_process = 'data_process_config.yaml'
    config_process = load_yaml_config(
        f'{base_path}/config/{config_filename_process}')
    process_data(base_path, intermediate_folder, config_process)

dag = DAG(
    dag_id='data_processsing_dag',
    schedule=None,  # Set to None to avoid automatic execution
    start_date=datetime.utcnow() - timedelta(minutes=1),  # Start immediately,
    catchup=False,  # Don't catch up if the DAG is paused and resumed
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'email': 'ranjithac14@gmail.com',
        'email_on_retry': False,
        'retries': 0,
        'schedule_interval': '@daily',
    }
)

validation = PythonOperator(
    task_id='data_validation',
    python_callable=data_validation,
    dag=dag,
    provide_context=True
)

recipe_pre_process = PythonOperator(
    task_id='data_pre_process',
    python_callable=data_pre_process,
    dag=dag,
    provide_context=True
)

recipe_process = PythonOperator(
    task_id='data_process',
    python_callable=data_process,
    dag=dag,
    provide_context=True
)

validation >> recipe_pre_process >> recipe_process
