import contextlib
import hashlib
import pandas as pd
import vertica_python
import json
from typing import Dict, List, Optional
import pendulum
from airflow.decorators import dag, task
import boto3
from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
 

AWS_ACCESS_KEY_ID =  Variable.get("KEY")
AWS_SECRET_ACCESS_KEY =  Variable.get("KEY_2")

conn_info = {
    "host": "51.250.75.20",
    "port": 5433,
    "user": "e8eca156yandexby",
    "password":  Variable.get("KEY_3"),
    "database": "dwh"
}

def dowload_file(dataset_path: str):
    
    session = boto3.session.Session()
    s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file(
    Bucket='sprint6',
    Key='group_log.csv',
    Filename= dataset_path)

def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str]):
    df = pd.read_csv(dataset_path)
    df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
    num_rows = len(df)
    vertica_conn = vertica_python.connect(**conn_info)
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/data/chunk.csv', index=False)
            with open('/data/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()

with DAG (
	"vertica",
	schedule_interval='0/15 * * * *',
	start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
	catchup = False,
        tags=['vertica'],
        is_paused_upon_creation=False
	) as dag:

    dowload_file = PythonOperator(
	task_id = 'dowload_file',
	python_callable = dowload_file,
    	op_kwargs = {'dataset_path':'/data/group_log.csv'}
	)

    load_dataset_file_to_vertica = PythonOperator(
        task_id='load_dataset_file_to_vertica',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',
            'schema': 'E8ECA156YANDEXBY__STAGING',
            'table': 'group_log',
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'],
        },
    )
