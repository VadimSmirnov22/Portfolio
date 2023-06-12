import airflow
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator)
import os
from datetime import date, datetime, timedelta
from airflow import DAG
import pendulum
from airflow.operators.python_operator import PythonOperator
import sys

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"


with DAG (
        "DataLake",
        schedule_interval='0/15 * * * *',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=['data'],
        is_paused_upon_creation=False
 
    ) as dag_spark:

    mart_1 = SparkSubmitOperator(
        task_id="mart_1",
        dag=dag_spark,
        application="/lessons/scripts/mart_1.py",
        conn_id="yarn_spark",
        application_args=[
            "/user/master/data/geo/events",
            "/user/vsmirnov22/data/geo.csv",
            "/user/vsmirnov22/analytics/mart_1"]
    )

    mart_2 = SparkSubmitOperator(
        task_id="mart_2",
        dag=dag_spark,
        application="/lessons/scripts/mart_2.py",
        conn_id="yarn_spark",
        application_args=[
            "/user/master/data/geo/events",
            "/user/vsmirnov22/data/geo.csv",
            "/user/vsmirnov22/analytics/mart_2"
        ]
    )

    mart_3 = SparkSubmitOperator(
        task_id="mart_3",
        dag=dag_spark,
        application="/lessons/scripts/mart_3.py",
        conn_id="yarn_spark",
        application_args=[
            "/user/master/data/geo/events",
            "/user/vsmirnov22/data/geo.csv",
            "/user/vsmirnov22/analytics/mart_3"
        ]
    )


mart_1 >> mart_2 >> mart_3
