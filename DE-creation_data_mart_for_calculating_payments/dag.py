from airflow import DAG
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable

import requests
import json
from bson import json_util
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine

from datetime import datetime
import sys, psycopg2

from typing import List
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient


url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
restaurant_id = '626a81cfefa404208fe9abae'
nickname = 'e8eca156'
cohort = '1'
sort_field = 'id'
sort_direction = 'asc'
limit = 50
offset = 0
headers = {
    "X-API-KEY": Variable.get("KEY"),
    "X-Nickname": nickname,
    "X-Cohort": str(cohort)
}


def copy_couriers():
    method_url_2 = f'/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
    r = requests.get(url + method_url_2, headers = headers)
    response_dict = json.loads(r.content)

    dest = PostgresHook(postgres_conn_id = 'PG_WAREHOUSE_CONNECTION')
    conn = dest.get_conn()
    cursor = conn.cursor()

    for i in response_dict:
        cursor.execute(
                        """
                            INSERT INTO stg.couriers(object_id, object_value)
                            VALUES (%(c_id)s, %(c_name)s);
                        """,
                        {
                            "c_id": i["_id"],
                            "c_name": i['name']
                        },
                    )

    conn.commit()

def copy_restaurants():
    method_url = f'/restaurants?sort_field={sort_field}&sort_direction={sort_direction }&limit={limit}&offset={offset }'
    r = requests.get(url + method_url, headers = headers)
    response_dict = json.loads(r.content)

    dest = PostgresHook(postgres_conn_id = 'PG_WAREHOUSE_CONNECTION')
    conn = dest.get_conn()
    cursor = conn.cursor()

    for i in response_dict:
        cursor.execute(
                        """
                            INSERT INTO stg.restaurants(object_id, object_value)
                            VALUES (%(c_id)s, %(c_name)s);
                        """,
                        {
                            "c_id": i["_id"],
                            "c_name": i['name']
                        },
                    )

    conn.commit()    

def copy_deliveries():
    method_url = f'/deliveries?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
    r = requests.get(url + method_url, headers = headers)
    response_dict = json.loads(r.content)

    dest = PostgresHook(postgres_conn_id = 'PG_WAREHOUSE_CONNECTION')
    conn = dest.get_conn()
    cursor = conn.cursor()

    for i in response_dict:
        cursor.execute(
                        """
                            INSERT INTO stg.deliveries(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, 
                            rate, sum, tip_sum)
                            VALUES (%(c_order_id)s, %(c_order_ts)s, %(c_delivery_id)s, %(c_courier_id)s, 
                            %(c_address)s, %(c_delivery_ts)s, %(c_rate)s, %(c_sum)s, %(c_tip_sum)s);
                        """,
                        {
                            "c_order_id": i["order_id"],
                            "c_order_ts": i['order_ts'],
                            "c_delivery_id": i["delivery_id"],
                            "c_courier_id": i["courier_id"],
                            "c_address": i["address"],
                            "c_delivery_ts": i["delivery_ts"],
                            "c_rate": i["rate"],
                            "c_sum": i["sum"],
                            "c_tip_sum": i["tip_sum"]
                        },
                    )

    conn.commit()   

with DAG (
		"mongo_to_stg_2",
		schedule_interval='0/15 * * * *',
		start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
		catchup = False,
            	tags=['mongo_stg_2'],
            	is_paused_upon_creation=False

	) as dag:

    copy_couriers = PythonOperator(
		task_id = 'copy_couriers',
		python_callable = copy_couriers
	)

    copy_restaurants = PythonOperator(
	    	task_id = 'copy_restaurants',
		python_callable = copy_restaurants
	)

    copy_deliveries = PythonOperator(
	    	task_id = 'copy_deliveries',
		python_callable = copy_deliveries
	)
