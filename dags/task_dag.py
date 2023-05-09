from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.mongo.hooks.mongo import MongoHook

from datetime import datetime, timedelta

import pandas as pd
import re


path = '/Users/khobotov/PycharmProjects/AirflowLessons/data/tiktok_google_play_reviews.csv'


def task1():
    df = pd.read_csv(path, delimiter=',', encoding='utf-8')
    df = df.fillna('-')
    df.to_csv(path)


def task2():
    df = pd.read_csv(path, delimiter=',', encoding='utf-8')
    df = df.rename(columns={'at': 'createdDate'})
    df = df.sort_values(by='createdDate')
    df['createdDate'] = pd.to_datetime(df['createdDate'])
    df.to_csv(path)


def task3():
    df = pd.read_csv(path, delimiter=',', encoding='utf-8')
    symbols = r'[^a-zA-Z\.\,\!\?\s]'
    df['content'] = df['content'].apply(lambda x: re.sub(symbols, '', x))
    df.to_csv(path)


def upload_to_mongo():
    df = pd.read_csv(path, delimiter=',', encoding='utf-8')
    data = df.to_dict(orient='records')

    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.db
    collection = db['tiktok_google_play_reviews']
    # print(f"Connected to MongoDB - {client.server_info()}")
    collection.insert_many(data)


with DAG("a_task_dag",
         start_date=datetime(2021, 1, 1),
         schedule_interval=timedelta(minutes=30),
         catchup=False) as dag:

    data_file_sensor = FileSensor(
        task_id='data_file_sensor',
        filepath='tiktok_google_play_reviews.csv',
        fs_conn_id='system_file_path',
        poke_interval=30
    )

    with TaskGroup(group_id='pandas_tasks') as pandas_tasks:

        task1 = PythonOperator(
            task_id="task1",
            python_callable=task1,
            do_xcom_push=False
        )

        task2 = PythonOperator(
            task_id="task2",
            python_callable=task2,
            do_xcom_push=False
        )

        task3 = PythonOperator(
            task_id="task3",
            python_callable=task3,
            do_xcom_push=False
        )

        task1 >> task2 >> task3

    upload_to_mongo = PythonOperator(
        task_id="upload_to_mongo",
        python_callable=upload_to_mongo
    )

    data_file_sensor >> pandas_tasks >> upload_to_mongo




