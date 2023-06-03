
from datetime import date, datetime 
import requests
import pandas as pd
import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import mysql.connector
from sqlalchemy import create_engine
import pymysql
import os




dag = DAG(
    dag_id="aladin_book", #dag id
    description="aladin book data", #dag의 설명
    start_date=datetime(2023, 5, 24, 0, 0),  # 시작 날짜 및 시간 설정
    schedule_interval='30 16 * * *',  
)


def _get_url():
    pathlib.Path("/home/airflow/data").mkdir(parents=True, exist_ok=True)
    TTBKey = 'ttbmoon9512051357001'
    items = []

    for start_value in range(1, 11):
        url = f"http://www.aladin.co.kr/ttb/api/ItemList.aspx?ttbkey={TTBKey}&QueryType=ItemNewAll&SearchTarget=Used&SubSearchTarget=Book&MaxResults=50&start={start_value}&output=js&Version=20131101&OptResult=usedList"
        res = requests.get(url)
        items.extend(res.json()['item'])

    pd.DataFrame(items).to_csv("/home/airflow/data/aladinbook.csv", index=False)


def _get_data():
    total = []
    today = str(date.today()).replace("-","")
    book = pd.read_csv("/home/airflow/data/aladinbook.csv")
    for index, items2 in book.iterrows():
        book_dic = {
            'itemId': items2['itemId'],
            'title': items2['title'][5:],
            'author': items2['author'],
            'priceStandard': items2['priceStandard'],
            'priceSales': items2['priceSales'],
            'customerReviewRank': items2['customerReviewRank'],
        }

        if isinstance(items2['categoryName'], str):
            category_splits = items2['categoryName'].split('>')
            book_dic['categoryName_L'] = category_splits[1]
            book_dic['categoryName_S'] = category_splits[2]
        else:
            book_dic['categoryName_L'] = None
            book_dic['categoryName_S'] = None

        subInfo = json.loads(items2['subInfo'].replace("'", '"'))

        if 'newBookList' in subInfo and subInfo['newBookList']:
            book_dic['SitepriceSales'] = subInfo['newBookList'][0]['priceSales']
        else:
            book_dic['SitepriceSales'] = None

        total.append(book_dic)
    pd.DataFrame(total).to_csv(f"/home/airflow/data/aladin{today}.csv", index=False)

def read_csv_and_store_in_mysql():
    # CSV 파일을 읽어 DataFrame으로 변환합니다.
    today = str(date.today()).replace("-", "")
    df = pd.read_csv(f"/home/airflow/data/aladin{today}.csv")

    connection=BaseHook.get_connection('airsql') #airflow에서 connection한 mysql id
    database_username=connection.login
    database_password=connection.password
    database_ip = connection.host
    database_port = connection.port
    database_name = connection.schema
    
    database_connection = f"mysql+pymysql://{database_username}:{database_password}@{database_ip}:{database_port}/{database_name}"

    engine = create_engine(database_connection)

    # 데이터를 MySQL의 새 테이블에 저장합니다. 기존에 있는 테이블이라면, 데이터를 덮어쓰거나 추가할 수 있습니다.
    df.to_sql(con=engine, name='mytable', if_exists='append', index=False)


get_url = PythonOperator(
    task_id="get_url", python_callable=_get_url, dag=dag
)

get_data = PythonOperator(
    task_id="get_data", python_callable=_get_data, dag=dag
)

task_read_csv_and_store_in_mysql = PythonOperator(
    task_id='read_csv_and_store_in_mysql',
    python_callable=read_csv_and_store_in_mysql,
    dag=dag,
)

get_url >> get_data >> task_read_csv_and_store_in_mysql 
