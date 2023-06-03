
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
from pymongo.mongo_client import MongoClient
from airflow.models import XCom
import os


dag = DAG(
    dag_id="aladin_book_mongo", #dag id
    description="aladin book data", #dag의 설명
    start_date=datetime(2023, 5, 24, 0, 0),  # 시작 날짜 및 시간 설정
    schedule_interval='30 16 * * *',  
)


def _get_url(ti):
    pathlib.Path("/home/airflow/data").mkdir(parents=True, exist_ok=True)
    TTBKey = 'ttbmoon9512051357001'
    items = []

    for start_value in range(1, 11):
        url = f"http://www.aladin.co.kr/ttb/api/ItemList.aspx?ttbkey={TTBKey}&QueryType=ItemNewAll&SearchTarget=Used&SubSearchTarget=Book&MaxResults=50&start={start_value}&output=js&Version=20131101&OptResult=usedList"
        res = requests.get(url)
        items.extend(res.json()['item'])
    ti.xcom_push(key="items", value=items)
    #pd.DataFrame(items).to_csv("/home/airflow/data/aladinbook.csv", index=False)

def _get_data(ti):
    total = []
    _book=ti.xcom_pull(key='items')
    book=pd.DataFrame(_book)
    #today = str(date.today()).replace("-","")
    #book = pd.read_csv("/home/airflow/data/aladinbook.csv")
    #items = ti.xcom_pull(key="items", task_ids="get_url") = pd.DataFrame(items)
    for _, items2 in book.iterrows():
        book_dic = {
            'itemId': items2['itemId'],
            'title': items2['title'][5:],
            'author': items2['author'],
            'priceStandard': items2['priceStandard'],
            'priceSales': items2['priceSales'],
            'customerReviewRank': items2['customerReviewRank'],
        }

        if isinstance(items2['categoryName'], str): # mysql에 할때는 3이상인것만 취급한게 아닌데 왜 됐을까?
            category_splits = items2['categoryName'].split('>')
            if len(category_splits) >= 3: # 원래 했던것에는 
                book_dic['categoryName_L'] = category_splits[1]
                book_dic['categoryName_S'] = category_splits[2]
            else:
                book_dic['categoryName_L'] = None
                book_dic['categoryName_S'] = None
        else:
            book_dic['categoryName_L'] = None
            book_dic['categoryName_S'] = None

        subInfo = items2['subInfo'] # 받아오는 형식이 전에는 csv파일에서 문자열로 오기때문에 JSON형태로 만들어 줬지만 이제는 안해도 된다

        if 'newBookList' in subInfo and subInfo['newBookList']:   
            book_dic['SitepriceSales'] = subInfo['newBookList'][0]['priceSales']
        else:
            book_dic['SitepriceSales'] = None

        total.append(book_dic)
    ti.xcom_push(key="total", value=total)

def insert_data_to_mongo_atlas(ti):
    data = ti.xcom_pull(key="total")
    # MongoDB Atlas 클러스터 주소와 자격 증명을 설정합니다.
    client = MongoClient('mongodb+srv://admin:123@mongoair.uyolqu4.mongodb.net/?retryWrites=true&w=majority')
    db = client['mongoair']
    collection = db['air']
    for item in data:
        collection.insert_one(item)


get_url = PythonOperator(
    task_id="get_url", python_callable=_get_url, provide_context=True, dag=dag
)

get_data = PythonOperator(
    task_id="get_data", python_callable=_get_data, provide_context=True,dag=dag
)
insert_task = PythonOperator(
    task_id='insert_to_mongo_atlas',
    python_callable=insert_data_to_mongo_atlas,
     provide_context=True,
    dag=dag,
)
get_url >> get_data >> insert_task 
