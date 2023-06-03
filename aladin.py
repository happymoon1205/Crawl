import requests
import json

book_dic = {}
book_lists=[]

for i in range(1,11):
    url = f"http://www.aladin.co.kr/ttb/api/ItemList.aspx?ttbkey={TTBKey}&QueryType=ItemNewAll&SearchTarget=Used&SubSearchTarget=Book&MaxResults=50&start={i}&output=js&Version=20131101&OptResult=usedList"
    res = requests.get(url)
    items = json.loads(res.text)['item']
    for items2 in items:
        book_dic['itemId']=items2['itemId']# 책 아이디
        book_dic['title']=items2['title'][5:] #제목
        book_dic['author']=items2['author'] # 글쓴이
        book_dic['priceStandard']=items2['priceStandard'] # 원가
        book_dic['SitepriceSales']=items2['subInfo']['newBookList'][0]['priceSales'] # 사이트 새책 할인가
        book_dic['priceSales']=items2['priceSales'] # 할인가
        book_dic['categoryName_L']=items2['categoryName'].split('>')[1] # 큰 카테고리
        book_dic['categoryName_S']=items2['categoryName'].split('>')[2] # 작은 카테고리
        book_dic['customerReviewRank']=items2['customerReviewRank'] # 순위
        book_lists.append(book_dic)
    print(f'{i}번째 입니다')
    
    
'''
items2['itemId']# 책 아이디
items2['title'][5:] #제목
items2['author'] # 글쓴이
items2['priceStandard'] # 원가
items2['subInfo']['newBookList'][0]['priceSales'] # 사이트 새책 할인가
items2['priceSales'] # 할인가
items2['categoryName'].split('>')[2] # 큰 카테고리
items2['categoryName'].split('>')[3] # 작은 카테고리
items2['customerReviewRank'] # 순위
'''

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




dag = DAG(
    dag_id="aladin_book",
    description="aladin book data",
    start_date=datetime(2023, 5, 24, 0, 0),  # 시작 날짜 및 시간 설정
    schedule_interval='30 16 * * *',  # 매일 오전 8시에 실행 (cron 표현식)
)


def _get_symbol():
    pathlib.Path("/home/airflow/data").mkdir(parents=True, exist_ok=True)
    TTBKey='ttbmoon9512051357001'
    url = f"http://www.aladin.co.kr/ttb/api/ItemList.aspx?ttbkey={TTBKey}&QueryType=ItemNewAll&SearchTarget=Used&SubSearchTarget=Book&MaxResults=50&start=1&output=js&Version=20131101&OptResult=usedList"
    res = requests.get(url)
    items = json.loads(res.text)['item']
    pd.DataFrame(items).to_csv("/home/airflow/data/aladinbook.csv", index=False)


get_symbol = PythonOperator(
    task_id="get_symbol", python_callable=_get_symbol, dag=dag
)

def _get_data():
    book_dic = {}
    book_lists=[]
    total=[]
    book = pd.read_csv("/home/airflow/data/aladinbook.csv")
    for items2 in book:
        book_dic['itemId']=items2['itemId']# 책 아이디
        book_dic['title']=items2['title'][5:] #제목
        book_dic['author']=items2['author'] # 글쓴이
        book_dic['priceStandard']=items2['priceStandard'] # 원가
        book_dic['SitepriceSales']=items2['subInfo']['newBookList'][0]['priceSales'] # 사이트 새책 할인가
        book_dic['priceSales']=items2['priceSales'] # 할인가
        book_dic['categoryName_L']=items2['categoryName'].split('>')[1] # 큰 카테고리
        book_dic['categoryName_S']=items2['categoryName'].split('>')[2] # 작은 카테고리
        book_dic['customerReviewRank']=items2['customerReviewRank'] # 순위
        df=pd.DataFrame(book_dic)
        total.append(df)
        
    pd.concat(total, ignore_index=True).to_csv(f"/home/airflow/data/{today}.csv", index=False)


get_data = PythonOperator(
    task_id="get_data", python_callable=_get_data, dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/stock/ | wc -l) images."',
    dag=dag,
)

get_symbol >> get_data >> notify

