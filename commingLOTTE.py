import pandas as pd
import requests
import json
import pymongo
from bs4 import BeautifulSoup as bs
import urllib.request as req

url ='https://www.lottecinema.co.kr/LCWS/Movie/MovieData.aspx'
pay={"MethodName":"GetMoviesToBe","channelType":"HO","osType":"Chrome","osVersion":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36","multiLanguageID":"KR","division":1,"moviePlayYN":"N","orderType":"5","blockSize":100,"pageNo":1,"memberOnNo":""}
data = {"paramList": str(pay).encode()}
r=requests.post(url,data=data).text
lottemovie=json.loads(r)['Movies']['Items']
mlist=[i['RepresentationMovieCode'] for i in lottemovie]

# 영화의 코드만 가져온다
cnt=0
for i in mlist: 
    if i == 'AD':
        del mlist[cnt]
    cnt+=1
    
total=[]
for a in mlist:
    direc=[]
    actor=[]
    lotteDic={}
    
    url='https://www.lottecinema.co.kr/LCWS/Movie/MovieData.aspx'
    pay={"MethodName":"GetMovieDetailTOBE","channelType":"HO","osType":"Chrome","osVersion":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36","multiLanguageID":"KR","representationMovieCode":a,"memberOnNo":""}
    data={"paramList":str(pay).encode()}
    r=requests.post(url,data=data).text
    info=json.loads(r)['Movie']
    casting=json.loads(r)['Casting']['Items']

    
    lotteDic['place']='롯데시네마'

    lotteDic['title'] = info['MovieNameKR']  # 제목
    
    for i in casting:
        if isinstance(i, dict) and 'Role' in i and 'StaffName' in i:
            role = i['Role'].strip()
            if '감독' in role:  # 감독
                direc.append(i['StaffName'])
            if '배우' in role:  # 배우
                actor.append(i['StaffName'])

    lotteDic['direc']=direc #감독
    lotteDic['actor']=actor #배우
    
    # 장르
    genre = [] 
    for key, value in info.items():
        if 'MovieGenreNameKR' in key and not None and value!='':
            genre.append(value)
    lotteDic['gen']=genre
    
    #줄거리
    if info['SynopsisKR'] is not None:
        lotteDic['story'] = info['SynopsisKR'].replace('<b>', '').replace('</b>', '').replace('<br>', '')
    else:
        lotteDic['story'] = ""
    
    # 등급, 상영시간, 국적
    lotteDic['info']=str(info['ViewGradeNameUS'])+', '+str(info['PlayTime'])+', '+str(info['MakingNationNameKR'])
    
    #포스터 링크
    lotteDic['poster']=info['PosterURL']
    
    # #이미지 다운로드
    # img_url = info['PosterURL']
    # fol = f'/Users/gangsickmun/workspace/movielist/final/static/{lotteDic["title"]}.jpg'
    # req.urlretrieve(img_url,fol)
    
    
    total.append(lotteDic)

# mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
# mdb = mongodb_client["test"]  # 데이터베이스 지정
# m_col = mdb["movie"]  # 콜렉션 지정

# for item in total:
#     m_col.insert_one(item)


