import requests
from bs4 import BeautifulSoup as bs
import pymongo
import urllib.request as req

url = 'http://www.cgv.co.kr/movies/pre-movies.aspx'
r = requests.get(url)
CGv = bs(r.text, 'html.parser')
cgv = CGv.select_one('div.sect-movie-chart')

total=[]
linklist=[]
for i in cgv.select('ol li'):
    linklist.append(i.select('a')[0]['href'])

    
for a in linklist:
    cgvDic = {}
    new_url='http://www.cgv.co.kr'+a
    r=requests.get(new_url)
    info=bs(r.text, 'html.parser')
    cgvDic['place']='CGV'
    cgvDic['title']=info.select_one('div.box-contents strong').text

    if info.select('div.spec dl dt')[0].text[0] == '감':
        g=[]
        for i in info.select_one('div.spec dd').text.split(','):
            if i in '\n':
                i=i.replace('\n', '')
            if i in '\r':
                i=i.replace('\r','')
            if i in '\xa0':
                i=i.replace('\xa0','')
            g.append(i.strip())
        cgvDic['direc']=g
    
    if info.select('div.spec dl dt')[1].text.split(' ')[1]=='배우':
        g=[]
        for i in info.select('div.spec dd.on a'):
            g.append(i.text)
        cgvDic['actor']=g
        
    if info.select('div.spec dt')[2].text[0]=='장':
        g=[]
        for i in info.select('div.spec dt')[2].text.replace('\xa0', '').split(':')[1:]:
            g.append(i)
        cgvDic['gen']=g
    cgvDic['story']=info.select_one('div.col-detail div.sect-story-movie').text.replace('\n',' ').replace('\r',' ')
    
    cgvDic['info']=info.select('div.spec dl dd.on')[1].text
    
    for a in info.select('div.box-contents em'):
        if a.text[0] == 'D':
            cgvDic['Dday']=a.text
            
    #이미지 다운로드
    # img_url=info.select_one('div.box-image a')['href']
    # fol=f'/Users/gangsickmun/Desktop/이미지 폴더/{cgvDic["title"]}.jpg'
    # req.urlretrieve(img_url,fol)
    
    total.append(cgvDic)
mongo_client=pymongo.MongoClient("mongodb://localhost:27017/")
mdb=mongo_client['test']
m_col=mdb['movie']

for i in total:
    m_col.insert_one(i)
    
    
    