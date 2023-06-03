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
