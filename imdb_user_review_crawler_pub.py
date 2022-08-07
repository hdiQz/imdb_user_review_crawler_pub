#from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import mysql.connector
import pandas as pd
import datetime
import requests
import os
import re

# 提前定义变量
data_key = ''
year = 2021

print(year, '年, 开始查找')

# 路径创建&更改
path = '~'
'''
if not os.path.exists(path):
    os.mkdir(path)
'''
os.chdir(path)

# 获取tt
data = pd.read_csv('~.csv', delimiter=',', encoding='utf-8')
df = pd.DataFrame(data)
row_num = df.shape[0] #读取行数

# 用于爬虫的MySQL函数
def mysql_executor(imdb_id, user_review_id, user_review_rating, user_review_title, user_review_user_name, user_review_review_date, user_review_content):
    DB = mysql.connector.connect(host = 'localhost', user = 'root', password = '~', db = 'imdb')
    DBO = DB.cursor()
    sql = ("INSERT INTO user_review "
            "(imdb_id, year, user_review_id, user_review_rating, user_review_title, user_review_user_name, user_review_review_date, user_review_content) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")  
    try:
        DBO.execute(sql,
            (
            imdb_id.encode('utf8'),
            year,
            user_review_id,
            user_review_rating,
            user_review_title.encode('utf8'),
            user_review_user_name.encode('utf8'),
            user_review_review_date,
            user_review_content.encode('utf8'),
            )
        )
        DB.commit()
        DBO.close()
        DB.close()
    except Exception as err:
        print(err)
        exit()

# 用于记录影评数量的MySQL函数
def review_num_mysql_executor(imdb_id, year, review_num):
    DB = mysql.connector.connect(host = 'localhost', user = 'root', password = '~', db = 'imdb')
    DBO = DB.cursor()
    sql = ("INSERT INTO user_review_num_list "
            "(imdb_id, year, review_num) "
            "VALUES (%s, %s, %s)")
    try:
        DBO.execute(sql,
            (
            imdb_id.encode('utf8'),
            year,
            review_num,
            )
        )
        DB.commit()
        DBO.close()
        DB.close()
    except Exception as err:
        print(err)
        exit()

# 用于记录无影评的MySQL函数
def no_review_mysql_executor(imdb_id, year):
    DB = mysql.connector.connect(host = 'localhost', user = 'root', password = '~', db = 'imdb')
    DBO = DB.cursor()
    sql = ("INSERT INTO no_user_review_list "
            "(imdb_id, year) "
            "VALUES (%s, %s)")  
    try:
        DBO.execute(sql,
            (
            imdb_id.encode('utf8'),
            year,
            )
        )
        DB.commit()
        DBO.close()
        DB.close()
    except Exception as err:
        print(err)
        exit()

# 爬虫
for m in range(0, row_num): #报错时修改这里，
    tt = df.iloc[m, 2] # 0年只有两列
    #print(tt)
    initial_url = 'https://www.imdb.com'
    url_user_reviews = 'https://www.imdb.com/title/' + str(tt) + '/reviews?sort=submissionDate&dir=desc&ratingFilter=0'
    #header = {'user-agent': UserAgent().random}
    html_page = requests.get(url = url_user_reviews).text
    soup = BeautifulSoup(html_page, 'html.parser')
    review_num_div = soup.find("div", {'class': 'header'})
    review_num_span = review_num_div.find('span').get_text().strip().replace(',', '')
    review_num_list = re.findall(r'\d+', review_num_span)
    review_num = review_num_list[0]
    print(tt, '影评数量: ', review_num)
    review_num = int(review_num)
    if review_num == 0:
        imdb_id = tt
        no_review_mysql_executor(imdb_id, year)
        print(tt, '记录完成')
        continue
    if review_num <= 25:
        imdb_id = tt
        review_num_mysql_executor(imdb_id, year, review_num)
        n = 1
        user_review_block = soup.find_all('div', {'class': ['lister-item mode-detail imdb-user-review collapsable', 'lister-item mode-detail imdb-user-review with-spoiler']})
        for line in user_review_block:
            line2 = line.find_all('span', {'class': 'rating-other-user-rating'})
            try:
                user_review_rating = line2[0].get_text().strip()
                user_review_rating = user_review_rating.replace('/10', '')
            except IndexError:
                user_review_rating = 0
                # 0 意思是none
                pass
            user_review_title = line.find('a', {'class': 'title'}).get_text().strip()
            user_review_user_name = line.find('span', {'class': 'display-name-link'}).get_text().strip()
            user_review_review_date = line.find('span', {'class': 'review-date'}).get_text().strip()
            user_review_review_date = datetime.datetime.strptime(user_review_review_date,'%d %B %Y')
            user_review_content = line.find('div', {'class': ['text show-more__control', 'text show-more__control clickable']}).get_text().strip()
            imdb_id = tt
            user_review_id = str(n)
            mysql_executor(imdb_id, user_review_id, user_review_rating, user_review_title, user_review_user_name, user_review_review_date, user_review_content)
            n = n + 1
        print(tt, '查找完成')
        continue
    imdb_id = tt
    review_num_mysql_executor(imdb_id, year, review_num)
    n = 1
    user_review_block = soup.find_all('div', {'class': ['lister-item mode-detail imdb-user-review collapsable', 'lister-item mode-detail imdb-user-review with-spoiler']})
    for line in user_review_block:
        line2 = line.find_all('span', {'class': 'rating-other-user-rating'})
        try:
            user_review_rating = line2[0].get_text().strip()
            user_review_rating = user_review_rating.replace('/10', '')
        except IndexError:
            user_review_rating = 0
            pass
        user_review_title = line.find('a', {'class': 'title'}).get_text().strip()
        user_review_user_name = line.find('span', {'class': 'display-name-link'}).get_text().strip()
        user_review_review_date = line.find('span', {'class': 'review-date'}).get_text().strip()
        user_review_review_date = datetime.datetime.strptime(user_review_review_date,'%d %B %Y')
        user_review_content = line.find('div', {'class': ['text show-more__control', 'text show-more__control clickable']}).get_text().strip()
        imdb_id = tt
        user_review_id = str(n)
        mysql_executor(imdb_id, user_review_id, user_review_rating, user_review_title, user_review_user_name, user_review_review_date, user_review_content)
        n = n + 1

    load_more = soup.find('div', {'class': "load-more-data"})
    flag = True
    if len(load_more):
        data_ajaxurl = load_more.attrs['data-ajaxurl']
        initial_url = initial_url + data_ajaxurl + '&ref_=undefined&paginationKey='
        data_key = load_more.attrs['data-key']
    else:
        flag = False

    while flag:
        url_user_reviews = initial_url + data_key
        #print(url_user_reviews)
        print(tt, '查找中')
        html_page = requests.get(url = url_user_reviews).text
        soup = BeautifulSoup(html_page, 'html.parser')
        user_review_block = soup.find_all('div', {'class': ['lister-item mode-detail imdb-user-review collapsable', 'lister-item mode-detail imdb-user-review with-spoiler']})
        for line in user_review_block:
            line2 = line.find_all('span', {'class': 'rating-other-user-rating'})
            try:
                user_review_rating = line2[0].get_text().strip()
                user_review_rating = user_review_rating.replace('/10', '')
            except IndexError:
                user_review_rating = 0
                pass
            user_review_title = line.find('a', {'class': 'title'}).get_text().strip()
            user_review_user_name = line.find('span', {'class': 'display-name-link'}).get_text().strip()
            user_review_review_date = line.find('span', {'class': 'review-date'}).get_text().strip()
            user_review_review_date = datetime.datetime.strptime(user_review_review_date,'%d %B %Y')
            user_review_content = line.find('div', {'class': ['text show-more__control', 'text show-more__control clickable']}).get_text().strip()
            imdb_id = tt
            user_review_id = str(n)
            mysql_executor(imdb_id, user_review_id, user_review_rating, user_review_title, user_review_user_name, user_review_review_date, user_review_content)
            n = n + 1
            if n > review_num:
                break
        if n > review_num:
            break

        load_more = soup.find('div', {'class': "load-more-data"})
        if load_more == None:
            flag = False
        elif len(load_more):
            data_key = load_more.attrs['data-key']
        else:
            flag = False
    print(tt, '查找完成')
print(year, '年, 查找完成')
