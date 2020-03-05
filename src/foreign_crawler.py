import logging
from logging.handlers import RotatingFileHandler

import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime

import json
import time

import pymysql
import mysql_foreign_property

logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/foreign_crawler.log', maxBytes=1024 * 1024 * 1024 * 9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info("every package loaded and start logging")


def insert_result(uid, data_list):
    logger.info("insert_result: function started")
    connection = pymysql.connect(host=mysql_foreign_property.hostname, user=mysql_foreign_property.user,
                                 password=mysql_foreign_property.password, db=mysql_foreign_property.database,
                                 charset=mysql_foreign_property.charset)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger.info("insert_result: database connection opened")

    for data in data_list[1:]:
        cursor.execute(
            f"insert into foreign_{data['country']} values({uid}, {data_list[0]}, {data['certified']}, {data['dead']});")
        logger.info("insert_result: status data inserted")

    connection.commit()
    connection.close()
    logger.info("insert_result: database connection closed")

    logger.info("insert_result: function ended")


def dump_result(uid, data):
    with open("./foreign-data/k_covid19_foreign_" + str(uid) + ".json", "w") as json_file:
        json.dump(data, json_file)


def get_foreign_data(target=''):
    logger.info("get_foreign_data: function started | target=" + target)

    downloaded_html = urlopen(target)
    logger.info("get_foreign_data: html downloaded")
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")
    logger.info("get_foreign_data: html parsed to beautifulsoup object")

    announced_time = ['2002',
                      re.findall('([0-9]+)[^0-9][0-9]+일', beautifulsoup_object.findAll('p', class_='s_descript')[1].text)[0],
                      re.findall('([0-9]+)일', beautifulsoup_object.findAll('p', class_='s_descript')[1].text)[0],
                      re.findall('([0-9]+)시', beautifulsoup_object.findAll('p', class_='s_descript')[1].text)[0]]

    datetime_object = datetime.datetime.strptime(str(announced_time), "['%Y', '%m', '%d', '%H']")
    announced_time_unix = int(time.mktime(datetime_object.timetuple()))

    raw_table = beautifulsoup_object.findAll('tbody')
    logger.info("get_foreign_data: numbers_raw picked out")
    raw_table_beautifulsoup_object = BeautifulSoup(str(raw_table[1]), "html.parser")

    foreign_data_list = [announced_time_unix]
    table_data_rows = raw_table_beautifulsoup_object.findAll('tr')
    table_data_rows.reverse()
    country_dictionary = {
        '중국': 'china',
        '홍콩': 'hongkong',
        '대만': 'taiwan',
        '마카오': 'macau',
        '일본': 'japan',
        '싱가포르': 'singapura',
        '태국': 'thailand',
        '말레이시아': 'malaysia',
        '베트남': 'vietnam',
        '인도': 'india',
        '필리핀': 'philippines',
        '캄보디아': 'cambodia',
        '네팔': 'nepal',
        '러시아': 'russia',
        '스리랑카': 'srilanka',
        '아프가니스탄': 'afghanistan',
        '인도네시아': 'indonesia',
        '파키스탄': 'pakistan',
        '투르크메니스탄': 'turkmenistan',
        '이란': 'iran',
        '쿠웨이트': 'kuwait',
        '바레인': 'bahrain',
        '아랍에미리트': 'uae',
        '이라크': 'iraq',
        '오만': 'oman',
        '레바논': 'lebanon',
        '이스라엘': 'israel',
        '이집트': 'egypt',
        '알제리': 'algeria',
        '카타르': 'qatar',
        '요르단': 'jordan',
        '튀니지': 'tunisia',
        '사우디': 'saudi',
        '모로코': 'morocco',
        '미국': 'usa',
        '캐나다': 'canada',
        '브라질': 'brasil',
        '멕시코': 'mexico',
        '에콰도르': 'ecuador',
        '도미니카공화국': 'dominican',
        '아르헨티나': 'argentina',
        '칠레': 'chile',
        '이탈리아': 'italiana',
        '독일': 'germany',
        '프랑스': 'france',
        '영국': 'england',
        '스페인': 'spain',
        '오스트리아': 'austria',
        '크로아티아': 'croatia',
        '핀란드': 'finland',
        '스웨덴': 'sweden',
        '스위스': 'swiss',
        '벨기에': 'belgium',
        '덴마크': 'danmark',
        '에스토니아': 'eesti',
        '조지아': 'georgia',
        '그리스': 'greece',
        '북마케도니아': 'macedonia',
        '노르웨이': 'norway',
        '루마니아': 'romania',
        '네덜란드': 'nederlands',
        '벨라루스': 'belarus',
        '리투아니아': 'lithuania',
        '산마리노': 'sanmarino',
        '아제르바이잔': 'azerbaijan',
        '아이슬란드': 'island',
        '모나코': 'monaco',
        '룩셈부르크': 'luxembourg',
        '아르메니아': 'armenia',
        '아일랜드': 'ireland',
        '체코': 'czecho',
        '포르투갈': 'portugal',
        '라트비아': 'latvia',
        '안도라': 'andora',
        '폴란드': 'poland',
        '우크라이나': 'ukraine',
        '호주': 'australia',
        '뉴질랜드': 'newzealand',
        '나이지리아': 'nigeria',
        '세네갈': 'senegal',
        '일본 크루즈': 'japan_cruise',
        '합계': 'synthesize'
    }

    table_data_beautifulsoup_object = BeautifulSoup(str(table_data_rows[0]), "html.parser")

    country = table_data_beautifulsoup_object.findAll('th')[0].text
    certified = re.sub('[,,명]', '',
                       re.sub('\(사망[  ][0-9,,]+\)', '', table_data_beautifulsoup_object.findAll('td')[0].text))
    dead = re.findall('\(사망[  ]([0-9,,]+)\)', table_data_beautifulsoup_object.findAll('td')[0].text)

    foreign_data = {
        'country': country_dictionary[country],
        'certified': int(certified),
        'dead': int(re.sub('[,,명]', '', dead[0])) if dead != [] else 0
    }

    foreign_data_list.append(foreign_data)

    for table_data in table_data_rows[1:]:
        table_data_beautifulsoup_object = BeautifulSoup(str(table_data), "html.parser")

        country = table_data_beautifulsoup_object.findAll('td')[0].text
        certified = re.sub('[,,명]', '',
                           re.sub('\(사망[  ][0-9,,]+\)', '', table_data_beautifulsoup_object.findAll('td')[1].text))
        dead = re.findall('\(사망[  ]([0-9,,]+)\)', table_data_beautifulsoup_object.findAll('td')[1].text)

        foreign_data = {
            'country': country_dictionary[country],
            'certified': int(certified),
            'dead': int(re.sub('[,,명]', '', dead[0])) if dead != [] else 0
        }

        foreign_data_list.append(foreign_data)

    return foreign_data_list


if __name__ == '__main__':
    timestamp = int(time.time())
    logger.info("recorded a time stamp | timestamp=" + str(timestamp))

    result = get_foreign_data(target="http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=11")
    logger.info("get result | result=" + str(result))

    dump_result(timestamp, result)
    logger.info("dump result | timestamp=" + str(timestamp) + " | result=" + str(result))
    insert_result(timestamp, result)
    logger.info("insert result | timestamp=" + str(timestamp) + " | result=" + str(result))
