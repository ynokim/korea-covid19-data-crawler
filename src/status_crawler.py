import logging
from logging.handlers import RotatingFileHandler

import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime

import json
import time

import pymysql
import mysql_status_property

logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/status_crawler.log', maxBytes=1024 * 1024 * 1024 * 9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info("every package loaded and start logging")


def insert_result(uid, data_list):
    logger.info("insert_result: function started")
    connection = pymysql.connect(host=mysql_status_property.hostname, user=mysql_status_property.user,
                                 password=mysql_status_property.password, db=mysql_status_property.database,
                                 charset=mysql_status_property.charset)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger.info("insert_result: database connection opened")

    for data in data_list[1:]:
        cursor.execute(
            f"insert into status_{data['region']} values({uid}, {data_list[0]}, {data['increased']}, {data['certified']}, {data['dead']}, {data['percentage']}, {data['check']});")
        logger.info("insert_result: status_" + str(data['region']) + " data inserted | " + str(data))

    connection.commit()
    connection.close()
    logger.info("insert_result: database connection closed")

    logger.info("insert_result: function ended")


def dump_result(uid, data):
    with open("./status-data/k_covid19_status_" + str(uid) + ".json", "w") as json_file:
        json.dump(data, json_file)


def get_status_data(target=''):
    logger.info("get_status_data: function started | target=" + target)

    downloaded_html = urlopen(target)
    logger.info("get_status_data: html downloaded")
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")
    logger.info("get_status_data: html parsed to beautifulsoup object")

    announced_time = ['2002',
                      re.findall('([0-9]+).', beautifulsoup_object.findAll('p', class_='info')[0].text)[0],
                      re.findall('.([0-9]+)', beautifulsoup_object.findAll('p', class_='info')[0].text)[0],
                      re.findall('([0-9]+)시', beautifulsoup_object.findAll('p', class_='info')[0].text)[0]]

    datetime_object = datetime.datetime.strptime(str(announced_time), "['%Y', '%m', '%d', '%H']")
    announced_time_unix = int(time.mktime(datetime_object.timetuple()))

    raw_table = beautifulsoup_object.findAll('tbody')
    logger.info("get_status_data: numbers_raw picked out")
    raw_table_beautifulsoup_object = BeautifulSoup(str(raw_table[0]), "html.parser")

    status_data_list = [announced_time_unix]
    table_data_rows = raw_table_beautifulsoup_object.findAll('tr')
    region_dictionary = {
        '합계': 'synthesize',
        '서울': 'seoul',
        '부산': 'busan',
        '대구': 'daegu',
        '인천': 'incheon',
        '광주': 'gwangju',
        '대전': 'daejeon',
        '울산': 'ulsan',
        '세종': 'sejong',
        '경기': 'gyeonggi',
        '강원': 'gangwon',
        '충북': 'chungbuk',
        '충남': 'chungnam',
        '전북': 'jeonbuk',
        '전남': 'jeonnam',
        '경북': 'gyeongbuk',
        '경남': 'gyeongnam',
        '제주': 'jeju',
        '검역': 'quarantine'
    }

    for table_data in table_data_rows:
        table_data_beautifulsoup_object = BeautifulSoup(str(table_data), "html.parser")

        region = region_dictionary[table_data_beautifulsoup_object.findAll('th')[0].text]
        data = table_data_beautifulsoup_object.findAll('td')

        status_data = {
            'region': region,
            'increased': int('0' + re.sub('[^0-9]', '', data[0].text)),
            'certified': int('0' + re.sub('[^0-9]', '', data[1].text)),
            'dead': int('0' + re.sub('[^0-9]', '', data[2].text)),
            'percentage': float('0' + re.sub('[^0-9.]', '', data[3].text)),
            'check': int('0' + re.sub('[^0-9]', '', data[4].text))
        }

        status_data_list.append(status_data)

    return status_data_list


if __name__ == '__main__':
    timestamp = int(time.time())
    logger.info("recorded a time stamp | timestamp=" + str(timestamp))

    result = get_status_data(target="http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=13")
    logger.info("get result | result=" + str(result))

    dump_result(timestamp, result)
    logger.info("dump result | timestamp=" + str(timestamp) + " | result=" + str(result))
    insert_result(timestamp, result)
    logger.info("insert result | timestamp=" + str(timestamp) + " | result=" + str(result))
