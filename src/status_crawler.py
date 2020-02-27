import logging
from logging.handlers import RotatingFileHandler

import re
from urllib.request import urlopen
from bs4 import BeautifulSoup

import json
import time

logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/status_crawler.log', maxBytes=1024*1024*1024*9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info("every package loaded and start logging")


def dump_result(uid, data):
    with open("./status-data/k_covid19_status_" + str(uid) + ".json", "w") as json_file:
        json.dump(data, json_file)


def get_status_data(target=''):
    logger.info("get_status_data: function started | target=" + target)

    downloaded_html = urlopen(target)
    logger.info("get_status_data: html downloaded")
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")
    logger.info("get_status_data: html parsed to beautifulsoup object")

    numbers_raw = beautifulsoup_object.findAll('a', class_='num')
    logger.info("get_status_data: numbers_raw picked out")

    confirmed_num_str = numbers_raw[0].text
    logger.info("get_status_data: confirmed_num_str extracted | dead_num_int=" + str(confirmed_num_str))
    confirmed_num_int = int(re.sub(',', '', confirmed_num_str[0:len(confirmed_num_str) - 2]))
    logger.info("get_status_data: confirmed_num_int extracted | dead_num_int=" + str(confirmed_num_int))

    unisolated_num_str = numbers_raw[1].text
    logger.info("get_status_data: unisolated_num_str extracted | dead_num_int=" + str(unisolated_num_str))
    unisolated_num_int = int(re.sub(',', '', unisolated_num_str[0:len(unisolated_num_str) - 2]))
    logger.info("get_status_data: unisolated_num_int extracted | dead_num_int=" + str(unisolated_num_int))

    dead_num_str = numbers_raw[2].text
    logger.info("get_status_data: dead_num_str extracted | dead_num_int=" + str(dead_num_str))
    dead_num_int = int(re.sub(',', '', dead_num_str[0:len(dead_num_str) - 2]))
    logger.info("get_status_data: dead_num_int extracted | dead_num_int=" + str(dead_num_int))

    collected_result = {
        'confirmed': confirmed_num_int,
        'unisolated': unisolated_num_int,
        'dead': dead_num_int
    }
    logger.info("get_status_data: collected_result generated | collected_result=" + str(collected_result))

    logger.info("get_status_data: function ended | collected_result=" + str(collected_result))
    return collected_result


if __name__ == '__main__':
    timestamp = int(time.time()*1000)

    result = get_status_data(target="http://ncov.mohw.go.kr/index_main.jsp")

    dump_result(timestamp, result)

    print(result)
