import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/status_crawler.log', maxBytes=1024*9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info("logging start")


def get_status(target=''):
    logger.info("get_status: function started | target=" + target)
    import re

    from urllib.request import urlopen
    from bs4 import BeautifulSoup
    logger.info("get_status: packages loaded")

    downloaded_html = urlopen(target)
    logger.info("get_status: html downloaded")
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")
    logger.info("get_status: html parsed to beautifulsoup object")

    numbers = beautifulsoup_object.findAll('a', class_='num')
    logger.info("get_status: result picked out")

    confirmed_num_str = numbers[0].text
    logger.info("get_status: confirmed_num_str extracted | dead_num_int=" + str(confirmed_num_str))
    confirmed_num_int = int(re.sub(',', '', confirmed_num_str[0:len(confirmed_num_str) - 2]))
    logger.info("get_status: confirmed_num_int extracted | dead_num_int=" + str(confirmed_num_int))

    unisolated_num_str = numbers[1].text
    logger.info("get_status: unisolated_num_str extracted | dead_num_int=" + str(unisolated_num_str))
    unisolated_num_int = int(re.sub(',', '', unisolated_num_str[0:len(unisolated_num_str) - 2]))
    logger.info("get_status: unisolated_num_int extracted | dead_num_int=" + str(unisolated_num_int))

    dead_num_str = numbers[2].text
    logger.info("get_status: dead_num_str extracted | dead_num_int=" + str(dead_num_str))
    dead_num_int = int(re.sub(',', '', dead_num_str[0:len(dead_num_str) - 2]))
    logger.info("get_status: dead_num_int extracted | dead_num_int=" + str(dead_num_int))

    result = {
        'confirmed': confirmed_num_int,
        'unisolated': unisolated_num_int,
        'dead': dead_num_int
    }
    logger.info("get_status: result generated | result=" + str(result))

    logger.info("get_status: function ended | result=" + str(result))
    return result


if __name__ == '__main__':
    result = get_status(target="http://ncov.mohw.go.kr/index_main.jsp")

    print(result)
