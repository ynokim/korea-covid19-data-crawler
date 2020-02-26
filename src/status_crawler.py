import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./status_crawler.log', maxBytes=1024*9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)


def get_status(target=''):
    import re

    from urllib.request import urlopen
    from bs4 import BeautifulSoup

    downloaded_html = urlopen(target)
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")

    result = beautifulsoup_object.findAll('a', class_='num')

    confirmed_num_str = result[0].text
    confirmed_num_int = int(re.sub(',', '', confirmed_num_str[0:len(confirmed_num_str) - 2]))

    unisolated_num_str = result[1].text
    unisolated_num_int = int(re.sub(',', '', unisolated_num_str[0:len(unisolated_num_str) - 2]))

    dead_num_str = result[2].text
    dead_num_int = int(re.sub(',', '', dead_num_str[0:len(dead_num_str) - 2]))

    return confirmed_num_int, unisolated_num_int, dead_num_int


if __name__ == '__main__':
    confirmed, unisolated, dead = get_status(target="http://ncov.mohw.go.kr/index_main.jsp")
    print(confirmed)
    print(unisolated)
    print(dead)
