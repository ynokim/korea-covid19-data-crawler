def get_status(target=''):
    if target == '':
        return -1, -1, -1
    else:
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
