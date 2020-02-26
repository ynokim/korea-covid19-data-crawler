import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/status_crawler.log', maxBytes=1024*1024*1024*9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)


def get_patient_data(page_index=0, patient_id=0):
    import re

    from urllib.request import urlopen
    from bs4 import BeautifulSoup

    target = 'http://ncov.mohw.go.kr/bdBoardList_Real.do?brdGubun=12&pageIndex=' + str(page_index)

    downloaded_html = urlopen(target)
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")

    patient_info_raw = beautifulsoup_object.findAll('a', href='#no' + str(patient_id))
    beautifulsoup_patient_info = BeautifulSoup(str(patient_info_raw[0]), "html.parser")
    patient_info_extracted = beautifulsoup_patient_info.findAll('span')

    patient_info = {
        'patient_no': str(patient_info_extracted[1].text),
        'sex': 1 if patient_info_extracted[3].text[0] == '남' else 0,
        'nationality': re.findall('\(([^,]+),', patient_info_extracted[3].text)[0],
        'age': re.findall('\'([^,]+)\)', patient_info_extracted[3].text)[0],
        'causation': '' if re.findall('([^|]+) \([확인0-9차]{2}', patient_info_extracted[5].text)[0] == '확인 중' else
        re.findall('([^|]+) \([확인0-9차]{2}', patient_info_extracted[5].text)[0],
        'order': -1 if re.findall(' \(([확인 중0-9차]+)\)', patient_info_extracted[5].text)[0] == '확인 중' else int(
            re.sub('[^0-9]', '', re.findall(' \(([확인 중0-9차]+)\)', patient_info_extracted[5].text)[0])),
        'confirmed_month': int(re.findall('([0-9]+).', patient_info_extracted[7].text)[0]),
        'confirmed_date': int(re.findall('[0-9].[^[0-9]+([0-9]+)', patient_info_extracted[7].text)[0]),
        'clinic': '' if patient_info_extracted[9].text == '확인 중' else patient_info_extracted[9].text,
        'contacted': -1 if re.findall('([확인 중0-9]+) [^확인중0-9]+\(', patient_info_extracted[11].text)[
                               0] == '확인 중' else int(
            re.findall('([확인 중0-9]+) [^확인중0-9]+\(', patient_info_extracted[11].text)[0]),
        'isolated_contacted': -1 if
        re.findall('\([^확인중0-9]+([확인 중0-9]+)[^확인중0-9]+\)', patient_info_extracted[11].text)[0] == '확인 중' else int(
            re.findall('\([^확인중0-9]+([확인 중0-9]+)[^확인중0-9]+\)', patient_info_extracted[11].text)[0])
    }

    patient_path_raw = beautifulsoup_object.findAll('div', id='no' + str(patient_id))
    beautifulsoup_patient_path = BeautifulSoup(str(patient_path_raw[0]), "html.parser")
    patient_path_extracted = beautifulsoup_patient_path.findAll('li')

    patient_path_info_list = []
    patient_path_list = []

    day_in_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    for i in range(len(patient_path_extracted)):
        patient_path_text = str(patient_path_extracted[i].text)

        if patient_path_text[0] != '(':
            patient_path_info = {
                'patient_no': patient_id,
                'content': patient_path_text
            }
            patient_path_info_list.append(patient_path_info)
        else:
            month_period_identifier = re.findall('[~∼][0-9]+([월])', patient_path_text)
            date_period_identifier = re.findall('[~∼][0-9]+([일])', patient_path_text)
            if month_period_identifier == ['월']:
                start_month = int(re.findall('\(([0-9]+)월 ', patient_path_text)[0])
                start_date = int(re.findall(' ([0-9]+)일[~∼]', patient_path_text)[0])
                end_month = int(re.findall('[~∼]([0-9]+)월 ', patient_path_text)[0])
                end_date = int(re.findall(' ([0-9]+)일\)', patient_path_text)[0])

                for month in range(start_month, end_month + 1):
                    if month != end_month:
                        for date in range(start_date, day_in_month[month - 1] + 1):
                            patient_path = {
                                'patient_no': patient_id,
                                'month': month,
                                'date': date,
                                'content': re.sub('\([0-9]+월 [0-9]+일[~∼][0-9]월 [0-9]+일\) ', '', patient_path_text)
                            }
                            patient_path_list.append(patient_path)
                    else:
                        for date in range(start_date, end_date + 1):
                            patient_path = {
                                'patient_no': patient_id,
                                'month': month,
                                'date': date,
                                'content': re.sub('\([0-9]+월 [0-9]+일[~∼][0-9]월 [0-9]+일\) ', '', patient_path_text)
                            }
                            patient_path_list.append(patient_path)
            elif date_period_identifier == ['일']:
                month = int(re.findall('\(([0-9]+)월 ', patient_path_text)[0])
                start_date = int(re.findall(' ([0-9]+)[~∼]', patient_path_text)[0])
                end_date = int(re.findall('[~∼]([0-9]+)일\)', patient_path_text)[0])
                for date in range(start_date, end_date + 1):
                    patient_path = {
                        'patient_no': patient_id,
                        'month': month,
                        'date': date,
                        'content': re.sub('\([0-9]+월 [0-9]+[~∼][0-9]+일\) ', '', patient_path_text)
                    }
                    patient_path_list.append(patient_path)
            else:
                patient_path = {
                    'patient_no': patient_id,
                    'month': int(re.findall('\(([0-9]+)월 ', patient_path_text)[0]),
                    'date': int(re.findall(' ([0-9]+)일\)', patient_path_text)[0]),
                    'content': re.sub('\([0-9]+월 [0-9]+일\) ', '', patient_path_text)
                }
                patient_path_list.append(patient_path)

    return patient_info, patient_path_info_list, patient_path_list


def get_patient_num():
    import re

    from urllib.request import urlopen
    from bs4 import BeautifulSoup

    downloaded_html = urlopen("http://ncov.mohw.go.kr/bdBoardList_Real.do?brdGubun=12&pageIndex=1")
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")

    result = beautifulsoup_object.find('a', title="클릭하시면 이동경로가 열립니다.")

    patient_num_int = int(re.findall('#no([0-9]+)', str(result))[0])

    return patient_num_int


def get_every_patient_data():
    patient_num = get_patient_num()

    patient_info_collected = []
    patient_path_info_list_collected = []
    patient_path_list_collected = []

    patient_id = patient_num
    patient_left_in_page = 10
    page_index = 1

    while patient_id != 0:
        patient_info, patient_path_info_list, patient_path_list = get_patient_data(page_index=page_index,
                                                                                   patient_id=patient_id)
        patient_info_collected.insert(0, patient_info)
        patient_path_info_list_collected.insert(0, patient_path_info_list)
        patient_path_list_collected.insert(0, patient_path_list)
        patient_id -= 1

        patient_left_in_page -= 1
        if patient_left_in_page <= 0:
            page_index += 1
            patient_left_in_page = 10

    return patient_info_collected, patient_path_info_list_collected, patient_path_list_collected


if __name__ == '__main__':
    patient_info_result, patient_path_info_list_result, patient_path_list_result = get_every_patient_data()

    print(patient_info_result)
    print(patient_path_info_list_result)
    print(patient_path_list_result)
