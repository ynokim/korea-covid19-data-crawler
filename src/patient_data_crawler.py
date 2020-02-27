import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/patient_data_crawler.log', maxBytes=1024*1024*1024*9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info("logging start")


class PatientInfoPicker():
    def __init__(self, data):
        self.data = data
        return

    def patient_no(self):
        return str(self.data[1].text)

    def sex(self):
        return 1 if self.data[3].text[0] == '남' else 0

    def nationality(self):
        import re

        return re.findall('\(([^,]+),', self.data[3].text)[0]

    def age(self):
        import re

        return re.findall('\'([^,]+)\)', self.data[3].text)[0]

    def causation(self):
        import re

        return '' if re.findall('([^|]+) \([확인0-9차]{2}', self.data[5].text)[0] == '확인 중' else re.findall('([^|]+) \([확인0-9차]{2}', self.data[5].text)[0]

    def order(self):
        import re

        return -1 if re.findall(' \(([확인 중0-9차]+)\)', self.data[5].text)[0] == '확인 중' else int(
            re.sub('[^0-9]', '', re.findall(' \(([확인 중0-9차]+)\)', self.data[5].text)[0]))

    def confirmed_month(self):
        import re

        return int(re.findall('([0-9]+).', self.data[7].text)[0])

    def confirmed_date(self):
        import re

        return int(re.findall('[0-9].[^[0-9]+([0-9]+)', self.data[7].text)[0])

    def clinic(self):
        import re

        return '' if self.data[9].text == '확인 중' else self.data[9].text

    def contacted(self):
        import re

        return -1 if re.findall('([확인 중0-9]+) [^확인중0-9]+\(', self.data[11].text)[
                               0] == '확인 중' else int(
            re.findall('([확인 중0-9]+) [^확인중0-9]+\(', self.data[11].text)[0])

    def isolated_contacted(self):
        import re

        return -1 if re.findall('\([^확인중0-9]+([확인 중0-9]+)[^확인중0-9]+\)', self.data[11].text)[0] == '확인 중' else int(re.findall('\([^확인중0-9]+([확인 중0-9]+)[^확인중0-9]+\)', self.data[11].text)[0])


def get_patient_data(page_index=0, patient_id=0):
    logger.info("get_patient_data: function started | page_index=" + str(page_index) + " | patient_id=" + str(patient_id))
    import re

    from urllib.request import urlopen
    from bs4 import BeautifulSoup
    logger.info("get_patient_data: packages loaded")

    target = 'http://ncov.mohw.go.kr/bdBoardList_Real.do?brdGubun=12&pageIndex=' + str(page_index)
    logger.info("get_patient_data: target declared | target=" + target)

    downloaded_html = urlopen(target)
    logger.info("get_patient_data: html downloaded")
    beautifulsoup_object = BeautifulSoup(downloaded_html, "html.parser")
    logger.info("get_patient_data: html parsed to beautifulsoup object")

    patient_info_raw = beautifulsoup_object.findAll('a', href='#no' + str(patient_id))
    logger.info("get_patient_data: patient_info_raw picked out")
    beautifulsoup_patient_info = BeautifulSoup(str(patient_info_raw[0]), "html.parser")
    logger.info("get_patient_data: patient_info_raw parsed to beautifulsoup object")
    patient_info_extracted = beautifulsoup_patient_info.findAll('span')
    logger.info("get_patient_data: patient_info_extracted picked out")

    picker = PatientInfoPicker(patient_info_extracted)

    patient_info = {
        'patient_no': picker.patient_no(),
        'sex': picker.sex(),
        'nationality': picker.nationality(),
        'age': picker.age(),
        'causation': picker.causation(),
        'order': picker.order(),
        'confirmed_month': picker.confirmed_month(),
        'confirmed_date': picker.confirmed_date(),
        'clinic': picker.clinic,
        'contacted': picker.contacted(),
        'isolated_contacted': picker.isolated_contacted()
    }
    logger.info("get_patient_data: patient_info generated | patient_info=" + str(patient_info))

    patient_path_raw = beautifulsoup_object.findAll('div', id='no' + str(patient_id))
    logger.info("get_patient_data: patient_path_raw picked out")
    beautifulsoup_patient_path = BeautifulSoup(str(patient_path_raw[0]), "html.parser")
    logger.info("get_patient_data: patient_path_raw parsed to beautifulsoup object")
    patient_path_extracted = beautifulsoup_patient_path.findAll('li')
    logger.info("get_patient_data: patient_path_extracted picked out")

    patient_path_info_list = []
    logger.info("get_patient_data: patient_path_info_list declared")
    patient_path_list = []
    logger.info("get_patient_data: patient_path_list declared")

    day_in_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    logger.info("get_patient_data: day_in_month declared")

    for i in range(len(patient_path_extracted)):
        logger.info("get_patient_data: processing patient_path_extracted " + str(i) + "/" + str(len(patient_path_extracted)-1))
        patient_path_text = str(patient_path_extracted[i].text)
        logger.info("get_patient_data: patient_path_text extracted | patient_path_text=" + patient_path_text)

        if patient_path_text[0] != '(':
            logger.info("get_patient_data: data confirmed as patient_path_info")
            patient_path_info = {
                'patient_no': patient_id,
                'content': patient_path_text
            }
            logger.info("get_patient_data: patient_path_info generated | patient_path_info=" + str(patient_path_info))
            patient_path_info_list.append(patient_path_info)
            logger.info("get_patient_data: patient_path_info appended to patient_path_info_list")
        else:
            logger.info("get_patient_data: data confirmed as patient_path")
            month_period_identifier = re.findall('[~∼][0-9]+([월])', patient_path_text)
            logger.info("get_patient_data: month_period_identifier picked out | month_period_identifier=" + str(month_period_identifier))
            date_period_identifier = re.findall('[~∼][0-9]+([일])', patient_path_text)
            logger.info("get_patient_data: date_period_identifier picked out | date_period_identifier=" + str(date_period_identifier))
            if month_period_identifier == ['월']:
                logger.info("get_patient_data: month_period_identifier has been confirmed that it is valid")
                start_month = int(re.findall('\(([0-9]+)월 ', patient_path_text)[0])
                logger.info("get_patient_data: start_month extracted | start_month=" + str(start_month))
                start_date = int(re.findall(' ([0-9]+)일[~∼]', patient_path_text)[0])
                logger.info("get_patient_data: start_date extracted | start_date=" + str(start_date))
                end_month = int(re.findall('[~∼]([0-9]+)월 ', patient_path_text)[0])
                logger.info("get_patient_data: end_month extracted | end_month=" + str(end_month))
                end_date = int(re.findall(' ([0-9]+)일\)', patient_path_text)[0])
                logger.info("get_patient_data: end_date extracted | end_date=" + str(end_date))

                for month in range(start_month, end_month + 1):
                    logger.info("get_patient_data: processing month " + str(month))
                    if month != end_month:
                        logger.info("get_patient_data: month confirmed as it is not end_month")
                        for date in range(start_date, day_in_month[month - 1] + 1):
                            logger.info("get_patient_data: processing date " + str(date))
                            patient_path = {
                                'patient_no': patient_id,
                                'month': month,
                                'date': date,
                                'content': re.sub('\([0-9]+월 [0-9]+일[~∼][0-9]월 [0-9]+일\) ', '', patient_path_text)
                            }
                            logger.info("get_patient_data: patient_path generated | patient_path=" + str(patient_path))
                            patient_path_list.append(patient_path)
                            logger.info("get_patient_data: patient_path appended to patient_path_list")
                    else:
                        logger.info("get_patient_data: month confirmed as it is end_month")
                        for date in range(start_date, end_date + 1):
                            logger.info("get_patient_data: processing date " + str(date))
                            patient_path = {
                                'patient_no': patient_id,
                                'month': month,
                                'date': date,
                                'content': re.sub('\([0-9]+월 [0-9]+일[~∼][0-9]월 [0-9]+일\) ', '', patient_path_text)
                            }
                            logger.info("get_patient_data: patient_path generated | patient_path=" + str(patient_path))
                            patient_path_list.append(patient_path)
                            logger.info("get_patient_data: patient_path appended to patient_path_list")
            elif date_period_identifier == ['일']:
                logger.info("get_patient_data: date_period_identifier has been confirmed that it is valid")
                month = int(re.findall('\(([0-9]+)월 ', patient_path_text)[0])
                logger.info("get_patient_data: month extracted | month=" + str(month))
                start_date = int(re.findall(' ([0-9]+)[~∼]', patient_path_text)[0])
                logger.info("get_patient_data: start_date extracted | start_date=" + str(start_date))
                end_date = int(re.findall('[~∼]([0-9]+)일\)', patient_path_text)[0])
                logger.info("get_patient_data: end_date extracted | end_date=" + str(end_date))
                for date in range(start_date, end_date + 1):
                    logger.info("get_patient_data: processing date " + str(date))
                    patient_path = {
                        'patient_no': patient_id,
                        'month': month,
                        'date': date,
                        'content': re.sub('\([0-9]+월 [0-9]+[~∼][0-9]+일\) ', '', patient_path_text)
                    }
                    logger.info("get_patient_data: patient_path generated | patient_path=" + str(patient_path))
                    patient_path_list.append(patient_path)
                    logger.info("get_patient_data: patient_path appended to patient_path_list")
            else:
                logger.info("get_patient_data: It has been confirmed that neither month_period_identifier nor date_period_identifier is valid")
                patient_path = {
                    'patient_no': patient_id,
                    'month': int(re.findall('\(([0-9]+)월 ', patient_path_text)[0]),
                    'date': int(re.findall(' ([0-9]+)일\)', patient_path_text)[0]),
                    'content': re.sub('\([0-9]+월 [0-9]+일\) ', '', patient_path_text)
                }
                logger.info("get_patient_data: patient_path generated | patient_path=" + str(patient_path))
                patient_path_list.append(patient_path)
                logger.info("get_patient_data: patient_path appended to patient_path_list")

    logger.info("get_patient_data: function ended | patient_info=" + str(patient_info) + " | patient_path_info_list=" + str(patient_path_info_list) + " | patient_path_list=" + str(patient_path_list))
    return patient_info, patient_path_info_list, patient_path_list


def get_patient_num():
    logger.info("get_patient_num: function started")
    import re

    from urllib.request import urlopen
    from bs4 import BeautifulSoup
    logger.info("get_patient_num: packages loaded")

    html = urlopen("http://ncov.mohw.go.kr/bdBoardList_Real.do?brdGubun=12&pageIndex=1")
    logger.info("get_patient_num: html downloaded")
    beautifulsoup_object = BeautifulSoup(html, "html.parser")
    logger.info("get_patient_num: html parsed to beautifulsoup object")

    patient_num_raw = beautifulsoup_object.find('a', title="클릭하시면 이동경로가 열립니다.")
    logger.info("get_patient_num: patient_num_raw picked up")

    patient_num_int = int(re.findall('#no([0-9]+)', str(patient_num_raw))[0])
    logger.info("get_patient_num: patient_num_int extracted | patient_num_int=" + str(patient_num_int))

    logger.info("get_patient_num: function ended | patient_num_int=" + str(patient_num_int))
    return patient_num_int


def get_every_patient_data():
    logger.info("get_every_patient_data: function started")
    patient_num = get_patient_num()
    logger.info("get_every_patient_data: get patient_num | patient_num=" + str(patient_num))

    patient_info_collected = []
    logger.info("get_every_patient_data: patient_info_collected declared")
    patient_path_info_list_collected = []
    logger.info("get_every_patient_data: patient_path_info_list_collected declared")
    patient_path_list_collected = []
    logger.info("get_every_patient_data: patient_path_list_collected declared")

    patient_id = patient_num
    logger.info("get_every_patient_data: patient_id has been set up | patient_id=" + str(patient_id))
    patient_left_in_page = 10
    logger.info("get_every_patient_data: initialize patient_left_in_page | patient_left_in_page=" + str(patient_left_in_page))
    page_index = 1
    logger.info("get_every_patient_data: initialize page_index | page_index=" + str(page_index))

    while patient_id != 0:
        logger.info("get_every_patient_data: processing patient_id " + str(patient_id))
        patient_info, patient_path_info_list, patient_path_list = get_patient_data(page_index=page_index,
                                                                                   patient_id=patient_id)
        logger.info("get_every_patient_data: get data from get_patient_data function | patient_info=" + str(patient_info) + " | patient_path_info_list=" + str(patient_path_info_list) + " | patient_path_list=" + str(patient_path_list))
        patient_info_collected.insert(0, patient_info)
        logger.info("get_patient_data: patient_info inserted to patient_info_collected")
        patient_path_info_list_collected.insert(0, patient_path_info_list)
        logger.info("get_patient_data: patient_path_info_list inserted to patient_path_info_list_collected")
        patient_path_list_collected.insert(0, patient_path_list)
        logger.info("get_patient_data: patient_path_list inserted to patient_path_list_collected")
        patient_id -= 1
        logger.info("get_patient_data: update patient_id | patient_id=" + str(patient_id))

        patient_left_in_page -= 1
        logger.info("get_patient_data: update patient_left_in_page | patient_left_in_page=" + str(patient_left_in_page))
        if patient_left_in_page <= 0:
            logger.info("get_patient_data: all patient in this page has been processed")
            page_index += 1
            logger.info("get_patient_data: update page_index | page_index=" + str(page_index))
            patient_left_in_page = 10
            logger.info("get_patient_data: initialize patient_left_in_page | patient_left_in_page=" + str(patient_left_in_page))

    collected_result = {
        'patient_info': patient_info_collected,
        'patient_path_info_list': patient_path_info_list_collected,
        'patient_path_list': patient_path_list_collected
    }
    logger.info("get_patient_data: collected_result generated | collected_result=" + str(collected_result))

    logger.info("get_patient_data: function ended | collected_result=" + str(collected_result))
    return collected_result


if __name__ == '__main__':
    result = get_every_patient_data()

    print(result)
