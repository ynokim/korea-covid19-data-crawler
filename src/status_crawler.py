import re

from urllib.request import urlopen
from bs4 import BeautifulSoup

target = "http://ncov.mohw.go.kr/index_main.jsp"

downloadedHtml = urlopen(target)
bsObject = BeautifulSoup(downloadedHtml, "html.parser")

result = bsObject.findAll('a', class_='num')

confirmedNumStr = result[0].text
confirmedNumInt = int(re.sub(',', '', confirmedNumStr[0:len(confirmedNumStr)-2]))

unisolatedNumStr = result[1].text
unisolatedNumInt = int(re.sub(',', '', unisolatedNumStr[0:len(unisolatedNumStr)-2]))

deadNumStr = result[2].text
deadNumInt = int(re.sub(',', '', deadNumStr[0:len(deadNumStr)-2]))

print(confirmedNumInt)
print(unisolatedNumInt)
print(deadNumInt)
