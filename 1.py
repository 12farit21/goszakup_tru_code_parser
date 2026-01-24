import requests
from lxml import html

URL = "https://goszakup.gov.kz/ru/search/lots?filter%5Bname%5D=&filter%5Bnumber%5D=&filter%5Bnumber_anno%5D=&filter%5Benstru%5D=&filter%5Bcustomer%5D=&filter%5Bamount_from%5D=5000000&filter%5Bamount_to%5D=&filter%5Btrade_type%5D=g&filter%5Bmonth%5D=&filter%5Bplan_number%5D=&filter%5Bend_date_from%5D=&filter%5Bend_date_to%5D=&filter%5Bstart_date_to%5D=&filter%5Byear%5D=&filter%5Bitogi_date_from%5D=&filter%5Bitogi_date_to%5D=&filter%5Bstart_date_from%5D=&filter%5Bmore%5D=&smb="

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9"
}

response = requests.get(URL, headers=headers, timeout=15)
response.raise_for_status()

tree = html.fromstring(response.text)

links = tree.xpath('//a[@target="_blank" and @style="font-size: 13px"]/@href')

for link in links:
    print(link)

print(f"\nНайдено ссылок: {len(links)}")