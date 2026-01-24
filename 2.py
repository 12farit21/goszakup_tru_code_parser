import requests
from lxml import html

url = "https://goszakup.gov.kz/ru/announce/index/16099116?tab=lots#"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://goszakup.gov.kz/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

try:
    resp = requests.post(url, headers=headers, timeout=15)
    resp.raise_for_status()          # выбросит исключение при 4xx/5xx
    # print(resp.text)
    # Парсим HTML
    tree = html.fromstring(resp.text)

    # Ищем все <a> с атрибутом data-lot-id
    lot_links = tree.xpath('//a[@data-lot-id]/@data-lot-id')

    # Выводим все найденные значения
    if lot_links:
        for lot_id in lot_links:
            print(lot_id)
    else:
        print("Не найдено ни одного data-lot-id")

except requests.exceptions.RequestException as e:
    print("Ошибка запроса:", e)
except Exception as e:
    print("Другая ошибка:", e)