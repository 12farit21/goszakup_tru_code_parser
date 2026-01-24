import requests
from lxml import html

url = "https://goszakup.gov.kz/ru/announce/ajax_load_lot/16099116?tab=lots"

payload = {
    "id": "84809767"
}

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://goszakup.gov.kz/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

try:
    resp = requests.post(url, data=payload, headers=headers, timeout=15)
    resp.raise_for_status()          # выбросит исключение при 4xx/5xx

    print(resp.text)

except requests.exceptions.RequestException as e:
    print("Ошибка запроса:", e)
except Exception as e:
    print("Другая ошибка:", e)