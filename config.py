"""
Configuration for Goszakup Tender Data Parser
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

# Database
DB_PATH = DATA_DIR / "goszakup_lots.db"

# Input JSON file
INPUT_JSON_FILE = OUTPUT_DIR / "goszakup_links_20260125_031156.json"

# HTTP settings
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
RETRY_BASE_DELAY = 3.0
REQUEST_DELAY = 0  # Delay between requests (rate limiting)
RATE_LIMIT_DELAY = 10.0  # Delay after 429 error

# HTTP headers (from 2.py and 3.py)
HTTP_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://goszakup.gov.kz/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Batch processing
BATCH_SIZE = 50  # Commit to DB every N records
CHECKPOINT_INTERVAL = 50  # Save progress every N URLs

# URLs (from 3.py)
AJAX_LOAD_LOT_URL = "https://goszakup.gov.kz/ru/announce/ajax_load_lot/{announce_id}?tab=lots"

# Field mapping: Russian table headers → Database columns
# Maps HTML table field names to database column names
FIELD_MAPPING = {
    'Лот №': 'lot_number',
    'Статус лота': 'lot_status',
    'БИН заказчика': 'customer_bin',
    'Наименование заказчика': 'customer_name',
    'Код ТРУ': 'tru_code',
    'Наименование ТРУ': 'tru_name',
    'Краткая характеристика': 'brief_description',
    'Дополнительная характеристика': 'additional_description',
    'Цена за единицу': 'price_per_unit',
    'Единица измерения': 'unit_of_measurement',
    'Количество': 'quantity',
    'Место поставки товара, КАТО': 'delivery_location_kato'
}

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
