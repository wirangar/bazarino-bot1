import asyncio
import json
import logging
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import SPREADSHEET, SHEET_CONFIG, ADMIN_ID

log = logging.getLogger("bazarino")

async def retry_gspread(func, *args, retries=3, delay=1, **kwargs):
    for attempt in range(retries):
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except gspread.exceptions.APIError as e:
            if attempt == retries - 1:
                raise
            log.warning(f"Google Sheets API error, retry {attempt + 1}/{retries}: {e}")
            await asyncio.sleep(delay * (2 ** attempt))
    raise Exception("Max retries reached for Google Sheets operation")

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_path = os.getenv("GOOGLE_CREDS", "/etc/secrets/bazarino-perugia-bot-f37c44dd9b14.json")
    try:
        with open(creds_path, "r", encoding="utf-8") as f:
            CREDS_JSON = json.load(f)
    except FileNotFoundError:
        log.error(f"Credentials file '{creds_path}' not found")
        raise SystemExit(f"❗️ فایل احراز هویت '{creds_path}' یافت نشد.")
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse credentials file '{creds_path}': {e}")
        raise SystemExit(f"❗️ خطا در تجزیه فایل احراز هویت '{creds_path}': {e}")
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(CREDS_JSON, scope))
    try:
        wb = gc.open(SPREADSHEET)
    except gspread.exceptions.SpreadsheetNotFound:
        log.error(f"Spreadsheet '{SPREADSHEET}' not found. Please check the SPREADSHEET_NAME and access permissions.")
        raise SystemExit(f"❗️ فایل Google Spreadsheet با نام '{SPREADSHEET}' یافت نشد.")
    try:
        orders_ws = wb.worksheet(SHEET_CONFIG["orders"]["name"])
        products_ws = wb.worksheet(SHEET_CONFIG["products"]["name"])
    except gspread.exceptions.WorksheetNotFound as e:
        log.error(f"Worksheet not found: {e}. Check config.yaml for correct worksheet names.")
        raise SystemExit(f"❗️ خطا در دسترسی به worksheet: {e}")
    try:
        abandoned_cart_ws = wb.worksheet(SHEET_CONFIG["abandoned_carts"]["name"])
    except gspread.exceptions.WorksheetNotFound:
        abandoned_cart_ws = wb.add_worksheet(title=SHEET_CONFIG["abandoned_carts"]["name"], rows=1000, cols=3)
    try:
        discounts_ws = wb.worksheet(SHEET_CONFIG["discounts"]["name"])
    except gspread.exceptions.WorksheetNotFound:
        discounts_ws = wb.add_worksheet(title=SHEET_CONFIG["discounts"]["name"], rows=1000, cols=4)
    try:
        uploads_ws = wb.worksheet(SHEET_CONFIG["uploads"]["name"])
    except gspread.exceptions.WorksheetNotFound:
        uploads_ws = wb.add_worksheet(title=SHEET_CONFIG["uploads"]["name"], rows=1000, cols=4)
except Exception as e:
    log.error(f"Failed to initialize Google Sheets: {e}")
    raise SystemExit(f"❗️ خطا در اتصال به Google Sheets: {e}")
