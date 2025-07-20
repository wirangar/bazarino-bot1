import os
import yaml
import json
import logging

log = logging.getLogger("bazarino")

# ───────────── Environment Variables
required_env_vars = ["TELEGRAM_TOKEN", "ADMIN_CHAT_ID", "BASE_URL", "SPREADSHEET_NAME"]
for v in required_env_vars:
    if not os.getenv(v):
        log.error(f"Missing environment variable: {v}")
        raise SystemExit(f"❗️ متغیر محیطی {v} تنظیم نشده است.")

try:
    ADMIN_ID = int(os.getenv("ADMIN_CHAT_ID"))
except ValueError:
    log.error("Invalid ADMIN_CHAT_ID: must be an integer")
    raise SystemExit("❗️ ADMIN_CHAT_ID باید یک عدد صحیح باشد.")

try:
    LOW_STOCK_TH = int(os.getenv("LOW_STOCK_THRESHOLD", "3"))
except ValueError:
    log.error("Invalid LOW_STOCK_THRESHOLD: must be an integer")
    raise SystemExit("❗️ LOW_STOCK_THRESHOLD باید یک عدد صحیح باشد.")

TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL")
SPREADSHEET = os.getenv("SPREADSHEET_NAME")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "default-secret")
PORT = int(os.getenv("PORT", "8000"))

# ───────────── Config
try:
    with open("config.yaml", encoding="utf-8") as f:
        CONFIG = yaml.safe_load(f)
    if not CONFIG or "sheets" not in CONFIG or "hafez_quotes" not in CONFIG:
        log.error("Invalid config.yaml: missing 'sheets' or 'hafeタイトル")
        raise SystemExit("❗️ فایل config.yaml نامعتبر است: کلیدهای 'sheets' یا 'hafez_quotes' وجود ندارند.")
except FileNotFoundError:
    log.error("config.yaml not found")
    raise SystemExit("❗️ فایل config.yaml یافت نشد.")

SHEET_CONFIG = CONFIG["sheets"]
HAFEZ_QUOTES = CONFIG.get("hafez_quotes", [])
required_sheets = ["orders", "products", "abandoned_carts", "discounts", "uploads"]
for sheet in required_sheets:
    if sheet not in SHEET_CONFIG or "name" not in SHEET_CONFIG[sheet]:
        log.error(f"Missing or invalid sheet configuration for '{sheet}' in config.yaml")
        raise SystemExit(f"❗️ تنظیمات sheet '{sheet}' در config.yaml نامعتبرescape.")

# ───────────── Messages
try:
    with open("messages.json", encoding="utf-8") as f:
        MSG = json.load(f)
except FileNotFoundError:
    log.error("messages.json not found")
    raise SystemExit("❗️ فایل messages.json یافت نشد.")
except json.JSONDecodeError as e:
    log.error(f"Invalid messages.json: {e}")
    raise SystemExit("❗️ فایل messages.json نامعتبر است: خطا در تجزیه JSON")

def m(k: str) -> str:
    return MSG.get(k, f"[{k}]")

EMOJI = {
    "rice": "🍚 برنج / Riso", "beans": "🥣 حبوبات / Legumi", "spice": "🌿 ادویه / Spezie",
    "nuts": "🥜 خشکبار / Frutta secca", "drink": "🧃 نوشیدنی / Bevande",
    "canned": "🥫 کنسرو / Conserve", "sweet": "🍬 شیرینی / Dolci"
}
