#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B دکترینو Telegram Bot – Optimized Version
- Webhook via FastAPI on Render with secret token
- Dynamic products from Google Sheets with versioned cache
- Features: Invoice with Hafez quote, discount codes, order notes, abandoned cart reminders,
  photo upload (file_id), push notifications (preparing/shipped), weekly backup
- Optimized for Render.com with Google Sheets
"""

from __future__ import annotations
import asyncio
import datetime as dt
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import uuid
import yaml
from typing import Dict, Any, List
import io
import random
import time

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters, JobQueue, PicklePersistence
)
from telegram.error import BadRequest, NetworkError

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("bazarino.log", maxBytes=5*1024*1024, backupCount=3)
    ]
)
log = logging.getLogger("bazarino")

# Global variables
tg_app = None
bot = None

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

# ───────────── Google Sheets Setup
async def retry_gspread(func, *args, retries=3, delay=1, **kwargs):
    for attempt in range(retries):
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except gspread.exceptions.APIError as e:
            if attempt == retries - 1:
                raise
            log.warning(f"Googlenae Sheets API error, retry {attempt + 1}/{retries}: {e}")
            await asyncio.sleep(delay * (2 ** attempt))
    raise ExceptionExpense("Max retries reached for Google Sheets operation")

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

# ───────────── Lazy-import Pillow and Invoice Generation
async def generate_invoice(order_id, user_data, cart, total, discount):
    from PIL import Image, ImageDraw, ImageFont

    width, height = 700, 1000
    bg_color = (248, 249, 250)
    header_color = (40, 167, 69)
    text_color = (33, 37, 41)
    secondary_text_color = (108, 117, 125)
    border_color = (222, 226, 230)

    img = Image.new("RGB", (width, height), color=bg_color)

    # Add background pattern
    pattern_path = "assets/background_pattern.png"
    if os.path.exists(pattern_path):
        try:
            pattern = Image.open(pattern_path).convert("L")
            pattern = Image.eval(pattern, lambda p: 255 - (255 - p) // 2) # Make it lighter
            img.paste(bg_color, (0, 0, width, height))
            for y in range(0, height, pattern.height):
                for x in range(0, width, pattern.width):
                    img.paste(pattern, (x, y), mask=pattern)
        except Exception as e:
            log.error(f"Background pattern error: {e}")

    draw = ImageDraw.Draw(img)

    # Fonts
    font_dir = "fonts"
    try:
        title_font = ImageFont.truetype(os.path.join(font_dir, "Vazir.ttf"), 32)
        header_font = ImageFont.truetype(os.path.join(font_dir, "Vazir.ttf"), 22)
        body_font = ImageFont.truetype(os.path.join(font_dir, "Vazir.ttf"), 18)
        small_font = ImageFont.truetype(os.path.join(font_dir, "arial.ttf"), 14)
    except Exception as e:
        log.error(f"Font loading error: {e}, falling back to default fonts")
        title_font = header_font = body_font = small_font = ImageFont.load_default()

    # Header
    draw.rectangle([(0, 0), (width, 100)], fill=header_color)
    logo_path = "logo.png"
    if os.path.exists(logo_path):
        try:
            logo = Image.open(logo_path).resize((80, 80))
            img.paste(logo, (20, 10), mask=logo)
        except Exception as e:
            log.error(f"Logo loading error: {e}")
    draw.text((width - 40, 50), "فاکتور فروش", fill=(255, 255, 255), font=title_font, anchor="ra")
    draw.text((width - 40, 85), "Bazarino Invoice", fill=(220, 220, 220), font=small_font, anchor="ra")

    # Order Info
    y = 140
    margin = 40
    draw.text((width - margin, y), f"شماره سفارش: {order_id}", font=header_font, fill=text_color, anchor="ra")
    draw.text((margin, y), f"Order ID: #{order_id}", font=small_font, fill=secondary_text_color, anchor="la")
    y += 50

    # Customer Info
    info_box_y = y
    draw.rounded_rectangle([(margin, y), (width - margin, y + 100)], radius=10, fill=(255, 255, 255), outline=border_color)

    info_y = y + 20
    draw.text((width - margin - 20, info_y), f"نام مشتری: {user_data.get('name', 'N/A')}", font=body_font, fill=text_color, anchor="ra")
    info_y += 30
    draw.text((width - margin - 20, info_y), f"مقصد: {user_data.get('dest', 'N/A')}", font=body_font, fill=text_color, anchor="ra")
    info_y = y + 20
    draw.text((margin + 20, info_y), f"Address: {user_data.get('address', 'N/A')}", font=small_font, fill=secondary_text_color, anchor="la")
    info_y += 20
    draw.text((margin + 20, info_y), f"Postal Code: {user_data.get('postal', 'N/A')}", font=small_font, fill=secondary_text_color, anchor="la")
    info_y += 20
    draw.text((margin + 20, info_y), f"Phone: {user_data.get('phone', 'N/A')}", font=small_font, fill=secondary_text_color, anchor="la")

    y += 130

    # Products Table
    draw.text((width - margin, y), "محصولات / Prodotti", font=header_font, fill=text_color, anchor="ra")
    y += 40

    table_header_y = y
    draw.line([(margin, table_header_y), (width - margin, table_header_y)], fill=border_color, width=2)
    y += 15
    draw.text((width - margin - 20, y), "محصول", font=body_font, fill=secondary_text_color, anchor="ra")
    draw.text((width / 2, y), "تعداد", font=body_font, fill=secondary_text_color, anchor="ma")
    draw.text((margin + 20, y), "قیمت", font=body_font, fill=secondary_text_color, anchor="la")
    y += 15
    draw.line([(margin, y), (width - margin, y)], fill=border_color, width=1)

    for item in cart:
        y += 25
        subtotal = item['qty'] * item['price']
        draw.text((width - margin - 20, y), item['fa'], font=body_font, fill=text_color, anchor="ra")
        draw.text((width / 2, y), str(item['qty']), font=body_font, fill=text_color, anchor="ma")
        draw.text((margin + 20, y), f"{subtotal:.2f}€", font=body_font, fill=text_color, anchor="la")
        y += 25
        draw.line([(margin + 20, y), (width - margin - 20, y)], fill=border_color, width=1)

    # Totals
    y += 30
    draw.text((width - margin, y), f"تخفیف: {discount:.2f}€", font=body_font, fill=text_color, anchor="ra")
    draw.text((margin, y), f"Sconto: {discount:.2f}€", font=small_font, fill=secondary_text_color, anchor="la")
    y += 40
    draw.line([(margin, y), (width - margin, y)], fill=border_color, width=2)
    y += 10
    draw.text((width - margin, y), f"مبلغ نهایی: {total:.2f}€", font=header_font, fill=header_color, anchor="ra")
    draw.text((margin, y), f"Totale: {total:.2f}€", font=small_font, fill=secondary_text_color, anchor="la")
    y += 50

    # Notes & Hafez
    if user_data.get('notes'):
        draw.text((width - margin, y), "یادداشت شما:", font=body_font, fill=text_color, anchor="ra")
        y += 25
        draw.text((width - margin, y), user_data['notes'], font=small_font, fill=secondary_text_color, anchor="ra")
        y += 40

    if HAFEZ_QUOTES:
        hafez = random.choice(HAFEZ_QUOTES)
        draw.text((width / 2, y), "✨ فال حافظ ✨", font=header_font, fill=text_color, anchor="mm")
        y += 35
        draw.text((width / 2, y), hafez["fa"], font=body_font, fill=secondary_text_color, anchor="mm")
        y += 25
        draw.text((width / 2, y), hafez["it"], font=small_font, fill=secondary_text_color, anchor="mm")

    # Footer
    footer_y = height - 60
    draw.rectangle([(0, footer_y), (width, height)], fill=header_color)
    draw.text((width / 2, footer_y + 30), "بازارینو - طعم ایران در ایتالیا | Bazarino - Sapori d'Iran in Italia", fill=(255, 255, 255), font=body_font, anchor="mm")

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer

# ───────────── Google Sheets Data
async def load_products() -> Dict[str, Dict[str, Any]]:
    try:
        records = await retry_gspread(products_ws.get_all_records)
        required_cols = ["id", "cat", "fa", "it", "brand", "description", "weight", "price", "stock"]
        if records and not all(col in records[0] for col in required_cols):
            missing = [col for col in required_cols if col not in records[0]]
            log.error(f"Missing required columns in products worksheet: {missing}")
            raise SystemExit(f"❗️ ستون‌های مورد نیاز در worksheet محصولات وجود ندارند: {missing}")
        products = {}
        for r in records:
            try:
                products[r["id"]] = dict(
                    cat=r["cat"],
                    fa=r["fa"],
                    it=r["it"],
                    brand=r["brand"],
                    desc=r["description"],
                    weight=r["weight"],
                    price=float(r["price"]),
                    image_url=r.get("image_url") or None,
                    stock=int(r.get("stock", 0)),
                    is_bestseller=r.get("is_bestseller", "FALSE").lower() == "true",
                    version=r.get("version", "0")
                )
            except (ValueError, KeyError) as e:
                log.error(f"Invalid product data in row: {r}, error: {e}")
                continue
        if not products:
            log.error("No valid products loaded from Google Sheets")
            raise SystemExit("❗️ هیچ محصول معتبری از Google Sheets بارگذاری نشد.")
        return products
    except Exception as e:
        log.error(f"Error loading products from Google Sheets: {e}")
        raise SystemExit(f"❗️ خطا در بارگذاری محصولات از Google Sheets: {e}")

async def load_discounts():
    try:
        records = await retry_gspread(discounts_ws.get_all_records)
        required_cols = ["code", "discount_percent", "valid_until", "is_active"]
        if records and not all(col in records[0] for col in required_cols):
            missing = [col for col in required_cols if col not in records[0]]
            log.error(f"Missing required columns in discounts worksheet: {missing}")
            return {}
        discounts = {}
        for r in records:
            try:
                discounts[r["code"]] = dict(
                    discount_percent=float(r["discount_percent"]),
                    valid_until=r["valid_until"],
                    is_active=r["is_active"].lower() == "true"
                )
            except (ValueError, KeyError) as e:
                log.error(f"Invalid discount data in row: {r}, error: {e}")
                continue
        return discounts
    except Exception as e:
        log.error(f"Error loading discounts: {e}")
        return {}

# Versioned cache for products
async def get_products():
    try:
        cell = await retry_gspread(products_ws.acell, "L1")
        current_version = cell.value or "0"
        if (not hasattr(get_products, "_data") or
                not hasattr(get_products, "_version") or
                get_products._version != current_version or
                dt.datetime.utcnow() > getattr(get_products, "_ts", dt.datetime.min)):
            get_products._data = await load_products()
            get_products._version = current_version
            get_products._ts = dt.datetime.utcnow() + dt.timedelta(seconds=60)
            log.info(f"Loaded {len(get_products._data)} products from Google Sheets, version {current_version}")
        return get_products._data
    except Exception as e:
        log.error(f"Error in get_products: {e}")
        if ADMIN_ID and bot:
            try:
                await bot.send_message(ADMIN_ID, f"⚠️ خطا در بارگذاری محصولات: {e}")
            except Exception as admin_e:
                log.error(f"Failed to notify admin: {admin_e}")
        raise

EMOJI = {
    "rice": "🍚 برنج / Riso", "beans": "🥣 حبوبات / Legumi", "spice": "🌿 ادویه / Spezie",
    "nuts": "🥜 خشکبار / Frutta secca", "drink": "🧃 نوشیدنی / Bevande",
    "canned": "🥫 کنسرو / Conserve", "sweet": "🍬 شیرینی / Dolci"
}

# ───────────── Helpers
cart_total = lambda c: sum(i["qty"] * i["price"] for i in c)
cart_count = lambda ctx: sum(i["qty"] for i in ctx.user_data.get("cart", []))

async def safe_edit(q, *args, **kwargs):
    try:
        parse_mode   = kwargs.get("parse_mode")
        reply_markup = kwargs.get("reply_markup")

        if q.message.text:
            await q.edit_message_text(*args, **kwargs)

        elif q.message.caption is not None or q.message.photo:
            await q.edit_message_caption(
                caption=args[0],
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )

        else:
            try:
                await q.message.delete()
            except Exception as e:
                log.warning(f"Failed to delete message: {e}")
            await q.message.chat.send_message(
                *args,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )

    except BadRequest as e:
        err = str(e)
        if "not modified" in err or "There is no text" in err:
            return
        log.error(f"Edit message error: {err}")
        try:
            await q.message.delete()
        except Exception as e:
            log.warning(f"Failed to delete message after error: {e}")
        await q.message.chat.send_message(
            *args,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )

    except NetworkError as e:
        log.error(f"Network error in safe_edit: {e}")
        await asyncio.sleep(1)
        await q.message.chat.send_message(
            *args,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )



async def alert_admin(pid, stock):
    if stock <= LOW_STOCK_TH and ADMIN_ID:
        for attempt in range(3):
            try:
                await bot.send_message(ADMIN_ID, f"⚠️ موجودی کم {stock}: {(await get_products())[pid]['fa']}")
                log.info(f"Low stock alert sent for {(await get_products())[pid]['fa']}")
                break
            except Exception as e:
                log.error(f"Alert fail attempt {attempt + 1}: {e}")
                await asyncio.sleep(1)
# ───────────── Keyboards
async def kb_main(ctx):
    try:
        cats = {p["cat"] for p in (await get_products()).values()}
        rows = [[InlineKeyboardButton(EMOJI.get(c, c), callback_data=f"cat_{c}")] for c in cats]
        cart = ctx.user_data.get("cart", [])
        cart_summary = f"{m('BTN_CART')} ({cart_count(ctx)} آیتم - {cart_total(cart):.2f}€)" if cart else m("BTN_CART")
        rows.append([
            InlineKeyboardButton(m("BTN_SEARCH"), callback_data="search"),
            InlineKeyboardButton("🔥 پرفروش‌ها / Più venduti", callback_data="bestsellers")
        ])
        rows.append([
            InlineKeyboardButton(cart_summary, callback_data="cart")
        ])
        rows.append([
            InlineKeyboardButton("📞 پشتیبانی / Supporto", callback_data="support")
        ])
        return InlineKeyboardMarkup(rows)
    except Exception as e:
        log.error(f"Error in kb_main: {e}")
        raise

async def kb_category(cat, ctx):
    try:
        rows = [[InlineKeyboardButton(f"{p['fa']} / {p['it']}", callback_data=f"show_{pid}")]
                for pid, p in (await get_products()).items() if p["cat"] == cat]
        rows.append([
            InlineKeyboardButton(m("BTN_SEARCH"), callback_data="search"),
            InlineKeyboardButton(m("BTN_BACK"), callback_data="back")
        ])
        return InlineKeyboardMarkup(rows)
    except Exception as e:
        log.error(f"Error in kb_category: {e}")
        raise

def kb_product(pid):
    try:
        p = get_products._data[pid] if hasattr(get_products, "_data") else (asyncio.run(get_products()))[pid]
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(m("CART_ADDED").split("\n")[0], callback_data=f"add_{pid}")],
            [InlineKeyboardButton(m("BTN_BACK"), callback_data=f"back_cat_{p['cat']}")]
        ])
    except Exception as e:
        log.error(f"Error in kb_product: {e}")
        raise

def kb_cart(cart):
    try:
        rows = []
        for it in cart:
            pid = it["id"]
            rows.append([
                InlineKeyboardButton("➕", callback_data=f"inc_{pid}"),
                InlineKeyboardButton(f"{it['qty']}× {it['fa']}", callback_data="ignore"),
                InlineKeyboardButton("➖", callback_data=f"dec_{pid}"),
                InlineKeyboardButton("❌", callback_data=f"del_{pid}")
            ])
        rows.append([
            InlineKeyboardButton(m("BTN_ORDER_PERUGIA"), callback_data="order_perugia"),
            InlineKeyboardButton(m("BTN_ORDER_ITALY"), callback_data="order_italy")
        ])
        rows.append([
            InlineKeyboardButton(m("BTN_CONTINUE"), callback_data="checkout"),
            InlineKeyboardButton(m("BTN_BACK"), callback_data="back")
        ])
        return InlineKeyboardMarkup(rows)
    except Exception as e:
        log.error(f"Error in kb_cart: {e}")
        raise

def kb_support():
    try:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📷 ارسال تصویر / Invia immagine", callback_data="upload_photo")],
            [InlineKeyboardButton(m("BTN_BACK"), callback_data="back")]
        ])
    except Exception as e:
        log.error(f"Error in kb_support: {e}")
        raise

# ───────────── Cart Operations
async def add_cart(ctx, pid, qty=1, update=None):
    try:
        prods = await get_products()
        if pid not in prods:
            return False, m("STOCK_EMPTY")
        p = prods[pid]
        stock = p["stock"]
        cart = ctx.user_data.setdefault("cart", [])
        cur = next((i for i in cart if i["id"] == pid), None)
        cur_qty = cur["qty"] if cur else 0
        if stock < cur_qty + qty:
            return False, m("STOCK_EMPTY")
        if cur:
            cur["qty"] += qty
        else:
            cart.append(dict(id=pid, fa=p["fa"], price=p["price"], weight=p["weight"], qty=qty))
        await alert_admin(pid, stock)
        try:
            await retry_gspread(
                abandoned_cart_ws.append_row,
                [dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                 ctx.user_data.get("user_id", update.effective_user.id if update else 0),
                 json.dumps(cart)]
            )
        except Exception as e:
            log.error(f"Error saving abandoned cart: {e}")
        return True, m("CART_ADDED")
    except Exception as e:
        log.error(f"Error in add_cart: {e}")
        return False, "❗️ خطا در افزودن به سبد خرید."

def fmt_cart(cart):
    """Cart summary formatted for parse_mode='HTML'."""
    try:
        if not cart:
            return m("CART_EMPTY")
        lines = ["<b>🛍 سبد خرید / Carrello:</b>", ""]
        total = 0
        for item in cart:
            subtotal = item["qty"] * item["price"]
            total += subtotal
            lines.append(f"▫️ {item['qty']}× {item['fa']} — {subtotal:.2f}€")
        lines.append("")
        lines.append(f"<b>💶 جمع / Totale: {total:.2f}€</b>")
        return "\n".join(lines)
    except Exception as e:
        log.error(f"Error in fmt_cart: {e}")
        return "❗️ خطا در نمایش سبد خرید."

#123456789
async def increment_item(ctx, pid):
    return (await add_cart(ctx, pid, 1))[0]

async def decrement_item(ctx, pid):
    cart = ctx.user_data.get("cart", [])
    for it in cart:
        if it["id"] == pid:
            it["qty"] -= 1
            if it["qty"] <= 0:
                cart.remove(it)
            return True
    return False

async def remove_item(ctx, pid):
    cart = ctx.user_data.get("cart", [])
    ctx.user_data["cart"] = [it for it in cart if it["id"] != pid]
    return True


# ───────────── Stock Update
async def update_stock(cart):
    try:
        records = await retry_gspread(products_ws.get_all_records)
        for it in cart:
            pid = it["id"]
            qty = it["qty"]
            for idx, row in enumerate(records, start=2):
                if row["id"] == pid:
                    new = int(row["stock"]) - qty
                    if new < 0:
                        log.error(f"Cannot update stock for {pid}: negative stock")
                        return False
                    await retry_gspread(products_ws.update_cell, idx, 10, new)
                    (await get_products())[pid]["stock"] = new
                    log.info(f"Updated stock for {pid}: {new}")
        return True
    except gspread.exceptions.APIError as e:
        log.error(f"Google Sheets API error during stock update: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در به‌روزرسانی موجودی: {e}")
        return False
    except Exception as e:
        log.error(f"Stock update error: {e}")
        return False

# ───────────── Order States
ASK_NAME, ASK_PHONE, ASK_ADDRESS, ASK_POSTAL, ASK_DISCOUNT, ASK_NOTES = range(6)

# ───────────── Order Process
async def start_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        q = update.callback_query
        if not ctx.user_data.get("dest"):
            await safe_edit(q, f"{m('CART_GUIDE')}\n\n{fmt_cart(ctx.user_data.get('cart', []))}", reply_markup=kb_cart(ctx.user_data.get("cart", [])), parse_mode="HTML")
            return
        ctx.user_data["name"] = f"{q.from_user.first_name} {(q.from_user.last_name or '')}".strip()
        ctx.user_data["handle"] = f"@{q.from_user.username}" if q.from_user.username else "-"
        ctx.user_data["user_id"] = update.effective_user.id
        await q.message.reply_text(m("INPUT_NAME"))
        return ASK_NAME
    except Exception as e:
        log.error(f"Error in start_order: {e}")
        await q.message.reply_text("❗️ خطا در شروع سفارش. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ctx.user_data["name"] = update.message.text.strip()
        await update.message.reply_text(m("INPUT_PHONE"))
        return ASK_PHONE
    except Exception as e:
        log.error(f"Error in ask_phone: {e}")
        await update.message.reply_text("❗️ خطا در ثبت نام. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_address(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ctx.user_data["phone"] = update.message.text.strip()
        await update.message.reply_text(m("INPUT_ADDRESS"))
        return ASK_ADDRESS
    except Exception as e:
        log.error(f"Error in ask_address: {e}")
        await update.message.reply_text("❗️ خطا در ثبت شماره تلفن. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_postal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ctx.user_data["address"] = update.message.text.strip()
        await update.message.reply_text(m("INPUT_POSTAL"))
        return ASK_POSTAL
    except Exception as e:
        log.error(f"Error in ask_postal: {e}")
        await update.message.reply_text("❗️ خطا در ثبت آدرس. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_discount(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ctx.user_data["postal"] = update.message.text.strip()
        await update.message.reply_text("🎁 کد تخفیف دارید؟ وارد کنید یا /skip را بزنید.\nHai un codice sconto? Inseriscilo o premi /skip.")
        return ASK_DISCOUNT
    except Exception as e:
        log.error(f"Error in ask_discount: {e}")
        await update.message.reply_text("❗️ خطا در ثبت کد پستی. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_notes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message.text == "/skip":
            ctx.user_data["discount_code"] = None
        else:
            code = update.message.text.strip()
            discounts = await load_discounts()
            if code in discounts and discounts[code]["is_active"] and dt.datetime.strptime(discounts[code]["valid_until"], "%Y-%m-%d") >= dt.datetime.utcnow():
                ctx.user_data["discount_code"] = code
            else:
                await update.message.reply_text("❌ کد تخفیف نامعتبر است. لطفاً دوباره وارد کنید یا /skip کنید.\nCodice sconto non valido.")
                return ASK_DISCOUNT
        await update.message.reply_text(m("INPUT_NOTES"))
        return ASK_NOTES
    except Exception as e:
        log.error(f"Error in ask_notes: {e}")
        await update.message.reply_text("❗️ خطا در بررسی کد تخفیف. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def confirm_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message.text == "/skip":
            ctx.user_data["notes"] = ""
        else:
            ctx.user_data["notes"] = update.message.text.strip()
        cart = ctx.user_data.get("cart", [])
        if not cart:
            await update.message.reply_text(m("CART_EMPTY"), reply_markup=ReplyKeyboardRemove())
            ctx.user_data.clear()
            return ConversationHandler.END

        if not await update_stock(cart):
            await update.message.reply_text(m("STOCK_EMPTY"), reply_markup=ReplyKeyboardRemove())
            ctx.user_data.clear()
            return ConversationHandler.END

        order_id = str(uuid.uuid4())[:8]
        ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        total = cart_total(cart)
        discount = 0
        if ctx.user_data.get("discount_code"):
            discounts = await load_discounts()
            discount = total * (discounts[ctx.user_data["discount_code"]]["discount_percent"] / 100)
            total -= discount
        address_full = f"{ctx.user_data['address']} | {ctx.user_data['postal']}"
        try:
            for it in cart:
                await retry_gspread(
                    orders_ws.append_row,
                    [ts, order_id, ctx.user_data["user_id"], ctx.user_data["handle"],
                     ctx.user_data["name"], ctx.user_data["phone"], address_full,
                     ctx.user_data["dest"], it["id"], it["fa"], it["qty"], it["price"],
                     it["qty"] * it["price"], ctx.user_data["notes"],
                     ctx.user_data.get("discount_code", ""), discount, "preparing", "FALSE"]
                )
            log.info(f"Order {order_id} saved to Google Sheets for user {ctx.user_data['handle']}")
            invoice_buffer = await generate_invoice(order_id, ctx.user_data, cart, total, discount)
            await update.message.reply_photo(
                photo=invoice_buffer,
                caption=f"{m('ORDER_CONFIRMED')}\n\n📍 مقصد / Destinazione: {ctx.user_data['dest']}\n💶 مجموع / Totale: {total:.2f}€\n🎁 تخفیف / Sconto: {discount:.2f}€\n📝 یادداشت / Nota: {ctx.user_data['notes'] or 'بدون یادداشت'}",
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception as e:
            log.error(f"Error saving order {order_id}: {e}")
            await update.message.reply_text(m("ERROR_SHEET"), reply_markup=ReplyKeyboardRemove())
            ctx.user_data.clear()
            return ConversationHandler.END

        if promo := MSG.get("PROMO_AFTER_ORDER"):
            await update.message.reply_text(promo, disable_web_page_preview=True)
        if ADMIN_ID:
            msg = [f"🆕 سفارش / Ordine {order_id}", f"{ctx.user_data['name']} — {total:.2f}€",
                   f"🎁 تخفیف / Sconto: {discount:.2f}€ ({ctx.user_data.get('discount_code', 'بدون کد')})",
                   f"📝 یادداشت / Nota: {ctx.user_data['notes'] or 'بدون یادداشت'}"] + \
                  [f"▫️ {i['qty']}× {i['fa']}" for i in cart]
            try:
                invoice_buffer.seek(0)
                await bot.send_photo(ADMIN_ID, photo=invoice_buffer, caption="\n".join(msg))
                log.info(f"Admin notified for order {order_id}")
            except Exception as e:
                log.error(f"Failed to notify admin for order {order_id}: {e}")
        try:
            await retry_gspread(abandoned_cart_ws.clear)
        except Exception as e:
            log.error(f"Error clearing abandoned carts: {e}")
        ctx.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        log.error(f"Error in confirm_order: {e}")
        await update.message.reply_text("❗️ خطا در ثبت سفارش. لطفاً دوباره امتحان کنید.")
        ctx.user_data.clear()
        return ConversationHandler.END

async def cancel_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ctx.user_data.clear()
        await update.message.reply_text(m("ORDER_CANCELLED"), reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    except Exception as e:
        log.error(f"Error in cancel_order: {e}")
        await update.message.reply_text("❗️ خطا در لغو سفارش. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

# ───────────── Photo Upload
async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        if not ctx.user_data.get("awaiting_photo"):
            return
        photo = update.message.photo[-1]
        if photo.file_size > 2 * 1024 * 1024:  # حداکثر 2 مگابایت
            await update.message.reply_text(m("ERROR_FILE_SIZE"), reply_markup=await kb_main(ctx))
            ctx.user_data["awaiting_photo"] = False
            return
        file = await photo.get_file()
        try:
            await retry_gspread(
                uploads_ws.append_row,
                [dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                 update.effective_user.id,
                 f"@{update.effective_user.username or '-'}",
                 file.file_id]
            )
            await bot.send_photo(
                ADMIN_ID,
                file.file_id,
                caption=f"تصویر از کاربر @{update.effective_user.username or update.effective_user.id}\n📝 توضیح: {ctx.user_data.get('photo_note', 'بدون توضیح')}"
            )
            await update.message.reply_text(m("PHOTO_UPLOADED"))
            ctx.user_data["awaiting_photo"] = False
            ctx.user_data["photo_note"] = ""
            await update.message.reply_text(m("SUPPORT_MESSAGE"), reply_markup=await kb_main(ctx))
        except Exception as e:
            log.error(f"Error handling photo upload: {e}")
            await update.message.reply_text(m("ERROR_UPLOAD"), reply_markup=await kb_main(ctx))
    except Exception as e:
        log.error(f"Error in handle_photo: {e}")
        await update.message.reply_text("❗️ خطا در آپلود تصویر. لطفاً دوباره امتحان کنید.")
        ctx.user_data["awaiting_photo"] = False

# ───────────── Push Notifications for Order Status
async def check_order_status(context: ContextTypes.DEFAULT_TYPE):
    try:
        last_checked_row = getattr(check_order_status, "_last_checked_row", 1)
        shipped_cells = await retry_gspread(orders_ws.findall, "shipped")
        preparing_cells = await retry_gspread(orders_ws.findall, "preparing")
        for cell in shipped_cells + preparing_cells:
            if cell.row <= last_checked_row:
                continue
            row_data = await retry_gspread(orders_ws.row_values, cell.row)
            if len(row_data) < 18 or row_data[17] == "TRUE":  # notified
                continue
            user_id = int(row_data[2])  # user_id
            order_id = row_data[1]  # order_id
            status = row_data[16]  # status
            msg = {
                "preparing": f"📦 سفارش شما (#{order_id}) در حال آماده‌سازی است!\nIl tuo ordine (#{order_id}) è in preparazione!",
                "shipped": f"🚚 سفارش شما (#{order_id}) ارسال شد!\nIl tuo ordine (#{order_id}) è stato spedito!"
            }[status]
            await context.bot.send_message(user_id, msg, reply_markup=await kb_main(context))
            await retry_gspread(orders_ws.update_cell, cell.row, 18, "TRUE")
            log.info(f"Sent {status} notification for order {order_id} to user {user_id}")
        check_order_status._last_checked_row = max(last_checked_row, max((c.row for c in shipped_cells + preparing_cells), default=1))
    except Exception as e:
        log.error(f"Error checking order status: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در بررسی وضعیت سفارشات: {e}")

# ───────────── Backup Google Sheets
async def backup_sheets(context: ContextTypes.DEFAULT_TYPE):
    try:
        sheets = [orders_ws, products_ws, discounts_ws, abandoned_cart_ws, uploads_ws]
        for sheet in sheets:
            records = await retry_gspread(sheet.get_all_values)
            csv_content = "\n".join([",".join(row) for row in records])
            csv_file = io.BytesIO(csv_content.encode("utf-8"))
            csv_file.name = f"{sheet.title}_backup_{dt.datetime.utcnow().strftime('%Y%m%d')}.csv"
            await context.bot.send_document(ADMIN_ID, document=csv_file, caption=f"📊 بکاپ {sheet.title} - {dt.datetime.utcnow().strftime('%Y-%m-%d')}")
            log.info(f"Backup sent for {sheet.title}")
    except Exception as e:
        log.error(f"Error creating backup: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در ایجاد بکاپ: {e}")

# ───────────── Abandoned Cart Reminder
async def send_cart_reminder(context: ContextTypes.DEFAULT_TYPE):
    try:
        records = await retry_gspread(abandoned_cart_ws.get_all_records)
        for record in records:
            cart = json.loads(record["cart"])
            user_id = int(record["user_id"])
            if cart:
                await context.bot.send_message(
                    user_id,
                    f"🛒 سبد خرید شما هنوز منتظر شماست!\nHai lasciato qualcosa nel carrello!\n{fmt_cart(cart)}\n👉 برای تکمیل سفارش: /start",
                    reply_markup=await kb_main(context)
                )
        await retry_gspread(abandoned_cart_ws.clear)
    except Exception as e:
        log.error(f"Error sending cart reminders: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در ارسال یادآور سبد خرید: {e}")

# ───────────── /search
from difflib import get_close_matches
async def cmd_search(u, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        q = " ".join(ctx.args).lower()
        if not q:
            await u.message.reply_text(m("SEARCH_USAGE"))
            return
        hits = [(pid, p) for pid, p in (await get_products()).items()
                if q in p['fa'].lower() or q in p['it'].lower()
                or get_close_matches(q, [p['fa'].lower() + " " + p['it'].lower()], cutoff=0.6)]
        if not hits:
            await u.message.reply_text(m("SEARCH_NONE"))
            return
        for pid, p in hits[:5]:
            cap = f"{p['fa']} / {p['it']}\n{p['desc']}\n{p['price']}€\nموجودی / Stock: {p['stock']}"
            btn = InlineKeyboardMarkup.from_button(InlineKeyboardButton(m("CART_ADDED").split("\n")[0], callback_data=f"add_{pid}"))
            if p["image_url"] and p["image_url"].strip():
                await u.message.reply_photo(p["image_url"], caption=cap, reply_markup=btn)
            else:
                await u.message.reply_text(cap, reply_markup=btn)
    except Exception as e:
        log.error(f"Error in cmd_search: {e}")
        await u.message.reply_text("❗️ خطا در جستجو. لطفاً دوباره امتحان کنید.")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در /search: {e}")

# ───────────── Commands
async def cmd_start(u, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ctx.user_data["user_id"] = u.effective_user.id
        await u.message.reply_html(m("WELCOME"), reply_markup=await kb_main(ctx))
    except Exception as e:
        log.error(f"Error in cmd_start: {e}")
        await u.message.reply_text("❗️ خطایی در بارگذاری منو رخ داد. لطفاً بعداً امتحان کنید یا با پشتیبانی تماس بگیرید.\nErrore nel caricamento del menu. Riprova più tardi o contatta il supporto.")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در /start: {e}")
        raise

async def cmd_about(u, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        await u.message.reply_text(m("ABOUT_US"), disable_web_page_preview=True)
    except Exception as e:
        log.error(f"Error in cmd_about: {e}")
        await u.message.reply_text("❗️ خطا در نمایش اطلاعات. لطفاً دوباره امتحان کنید.")

async def cmd_privacy(u, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        await u.message.reply_text(m("PRIVACY"), disable_web_page_preview=True)
    except Exception as e:
        log.error(f"Error in cmd_privacy: {e}")
        await u.message.reply_text("❗️ خطا در نمایش سیاست حریم خصوصی. لطفاً دوباره امتحان کنید.")


# ───────────── Cart Buttons Handler
async def handle_cart_buttons(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    data = q.data
    pid  = data.split("_", 1)[1]

    if data.startswith("inc_"):
        ok = await increment_item(ctx, pid)
        await q.answer("✅ یک عدد افزوده شد" if ok else m("STOCK_EMPTY"), show_alert=not ok)

    elif data.startswith("dec_"):
        await decrement_item(ctx, pid)
        await q.answer("➖ کم شد")

    elif data.startswith("del_"):
        await remove_item(ctx, pid)
        await q.answer("❌ حذف شد")

    elif data.startswith("add_"):
        ok, msg = await add_cart(ctx, pid, 1, update=update)
        await q.answer(msg, show_alert=not ok)

    cart = ctx.user_data.get("cart", [])
    if cart:
        await safe_edit(q,
                        f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}",
                        reply_markup=kb_cart(cart), parse_mode="HTML")
    else:
        await safe_edit(q, m("CART_EMPTY"), reply_markup=await kb_main(ctx))



# ───────────── App, Webhook, and FastAPI
async def post_init(app: Application):
    try:
        log.info("Application initialized")
        webhook_url = f"{BASE_URL}/webhook/{WEBHOOK_SECRET}"
        await app.bot.set_webhook(webhook_url)
        log.info(f"Webhook set to {webhook_url}")
    except Exception as e:
        log.error(f"Failed to set webhook: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در تنظیم Webhook: {e}")
        raise

async def post_shutdown(app: Application):
    log.info("Application shutting down")
    try:
        await app.bot.delete_webhook()
    except Exception as e:
        log.error(f"Failed to delete webhook: {e}")

async def lifespan(app: FastAPI):
    global tg_app, bot
    try:
        persistence = PicklePersistence(filepath="bazarino_persistence.pickle")
        builder = ApplicationBuilder().token(TOKEN).persistence(persistence).post_init(post_init).post_shutdown(post_shutdown)
        tg_app = builder.build()
        bot = tg_app.bot
        await tg_app.initialize()
        if not tg_app.job_queue:
            tg_app.job_queue = JobQueue()
        await tg_app.job_queue.start()
        job_queue = tg_app.job_queue
        job_queue.run_daily(send_cart_reminder, time=dt.time(hour=18, minute=0))
        job_queue.run_repeating(check_order_status, interval=600)
        job_queue.run_daily(backup_sheets, time=dt.time(hour=0, minute=0))
        tg_app.add_handler(CommandHandler("start", cmd_start))
        tg_app.add_handler(CommandHandler("search", cmd_search))
        tg_app.add_handler(CommandHandler("about", cmd_about))
        tg_app.add_handler(CommandHandler("privacy", cmd_privacy))
        tg_app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
        tg_app.add_handler(ConversationHandler(
            entry_points=[CallbackQueryHandler(start_order, pattern="^checkout$")],
            states={
                ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_phone)],
                ASK_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_address)],
                ASK_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_postal)],
                ASK_POSTAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_discount)],
                ASK_DISCOUNT: [MessageHandler(filters.TEXT | filters.COMMAND, ask_notes)],
                ASK_NOTES: [MessageHandler(filters.TEXT | filters.COMMAND, confirm_order)],
            },
            fallbacks=[CommandHandler("cancel", cancel_order)]
        ))
        tg_app.add_handler(
            CallbackQueryHandler(handle_cart_buttons, pattern=r"^(inc_|dec_|del_|add_)")
        )
        tg_app.add_handler(CallbackQueryHandler(router))
        yield
        await tg_app.shutdown()
    except Exception as e:
        log.error(f"Error in lifespan: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در راه‌اندازی برنامه: {e}")
        raise


app = FastAPI(lifespan=lifespan)

@app.get("/")
async def keep_alive():
    return {"status": "Bazarino is alive 🚀"}

@app.post("/webhook/{secret}")
async def wh(req: Request, secret: str):
    try:
        if secret != WEBHOOK_SECRET:
            log.error("Invalid webhook secret")
            raise HTTPException(status_code=403, detail="Invalid secret")
        data = await req.json()
        update = Update.de_json(data, bot)
        if not update:
            log.error("Invalid webhook update received")
            raise HTTPException(status_code=400, detail="Invalid update")
        await tg_app.process_update(update)
        return {"ok": True}
    except Exception as e:
        log.error(f"Webhook error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        q = update.callback_query
        d = q.data
        await q.answer()

        if d == "back":
            await safe_edit(q, m("WELCOME"), reply_markup=await kb_main(ctx), parse_mode="HTML")
            return

        if d == "support":
            await safe_edit(q, m("SUPPORT_MESSAGE"), reply_markup=kb_support(), parse_mode="HTML")
            return

        if d == "upload_photo":
            ctx.user_data["awaiting_photo"] = True
            await safe_edit(q, m("UPLOAD_PHOTO"), reply_markup=kb_support())
            return

        if d == "bestsellers":
            bestsellers = [(pid, p) for pid, p in (await get_products()).items() if p.get("is_bestseller", False)]
            if not bestsellers:
                await safe_edit(q, "🔥 در حال حاضر محصول پرفروشی وجود ندارد.\nNessun prodotto più venduto al momento.", reply_markup=await kb_main(ctx), parse_mode="HTML")
                return
            rows = [[InlineKeyboardButton(f"{p['fa']} / {p['it']}", callback_data=f"show_{pid}")] for pid, p in bestsellers]
            rows.append([InlineKeyboardButton(m("BTN_BACK"), callback_data="back")])
            await safe_edit(q, "🔥 محصولات پرفروش / Più venduti", reply_markup=InlineKeyboardMarkup(rows), parse_mode="HTML")
            return

        if d == "search":
            await safe_edit(q, m("SEARCH_USAGE"), reply_markup=await kb_main(ctx))
            return

        if d.startswith("cat_"):
            cat = d[4:]
            await safe_edit(q, EMOJI.get(cat, cat), reply_markup=await kb_category(cat, ctx), parse_mode="HTML")
            return

        if d.startswith("show_"):
            pid = d[5:]
            p = (await get_products())[pid]
            cap = f"<b>{p['fa']} / {p['it']}</b>\n{p['desc']}\n{p['price']}€ / {p['weight']}\n||موجودی / Stock:|| {p['stock']}"
            try:
                await q.message.delete()
            except Exception as e:
                log.error(f"Error deleting previous message: {e}")
            if p["image_url"] and p["image_url"].strip():
                await ctx.bot.send_photo(
                    chat_id=q.message.chat.id,
                    photo=p["image_url"],
                    caption=cap,
                    reply_markup=kb_product(pid),
                    parse_mode="HTML"
                )
            else:
                await ctx.bot.send_message(
                    chat_id=q.message.chat.id,
                    text=cap,
                    reply_markup=kb_product(pid),
                    parse_mode="HTML"
                )
            return

        if d.startswith("add_"):
            pid = d[4:]
            ok, msg = await add_cart(ctx, pid, qty=1, update=update)
            await q.answer(msg, show_alert=not ok)
            cat = (await get_products())[pid]["cat"]
            await safe_edit(q, EMOJI.get(cat, cat), reply_markup=await kb_category(cat, ctx), parse_mode="HTML")
            return

        if d.startswith("back_cat_"):
            cat = d.split("_")[2]
            await safe_edit(q, EMOJI.get(cat, cat), reply_markup=await kb_category(cat, ctx), parse_mode="HTML")
            return

        if d == "cart":
            cart = ctx.user_data.get("cart", [])
            await safe_edit(q, f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}", reply_markup=kb_cart(cart), parse_mode="HTML")
            return

        if d.startswith(("inc_", "dec_", "del_")):
            pid = d.split("_")[1]
            cart = ctx.user_data.get("cart", [])
            it = next((i for i in cart if i["id"] == pid), None)
            if not it:
                await q.answer(m("CART_ITEM_NOT_FOUND"), show_alert=True)
                return
            if d.startswith("inc_"):
                ok, msg = await add_cart(ctx, pid, 1, update=update)
                await q.answer(msg if not ok else m("CART_ADDED"), show_alert=not ok)
            elif d.startswith("dec_"):
                if it["qty"] > 1:
                    it["qty"] -= 1
                    await q.answer(m("CART_DECREASED"), show_alert=False)
                else:
                    cart.remove(it)
                    await q.answer(m("CART_ITEM_REMOVED"), show_alert=False)
            else:  # del_
                cart.remove(it)
                await q.answer(m("CART_ITEM_REMOVED"), show_alert=False)
            try:
                await retry_gspread(
                    abandoned_cart_ws.append_row,
                    [dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                     ctx.user_data.get("user_id", update.effective_user.id),
                     json.dumps(cart)]
                )
            except Exception as e:
                log.error(f"Error saving abandoned cart: {e}")
            if cart:
                await safe_edit(q, f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}", reply_markup=kb_cart(cart), parse_mode="HTML")
            else:
                await safe_edit(q, m("CART_EMPTY"), reply_markup=await kb_main(ctx))
            return

        if d in ["order_perugia", "order_italy"]:
            ctx.user_data["dest"] = "Perugia" if d == "order_perugia" else "Italy"
            await safe_edit(q, f"{m('CART_GUIDE')}\n\n{fmt_cart(ctx.user_data.get('cart', []))}", reply_markup=kb_cart(ctx.user_data.get("cart", [])), parse_mode="HTML")
            return

        if d == "checkout":
            return await start_order(update, ctx)

    except Exception as e:
        log.error(f"Error in router: {e}")
        await q.message.reply_text("❗️ خطا در پردازش درخواست. لطفاً دوباره امتحان کنید.")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در router: {e}")

def main():
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
