#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bazarino Telegram Bot - نسخه نهایی با فاکتور PDF حرفه‌ای
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

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters, JobQueue
)
from telegram.error import BadRequest, NetworkError

# گزارش‌گیری
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("bazarino.log", maxBytes=5*1024*1024, backupCount=3)
    ]
)
log = logging.getLogger("bazarino")

# متغیرهای جهانی
tg_app = None
bot = None

# ───────────── فاکتور PDF حرفه‌ای
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

async def generate_invoice_pdf(order_id, user_data, cart, total, discount):
    """ایجاد فاکتور PDF حرفه‌ای"""
    try:
        # تنظیمات اولیه
        width, height = A4
        buffer = io.BytesIO()
        
        # پالت رنگی
        color_palette = {
            'dark_green': '#2E7D32',
            'gold': '#D4AF37',
            'cream': '#FFFDE7',
            'light_gold': '#F9F5E7',
            'text_dark': '#263238'
        }
        
        # ایجاد سند
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=1*cm,
            leftMargin=1*cm,
            topMargin=1*cm,
            bottomMargin=1*cm
        )
        
        # استایل‌ها
        styles = getSampleStyleSheet()
        
        # ثبت فونت‌ها
        try:
            pdfmetrics.registerFont(TTFont('Nastaliq', 'fonts/Nastaliq.ttf'))
            pdfmetrics.registerFont(TTFont('Vazir', 'fonts/Vazir.ttf'))
            font_fa = 'Nastaliq'
        except:
            font_fa = 'Helvetica'
        
        # استایل‌های سفارشی
        styles.add(ParagraphStyle(
            name='PersianTitle',
            fontName=font_fa,
            fontSize=18,
            textColor=color_palette['dark_green'],
            alignment=1,  # TA_CENTER
            spaceAfter=12
        ))
        
        styles.add(ParagraphStyle(
            name='PersianText',
            fontName=font_fa,
            fontSize=12,
            textColor=color_palette['text_dark'],
            alignment=2,  # TA_RIGHT
            leading=20
        ))
        
        styles.add(ParagraphStyle(
            name='TableHeader',
            fontName='Helvetica-Bold',
            fontSize=10,
            textColor=colors.white,
            alignment=1,  # TA_CENTER
            backColor=color_palette['dark_green']
        ))
        
        styles.add(ParagraphStyle(
            name='HafezFa',
            fontName=font_fa,
            fontSize=14,
            textColor=color_palette['dark_green'],
            alignment=1,  # TA_CENTER
            leading=25
        ))
        
        # محتوا
        story = []
        
        # 1. طرح تذهیب و هدر
        try:
            tazhib = Image("tazhib_pattern.png", width=doc.width, height=2.5*cm)
            story.append(tazhib)
        except:
            pass
        
        # لوگو و عنوان
        try:
            logo = Image("logo.png", width=3*cm, height=3*cm)
            logo.hAlign = 'CENTER'
            story.append(logo)
            story.append(Spacer(1, 0.2*cm))
        except:
            pass
        
        story.append(Paragraph("فروشگاه بازارینو", styles['PersianTitle']))
        story.append(Paragraph("Bazarino Shop", styles['Normal']))
        story.append(Spacer(1, 0.5*cm))
        
        # 2. اطلاعات فاکتور
        invoice_info = [
            ["شماره فاکتور", order_id],
            ["تاریخ", dt.datetime.now().strftime("%Y/%m/%d")],
            ["مشتری", user_data['name']],
            ["تلفن", user_data['phone']],
            ["آدرس", f"{user_data['address']} | {user_data['postal']}"],
            ["مقصد", user_data['dest']]
        ]
        
        info_data = []
        for label, value in invoice_info:
            info_data.append([
                Paragraph(f"<b>{label}:</b>", styles['PersianText']),
                Paragraph(value, styles['PersianText'])
            ])
        
        info_table = Table(info_data, colWidths=[4*cm, 10*cm])
        info_table.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('RIGHTPADDING', (0,0), (0,-1), 10),
            ('LEFTPADDING', (1,0), (1,-1), 10),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(info_table)
        story.append(Spacer(1, 1*cm))
        
        # 3. جدول محصولات
        product_header = ["محصول", "تعداد", "قیمت واحد (€)", "جمع (€)"]
        header_row = [Paragraph(item, styles['TableHeader']) for item in product_header]
        
        product_data = [header_row]
        
        for item in cart:
            product_data.append([
                Paragraph(item['fa'], styles['PersianText']),
                Paragraph(str(item['qty']), styles['Normal']),
                Paragraph(f"{item['price']:.2f}", styles['Normal']),
                Paragraph(f"{item['qty'] * item['price']:.2f}", styles['Normal'])
            ])
        
        # خلاصه مالی
        subtotal = cart_total(cart)
        product_data.append(["", "", "جمع کل:", f"{subtotal:.2f}€"])
        
        if discount > 0:
            product_data.append(["", "", "تخفیف:", f"-{discount:.2f}€"])
        
        product_data.append(["", "", "<b>مبلغ قابل پرداخت:</b>", f"<b>{total:.2f}€</b>"])
        
        product_table = Table(
            product_data, 
            colWidths=[8*cm, 2*cm, 3*cm, 3*cm],
            repeatRows=1
        )
        
        product_table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), color_palette['dark_green']),
            ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('ALIGN', (1,0), (-1,-1), 'CENTER'),
            ('FONTSIZE', (0,0), (-1,-1), 10),
            ('BOTTOMPADDING', (0,0), (-1,0), 8),
            ('TOPPADDING', (0,0), (-1,0), 8),
            ('LINEABOVE', (0,-3), (-1,-3), 1, colors.black),
            ('LINEABOVE', (0,-1), (-1,-1), 1, colors.black),
            ('BOLD', (0,-1), (-1,-1), 1),
            ('BACKGROUND', (0,-1), (-1,-1), color_palette['cream']),
        ]))
        
        story.append(product_table)
        story.append(Spacer(1, 1.5*cm))
        
        # 4. فال حافظ در کادر طلایی
        if HAFEZ_QUOTES:
            hafez = random.choice(HAFEZ_QUOTES)
            
            hafez_box = [
                [Paragraph("✨ فال حافظ", styles['HafezFa'])],
                [Paragraph(hafez["fa"], styles['HafezFa'])],
                [Spacer(1, 0.2*cm)],
                [Paragraph(hafez["it"], styles['Normal'])]
            ]
            
            hafez_table = Table(hafez_box, colWidths=[doc.width])
            hafez_table.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,-1), color_palette['light_gold']),
                ('BOX', (0,0), (-1,-1), 1.5, color_palette['gold']),
                ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                ('PADDING', (0,0), (-1,-1), 12),
                ('ROUNDEDCORNERS', [10,10,10,10]),
            ]))
            
            story.append(hafez_table)
            story.append(Spacer(1, 1*cm))
        
        # 5. امضا و شعار
        try:
            signature = Image("signature.png", width=6*cm, height=1.5*cm)
            signature.hAlign = 'LEFT'
            story.append(signature)
        except:
            pass
        
        slogan = Paragraph(
            "از ایران تا ایتالیا با شما هستیم",
            ParagraphStyle(
                name='Slogan',
                fontName=font_fa,
                fontSize=14,
                textColor=color_palette['dark_green'],
                alignment=1,  # TA_CENTER
                spaceBefore=20
            )
        )
        story.append(slogan)
        
        # ساخت PDF
        doc.build(story)
        buffer.seek(0)
        return buffer
        
    except Exception as e:
        log.error(f"خطا در تولید فاکتور PDF: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در تولید فاکتور: {e}")
        raise

# ───────────── بارگیری پیکربندی
try:
    with open("config.yaml", encoding="utf-8") as f:
        CONFIG = yaml.safe_load(f)
    
    # اعتبارسنجی پیکربندی
    if not CONFIG:
        log.error("فایل config.yaml خالی است")
        raise SystemExit("❗️ فایل config.yaml خالی است")
    
    if "sheets" not in CONFIG:
        log.error("کلید 'sheets' در config.yaml وجود ندارد")
        raise SystemExit("❗️ کلید 'sheets' در config.yaml وجود ندارد")
    
    if "hafez_quotes" not in CONFIG:
        log.error("کلید 'hafez_quotes' در config.yaml وجود ندارد")
        raise SystemExit("❗️ کلید 'hafez_quotes' در config.yaml وجود ندارد")
        
except FileNotFoundError:
    log.error("فایل config.yaml یافت نشد")
    raise SystemExit("❗️ فایل config.yaml یافت نشد")

SHEET_CONFIG = CONFIG["sheets"]
HAFEZ_QUOTES = CONFIG["hafez_quotes"]

# اعتبارسنجی ورق‌های ضروری
required_sheets = ["orders", "products", "abandoned_carts", "discounts", "uploads"]
for sheet in required_sheets:
    if sheet not in SHEET_CONFIG or "name" not in SHEET_CONFIG[sheet]:
        log.error(f"تنظیمات ورق '{sheet}' در config.yaml نامعتبر است")
        raise SystemExit(f"❗️ تنظیمات ورق '{sheet}' در config.yaml نامعتبر است")

# ───────────── پیام‌ها
try:
    with open("messages.json", encoding="utf-8") as f:
        MSG = json.load(f)
except FileNotFoundError:
    log.error("فایل messages.json یافت نشد")
    raise SystemExit("❗️ فایل messages.json یافت نشد")
except json.JSONDecodeError as e:
    log.error(f"خطا در تجزیه messages.json: {e}")
    raise SystemExit(f"❗️ خطا در تجزیه فایل messages.json: {e}")

def m(k: str) -> str:
    """دریافت متن چندزبانه از فایل پیام‌ها"""
    return MSG.get(k, f"[{k}]")

# ───────────── متغیرهای محیطی
# بررسی متغیرهای ضروری
REQUIRED_ENV = ["TELEGRAM_TOKEN", "ADMIN_CHAT_ID", "BASE_URL"]
missing_envs = [v for v in REQUIRED_ENV if not os.getenv(v)]

if missing_envs:
    log.error(f"متغیرهای محیطی ضروری تنظیم نشده‌اند: {', '.join(missing_envs)}")
    raise SystemExit(f"❗️ متغیرهای محیطی ضروری تنظیم نشده‌اند: {', '.join(missing_envs)}")

try:
    ADMIN_ID = int(os.getenv("ADMIN_CHAT_ID"))
except ValueError:
    log.error("ADMIN_CHAT_ID باید یک عدد صحیح باشد")
    raise SystemExit("❗️ ADMIN_CHAT_ID باید یک عدد صحیح باشد")

try:
    LOW_STOCK_TH = int(os.getenv("LOW_STOCK_THRESHOLD", "3"))
except ValueError:
    log.error("LOW_STOCK_THRESHOLD باید یک عدد صحیح باشد")
    raise SystemExit("❗️ LOW_STOCK_THRESHOLD باید یک عدد صحیح باشد")

TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL = os.getenv("BASE_URL").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "EhsaNegar1394")
SPREADSHEET = os.getenv("SPREADSHEET_NAME", "Bazarnio Orders")
PORT = int(os.getenv("PORT", "8000"))

# ───────────── اتصال به Google Sheets
try:
    scope = ["https://spreadsheets.google.com/feeds", 
             "https://www.googleapis.com/auth/drive"]
    
    creds_path = os.getenv("GOOGLE_CREDS", "/etc/secrets/bazarino-bot1")
    
    try:
        with open(creds_path, "r", encoding="utf-8") as f:
            CREDS_JSON = json.load(f)
    except FileNotFoundError:
        log.error(f"فایل احراز هویت '{creds_path}' یافت نشد")
        raise SystemExit(f"❗️ فایل احراز هویت '{creds_path}' یافت نشد")
    except json.JSONDecodeError as e:
        log.error(f"خطا در تجزیه فایل احراز هویت: {e}")
        raise SystemExit(f"❗️ خطا در تجزیه فایل احراز هویت: {e}")
    
    # احراز هویت
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(CREDS_JSON, scope))
    
    try:
        wb = gc.open(SPREADSHEET)
    except gspread.exceptions.SpreadsheetNotFound:
        log.error(f"فایل Google Spreadsheet با نام '{SPREADSHEET}' یافت نشد")
        raise SystemExit(f"❗️ فایل Google Spreadsheet با نام '{SPREADSHEET}' یافت نشد")
    
    # بارگیری ورق‌ها
    try:
        orders_ws = wb.worksheet(SHEET_CONFIG["orders"]["name"])
        products_ws = wb.worksheet(SHEET_CONFIG["products"]["name"])
    except gspread.exceptions.WorksheetNotFound as e:
        log.error(f"خطا در دسترسی به ورق: {e}")
        raise SystemExit(f"❗️ خطا در دسترسی به ورق: {e}")
    
    # ایجاد ورق‌های اختیاری در صورت عدم وجود
    try:
        abandoned_cart_ws = wb.worksheet(SHEET_CONFIG["abandoned_carts"]["name"])
    except gspread.exceptions.WorksheetNotFound:
        abandoned_cart_ws = wb.add_worksheet(
            title=SHEET_CONFIG["abandoned_carts"]["name"], 
            rows=1000, 
            cols=3
        )
    
    try:
        discounts_ws = wb.worksheet(SHEET_CONFIG["discounts"]["name"])
    except gspread.exceptions.WorksheetNotFound:
        discounts_ws = wb.add_worksheet(
            title=SHEET_CONFIG["discounts"]["name"], 
            rows=1000, 
            cols=4
        )
    
    try:
        uploads_ws = wb.worksheet(SHEET_CONFIG["uploads"]["name"])
    except gspread.exceptions.WorksheetNotFound:
        uploads_ws = wb.add_worksheet(
            title=SHEET_CONFIG["uploads"]["name"], 
            rows=1000, 
            cols=4
        )
        
except Exception as e:
    log.error(f"خطا در اتصال به Google Sheets: {e}")
    raise SystemExit(f"❗️ خطا در اتصال به Google Sheets: {e}")

# ───────────── مدیریت داده‌های Google Sheets
async def load_products() -> Dict[str, Dict[str, Any]]:
    """بارگیری محصولات از Google Sheets"""
    try:
        records = await asyncio.to_thread(products_ws.get_all_records)
        
        # اعتبارسنجی ستون‌های ضروری
        required_cols = ["id", "cat", "fa", "it", "brand", "description", "weight", "price"]
        
        if records and not all(col in records[0] for col in required_cols):
            missing = [col for col in required_cols if col not in records[0]]
            log.error(f"ستون‌های ضروری در ورق محصولات وجود ندارند: {missing}")
            raise SystemExit(f"❗️ ستون‌های ضروری در ورق محصولات وجود ندارند: {missing}")
        
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
                log.error(f"داده محصول نامعتبر در ردیف: {r}, خطا: {e}")
                continue
        
        if not products:
            log.error("هیچ محصول معتبری از Google Sheets بارگذاری نشد")
            raise SystemExit("❗️ هیچ محصول معتبری از Google Sheets بارگذاری نشد")
        
        return products
    except Exception as e:
        log.error(f"خطا در بارگذاری محصولات: {e}")
        raise SystemExit(f"❗️ خطا در بارگذاری محصولات: {e}")

async def load_discounts():
    """بارگیری کدهای تخفیف از Google Sheets"""
    try:
        records = await asyncio.to_thread(discounts_ws.get_all_records)
        
        # اعتبارسنجی ستون‌ها
        required_cols = ["code", "discount_percent", "valid_until", "is_active"]
        
        if records and not all(col in records[0] for col in required_cols):
            missing = [col for col in required_cols if col not in records[0]]
            log.error(f"ستون‌های ضروری در ورق تخفیف‌ها وجود ندارند: {missing}")
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
                log.error(f"داده تخفیف نامعتبر در ردیف: {r}, خطا: {e}")
                continue
        return discounts
    except Exception as e:
        log.error(f"خطا در بارگذاری تخفیف‌ها: {e}")
        return {}

# کش نسخه‌دار محصولات
async def get_products():
    """دریافت محصولات با کش نسخه‌دار"""
    try:
        cell = await asyncio.to_thread(products_ws.acell, "L1")
        current_version = cell.value or "0"
        
        # بارگیری مجدد در صورت تغییر نسخه یا انقضای کش
        if (not hasattr(get_products, "_data") or
            not hasattr(get_products, "_version") or
            get_products._version != current_version or
            dt.datetime.utcnow() > getattr(get_products, "_ts", dt.datetime.min)):
            
            get_products._data = await load_products()
            get_products._version = current_version
            get_products._ts = dt.datetime.utcnow() + dt.timedelta(seconds=60)
            log.info(f"تعداد {len(get_products._data)} محصول بارگذاری شد، نسخه {current_version}")
        
        return get_products._data
    except Exception as e:
        log.error(f"خطا در دریافت محصولات: {e}")
        if ADMIN_ID and bot:
            try:
                await bot.send_message(ADMIN_ID, f"⚠️ خطا در بارگذاری محصولات: {e}")
            except Exception as admin_e:
                log.error(f"خطا در اطلاع‌رسانی به ادمین: {admin_e}")
        raise

# نگاشت دسته‌بندی‌ها به ایموجی
EMOJI = {
    "rice": "🍚 برنج / Riso", 
    "beans": "🥣 حبوبات / Legumi", 
    "spice": "🌿 ادویه / Spezie",
    "nuts": "🥜 خشکبار / Frutta secca", 
    "drink": "🧃 نوشیدنی / Bevande",
    "canned": "🥫 کنسرو / Conserve", 
    "sweet": "🍬 شیرینی / Dolci"
}

# ───────────── توابع کمکی
cart_total = lambda c: sum(i["qty"] * i["price"] for i in c)
cart_count = lambda ctx: sum(i["qty"] for i in ctx.user_data.get("cart", []))

async def safe_edit(q, *args, **kwargs):
    """ویرایش ایمن پیام با مدیریت خطاها"""
    try:
        if q.message.text:
            await q.edit_message_text(*args, **kwargs)
        elif q.message.caption is not None or q.message.photo:
            await q.edit_message_caption(
                caption=args[0], 
                reply_markup=kwargs.get("reply_markup")
            )
        else:
            try:
                await q.message.delete()
            except Exception:
                pass
            await q.message.chat.send_message(*args, **kwargs)

    except BadRequest as e:
        err = str(e)
        if "not modified" in err or "There is no text" in err:
            return
        log.error(f"خطا در ویرایش پیام: {err}")
        try:
            await q.message.delete()
        except Exception:
            pass
        await q.message.chat.send_message(*args, **kwargs)
    except NetworkError as e:
        log.error(f"خطای شبکه: {e}")

async def alert_admin(pid, stock):
    """اطلاع‌رسانی به ادمین در صورت کمبود موجودی"""
    if stock <= LOW_STOCK_TH and ADMIN_ID:
        for _ in range(3):  # 3 تلاش برای ارسال
            try:
                product_name = (await get_products())[pid]['fa']
                await bot.send_message(
                    ADMIN_ID, 
                    f"⚠️ موجودی کم ({stock}): {product_name}"
                )
                log.info(f"هشدار کمبود موجودی برای {product_name} ارسال شد")
                break
            except Exception as e:
                log.error(f"خطا در ارسال هشدار: {e}")
                await asyncio.sleep(1)

# ───────────── صفحه‌کلیدها
async def kb_main(ctx):
    """صفحه‌کلید اصلی"""
    try:
        cats = {p["cat"] for p in (await get_products()).values()}
        rows = [
            [InlineKeyboardButton(EMOJI.get(c, c), callback_data=f"cat_{c}")] 
            for c in cats
        ]
        
        cart = ctx.user_data.get("cart", [])
        cart_count_val = cart_count(ctx)
        cart_total_val = cart_total(cart)
        
        cart_summary = (
            f"{m('BTN_CART')} ({cart_count_val} آیتم - {cart_total_val:.2f}€)"
            if cart else m("BTN_CART")
        )
        
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
        log.error(f"خطا در ایجاد صفحه‌کلید اصلی: {e}")
        raise

async def kb_category(cat, ctx):
    """صفحه‌کلید دسته‌بندی"""
    try:
        rows = [
            [InlineKeyboardButton(f"{p['fa']} / {p['it']}", callback_data=f"show_{pid}")]
            for pid, p in (await get_products()).items() 
            if p["cat"] == cat
        ]
        
        rows.append([
            InlineKeyboardButton(m("BTN_SEARCH"), callback_data="search"),
            InlineKeyboardButton(m("BTN_BACK"), callback_data="back")
        ])
        
        return InlineKeyboardMarkup(rows)
    except Exception as e:
        log.error(f"خطا در ایجاد صفحه‌کلید دسته‌بندی: {e}")
        raise

def kb_product(pid):
    """صفحه‌کلید محصول"""
    try:
        p = get_products._data[pid] if hasattr(get_products, "_data") else (asyncio.run(get_products()))[pid]
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(m("CART_ADDED").split("\n")[0], callback_data=f"add_{pid}")],
            [InlineKeyboardButton(m("BTN_BACK"), callback_data=f"back_cat_{p['cat']}")]
        ])
    except Exception as e:
        log.error(f"خطا در ایجاد صفحه‌کلید محصول: {e}")
        raise

def kb_cart(cart):
    """صفحه‌کلید سبد خرید"""
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
        log.error(f"خطا در ایجاد صفحه‌کلید سبد خرید: {e}")
        raise

def kb_support():
    """صفحه‌کلید پشتیبانی"""
    try:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📷 ارسال تصویر / Invia immagine", callback_data="upload_photo")],
            [InlineKeyboardButton(m("BTN_BACK"), callback_data="back")]
        ])
    except Exception as e:
        log.error(f"خطا در ایجاد صفحه‌کلید پشتیبانی: {e}")
        raise

# ───────────── عملیات سبد خرید
async def add_cart(ctx, pid, qty=1, update=None):
    """افزودن محصول به سبد خرید"""
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
            cart.append(dict(
                id=pid, 
                fa=p["fa"], 
                price=p["price"], 
                weight=p["weight"], 
                qty=qty
            ))
        
        await alert_admin(pid, stock)
        
        try:
            user_id = ctx.user_data.get("user_id", update.effective_user.id if update else 0)
            await asyncio.to_thread(
                abandoned_cart_ws.append_row,
                [
                    dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    user_id,
                    json.dumps(cart)
                ]
            )
        except Exception as e:
            log.error(f"خطا در ذخیره سبد رها شده: {e}")
        
        return True, m("CART_ADDED")
    except Exception as e:
        log.error(f"خطا در افزودن به سبد خرید: {e}")
        return False, "❗️ خطا در افزودن به سبد خرید."

def fmt_cart(cart):
    """قالب‌بندی سبد خرید برای نمایش"""
    try:
        if not cart:
            return m("CART_EMPTY")
        
        lines = ["🛍 **سبد خرید / Carrello:**", ""]
        tot = 0
        
        for it in cart:
            sub = it["qty"] * it["price"]
            tot += sub
            lines.append(f"▫️ {it['qty']}× {it['fa']} — {sub:.2f}€")
        
        lines.append("")
        lines.append(f"💶 **جمع / Totale:** {tot:.2f}€")
        return "\n".join(lines)
    except Exception as e:
        log.error(f"خطا در نمایش سبد خرید: {e}")
        return "❗️ خطا در نمایش سبد خرید."

# ───────────── به‌روزرسانی موجودی
async def update_stock(cart):
    """کاهش موجودی محصولات پس از سفارش"""
    try:
        records = await asyncio.to_thread(products_ws.get_all_records)
        
        for it in cart:
            pid = it["id"]
            qty = it["qty"]
            
            for idx, row in enumerate(records, start=2):
                if row["id"] == pid:
                    new_stock = int(row["stock"]) - qty
                    
                    if new_stock < 0:
                        log.error(f"امکان به‌روزرسانی موجودی برای {pid}: موجودی منفی")
                        return False
                    
                    await asyncio.to_thread(products_ws.update_cell, idx, 10, new_stock)
                    (await get_products())[pid]["stock"] = new_stock
                    log.info(f"موجودی {pid} به‌روزرسانی شد: {new_stock}")
        
        return True
    except gspread.exceptions.APIError as e:
        log.error(f"خطای Google Sheets API: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در به‌روزرسانی موجودی: {e}")
        return False
    except Exception as e:
        log.error(f"خطا در به‌روزرسانی موجودی: {e}")
        return False

# ───────────── مراحل سفارش
ASK_NAME, ASK_PHONE, ASK_ADDRESS, ASK_POSTAL, ASK_DISCOUNT, ASK_NOTES = range(6)

# ───────────── فرآیند سفارش
async def start_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """شروع فرآیند سفارش"""
    try:
        q = update.callback_query
        if not ctx.user_data.get("dest"):
            await safe_edit(
                q, 
                f"{m('CART_GUIDE')}\n\n{fmt_cart(ctx.user_data.get('cart', []))}", 
                reply_markup=kb_cart(ctx.user_data.get("cart", [])), 
                parse_mode="HTML"
            )
            return
        
        ctx.user_data["name"] = f"{q.from_user.first_name} {(q.from_user.last_name or '')}".strip()
        ctx.user_data["handle"] = f"@{q.from_user.username}" if q.from_user.username else "-"
        ctx.user_data["user_id"] = update.effective_user.id
        
        await q.message.reply_text(m("INPUT_NAME"))
        return ASK_NAME
    except Exception as e:
        log.error(f"خطا در شروع سفارش: {e}")
        await q.message.reply_text("❗️ خطا در شروع سفارش. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """درخواست شماره تلفن"""
    try:
        ctx.user_data["name"] = update.message.text.strip()
        await update.message.reply_text(m("INPUT_PHONE"))
        return ASK_PHONE
    except Exception as e:
        log.error(f"خطا در ثبت نام: {e}")
        await update.message.reply_text("❗️ خطا در ثبت نام. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_address(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """درخواست آدرس"""
    try:
        ctx.user_data["phone"] = update.message.text.strip()
        await update.message.reply_text(m("INPUT_ADDRESS"))
        return ASK_ADDRESS
    except Exception as e:
        log.error(f"خطا در ثبت شماره تلفن: {e}")
        await update.message.reply_text("❗️ خطا در ثبت شماره تلفن. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_postal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """درخواست کد پستی"""
    try:
        ctx.user_data["address"] = update.message.text.strip()
        await update.message.reply_text(m("INPUT_POSTAL"))
        return ASK_POSTAL
    except Exception as e:
        log.error(f"خطا در ثبت آدرس: {e}")
        await update.message.reply_text("❗️ خطا در ثبت آدرس. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_discount(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """درخواست کد تخفیف"""
    try:
        ctx.user_data["postal"] = update.message.text.strip()
        await update.message.reply_text("🎁 کد تخفیف دارید؟ وارد کنید یا /skip را بزنید.\nHai un codice sconto? Inseriscilo o premi /skip.")
        return ASK_DISCOUNT
    except Exception as e:
        log.error(f"خطا در ثبت کد پستی: {e}")
        await update.message.reply_text("❗️ خطا در ثبت کد پستی. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def ask_notes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """درخواست یادداشت سفارش"""
    try:
        if update.message.text == "/skip":
            ctx.user_data["discount_code"] = None
        else:
            code = update.message.text.strip()
            discounts = await load_discounts()
            
            if (code in discounts and 
                discounts[code]["is_active"] and 
                dt.datetime.strptime(discounts[code]["valid_until"], "%Y-%m-%d") >= dt.datetime.utcnow()):
                
                ctx.user_data["discount_code"] = code
            else:
                await update.message.reply_text("❌ کد تخفیف نامعتبر است. لطفاً دوباره وارد کنید یا /skip کنید.\nCodice sconto non valido.")
                return ASK_DISCOUNT
        
        await update.message.reply_text(m("INPUT_NOTES"))
        return ASK_NOTES
    except Exception as e:
        log.error(f"خطا در بررسی کد تخفیف: {e}")
        await update.message.reply_text("❗️ خطا در بررسی کد تخفیف. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

async def confirm_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """تأیید نهایی سفارش"""
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
            # ذخیره سفارش در Google Sheets
            for it in cart:
                await asyncio.to_thread(
                    orders_ws.append_row,
                    [
                        ts, order_id, ctx.user_data["user_id"], ctx.user_data["handle"],
                        ctx.user_data["name"], ctx.user_data["phone"], address_full,
                        ctx.user_data["dest"], it["id"], it["fa"], it["qty"], it["price"],
                        it["qty"] * it["price"], ctx.user_data["notes"],
                        ctx.user_data.get("discount_code", ""), discount, "preparing", "FALSE"
                    ]
                )
            
            log.info(f"سفارش {order_id} برای کاربر {ctx.user_data['handle']} ذخیره شد")
            
            # ایجاد و ارسال فاکتور PDF
            pdf_buffer = await generate_invoice_pdf(order_id, ctx.user_data, cart, total, discount)
            
            await update.message.reply_document(
                document=pdf_buffer,
                filename=f"Bazarino_Invoice_{order_id}.pdf",
                caption=(
                    f"{m('ORDER_CONFIRMED')}\n\n"
                    f"📍 مقصد / Destinazione: {ctx.user_data['dest']}\n"
                    f"💶 مجموع / Totale: {total:.2f}€\n"
                    f"🎁 تخفیف / Sconto: {discount:.2f}€\n"
                    f"📝 یادداشت / Nota: {ctx.user_data['notes'] or 'بدون یادداشت'}"
                ),
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception as e:
            log.error(f"خطا در ذخیره سفارش {order_id}: {e}")
            await update.message.reply_text(m("ERROR_SHEET"), reply_markup=ReplyKeyboardRemove())
            ctx.user_data.clear()
            return ConversationHandler.END

        # ارسال پیام تبلیغاتی در صورت وجود
        if promo := MSG.get("PROMO_AFTER_ORDER"):
            await update.message.reply_text(promo, disable_web_page_preview=True)
        
        # اطلاع‌رسانی به ادمین
        if ADMIN_ID:
            msg_lines = [
                f"🆕 سفارش / Ordine {order_id}",
                f"{ctx.user_data['name']} — {total:.2f}€",
                f"🎁 تخفیف / Sconto: {discount:.2f}€ ({ctx.user_data.get('discount_code', 'بدون کد')})",
                f"📝 یادداشت / Nota: {ctx.user_data['notes'] or 'بدون یادداشت'}"
            ]
            
            msg_lines += [f"▫️ {i['qty']}× {i['fa']}" for i in cart]
            msg_text = "\n".join(msg_lines)
            
            try:
                pdf_buffer.seek(0)
                await bot.send_document(
                    ADMIN_ID, 
                    document=pdf_buffer,
                    filename=f"Bazarino_Invoice_{order_id}.pdf",
                    caption=msg_text
                )
                log.info(f"ادمین برای سفارش {order_id} مطلع شد")
            except Exception as e:
                log.error(f"خطا در اطلاع‌رسانی به ادمین برای سفارش {order_id}: {e}")
        
        # پاکسازی سبدهای رها شده
        try:
            await asyncio.to_thread(abandoned_cart_ws.clear)
        except Exception as e:
            log.error(f"خطا در پاکسازی سبدهای رها شده: {e}")
        
        ctx.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        log.error(f"خطا در تأیید سفارش: {e}")
        await update.message.reply_text("❗️ خطا در ثبت سفارش. لطفاً دوباره امتحان کنید.")
        ctx.user_data.clear()
        return ConversationHandler.END

async def cancel_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """لغو سفارش"""
    try:
        ctx.user_data.clear()
        await update.message.reply_text(m("ORDER_CANCELLED"), reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    except Exception as e:
        log.error(f"خطا در لغو سفارش: {e}")
        await update.message.reply_text("❗️ خطا در لغو سفارش. لطفاً دوباره امتحان کنید.")
        return ConversationHandler.END

# ───────────── آپلود عکس
async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """مدیریت آپلود عکس برای پشتیبانی"""
    try:
        if not ctx.user_data.get("awaiting_photo"):
            return
        
        photo = update.message.photo[-1]
        if photo.file_size > 2 * 1024 * 1024:  # حداکثر 2 مگابایت
            await update.message.reply_text(
                m("ERROR_FILE_SIZE"), 
                reply_markup=await kb_main(ctx)
            )
            ctx.user_data["awaiting_photo"] = False
            return
        
        file = await photo.get_file()
        
        try:
            await asyncio.to_thread(
                uploads_ws.append_row,
                [
                    dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    update.effective_user.id,
                    f"@{update.effective_user.username or '-'}",
                    file.file_id
                ]
            )
            
            await bot.send_photo(
                ADMIN_ID,
                file.file_id,
                caption=(
                    f"تصویر از کاربر @{update.effective_user.username or update.effective_user.id}\n"
                    f"📝 توضیح: {ctx.user_data.get('photo_note', 'بدون توضیح')}"
                )
            )
            
            await update.message.reply_text(m("PHOTO_UPLOADED"))
            ctx.user_data["awaiting_photo"] = False
            ctx.user_data["photo_note"] = ""
            
            await update.message.reply_text(
                m("SUPPORT_MESSAGE"), 
                reply_markup=await kb_main(ctx)
            )
        except Exception as e:
            log.error(f"خطا در آپلود عکس: {e}")
            await update.message.reply_text(
                m("ERROR_UPLOAD"), 
                reply_markup=await kb_main(ctx)
            )
    except Exception as e:
        log.error(f"خطا در مدیریت آپلود عکس: {e}")
        await update.message.reply_text("❗️ خطا در آپلود تصویر. لطفاً دوباره امتحان کنید.")
        ctx.user_data["awaiting_photo"] = False

# ───────────── نوتیفیکیشن وضعیت سفارش
async def check_order_status(context: ContextTypes.DEFAULT_TYPE):
    """بررسی وضعیت سفارش‌ها برای ارسال نوتیفیکیشن"""
    try:
        last_checked_row = getattr(check_order_status, "_last_checked_row", 1)
        
        shipped_cells = await asyncio.to_thread(orders_ws.findall, "shipped")
        preparing_cells = await asyncio.to_thread(orders_ws.findall, "preparing")
        
        for cell in shipped_cells + preparing_cells:
            if cell.row <= last_checked_row:
                continue
            
            row_data = await asyncio.to_thread(orders_ws.row_values, cell.row)
            
            if len(row_data) < 18 or row_data[17] == "TRUE":  # قبلاً اطلاع‌رسانی شده
                continue
            
            user_id = int(row_data[2])  # user_id
            order_id = row_data[1]      # order_id
            status = row_data[16]       # status
            
            msg = {
                "preparing": (
                    f"📦 سفارش شما (#{order_id}) در حال آماده‌سازی است!\n"
                    f"Il tuo ordine (#{order_id}) è in preparazione!"
                ),
                "shipped": (
                    f"🚚 سفارش شما (#{order_id}) ارسال شد!\n"
                    f"Il tuo ordine (#{order_id}) è stato spedito!"
                )
            }[status]
            
            await context.bot.send_message(
                user_id, 
                msg, 
                reply_markup=await kb_main(context)
            )
            
            await asyncio.to_thread(orders_ws.update_cell, cell.row, 18, "TRUE")
            log.info(f"نوتیفیکیشن {status} برای سفارش {order_id} به کاربر {user_id} ارسال شد")
        
        check_order_status._last_checked_row = max(
            last_checked_row, 
            max((c.row for c in shipped_cells + preparing_cells), default=1)
        )
    except Exception as e:
        log.error(f"خطا در بررسی وضعیت سفارش‌ها: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در بررسی وضعیت سفارشات: {e}")

# ───────────── پشتیبان‌گیری از Google Sheets
async def backup_sheets(context: ContextTypes.DEFAULT_TYPE):
    """پشتیبان‌گیری هفتگی از ورق‌ها"""
    try:
        sheets = [orders_ws, products_ws, discounts_ws, abandoned_cart_ws, uploads_ws]
        
        for sheet in sheets:
            records = await asyncio.to_thread(sheet.get_all_values)
            csv_content = "\n".join([",".join(row) for row in records])
            csv_file = io.BytesIO(csv_content.encode("utf-8"))
            
            date_str = dt.datetime.utcnow().strftime('%Y%m%d')
            csv_file.name = f"{sheet.title}_backup_{date_str}.csv"
            
            await context.bot.send_document(
                ADMIN_ID, 
                document=csv_file, 
                caption=f"📊 بکاپ {sheet.title} - {date_str}"
            )
            log.info(f"پشتیبان برای {sheet.title} ارسال شد")
    except Exception as e:
        log.error(f"خطا در ایجاد پشتیبان: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در ایجاد بکاپ: {e}")

# ───────────── یادآوری سبدهای رها شده
async def send_cart_reminder(context: ContextTypes.DEFAULT_TYPE):
    """ارسال یادآوری برای سبدهای خرید رها شده"""
    try:
        records = await asyncio.to_thread(abandoned_cart_ws.get_all_records)
        
        for record in records:
            cart = json.loads(record["cart"])
            user_id = int(record["user_id"])
            
            if cart:
                await context.bot.send_message(
                    user_id,
                    (
                        f"🛒 سبد خرید شما هنوز منتظر شماست!\n"
                        f"Hai lasciato qualcosa nel carrello!\n"
                        f"{fmt_cart(cart)}\n"
                        f"👉 برای تکمیل سفارش: /start"
                    ),
                    reply_markup=await kb_main(context)
                )
        
        await asyncio.to_thread(abandoned_cart_ws.clear)
    except Exception as e:
        log.error(f"خطا در ارسال یادآور سبد خرید: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در ارسال یادآور سبد خرید: {e}")

# ───────────── دستور /search
from difflib import get_close_matches

async def cmd_search(u, ctx: ContextTypes.DEFAULT_TYPE):
    """جستجوی محصولات"""
    try:
        query = " ".join(ctx.args).lower()
        
        if not query:
            await u.message.reply_text(m("SEARCH_USAGE"))
            return
        
        products = await get_products()
        hits = []
        
        for pid, p in products.items():
            search_text = f"{p['fa'].lower()} {p['it'].lower()}"
            
            if (query in p['fa'].lower() or 
                query in p['it'].lower() or
                get_close_matches(query, [search_text], cutoff=0.6)):
                hits.append((pid, p))
        
        if not hits:
            await u.message.reply_text(m("SEARCH_NONE"))
            return
        
        for pid, p in hits[:5]:  # حداکثر 5 نتیجه
            caption = (
                f"{p['fa']} / {p['it']}\n"
                f"{p['desc']}\n"
                f"{p['price']}€\n"
                f"موجودی / Stock: {p['stock']}"
            )
            
            button = InlineKeyboardMarkup.from_button(
                InlineKeyboardButton(
                    m("CART_ADDED").split("\n")[0], 
                    callback_data=f"add_{pid}"
                )
            )
            
            if p["image_url"] and p["image_url"].strip():
                await u.message.reply_photo(
                    p["image_url"], 
                    caption=caption, 
                    reply_markup=button
                )
            else:
                await u.message.reply_text(
                    caption, 
                    reply_markup=button
                )
    except Exception as e:
        log.error(f"خطا در جستجو: {e}")
        await u.message.reply_text("❗️ خطا در جستجو. لطفاً دوباره امتحان کنید.")
        
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در /search: {e}")

# ───────────── دستورات
async def cmd_start(u, ctx: ContextTypes.DEFAULT_TYPE):
    """دستور شروع /start"""
    try:
        ctx.user_data["user_id"] = u.effective_user.id
        await u.message.reply_html(
            m("WELCOME"), 
            reply_markup=await kb_main(ctx)
        )
    except Exception as e:
        log.error(f"خطا در دستور /start: {e}")
        await u.message.reply_text(
            "❗️ خطایی در بارگذاری منو رخ داد. لطفاً بعداً امتحان کنید یا با پشتیبانی تماس بگیرید.\n"
            "Errore nel caricamento del menu. Riprova più tardi o contatta il supporto."
        )
        
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در /start: {e}")
        raise

async def cmd_about(u, ctx: ContextTypes.DEFAULT_TYPE):
    """دستور درباره ما /about"""
    try:
        await u.message.reply_text(
            m("ABOUT_US"), 
            disable_web_page_preview=True
        )
    except Exception as e:
        log.error(f"خطا در دستور /about: {e}")
        await u.message.reply_text("❗️ خطا در نمایش اطلاعات. لطفاً دوباره امتحان کنید.")

async def cmd_privacy(u, ctx: ContextTypes.DEFAULT_TYPE):
    """دستور حریم خصوصی /privacy"""
    try:
        await u.message.reply_text(
            m("PRIVACY"), 
            disable_web_page_preview=True
        )
    except Exception as e:
        log.error(f"خطا در دستور /privacy: {e}")
        await u.message.reply_text("❗️ خطا در نمایش سیاست حریم خصوصی. لطفاً دوباره امتحان کنید.")

# ───────────── راه‌اندازی برنامه
async def post_init(app: Application):
    """عملیات پس از راه‌اندازی"""
    try:
        log.info("برنامه راه‌اندازی شد")
        webhook_url = f"{BASE_URL}/webhook/{WEBHOOK_SECRET}"
        await app.bot.set_webhook(webhook_url)
        log.info(f"وب‌هوک تنظیم شد: {webhook_url}")
    except Exception as e:
        log.error(f"خطا در تنظیم وب‌هوک: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در تنظیم Webhook: {e}")
        raise

async def post_shutdown(app: Application):
    """عملیات پیش از خاموشی"""
    log.info("برنامه در حال خاموش شدن است")
    try:
        await app.bot.delete_webhook()
    except Exception as e:
        log.error(f"خطا در حذف وب‌هوک: {e}")

async def lifespan(app: FastAPI):
    """چرخه حیات برنامه FastAPI"""
    global tg_app, bot
    try:
        builder = (
            ApplicationBuilder()
            .token(TOKEN)
            .post_init(post_init)
            .post_shutdown(post_shutdown)
        )
        
        tg_app = builder.build()
        bot = tg_app.bot
        
        # راه‌اندازی اولیه
        await tg_app.initialize()
        
        # راه‌اندازی صف مشاغل
        if not tg_app.job_queue:
            tg_app.job_queue = JobQueue()
            await tg_app.job_queue.start()
        
        job_queue = tg_app.job_queue
        
        # زمان‌بندی مشاغل
        job_queue.run_daily(send_cart_reminder, time=dt.time(hour=18, minute=0))  # ساعت 18 هر روز
        job_queue.run_repeating(check_order_status, interval=600)  # هر 10 دقیقه
        job_queue.run_daily(backup_sheets, time=dt.time(hour=0, minute=0))  # نیمه شب
        
        # ثبت هندلرها
        tg_app.add_handler(CommandHandler("start", cmd_start))
        tg_app.add_handler(CommandHandler("search", cmd_search))
        tg_app.add_handler(CommandHandler("about", cmd_about))
        tg_app.add_handler(CommandHandler("privacy", cmd_privacy))
        tg_app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
        
        # هندلر مکالمه سفارش
        order_conv_handler = ConversationHandler(
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
        )
        tg_app.add_handler(order_conv_handler)
        
        # هندلر کلیک‌های اینلاین
        tg_app.add_handler(CallbackQueryHandler(router))
        
        yield
        
        # خاموش کردن برنامه
        await tg_app.shutdown()
        
    except Exception as e:
        log.error(f"خطا در چرخه حیات برنامه: {e}")
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در راه‌اندازی برنامه: {e}")
        raise

app = FastAPI(lifespan=lifespan)

@app.post("/webhook/{secret}")
async def webhook_handler(req: Request, secret: str):
    """مدیریت درخواست‌های وب‌هوک"""
    try:
        if secret != WEBHOOK_SECRET:
            log.error("توکن وب‌هوک نامعتبر")
            raise HTTPException(status_code=403, detail="Invalid secret")
        
        data = await req.json()
        update = Update.de_json(data, bot)
        
        if not update:
            log.error("درخواست وب‌هوک نامعتبر")
            raise HTTPException(status_code=400, detail="Invalid update")
        
        await tg_app.process_update(update)
        return {"ok": True}
    
    except Exception as e:
        log.error(f"خطای وب‌هوک: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """مسیریابی کلیک‌های اینلاین"""
    try:
        q = update.callback_query
        data = q.data
        await q.answer()

        if data == "back":
            await safe_edit(
                q, 
                m("WELCOME"), 
                reply_markup=await kb_main(ctx), 
                parse_mode="HTML"
            )
            return

        if data == "support":
            await safe_edit(
                q, 
                m("SUPPORT_MESSAGE"), 
                reply_markup=kb_support(), 
                parse_mode="HTML"
            )
            return

        if data == "upload_photo":
            ctx.user_data["awaiting_photo"] = True
            await safe_edit(
                q, 
                m("UPLOAD_PHOTO"), 
                reply_markup=kb_support()
            )
            return

        if data == "bestsellers":
            bestsellers = [
                (pid, p) 
                for pid, p in (await get_products()).items() 
                if p.get("is_bestseller", False)
            ]
            
            if not bestsellers:
                await safe_edit(
                    q, 
                    "🔥 در حال حاضر محصول پرفروشی وجود ندارد.\nNessun prodotto più venduto al momento.", 
                    reply_markup=await kb_main(ctx), 
                    parse_mode="HTML"
                )
                return
            
            rows = [
                [InlineKeyboardButton(f"{p['fa']} / {p['it']}", callback_data=f"show_{pid}")] 
                for pid, p in bestsellers
            ]
            rows.append([InlineKeyboardButton(m("BTN_BACK"), callback_data="back")])
            
            await safe_edit(
                q, 
                "🔥 محصولات پرفروش / Più venduti", 
                reply_markup=InlineKeyboardMarkup(rows), 
                parse_mode="HTML"
            )
            return

        if data == "search":
            await safe_edit(
                q, 
                m("SEARCH_USAGE"), 
                reply_markup=await kb_main(ctx)
            )
            return

        if data.startswith("cat_"):
            cat = data[4:]
            await safe_edit(
                q, 
                EMOJI.get(cat, cat), 
                reply_markup=await kb_category(cat, ctx), 
                parse_mode="HTML"
            )
            return

        if data.startswith("show_"):
            pid = data[5:]
            p = (await get_products())[pid]
            caption = (
                f"<b>{p['fa']} / {p['it']}</b>\n"
                f"{p['desc']}\n"
                f"{p['price']}€ / {p['weight']}\n"
                f"||موجودی / Stock:|| {p['stock']}"
            )
            
            try:
                await q.message.delete()
            except Exception as e:
                log.error(f"خطا در حذف پیام قبلی: {e}")
            
            if p["image_url"] and p["image_url"].strip():
                await ctx.bot.send_photo(
                    chat_id=q.message.chat.id,
                    photo=p["image_url"],
                    caption=caption,
                    reply_markup=kb_product(pid),
                    parse_mode="HTML"
                )
            else:
                await ctx.bot.send_message(
                    chat_id=q.message.chat.id,
                    text=caption,
                    reply_markup=kb_product(pid),
                    parse_mode="HTML"
                )
            return

        if data.startswith("add_"):
            pid = data[4:]
            ok, msg = await add_cart(ctx, pid, qty=1, update=update)
            await q.answer(msg, show_alert=not ok)
            
            cat = (await get_products())[pid]["cat"]
            await safe_edit(
                q, 
                EMOJI.get(cat, cat), 
                reply_markup=await kb_category(cat, ctx), 
                parse_mode="HTML"
            )
            return

        if data.startswith("back_cat_"):
            cat = data.split("_")[2]
            await safe_edit(
                q, 
                EMOJI.get(cat, cat), 
                reply_markup=await kb_category(cat, ctx), 
                parse_mode="HTML"
            )
            return

        if data == "cart":
            cart = ctx.user_data.get("cart", [])
            await safe_edit(
                q, 
                f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}", 
                reply_markup=kb_cart(cart), 
                parse_mode="HTML"
            )
            return

        if data.startswith(("inc_", "dec_", "del_")):
            pid = data.split("_")[1]
            cart = ctx.user_data.get("cart", [])
            item = next((i for i in cart if i["id"] == pid), None)
            
            if not item:
                return
            
            if data.startswith("inc_"):
                await add_cart(ctx, pid, 1, update=update)
            elif data.startswith("dec_"):
                item["qty"] = max(1, item["qty"] - 1)
            else:
                cart.remove(item)
            
            try:
                user_id = ctx.user_data.get("user_id", update.effective_user.id)
                await asyncio.to_thread(
                    abandoned_cart_ws.append_row,
                    [
                        dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        user_id,
                        json.dumps(cart)
                    ]
                )
            except Exception as e:
                log.error(f"خطا در ذخیره سبد رها شده: {e}")
            
            await safe_edit(
                q, 
                f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}", 
                reply_markup=kb_cart(cart), 
                parse_mode="HTML"
            )
            return

        if data in ["order_perugia", "order_italy"]:
            ctx.user_data["dest"] = "Perugia" if data == "order_perugia" else "Italy"
            cart = ctx.user_data.get("cart", [])
            
            await safe_edit(
                q, 
                f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}", 
                reply_markup=kb_cart(cart), 
                parse_mode="HTML"
            )
            return

        if data == "checkout":
            return await start_order(update, ctx)
            
    except Exception as e:
        log.error(f"خطا در مسیریابی: {e}")
        await q.message.reply_text("❗️ خطا در پردازش درخواست. لطفاً دوباره امتحان کنید.")
        
        if ADMIN_ID and bot:
            await bot.send_message(ADMIN_ID, f"⚠️ خطا در router: {e}")

def main():
    """تابع اصلی اجرای برنامه"""
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()