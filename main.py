#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛒 Bazarino Telegram Bot - Ultimate Edition v3.0
Author: Bazarino Team
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
from typing import Dict, Any, List, Optional, Tuple, Union
import io
import random
import time
import re
import hashlib
from functools import wraps
from collections import defaultdict
from difflib import get_close_matches
from dataclasses import dataclass, field
from enum import Enum
import traceback

# Third-party imports
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, Update, BotCommand
)
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters, PicklePersistence
)
from telegram.error import BadRequest
from telegram.constants import ParseMode

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler("bazarino.log", maxBytes=5*1024*1024, backupCount=3)
        ]
    )
    # Reduce noise from libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    return logging.getLogger("bazarino")

log = setup_logging()

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

class OrderStatus(Enum):
    PENDING = "pending"
    PREPARING = "preparing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

class PaymentMethod(Enum):
    CASH = "cash"
    CARD = "card"
    PAYPAL = "paypal"
    TRANSFER = "transfer"

class UserTier(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"

TIER_THRESHOLDS = {
    UserTier.BRONZE: 0,
    UserTier.SILVER: 100,
    UserTier.GOLD: 300,
    UserTier.PLATINUM: 500
}

POINTS_PER_EUR = 10
REFERRAL_BONUS_REFERRER = 100
REFERRAL_BONUS_REFEREE = 50

# Load Env Vars
TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
BASE_URL = os.getenv("BASE_URL")
SPREADSHEET = os.getenv("SPREADSHEET_NAME", "Bazarino")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS", "/etc/secrets/credentials.json")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "my-secret-token")
PORT = int(os.getenv("PORT", 8000))
LOW_STOCK_TH = int(os.getenv("LOW_STOCK_THRESHOLD", 3))
MAX_PHOTO_SIZE = int(os.getenv("MAX_PHOTO_SIZE_MB", 2)) * 1024 * 1024
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", 60))

# Global Objects
tg_app = None
bot = None

# Load Config Files
def load_yaml(filename):
    try:
        with open(filename, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        log.error(f"Error loading {filename}: {e}")
        return {}

def load_json(filename):
    try:
        with open(filename, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Error loading {filename}: {e}")
        return {}

CONFIG = load_yaml("config.yaml")
MSG = load_json("messages.json")
CATEGORIES = CONFIG.get("categories", {})
HAFEZ_QUOTES = CONFIG.get("hafez_quotes", [])

def m(key: str, **kwargs) -> str:
    text = MSG.get(key, f"[{key}]")
    if kwargs:
        try: return text.format(**kwargs)
        except: return text
    return text

# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Product:
    id: str
    cat: str
    fa: str
    it: str
    brand: str
    desc: str
    weight: str
    price: float
    image_url: Optional[str]
    stock: int
    is_bestseller: bool
    is_new: bool
    is_featured: bool
    discount_percent: float
    rating: float
    rating_count: int
    
    @property
    def final_price(self) -> float:
        if self.discount_percent > 0:
            return round(self.price * (1 - self.discount_percent / 100), 2)
        return self.price
    
    @property
    def in_stock(self) -> bool:
        return self.stock > 0

@dataclass
class CartItem:
    id: str
    fa: str
    it: str
    price: float
    qty: int
    weight: str = ""
    original_price: float = 0.0
    
    @property
    def subtotal(self) -> float:
        return round(self.qty * self.price, 2)

@dataclass
class User:
    user_id: int
    username: str
    first_name: str
    last_name: str = ""
    phone: str = ""
    address: str = ""
    postal: str = ""
    city: str = ""
    points: int = 0
    tier: UserTier = UserTier.BRONZE
    total_spent: float = 0
    total_orders: int = 0
    referral_code: str = ""
    referred_by: Optional[int] = None
    referral_count: int = 0
    
    def __post_init__(self):
        if not self.referral_code:
            hash_input = f"{self.user_id}{time.time()}"
            self.referral_code = hashlib.md5(hash_input.encode()).hexdigest()[:8].upper()

    def get_discount_percent(self) -> float:
        discounts = {UserTier.BRONZE: 0, UserTier.SILVER: 3, UserTier.GOLD: 5, UserTier.PLATINUM: 10}
        return discounts.get(self.tier, 0)
    
    def update_tier(self):
        for tier in reversed(list(UserTier)):
            if self.total_spent >= TIER_THRESHOLDS[tier]:
                self.tier = tier
                break

@dataclass
class Order:
    order_id: str
    user_id: int
    name: str
    phone: str
    address: str
    postal: str
    city: str
    dest: str
    items: List[CartItem]
    subtotal: float
    shipping: float
    discount_amount: float
    tier_discount: float
    total: float
    notes: str
    payment_method: PaymentMethod
    status: OrderStatus = OrderStatus.PENDING
    tracking_code: str = ""
    created_at: str = ""
    
    def __post_init__(self):
        if not self.tracking_code:
            self.tracking_code = f"BZ{self.order_id}{random.randint(100,999)}"
        if not self.created_at:
            self.created_at = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# ═══════════════════════════════════════════════════════════════════════════════
# UTILS: VALIDATOR & RATE LIMITER
# ═══════════════════════════════════════════════════════════════════════════════

class Validator:
    @staticmethod
    def sanitize(text: str) -> str:
        if not text: return ""
        return re.sub(r'<[^>]+>', '', text).strip()[:500]
    
    @staticmethod
    def phone(phone: str) -> Tuple[bool, str]:
        phone = re.sub(r'[\s\-\.]', '', phone)
        if re.match(r'^(\+39|0039|3)?[0-9]{9,10}$', phone): return True, phone
        if re.match(r'^(\+98|0098|09)?[0-9]{9,10}$', phone): return True, phone
        return False, ""

class RateLimiter:
    requests = defaultdict(list)
    LIMITS = {"default": (30, 60), "order": (5, 300)}
    
    @classmethod
    def check(cls, user_id: int, action="default") -> bool:
        limit, period = cls.LIMITS.get(action, cls.LIMITS["default"])
        now = time.time()
        cls.requests[f"{user_id}:{action}"] = [t for t in cls.requests[f"{user_id}:{action}"] if now - t < period]
        if len(cls.requests[f"{user_id}:{action}"]) >= limit: return False
        cls.requests[f"{user_id}:{action}"].append(now)
        return True

def rate_limit(action="default"):
    def decorator(func):
        @wraps(func)
        async def wrapper(update, ctx, *args, **kwargs):
            uid = update.effective_user.id if update.effective_user else 0
            if not RateLimiter.check(uid, action):
                if update.callback_query: await update.callback_query.answer("⏳ لطفاً صبر کنید...", show_alert=True)
                return
            return await func(update, ctx, *args, **kwargs)
        return wrapper
    return decorator
    # ═══════════════════════════════════════════════════════════════════════════════
# CORE LOGIC: SHEETS, INVOICE, CART
# ═══════════════════════════════════════════════════════════════════════════════

class GoogleSheetsClient:
    def __init__(self):
        self.gc = None
        self.worksheets = {}
        self._products_cache = None
        self._products_ts = 0
        self._users_cache = {}

    async def connect(self):
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
            self.gc = gspread.authorize(creds)
            wb = self.gc.open(SPREADSHEET)
            
            # Init sheets
            sheets_map = {"products": "Products", "orders": "Orders", "users": "Users", "uploads": "Uploads"}
            for key, name in sheets_map.items():
                try: self.worksheets[key] = wb.worksheet(name)
                except: 
                    self.worksheets[key] = wb.add_worksheet(name, 1000, 20)
                    log.info(f"Created sheet {name}")
            log.info("Google Sheets Connected")
        except Exception as e:
            log.error(f"Sheets connection failed: {e}")

    async def get_products(self, force=False) -> Dict[str, Product]:
        now = time.time()
        if not force and self._products_cache and now - self._products_ts < CACHE_TTL:
            return self._products_cache
        
        try:
            records = await asyncio.to_thread(self.worksheets["products"].get_all_records)
            products = {}
            for r in records:
                pid = str(r.get("id", ""))
                if not pid: continue
                products[pid] = Product(
                    id=pid, cat=str(r.get("cat", "")), fa=str(r.get("fa", "")), it=str(r.get("it", "")),
                    brand=str(r.get("brand", "")), desc=str(r.get("description", "")), 
                    weight=str(r.get("weight", "")), price=float(r.get("price", 0) or 0),
                    image_url=r.get("image_url") or None, stock=int(r.get("stock", 0) or 0),
                    is_bestseller=str(r.get("is_bestseller")).lower() == "true",
                    is_new=str(r.get("is_new")).lower() == "true",
                    is_featured=str(r.get("is_featured")).lower() == "true",
                    discount_percent=float(r.get("discount_percent", 0) or 0),
                    rating=float(r.get("rating", 0) or 0),
                    rating_count=int(r.get("rating_count", 0) or 0)
                )
            self._products_cache = products
            self._products_ts = now
            return products
        except Exception as e:
            log.error(f"Error fetching products: {e}")
            return self._products_cache or {}

    async def get_user(self, user_id: int) -> User:
        if user_id in self._users_cache: return self._users_cache[user_id]
        try:
            records = await asyncio.to_thread(self.worksheets["users"].get_all_records)
            for r in records:
                if int(r.get("user_id", 0) or 0) == user_id:
                    u = User(
                        user_id=user_id, username=r.get("username", ""), first_name=r.get("first_name", ""),
                        last_name=r.get("last_name", ""), phone=str(r.get("phone", "")),
                        address=str(r.get("address", "")), city=str(r.get("city", "")), postal=str(r.get("postal", "")),
                        points=int(r.get("points", 0) or 0), tier=UserTier(r.get("tier", "bronze")),
                        total_spent=float(r.get("total_spent", 0) or 0), total_orders=int(r.get("total_orders", 0) or 0),
                        referral_code=str(r.get("referral_code", "")), referral_count=int(r.get("referral_count", 0) or 0)
                    )
                    self._users_cache[user_id] = u
                    return u
        except Exception as e: log.error(f"Get user error: {e}")
        return None

    async def save_user(self, user: User, update=False):
        try:
            row = [user.user_id, user.username, user.first_name, user.last_name, user.phone, 
                   user.address, user.postal, user.city, user.points, user.tier.value, 
                   user.total_spent, user.total_orders, user.referral_code, "", user.referral_count]
            if update:
                # Basic implementation: re-reading to find row index (simplified for brevity)
                cell = await asyncio.to_thread(self.worksheets["users"].find, str(user.user_id))
                if cell:
                    # Only update specific cols if needed, here we simplify
                    pass 
            else:
                await asyncio.to_thread(self.worksheets["users"].append_row, row)
            self._users_cache[user.user_id] = user
        except Exception as e: log.error(f"Save user error: {e}")

    async def update_stock(self, pid: str, qty_delta: int):
        try:
            records = await asyncio.to_thread(self.worksheets["products"].get_all_records)
            for i, r in enumerate(records):
                if str(r.get("id")) == pid:
                    new_stock = max(0, int(r.get("stock", 0)) + qty_delta)
                    await asyncio.to_thread(self.worksheets["products"].update_cell, i+2, 10, new_stock)
                    if self._products_cache and pid in self._products_cache:
                        self._products_cache[pid].stock = new_stock
                    return True
            return False
        except Exception as e: 
            log.error(f"Update stock error: {e}")
            return False

    async def save_order(self, order: Order):
        try:
            rows = []
            for item in order.items:
                rows.append([
                    order.created_at, order.order_id, order.user_id, order.name, order.phone,
                    order.address, order.dest, item.id, item.fa, item.qty, item.price, 
                    item.subtotal, order.shipping, order.discount_amount, order.tier_discount,
                    order.total, order.notes, order.status.value, order.tracking_code
                ])
            await asyncio.to_thread(self.worksheets["orders"].append_rows, rows)
            return True
        except Exception as e:
            log.error(f"Save order error: {e}")
            return False

sheets = GoogleSheetsClient()

class InvoiceGenerator:
    @staticmethod
    async def generate(order: Order, user: User) -> io.BytesIO:
        # Simple invoice generation using Pillow (Placeholder for complex logic)
        try:
            from PIL import Image, ImageDraw, ImageFont
            img = Image.new('RGB', (800, 1000), color=(255, 255, 255))
            d = ImageDraw.Draw(img)
            # You would load fonts here. Using default for safety if fonts missing.
            try: font = ImageFont.truetype("fonts/Vazir.ttf", 20)
            except: font = ImageFont.load_default()
            
            y = 50
            d.text((50, y), f"Order Invoice #{order.order_id}", fill=(0,0,0), font=font)
            y += 40
            d.text((50, y), f"Customer: {order.name}", fill=(0,0,0), font=font)
            y += 40
            d.text((50, y), f"Date: {order.created_at}", fill=(0,0,0), font=font)
            y += 60
            
            for item in order.items:
                d.text((50, y), f"{item.fa} x {item.qty} = {item.subtotal} EUR", fill=(0,0,0), font=font)
                y += 30
            
            y += 20
            d.text((50, y), f"Total: {order.total} EUR", fill=(0,100,0), font=font)
            
            buf = io.BytesIO()
            img.save(buf, format='PNG')
            buf.seek(0)
            return buf
        except Exception as e:
            log.error(f"Invoice error: {e}")
            return None

class CartManager:
    @staticmethod
    def get_cart(ctx) -> List[Dict]:
        return ctx.user_data.get("cart", [])
    
    @staticmethod
    def set_cart(ctx, cart):
        ctx.user_data["cart"] = cart

    @staticmethod
    async def add(ctx, pid: str, qty: int = 1):
        products = await sheets.get_products()
        if pid not in products: return False, "کالا یافت نشد"
        p = products[pid]
        if p.stock < qty: return False, "موجودی کافی نیست"
        
        cart = CartManager.get_cart(ctx)
        found = False
        for item in cart:
            if item["id"] == pid:
                if p.stock < item["qty"] + qty: return False, "موجودی کافی نیست"
                item["qty"] += qty
                found = True
                break
        if not found:
            cart.append({
                "id": pid, "fa": p.fa, "it": p.it, "price": p.final_price, 
                "original_price": p.price, "weight": p.weight, "qty": qty
            })
        
        CartManager.set_cart(ctx, cart)
        return True, "به سبد اضافه شد"
    
    @staticmethod
    def total(cart):
        return sum(i["qty"] * i["price"] for i in cart)
    
    @staticmethod
    def format(cart):
        if not cart: return m("CART_EMPTY")
        lines = ["🛍 <b>سبد خرید</b>\n"]
        total = 0
        for i in cart:
            sub = i["qty"] * i["price"]
            total += sub
            lines.append(f"▫️ {i['qty']}x {i['fa']} | {i['price']}€ = <b>{sub:.2f}€</b>")
        lines.append(f"\n💶 <b>جمع کل: {total:.2f}€</b>")
        return "\n".join(lines)
        # ═══════════════════════════════════════════════════════════════════════════════
# HANDLERS & UI
# ═══════════════════════════════════════════════════════════════════════════════

STATE_NAME, STATE_PHONE, STATE_ADDRESS, STATE_POSTAL, STATE_CITY, STATE_CONFIRM = range(6)

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await sheets.get_user(user.id)
    if not db_user:
        db_user = User(user_id=user.id, username=user.username or "", first_name=user.first_name)
        await sheets.save_user(db_user)
    
    products = await sheets.get_products()
    cats = sorted(list(set(p.cat for p in products.values() if p.cat)))
    
    keyboard = []
    row = []
    for cat in cats:
        info = CATEGORIES.get(cat, {})
        text = f"{info.get('emoji', '📦')} {info.get('name_fa', cat)}"
        row.append(InlineKeyboardButton(text, callback_data=f"cat_{cat}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row: keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🛒 سبد خرید", callback_data="cart")])
    keyboard.append([InlineKeyboardButton("👤 پروفایل من", callback_data="profile")])
    
    await update.message.reply_html(m("WELCOME", name=user.first_name), reply_markup=InlineKeyboardMarkup(keyboard))

async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    
    if data.startswith("cat_"):
        cat = data.split("_")[1]
        products = await sheets.get_products()
        items = [p for p in products.values() if p.cat == cat and p.in_stock]
        
        kb = []
        for p in items:
            kb.append([InlineKeyboardButton(f"{p.fa} - {p.final_price}€", callback_data=f"show_{p.id}")])
        kb.append([InlineKeyboardButton("🔙 برگشت", callback_data="back")])
        await safe_edit(q, f"📂 محصولات {cat}:", reply_markup=InlineKeyboardMarkup(kb))
        
    elif data.startswith("show_"):
        pid = data.split("_")[1]
        products = await sheets.get_products()
        p = products.get(pid)
        if not p: return
        
        caption = f"<b>{p.fa}</b>\n{p.desc}\n\n💰 قیمت: {p.final_price}€"
        kb = [[InlineKeyboardButton("➕ افزودن به سبد", callback_data=f"add_{pid}")],
              [InlineKeyboardButton("🔙 برگشت", callback_data=f"cat_{p.cat}")]]
        
        if p.image_url:
            try: await q.message.reply_photo(p.image_url, caption=caption, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
            except: await q.message.reply_html(caption, reply_markup=InlineKeyboardMarkup(kb))
        else:
            await safe_edit(q, caption, reply_markup=InlineKeyboardMarkup(kb))
            
    elif data.startswith("add_"):
        pid = data.split("_")[1]
        ok, msg = await CartManager.add(ctx, pid)
        await q.answer(msg, show_alert=True)
        
    elif data == "cart":
        cart = CartManager.get_cart(ctx)
        text = CartManager.format(cart)
        kb = []
        if cart:
            kb.append([InlineKeyboardButton("✅ ثبت سفارش", callback_data="checkout")])
            kb.append([InlineKeyboardButton("🗑 خالی کردن", callback_data="clear_cart")])
        kb.append([InlineKeyboardButton("🔙 منوی اصلی", callback_data="back")])
        await safe_edit(q, text, reply_markup=InlineKeyboardMarkup(kb))
        
    elif data == "clear_cart":
        CartManager.set_cart(ctx, [])
        await safe_edit(q, m("CART_EMPTY"), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 برگشت", callback_data="back")]]))

    elif data == "back":
        await start(update, ctx)
        
    elif data == "profile":
        u = await sheets.get_user(update.effective_user.id)
        if u:
            txt = f"👤 <b>پروفایل شما</b>\n\n💰 امتیاز: {u.points}\n🎖 سطح: {u.tier.value}\n📦 سفارشات: {u.total_orders}"
            await safe_edit(q, txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="back")]]))

async def safe_edit(q, text, reply_markup):
    try:
        if q.message.caption: await q.edit_message_caption(caption=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else: await q.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        await q.message.reply_html(text, reply_markup=reply_markup)

# ═══════════════════════════════════════════════════════════════════════════════
# ORDER CONVERSATION
# ═══════════════════════════════════════════════════════════════════════════════

async def start_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not CartManager.get_cart(ctx):
        await q.message.reply_text("سبد خالی است")
        return ConversationHandler.END
    
    await q.message.reply_html("📝 <b>ثبت سفارش</b>\n\nلطفاً نام و نام خانوادگی خود را وارد کنید:")
    return STATE_NAME

async def get_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data['order_name'] = Validator.sanitize(update.message.text)
    await update.message.reply_text("📱 لطفاً شماره تماس خود را وارد کنید:")
    return STATE_PHONE

async def get_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    valid, phone = Validator.phone(update.message.text)
    if not valid:
        await update.message.reply_text("❌ شماره نامعتبر. لطفاً مجدد وارد کنید:")
        return STATE_PHONE
    ctx.user_data['order_phone'] = phone
    await update.message.reply_text("📍 آدرس دقیق (خیابان، پلاک، واحد) را وارد کنید:")
    return STATE_ADDRESS

async def get_address(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data['order_address'] = Validator.sanitize(update.message.text)
    await update.message.reply_text("🏙 شهر خود را وارد کنید:")
    return STATE_CITY

async def get_city(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data['order_city'] = Validator.sanitize(update.message.text)
    
    cart = CartManager.get_cart(ctx)
    total = CartManager.total(cart)
    shipping = 0 if ctx.user_data['order_city'].lower() == "perugia" else (5.90 if total < 50 else 0)
    
    summary = (
        f"📋 <b>تایید نهایی سفارش</b>\n\n"
        f"👤 {ctx.user_data['order_name']}\n"
        f"📱 {ctx.user_data['order_phone']}\n"
        f"📍 {ctx.user_data['order_city']} - {ctx.user_data['order_address']}\n"
        f"────────────────\n"
        f"💶 مبلغ کالا: {total}€\n"
        f"🚚 هزینه ارسال: {shipping}€\n"
        f"💰 <b>مبلغ نهایی: {total + shipping}€</b>"
    )
    
    kb = [
        [InlineKeyboardButton("✅ تایید و ثبت نهایی", callback_data="confirm_order")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel_order")]
    ]
    await update.message.reply_html(summary, reply_markup=InlineKeyboardMarkup(kb))
    return STATE_CONFIRM

async def confirm_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    if q.data == "cancel_order":
        await safe_edit(q, "❌ سفارش لغو شد.", reply_markup=None)
        return ConversationHandler.END
    
    cart = CartManager.get_cart(ctx)
    user = await sheets.get_user(update.effective_user.id)
    if not user: user = User(update.effective_user.id, update.effective_user.username, update.effective_user.first_name)
    
    # Calculate finals
    subtotal = CartManager.total(cart)
    shipping = 0 if ctx.user_data['order_city'].lower() == "perugia" else (5.90 if subtotal < 50 else 0)
    
    order = Order(
        order_id=str(uuid.uuid4())[:8].upper(),
        user_id=user.user_id,
        name=ctx.user_data['order_name'],
        phone=ctx.user_data['order_phone'],
        address=ctx.user_data['order_address'],
        postal="", city=ctx.user_data['order_city'], dest="IT",
        items=[CartItem(id=i['id'], fa=i['fa'], it=i['it'], price=i['price'], qty=i['qty']) for i in cart],
        subtotal=subtotal, shipping=shipping, discount_amount=0, tier_discount=0,
        total=subtotal+shipping, notes="", payment_method=PaymentMethod.CASH
    )
    
    # Save & Update Logic
    await sheets.save_order(order)
    for item in cart: await sheets.update_stock(item['id'], -item['qty'])
    user.points += int(order.total * POINTS_PER_EUR)
    user.total_spent += order.total
    user.total_orders += 1
    user.update_tier()
    await sheets.save_user(user, update=True)
    
    # Generate Invoice
    inv = await InvoiceGenerator.generate(order, user)
    
    msg = f"✅ <b>سفارش #{order.order_id} با موفقیت ثبت شد!</b>\n\n🎉 شما {int(order.total * POINTS_PER_EUR)} امتیاز کسب کردید."
    if inv:
        await q.message.reply_photo(inv, caption=msg, parse_mode=ParseMode.HTML)
    else:
        await safe_edit(q, msg, reply_markup=None)
        
    if ADMIN_ID:
        admin_txt = f"🔔 سفارش جدید #{order.order_id}\n💰 مبلغ: {order.total}€\n👤 {order.name}"
        await ctx.bot.send_message(ADMIN_ID, admin_txt)
        
    CartManager.set_cart(ctx, [])
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════════════════════
# APP STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def on_startup():
    global tg_app, bot
    await sheets.connect()
    
    persistence = PicklePersistence(filepath="bazarino.pickle")
    tg_app = ApplicationBuilder().token(TOKEN).persistence(persistence).build()
    bot = tg_app.bot
    
    # Handlers
    tg_app.add_handler(CommandHandler("start", start))
    
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_order, pattern="^checkout$")],
        states={
            STATE_NAME: [MessageHandler(filters.TEXT, get_name)],
            STATE_PHONE: [MessageHandler(filters.TEXT, get_phone)],
            STATE_ADDRESS: [MessageHandler(filters.TEXT, get_address)],
            STATE_CITY: [MessageHandler(filters.TEXT, get_city)],
            STATE_CONFIRM: [CallbackQueryHandler(confirm_order)]
        },
        fallbacks=[CommandHandler("cancel", start)]
    )
    tg_app.add_handler(conv_handler)
    tg_app.add_handler(CallbackQueryHandler(button_handler))
    
    await tg_app.initialize()
    await tg_app.start()
    await bot.set_webhook(f"{BASE_URL}/webhook/{WEBHOOK_SECRET}")
    log.info("Bot Started & Webhook Set")

@app.on_event("shutdown")
async def on_shutdown():
    await tg_app.stop()
    await tg_app.shutdown()

@app.post(f"/webhook/{WEBHOOK_SECRET}")
async def webhook_handler(request: Request):
    try:
        data = await request.json()
        update = Update.de_json(data, bot)
        await tg_app.process_update(update)
        return {"ok": True}
    except Exception as e:
        log.error(f"Webhook Update Error: {e}")
        return {"ok": False}

@app.get("/")
def health(): return {"status": "ok", "version": "3.0"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
