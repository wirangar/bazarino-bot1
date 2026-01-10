#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛒 Bazarino Telegram Bot - Enhanced Version 2.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
- Webhook via FastAPI on Render with secret token
- Dynamic products from Google Sheets with versioned cache
- Invoice generation with Hafez quotes
- Discount codes & order notes
- Abandoned cart reminders
- Photo upload support
- Push notifications (preparing/shipped)
- Weekly backup
- Admin panel with statistics
- Rate limiting & input validation
- Order tracking system
- Rating system
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Optimized for Render.com with Google Sheets
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
from typing import Dict, Any, List, Optional, Tuple
import io
import random
import time
import re
from functools import wraps
from collections import defaultdict
from difflib import get_close_matches
from dataclasses import dataclass, field

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, 
    Update, BotCommand
)
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, 
    ContextTypes, ConversationHandler, MessageHandler, filters, JobQueue, 
    PicklePersistence
)
from telegram.error import BadRequest, NetworkError, TelegramError

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            "bazarino.log", 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
    ]
)
log = logging.getLogger("bazarino")

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL VARIABLES
# ═══════════════════════════════════════════════════════════════════════════════

tg_app: Optional[Application] = None
bot = None

# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Product:
    """Product data class for type safety"""
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
    is_bestseller: bool = False
    version: str = "0"
    rating: float = 0.0
    rating_count: int = 0

@dataclass
class CartItem:
    """Cart item data class"""
    id: str
    fa: str
    price: float
    weight: str
    qty: int

@dataclass
class Order:
    """Order data class"""
    order_id: str
    user_id: int
    handle: str
    name: str
    phone: str
    address: str
    dest: str
    items: List[CartItem]
    total: float
    discount: float
    discount_code: Optional[str]
    notes: str
    status: str = "preparing"
    created_at: str = ""

@dataclass 
class RateLimiter:
    """Rate limiter for preventing spam"""
    requests: Dict[int, List[float]] = field(default_factory=lambda: defaultdict(list))
    max_requests: int = 30
    time_window: int = 60  # seconds
    
    def is_allowed(self, user_id: int) -> bool:
        now = time.time()
        user_requests = self.requests[user_id]
        # Remove old requests
        user_requests[:] = [t for t in user_requests if now - t < self.time_window]
        if len(user_requests) >= self.max_requests:
            return False
        user_requests.append(now)
        return True

rate_limiter = RateLimiter()

# ═══════════════════════════════════════════════════════════════════════════════
# ENVIRONMENT VARIABLES VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_env_vars() -> Dict[str, Any]:
    """Validate and return all required environment variables"""
    required_env_vars = {
        "TELEGRAM_TOKEN": str,
        "ADMIN_CHAT_ID": int,
        "BASE_URL": str,
        "SPREADSHEET_NAME": str
    }
    
    optional_env_vars = {
        "LOW_STOCK_THRESHOLD": (int, 3),
        "WEBHOOK_SECRET": (str, "default-secret"),
        "PORT": (int, 8000),
        "GOOGLE_CREDS": (str, "/etc/secrets/bazarino-bot-credentials.json"),
        "MAX_PHOTO_SIZE_MB": (int, 2),
        "CACHE_TTL_SECONDS": (int, 60),
        "RATE_LIMIT_MAX": (int, 30),
        "RATE_LIMIT_WINDOW": (int, 60),
    }
    
    config = {}
    
    for var, var_type in required_env_vars.items():
        value = os.getenv(var)
        if not value:
            log.error(f"Missing required environment variable: {var}")
            raise SystemExit(f"❗️ متغیر محیطی {var} تنظیم نشده است.")
        try:
            config[var] = var_type(value)
        except ValueError:
            log.error(f"Invalid {var}: must be {var_type.__name__}")
            raise SystemExit(f"❗️ {var} باید از نوع {var_type.__name__} باشد.")
    
    for var, (var_type, default) in optional_env_vars.items():
        value = os.getenv(var, str(default))
        try:
            config[var] = var_type(value)
        except ValueError:
            log.warning(f"Invalid {var}, using default: {default}")
            config[var] = default
    
    return config

# Load environment configuration
ENV = validate_env_vars()

TOKEN = ENV["TELEGRAM_TOKEN"]
ADMIN_ID = ENV["ADMIN_CHAT_ID"]
BASE_URL = ENV["BASE_URL"]
SPREADSHEET = ENV["SPREADSHEET_NAME"]
LOW_STOCK_TH = ENV["LOW_STOCK_THRESHOLD"]
WEBHOOK_SECRET = ENV["WEBHOOK_SECRET"]
PORT = ENV["PORT"]
MAX_PHOTO_SIZE = ENV["MAX_PHOTO_SIZE_MB"] * 1024 * 1024
CACHE_TTL = ENV["CACHE_TTL_SECONDS"]

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_config() -> Dict[str, Any]:
    """Load and validate config.yaml"""
    try:
        with open("config.yaml", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        required_keys = ["sheets", "hafez_quotes"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key: {key}")
        
        required_sheets = ["orders", "products", "abandoned_carts", "discounts", "uploads", "ratings"]
        for sheet in required_sheets:
            if sheet not in config["sheets"] or "name" not in config["sheets"][sheet]:
                raise ValueError(f"Missing or invalid sheet config: {sheet}")
        
        return config
    except FileNotFoundError:
        log.error("config.yaml not found")
        raise SystemExit("❗️ فایل config.yaml یافت نشد.")
    except yaml.YAMLError as e:
        log.error(f"Invalid YAML in config.yaml: {e}")
        raise SystemExit(f"❗️ خطا در تجزیه config.yaml: {e}")
    except ValueError as e:
        log.error(str(e))
        raise SystemExit(f"❗️ config.yaml نامعتبر: {e}")

CONFIG = load_config()
SHEET_CONFIG = CONFIG["sheets"]
HAFEZ_QUOTES = CONFIG.get("hafez_quotes", [])

# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGES LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_messages() -> Dict[str, str]:
    """Load messages.json with validation"""
    try:
        with open("messages.json", encoding="utf-8") as f:
            messages = json.load(f)
        return messages
    except FileNotFoundError:
        log.error("messages.json not found")
        raise SystemExit("❗️ فایل messages.json یافت نشد.")
    except json.JSONDecodeError as e:
        log.error(f"Invalid messages.json: {e}")
        raise SystemExit(f"❗️ messages.json نامعتبر: {e}")

MSG = load_messages()

def m(key: str, **kwargs) -> str:
    """Get message by key with optional formatting"""
    text = MSG.get(key, f"[{key}]")
    if kwargs:
        try:
            return text.format(**kwargs)
        except KeyError:
            return text
    return text

# ═══════════════════════════════════════════════════════════════════════════════
# INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

class InputValidator:
    """Input validation utilities"""
    
    @staticmethod
    def sanitize_text(text: str, max_length: int = 500) -> str:
        """Sanitize and truncate text input"""
        if not text:
            return ""
        # Remove potentially dangerous characters
        text = re.sub(r'[<>{}]', '', text)
        return text.strip()[:max_length]
    
    @staticmethod
    def validate_phone(phone: str) -> Tuple[bool, str]:
        """Validate phone number format"""
        phone = re.sub(r'\s+', '', phone)
        # Accept Italian and Iranian phone formats
        patterns = [
            r'^(\+39|0039)?[0-9]{9,10}$',  # Italian
            r'^(\+98|0098|0)?9[0-9]{9}$',   # Iranian
            r'^[0-9]{10,15}$'               # General
        ]
        for pattern in patterns:
            if re.match(pattern, phone):
                return True, phone
        return False, "شماره تلفن نامعتبر / Numero non valido"
    
    @staticmethod
    def validate_postal_code(code: str) -> Tuple[bool, str]:
        """Validate postal code"""
        code = code.strip()
        # Italian postal code: 5 digits
        # Iranian postal code: 10 digits
        if re.match(r'^[0-9]{5}$', code) or re.match(r'^[0-9]{10}$', code):
            return True, code
        return False, "کد پستی نامعتبر / CAP non valido"
    
    @staticmethod
    def validate_name(name: str) -> Tuple[bool, str]:
        """Validate name"""
        name = InputValidator.sanitize_text(name, 100)
        if len(name) < 2:
            return False, "نام باید حداقل ۲ کاراکتر باشد / Nome troppo corto"
        if re.search(r'[0-9]', name):
            return False, "نام نباید شامل عدد باشد / Nome non può contenere numeri"
        return True, name

validator = InputValidator()

# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS SETUP
# ═══════════════════════════════════════════════════════════════════════════════

class GoogleSheetsClient:
    """Google Sheets client with retry logic and caching"""
    
    def __init__(self):
        self.gc = None
        self.workbook = None
        self.worksheets: Dict[str, gspread.Worksheet] = {}
        self._products_cache: Optional[Dict[str, Product]] = None
        self._products_version: str = ""
        self._products_ts: dt.datetime = dt.datetime.min
        self._discounts_cache: Optional[Dict[str, Dict]] = None
        self._discounts_ts: dt.datetime = dt.datetime.min
        
    async def initialize(self):
        """Initialize Google Sheets connection"""
        try:
            scope = [
                "https://spreadsheets.google.com/feeds", 
                "https://www.googleapis.com/auth/drive"
            ]
            creds_path = ENV["GOOGLE_CREDS"]
            
            try:
                with open(creds_path, "r", encoding="utf-8") as f:
                    creds_json = json.load(f)
            except FileNotFoundError:
                log.error(f"Credentials file '{creds_path}' not found")
                raise SystemExit(f"❗️ فایل احراز هویت '{creds_path}' یافت نشد.")
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse credentials: {e}")
                raise SystemExit(f"❗️ خطا در تجزیه فایل احراز هویت: {e}")
            
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
            self.gc = gspread.authorize(creds)
            
            try:
                self.workbook = self.gc.open(SPREADSHEET)
            except gspread.exceptions.SpreadsheetNotFound:
                log.error(f"Spreadsheet '{SPREADSHEET}' not found")
                raise SystemExit(f"❗️ Spreadsheet '{SPREADSHEET}' یافت نشد.")
            
            # Load or create worksheets
            sheet_names = {
                "orders": SHEET_CONFIG["orders"]["name"],
                "products": SHEET_CONFIG["products"]["name"],
                "abandoned_carts": SHEET_CONFIG["abandoned_carts"]["name"],
                "discounts": SHEET_CONFIG["discounts"]["name"],
                "uploads": SHEET_CONFIG["uploads"]["name"],
                "ratings": SHEET_CONFIG.get("ratings", {}).get("name", "ratings"),
            }
            
            for key, name in sheet_names.items():
                try:
                    self.worksheets[key] = self.workbook.worksheet(name)
                except gspread.exceptions.WorksheetNotFound:
                    log.info(f"Creating worksheet: {name}")
                    self.worksheets[key] = self.workbook.add_worksheet(
                        title=name, rows=1000, cols=20
                    )
            
            log.info("Google Sheets initialized successfully")
            
        except Exception as e:
            log.error(f"Failed to initialize Google Sheets: {e}")
            raise SystemExit(f"❗️ خطا در اتصال به Google Sheets: {e}")
    
    async def retry_operation(self, func, *args, retries=3, delay=1, **kwargs):
        """Execute Google Sheets operation with retry logic"""
        for attempt in range(retries):
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            except gspread.exceptions.APIError as e:
                if "RATE_LIMIT_EXCEEDED" in str(e):
                    wait_time = delay * (2 ** attempt) + random.uniform(0, 1)
                    log.warning(f"Rate limit hit, waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                elif attempt == retries - 1:
                    raise
                else:
                    log.warning(f"API error, retry {attempt + 1}/{retries}: {e}")
                    await asyncio.sleep(delay * (2 ** attempt))
            except Exception as e:
                if attempt == retries - 1:
                    raise
                log.warning(f"Error, retry {attempt + 1}/{retries}: {e}")
                await asyncio.sleep(delay)
        raise Exception("Max retries reached for Google Sheets operation")
    
    async def get_products(self, force_refresh=False) -> Dict[str, Product]:
        """Get products with versioned caching"""
        try:
            # Check cache validity
            now = dt.datetime.utcnow()
            cache_valid = (
                self._products_cache is not None and 
                now < self._products_ts and 
                not force_refresh
            )
            
            if not cache_valid:
                # Check version
                try:
                    cell = await self.retry_operation(
                        self.worksheets["products"].acell, "L1"
                    )
                    current_version = cell.value if cell else "0"
                except:
                    current_version = "0"
                
                if (self._products_cache is None or 
                    self._products_version != current_version or 
                    force_refresh):
                    
                    records = await self.retry_operation(
                        self.worksheets["products"].get_all_records
                    )
                    
                    products = {}
                    for r in records:
                        try:
                            pid = str(r.get("id", ""))
                            if not pid:
                                continue
                            
                            products[pid] = Product(
                                id=pid,
                                cat=str(r.get("cat", "")),
                                fa=str(r.get("fa", "")),
                                it=str(r.get("it", "")),
                                brand=str(r.get("brand", "")),
                                desc=str(r.get("description", "")),
                                weight=str(r.get("weight", "")),
                                price=float(r.get("price", 0)),
                                image_url=r.get("image_url") or None,
                                stock=int(r.get("stock", 0)),
                                is_bestseller=str(r.get("is_bestseller", "")).lower() == "true",
                                version=str(r.get("version", "0")),
                                rating=float(r.get("rating", 0)),
                                rating_count=int(r.get("rating_count", 0))
                            )
                        except (ValueError, KeyError) as e:
                            log.warning(f"Invalid product data: {r}, error: {e}")
                            continue
                    
                    if not products:
                        log.error("No valid products loaded")
                        raise Exception("No products available")
                    
                    self._products_cache = products
                    self._products_version = current_version
                    self._products_ts = now + dt.timedelta(seconds=CACHE_TTL)
                    log.info(f"Loaded {len(products)} products, version {current_version}")
            
            return self._products_cache
            
        except Exception as e:
            log.error(f"Error loading products: {e}")
            if self._products_cache:
                return self._products_cache
            raise
    
    async def get_discounts(self) -> Dict[str, Dict]:
        """Get discount codes with caching"""
        try:
            now = dt.datetime.utcnow()
            if self._discounts_cache and now < self._discounts_ts:
                return self._discounts_cache
            
            records = await self.retry_operation(
                self.worksheets["discounts"].get_all_records
            )
            
            discounts = {}
            for r in records:
                try:
                    code = str(r.get("code", "")).strip().upper()
                    if not code:
                        continue
                    discounts[code] = {
                        "discount_percent": float(r.get("discount_percent", 0)),
                        "valid_until": str(r.get("valid_until", "")),
                        "is_active": str(r.get("is_active", "")).lower() == "true",
                        "min_order": float(r.get("min_order", 0)),
                        "max_discount": float(r.get("max_discount", 0)),
                    }
                except (ValueError, KeyError) as e:
                    log.warning(f"Invalid discount data: {r}, error: {e}")
            
            self._discounts_cache = discounts
            self._discounts_ts = now + dt.timedelta(seconds=300)  # 5 min cache
            return discounts
            
        except Exception as e:
            log.error(f"Error loading discounts: {e}")
            return self._discounts_cache or {}
    
    async def append_order(self, order: Order) -> bool:
        """Append order to orders sheet"""
        try:
            for item in order.items:
                row = [
                    order.created_at,
                    order.order_id,
                    order.user_id,
                    order.handle,
                    order.name,
                    order.phone,
                    order.address,
                    order.dest,
                    item.id,
                    item.fa,
                    item.qty,
                    item.price,
                    item.qty * item.price,
                    order.notes,
                    order.discount_code or "",
                    order.discount,
                    order.status,
                    "FALSE"  # notified
                ]
                await self.retry_operation(
                    self.worksheets["orders"].append_row, row
                )
            return True
        except Exception as e:
            log.error(f"Error appending order: {e}")
            return False
    
    async def update_stock(self, cart: List[Dict]) -> bool:
        """Update product stock after order"""
        try:
            records = await self.retry_operation(
                self.worksheets["products"].get_all_records
            )
            
            for item in cart:
                pid = item["id"]
                qty = item["qty"]
                
                for idx, row in enumerate(records, start=2):
                    if str(row.get("id", "")) == pid:
                        current_stock = int(row.get("stock", 0))
                        new_stock = max(0, current_stock - qty)
                        await self.retry_operation(
                            self.worksheets["products"].update_cell, idx, 10, new_stock
                        )
                        log.info(f"Updated stock for {pid}: {current_stock} -> {new_stock}")
                        break
            
            # Invalidate cache
            self._products_ts = dt.datetime.min
            return True
            
        except Exception as e:
            log.error(f"Stock update error: {e}")
            return False
    
    async def save_abandoned_cart(self, user_id: int, cart: List[Dict]):
        """Save abandoned cart for reminder"""
        try:
            await self.retry_operation(
                self.worksheets["abandoned_carts"].append_row,
                [
                    dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    user_id,
                    json.dumps(cart, ensure_ascii=False)
                ]
            )
        except Exception as e:
            log.error(f"Error saving abandoned cart: {e}")
    
    async def save_rating(self, user_id: int, product_id: str, rating: int, comment: str):
        """Save product rating"""
        try:
            await self.retry_operation(
                self.worksheets["ratings"].append_row,
                [
                    dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    user_id,
                    product_id,
                    rating,
                    comment
                ]
            )
        except Exception as e:
            log.error(f"Error saving rating: {e}")
    
    async def get_user_orders(self, user_id: int) -> List[Dict]:
        """Get orders for a specific user"""
        try:
            records = await self.retry_operation(
                self.worksheets["orders"].get_all_records
            )
            
            orders = {}
            for r in records:
                if str(r.get("user_id", "")) == str(user_id):
                    oid = r.get("order_id", "")
                    if oid not in orders:
                        orders[oid] = {
                            "order_id": oid,
                            "created_at": r.get("timestamp", ""),
                            "status": r.get("status", ""),
                            "total": 0,
                            "items": []
                        }
                    orders[oid]["items"].append({
                        "fa": r.get("product_fa", ""),
                        "qty": r.get("qty", 0),
                        "price": r.get("price", 0)
                    })
                    orders[oid]["total"] += float(r.get("subtotal", 0))
            
            return sorted(orders.values(), key=lambda x: x["created_at"], reverse=True)[:10]
            
        except Exception as e:
            log.error(f"Error getting user orders: {e}")
            return []
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get shop statistics for admin"""
        try:
            orders = await self.retry_operation(
                self.worksheets["orders"].get_all_records
            )
            products = await self.get_products()
            
            today = dt.datetime.utcnow().strftime("%Y-%m-%d")
            
            stats = {
                "total_orders": len(set(r.get("order_id") for r in orders)),
                "total_revenue": sum(float(r.get("subtotal", 0)) for r in orders),
                "today_orders": len(set(
                    r.get("order_id") for r in orders 
                    if r.get("timestamp", "").startswith(today)
                )),
                "low_stock_products": [
                    p for p in products.values() if p.stock <= LOW_STOCK_TH
                ],
                "pending_orders": len([
                    r for r in orders if r.get("status") == "preparing"
                ]),
                "total_products": len(products),
            }
            
            return stats
            
        except Exception as e:
            log.error(f"Error getting statistics: {e}")
            return {}

# Global sheets client
sheets = GoogleSheetsClient()

# ═══════════════════════════════════════════════════════════════════════════════
# INVOICE GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

class InvoiceGenerator:
    """Generate invoice images"""
    
    def __init__(self):
        self.font_dir = "fonts"
        self.logo_path = "logo.png"
    
    async def generate(
        self, 
        order_id: str, 
        user_data: Dict, 
        cart: List[Dict], 
        total: float, 
        discount: float
    ) -> io.BytesIO:
        """Generate invoice image"""
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            log.error("Pillow not installed")
            raise
        
        width, height = 650, 950
        img = Image.new("RGB", (width, height), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        # Colors
        header_color = (0, 100, 80)  # Dark green
        text_color = (30, 30, 30)
        accent_color = (255, 200, 50)
        border_color = (200, 200, 200)
        beige = (250, 248, 240)
        
        # Load fonts
        fonts = self._load_fonts()
        
        # Draw header
        draw.rectangle([(0, 0), (width, 90)], fill=header_color)
        draw.text(
            (width // 2, 45), 
            "🛒 فاکتور بازارینو / Fattura Bazarino",
            fill=(255, 255, 255), 
            font=fonts["title"], 
            anchor="mm"
        )
        
        # Load logo
        if os.path.exists(self.logo_path):
            try:
                logo = Image.open(self.logo_path).resize((70, 70))
                img.paste(logo, (15, 10))
            except Exception as e:
                log.warning(f"Logo loading error: {e}")
        
        y = 110
        
        # Order info section
        draw.rectangle([(20, y - 10), (width - 20, y + 130)], outline=border_color, width=2)
        
        info_lines = [
            f"📋 شماره سفارش / Ordine: #{order_id}",
            f"👤 نام / Nome: {user_data.get('name', 'N/A')}",
            f"📞 تلفن / Tel: {user_data.get('phone', 'N/A')}",
            f"📍 مقصد / Dest: {user_data.get('dest', 'N/A')}",
            f"🏠 آدرس / Indirizzo: {user_data.get('address', 'N/A')[:40]}...",
        ]
        
        for line in info_lines:
            draw.text((35, y), line, font=fonts["body"], fill=text_color)
            y += 25
        
        y += 30
        
        # Products section
        draw.text((35, y), "📦 محصولات / Prodotti:", font=fonts["body"], fill=text_color)
        y += 25
        
        table_start = y
        draw.rectangle(
            [(20, y), (width - 20, y + 25 + len(cart) * 28)], 
            outline=border_color, 
            width=1,
            fill=beige
        )
        
        y += 5
        for item in cart:
            subtotal = item["qty"] * item["price"]
            line = f"  ▫️ {item['qty']}× {item['fa']} — {subtotal:.2f}€"
            draw.text((35, y), line, font=fonts["small"], fill=text_color)
            y += 28
        
        y += 30
        
        # Totals section
        draw.line([(20, y), (width - 20, y)], fill=border_color, width=2)
        y += 15
        
        if discount > 0:
            draw.text(
                (35, y), 
                f"🎁 تخفیف / Sconto: -{discount:.2f}€", 
                font=fonts["body"], 
                fill=(200, 50, 50)
            )
            y += 30
        
        draw.text(
            (35, y), 
            f"💰 مجموع نهایی / Totale: {total:.2f}€", 
            font=fonts["title"], 
            fill=header_color
        )
        y += 40
        
        # Notes
        if user_data.get("notes"):
            draw.text(
                (35, y), 
                f"📝 یادداشت / Nota: {user_data['notes'][:50]}", 
                font=fonts["small"], 
                fill=text_color
            )
            y += 35
        
        # Hafez quote section
        y += 10
        draw.rectangle(
            [(20, y), (width - 20, y + 90)], 
            outline=accent_color, 
            width=2, 
            fill=(255, 252, 240)
        )
        
        if HAFEZ_QUOTES:
            hafez = random.choice(HAFEZ_QUOTES)
            draw.text(
                (35, y + 10), 
                "✨ فال حافظ / Fal di Hafez:", 
                font=fonts["small"], 
                fill=text_color
            )
            draw.text(
                (35, y + 35), 
                hafez.get("fa", "")[:60], 
                font=fonts["small"], 
                fill=(100, 50, 0)
            )
            draw.text(
                (35, y + 60), 
                hafez.get("it", "")[:60], 
                font=fonts["small"], 
                fill=(100, 50, 0)
            )
        
        # Footer
        draw.rectangle([(0, height - 50), (width, height)], fill=header_color)
        draw.text(
            (width // 2, height - 25), 
            "🇮🇷 بازارینو - طعم ایران در ایتالیا 🇮🇹", 
            fill=(255, 255, 255), 
            font=fonts["small"], 
            anchor="mm"
        )
        
        # Save to buffer
        buffer = io.BytesIO()
        img.save(buffer, format="PNG", quality=95)
        buffer.seek(0)
        return buffer
    
    def _load_fonts(self) -> Dict[str, Any]:
        """Load fonts with fallback"""
        from PIL import ImageFont
        
        fonts = {}
        default = ImageFont.load_default()
        
        if os.path.exists(self.font_dir):
            try:
                fonts["title"] = ImageFont.truetype(
                    os.path.join(self.font_dir, "Vazir-Bold.ttf"), 22
                )
                fonts["body"] = ImageFont.truetype(
                    os.path.join(self.font_dir, "Vazir.ttf"), 16
                )
                fonts["small"] = ImageFont.truetype(
                    os.path.join(self.font_dir, "Vazir.ttf"), 14
                )
                return fonts
            except Exception as e:
                log.warning(f"Font loading error: {e}")
        
        return {"title": default, "body": default, "small": default}

invoice_generator = InvoiceGenerator()

# ═══════════════════════════════════════════════════════════════════════════════
# EMOJI CATEGORIES
# ═══════════════════════════════════════════════════════════════════════════════

EMOJI = {
    "rice": "🍚 برنج / Riso",
    "beans": "🥣 حبوبات / Legumi",
    "spice": "🌿 ادویه / Spezie",
    "nuts": "🥜 خشکبار / Frutta secca",
    "drink": "🧃 نوشیدنی / Bevande",
    "canned": "🥫 کنسرو / Conserve",
    "sweet": "🍬 شیرینی / Dolci",
    "dairy": "🧀 لبنیات / Latticini",
    "bread": "🥖 نان / Pane",
    "sauce": "🥫 سس / Salse",
}

# ═══════════════════════════════════════════════════════════════════════════════
# CART HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def cart_total(cart: List[Dict]) -> float:
    """Calculate cart total"""
    return sum(item["qty"] * item["price"] for item in cart)

def cart_count(ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Count items in cart"""
    return sum(item["qty"] for item in ctx.user_data.get("cart", []))

def fmt_cart(cart: List[Dict]) -> str:
    """Format cart for display (HTML)"""
    if not cart:
        return m("CART_EMPTY")
    
    lines = ["<b>🛍 سبد خرید شما / Il tuo carrello:</b>", ""]
    total = 0
    
    for item in cart:
        subtotal = item["qty"] * item["price"]
        total += subtotal
        lines.append(f"  ▫️ {item['qty']}× {item['fa']} — <b>{subtotal:.2f}€</b>")
    
    lines.extend([
        "",
        f"━━━━━━━━━━━━━━━━",
        f"<b>💶 جمع کل / Totale: {total:.2f}€</b>"
    ])
    
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
# SAFE MESSAGE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

async def safe_edit(query, text: str, **kwargs):
    """Safely edit message with fallback"""
    try:
        parse_mode = kwargs.get("parse_mode", "HTML")
        reply_markup = kwargs.get("reply_markup")
        
        if query.message.text:
            await query.edit_message_text(
                text, 
                parse_mode=parse_mode, 
                reply_markup=reply_markup
            )
        elif query.message.caption is not None or query.message.photo:
            await query.edit_message_caption(
                caption=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
        else:
            await query.message.delete()
            await query.message.chat.send_message(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
    except BadRequest as e:
        if "not modified" in str(e).lower():
            return
        log.warning(f"Edit error: {e}")
        try:
            await query.message.chat.send_message(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
        except Exception as e2:
            log.error(f"Send fallback failed: {e2}")
    except Exception as e:
        log.error(f"Safe edit error: {e}")

async def notify_admin(message: str, photo: io.BytesIO = None):
    """Send notification to admin"""
    if not ADMIN_ID or not bot:
        return
    
    for attempt in range(3):
        try:
            if photo:
                photo.seek(0)
                await bot.send_photo(ADMIN_ID, photo=photo, caption=message[:1024])
            else:
                await bot.send_message(ADMIN_ID, message[:4096])
            return
        except Exception as e:
            log.error(f"Admin notify attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(1)

# ═══════════════════════════════════════════════════════════════════════════════
# RATE LIMITING DECORATOR
# ═══════════════════════════════════════════════════════════════════════════════

def rate_limit(func):
    """Rate limiting decorator for handlers"""
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id if update.effective_user else 0
        
        if not rate_limiter.is_allowed(user_id):
            log.warning(f"Rate limit exceeded for user {user_id}")
            if update.callback_query:
                await update.callback_query.answer(
                    "⏳ لطفاً کمی صبر کنید / Attendere prego...",
                    show_alert=True
                )
            elif update.message:
                await update.message.reply_text(
                    "⏳ درخواست‌های شما بیش از حد است. لطفاً کمی صبر کنید.\n"
                    "Troppe richieste. Attendere prego."
                )
            return
        
        return await func(update, ctx, *args, **kwargs)
    return wrapper

# ═══════════════════════════════════════════════════════════════════════════════
# KEYBOARD BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

async def kb_main(ctx: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    """Main menu keyboard"""
    try:
        products = await sheets.get_products()
        cats = sorted(set(p.cat for p in products.values() if p.cat))
        
        rows = []
        
        # Category buttons (2 per row)
        for i in range(0, len(cats), 2):
            row = []
            for cat in cats[i:i+2]:
                row.append(InlineKeyboardButton(
                    EMOJI.get(cat, f"📦 {cat}"),
                    callback_data=f"cat_{cat}"
                ))
            rows.append(row)
        
        # Feature buttons
        rows.append([
            InlineKeyboardButton("🔍 جستجو / Cerca", callback_data="search"),
            InlineKeyboardButton("🔥 پرفروش‌ها / Top", callback_data="bestsellers")
        ])
        
        # Cart button with summary
        cart = ctx.user_data.get("cart", [])
        if cart:
            cart_text = f"🛒 سبد ({cart_count(ctx)}) — {cart_total(cart):.2f}€"
        else:
            cart_text = "🛒 سبد خرید / Carrello"
        rows.append([InlineKeyboardButton(cart_text, callback_data="cart")])
        
        # Extra buttons
        rows.append([
            InlineKeyboardButton("📦 سفارشات من / I miei ordini", callback_data="my_orders"),
            InlineKeyboardButton("📞 پشتیبانی / Supporto", callback_data="support")
        ])
        
        return InlineKeyboardMarkup(rows)
        
    except Exception as e:
        log.error(f"Error in kb_main: {e}")
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 تلاش مجدد / Riprova", callback_data="back")
        ]])

async def kb_category(cat: str, ctx: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    """Category products keyboard"""
    try:
        products = await sheets.get_products()
        cat_products = [
            (pid, p) for pid, p in products.items() 
            if p.cat == cat and p.stock > 0
        ]
        
        rows = []
        for pid, p in cat_products:
            stock_emoji = "✅" if p.stock > 5 else "⚠️" if p.stock > 0 else "❌"
            rating = f"⭐{p.rating:.1f}" if p.rating > 0 else ""
            rows.append([InlineKeyboardButton(
                f"{stock_emoji} {p.fa} / {p.it} {rating}",
                callback_data=f"show_{pid}"
            )])
        
        if not rows:
            rows.append([InlineKeyboardButton(
                "😔 محصولی موجود نیست / Nessun prodotto",
                callback_data="back"
            )])
        
        rows.append([
            InlineKeyboardButton("🔍 جستجو / Cerca", callback_data="search"),
            InlineKeyboardButton("🔙 برگشت / Indietro", callback_data="back")
        ])
        
        return InlineKeyboardMarkup(rows)
        
    except Exception as e:
        log.error(f"Error in kb_category: {e}")
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 برگشت / Indietro", callback_data="back")
        ]])

def kb_product(pid: str, product: Product) -> InlineKeyboardMarkup:
    """Product detail keyboard"""
    rows = [
        [
            InlineKeyboardButton("➖", callback_data=f"pdec_{pid}"),
            InlineKeyboardButton("1️⃣ افزودن / Aggiungi", callback_data=f"add_{pid}"),
            InlineKeyboardButton("➕", callback_data=f"pinc_{pid}")
        ],
        [
            InlineKeyboardButton("⭐ امتیاز / Valuta", callback_data=f"rate_{pid}"),
            InlineKeyboardButton(f"🔙 {EMOJI.get(product.cat, 'برگشت')}", callback_data=f"back_cat_{product.cat}")
        ]
    ]
    return InlineKeyboardMarkup(rows)

def kb_cart(cart: List[Dict]) -> InlineKeyboardMarkup:
    """Cart management keyboard"""
    rows = []
    
    for item in cart:
        pid = item["id"]
        rows.append([
            InlineKeyboardButton("➖", callback_data=f"dec_{pid}"),
            InlineKeyboardButton(f"{item['qty']}× {item['fa'][:15]}", callback_data="ignore"),
            InlineKeyboardButton("➕", callback_data=f"inc_{pid}"),
            InlineKeyboardButton("🗑", callback_data=f"del_{pid}")
        ])
    
    rows.append([
        InlineKeyboardButton("🏠 پروجا / Perugia", callback_data="order_perugia"),
        InlineKeyboardButton("🇮🇹 سراسر ایتالیا / Italia", callback_data="order_italy")
    ])
    
    rows.append([
        InlineKeyboardButton("✅ ادامه سفارش / Continua", callback_data="checkout"),
        InlineKeyboardButton("🔙 برگشت / Indietro", callback_data="back")
    ])
    
    rows.append([
        InlineKeyboardButton("🗑 پاک کردن سبد / Svuota", callback_data="clear_cart")
    ])
    
    return InlineKeyboardMarkup(rows)

def kb_support() -> InlineKeyboardMarkup:
    """Support menu keyboard"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📷 ارسال تصویر / Invia foto", callback_data="upload_photo")],
        [InlineKeyboardButton("❓ سوالات متداول / FAQ", callback_data="faq")],
        [InlineKeyboardButton("📞 تماس / Contatto", callback_data="contact_info")],
        [InlineKeyboardButton("🔙 برگشت / Indietro", callback_data="back")]
    ])

def kb_rating() -> InlineKeyboardMarkup:
    """Rating keyboard"""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⭐", callback_data="rate_1"),
        InlineKeyboardButton("⭐⭐", callback_data="rate_2"),
        InlineKeyboardButton("⭐⭐⭐", callback_data="rate_3"),
        InlineKeyboardButton("⭐⭐⭐⭐", callback_data="rate_4"),
        InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="rate_5"),
    ]])

def kb_admin() -> InlineKeyboardMarkup:
    """Admin panel keyboard"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 آمار / Statistiche", callback_data="admin_stats")],
        [InlineKeyboardButton("📦 سفارشات در انتظار / Ordini pendenti", callback_data="admin_pending")],
        [InlineKeyboardButton("⚠️ موجودی کم / Stock basso", callback_data="admin_low_stock")],
        [InlineKeyboardButton("📤 بکاپ / Backup", callback_data="admin_backup")],
        [InlineKeyboardButton("🔄 رفرش کش / Refresh cache", callback_data="admin_refresh")],
        [InlineKeyboardButton("🔙 برگشت / Indietro", callback_data="back")]
    ])
  # ═══════════════════════════════════════════════════════════════════════════════
# CART OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

async def add_to_cart(
    ctx: ContextTypes.DEFAULT_TYPE, 
    pid: str, 
    qty: int = 1,
    update: Update = None
) -> Tuple[bool, str]:
    """Add product to cart with stock validation"""
    try:
        products = await sheets.get_products()
        
        if pid not in products:
            return False, m("PRODUCT_NOT_FOUND")
        
        product = products[pid]
        
        if product.stock <= 0:
            return False, m("STOCK_EMPTY")
        
        cart = ctx.user_data.setdefault("cart", [])
        existing = next((i for i in cart if i["id"] == pid), None)
        current_qty = existing["qty"] if existing else 0
        
        if product.stock < current_qty + qty:
            return False, f"⚠️ فقط {product.stock} عدد موجود است / Solo {product.stock} disponibili"
        
        if existing:
            existing["qty"] += qty
        else:
            cart.append({
                "id": pid,
                "fa": product.fa,
                "it": product.it,
                "price": product.price,
                "weight": product.weight,
                "qty": qty
            })
        
        # Alert admin for low stock
        if product.stock <= LOW_STOCK_TH:
            await notify_admin(f"⚠️ موجودی کم: {product.fa}\nStock: {product.stock}")
        
        # Save to abandoned carts
        user_id = ctx.user_data.get("user_id", 0)
        if update and update.effective_user:
            user_id = update.effective_user.id
        await sheets.save_abandoned_cart(user_id, cart)
        
        return True, m("CART_ADDED")
        
    except Exception as e:
        log.error(f"Error adding to cart: {e}")
        return False, "❗️ خطا در افزودن به سبد / Errore"

async def update_cart_item(
    ctx: ContextTypes.DEFAULT_TYPE, 
    pid: str, 
    action: str
) -> Tuple[bool, str]:
    """Update cart item quantity"""
    cart = ctx.user_data.get("cart", [])
    item = next((i for i in cart if i["id"] == pid), None)
    
    if not item:
        return False, m("CART_ITEM_NOT_FOUND")
    
    if action == "inc":
        products = await sheets.get_products()
        if pid in products and products[pid].stock > item["qty"]:
            item["qty"] += 1
            return True, "✅ افزوده شد / Aggiunto"
        return False, m("STOCK_EMPTY")
    
    elif action == "dec":
        if item["qty"] > 1:
            item["qty"] -= 1
            return True, "✅ کم شد / Ridotto"
        else:
            cart.remove(item)
            return True, "🗑 حذف شد / Rimosso"
    
    elif action == "del":
        cart.remove(item)
        return True, "🗑 حذف شد / Rimosso"
    
    return False, "❗️ عملیات نامعتبر"

async def clear_cart(ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    """Clear entire cart"""
    ctx.user_data["cart"] = []
    return True

# ═══════════════════════════════════════════════════════════════════════════════
# ORDER CONVERSATION STATES
# ═══════════════════════════════════════════════════════════════════════════════

(
    ASK_NAME, 
    ASK_PHONE, 
    ASK_ADDRESS, 
    ASK_POSTAL, 
    ASK_DISCOUNT, 
    ASK_NOTES,
    CONFIRM_ORDER
) = range(7)

# ═══════════════════════════════════════════════════════════════════════════════
# ORDER HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

@rate_limit
async def start_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Start order process"""
    try:
        q = update.callback_query
        cart = ctx.user_data.get("cart", [])
        
        if not cart:
            await q.answer(m("CART_EMPTY"), show_alert=True)
            return ConversationHandler.END
        
        if not ctx.user_data.get("dest"):
            await q.answer("⚠️ لطفاً ابتدا مقصد را انتخاب کنید / Seleziona la destinazione", show_alert=True)
            await safe_edit(q, fmt_cart(cart), reply_markup=kb_cart(cart))
            return ConversationHandler.END
        
        # Pre-fill user info from Telegram
        user = q.from_user
        ctx.user_data["handle"] = f"@{user.username}" if user.username else "-"
        ctx.user_data["user_id"] = user.id
        ctx.user_data["tg_name"] = f"{user.first_name} {user.last_name or ''}".strip()
        
        await q.message.reply_text(
            f"📝 <b>ثبت سفارش / Ordine</b>\n\n"
            f"نام و نام خانوادگی خود را وارد کنید:\n"
            f"Inserisci il tuo nome e cognome:\n\n"
            f"<i>(پیشنهادی / Suggerito: {ctx.user_data['tg_name']})</i>",
            parse_mode="HTML"
        )
        return ASK_NAME
        
    except Exception as e:
        log.error(f"Error in start_order: {e}")
        await update.callback_query.message.reply_text(
            "❗️ خطا در شروع سفارش / Errore nell'ordine"
        )
        return ConversationHandler.END

async def ask_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Get customer name and ask for phone"""
    try:
        text = update.message.text.strip()
        
        # Allow using suggested name
        if text.lower() in ["/use", "use", "استفاده"]:
            text = ctx.user_data.get("tg_name", "")
        
        valid, result = validator.validate_name(text)
        if not valid:
            await update.message.reply_text(f"❌ {result}\n\nلطفاً دوباره وارد کنید / Riprova:")
            return ASK_NAME
        
        ctx.user_data["name"] = result
        
        await update.message.reply_text(
            "📱 شماره تلفن خود را وارد کنید:\n"
            "Inserisci il tuo numero di telefono:\n\n"
            "<i>مثال / Esempio: +39 333 1234567</i>",
            parse_mode="HTML"
        )
        return ASK_PHONE
        
    except Exception as e:
        log.error(f"Error in ask_phone: {e}")
        await update.message.reply_text("❗️ خطا / Errore. /cancel برای لغو")
        return ASK_NAME

async def ask_address(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Get phone and ask for address"""
    try:
        text = update.message.text.strip()
        
        valid, result = validator.validate_phone(text)
        if not valid:
            await update.message.reply_text(f"❌ {result}\n\nلطفاً دوباره وارد کنید / Riprova:")
            return ASK_PHONE
        
        ctx.user_data["phone"] = result
        
        await update.message.reply_text(
            "🏠 آدرس کامل خود را وارد کنید:\n"
            "Inserisci il tuo indirizzo completo:\n\n"
            "<i>شامل: شهر، خیابان، پلاک\n"
            "Include: città, via, numero civico</i>",
            parse_mode="HTML"
        )
        return ASK_ADDRESS
        
    except Exception as e:
        log.error(f"Error in ask_address: {e}")
        await update.message.reply_text("❗️ خطا / Errore. /cancel برای لغو")
        return ASK_PHONE

async def ask_postal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Get address and ask for postal code"""
    try:
        text = validator.sanitize_text(update.message.text, 300)
        
        if len(text) < 10:
            await update.message.reply_text(
                "❌ آدرس خیلی کوتاه است / Indirizzo troppo corto\n"
                "لطفاً آدرس کامل وارد کنید / Inserisci l'indirizzo completo:"
            )
            return ASK_ADDRESS
        
        ctx.user_data["address"] = text
        
        await update.message.reply_text(
            "📮 کد پستی را وارد کنید:\n"
            "Inserisci il CAP:\n\n"
            "<i>مثال ایتالیا / Esempio Italia: 06123</i>",
            parse_mode="HTML"
        )
        return ASK_POSTAL
        
    except Exception as e:
        log.error(f"Error in ask_postal: {e}")
        await update.message.reply_text("❗️ خطا / Errore. /cancel برای لغو")
        return ASK_ADDRESS

async def ask_discount(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Get postal code and ask for discount code"""
    try:
        text = update.message.text.strip()
        
        valid, result = validator.validate_postal_code(text)
        if not valid:
            await update.message.reply_text(f"❌ {result}\n\nلطفاً دوباره وارد کنید / Riprova:")
            return ASK_POSTAL
        
        ctx.user_data["postal"] = result
        
        await update.message.reply_text(
            "🎁 کد تخفیف دارید؟\n"
            "Hai un codice sconto?\n\n"
            "کد را وارد کنید یا /skip بزنید\n"
            "Inseriscilo o premi /skip",
            parse_mode="HTML"
        )
        return ASK_DISCOUNT
        
    except Exception as e:
        log.error(f"Error in ask_discount: {e}")
        await update.message.reply_text("❗️ خطا / Errore. /cancel برای لغو")
        return ASK_POSTAL

async def ask_notes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Process discount code and ask for notes"""
    try:
        text = update.message.text.strip()
        
        if text.lower() not in ["/skip", "skip"]:
            code = text.upper()
            discounts = await sheets.get_discounts()
            
            if code in discounts:
                discount_info = discounts[code]
                
                # Validate discount
                if not discount_info["is_active"]:
                    await update.message.reply_text(
                        "❌ این کد غیرفعال است / Codice non attivo\n"
                        "کد دیگر وارد کنید یا /skip:"
                    )
                    return ASK_DISCOUNT
                
                try:
                    valid_until = dt.datetime.strptime(
                        discount_info["valid_until"], "%Y-%m-%d"
                    )
                    if valid_until < dt.datetime.utcnow():
                        await update.message.reply_text(
                            "❌ کد منقضی شده / Codice scaduto\n"
                            "کد دیگر وارد کنید یا /skip:"
                        )
                        return ASK_DISCOUNT
                except:
                    pass
                
                # Check minimum order
                cart_sum = cart_total(ctx.user_data.get("cart", []))
                min_order = discount_info.get("min_order", 0)
                if cart_sum < min_order:
                    await update.message.reply_text(
                        f"❌ حداقل سفارش: {min_order}€ / Ordine minimo: {min_order}€\n"
                        "کد دیگر وارد کنید یا /skip:"
                    )
                    return ASK_DISCOUNT
                
                ctx.user_data["discount_code"] = code
                await update.message.reply_text(
                    f"✅ کد تخفیف اعمال شد: {discount_info['discount_percent']}%\n"
                    f"Codice applicato: {discount_info['discount_percent']}%"
                )
            else:
                await update.message.reply_text(
                    "❌ کد تخفیف نامعتبر / Codice non valido\n"
                    "کد دیگر وارد کنید یا /skip:"
                )
                return ASK_DISCOUNT
        else:
            ctx.user_data["discount_code"] = None
        
        await update.message.reply_text(
            "📝 یادداشت یا توضیحات اضافی:\n"
            "Note o istruzioni speciali:\n\n"
            "<i>(یا /skip برای رد شدن / o /skip per saltare)</i>",
            parse_mode="HTML"
        )
        return ASK_NOTES
        
    except Exception as e:
        log.error(f"Error in ask_notes: {e}")
        await update.message.reply_text("❗️ خطا / Errore. /cancel برای لغو")
        return ASK_DISCOUNT

async def confirm_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Confirm and process order"""
    try:
        text = update.message.text.strip()
        
        if text.lower() in ["/skip", "skip"]:
            ctx.user_data["notes"] = ""
        else:
            ctx.user_data["notes"] = validator.sanitize_text(text, 500)
        
        cart = ctx.user_data.get("cart", [])
        if not cart:
            await update.message.reply_text(
                m("CART_EMPTY"), 
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        # Calculate totals
        subtotal = cart_total(cart)
        discount = 0
        discount_code = ctx.user_data.get("discount_code")
        
        if discount_code:
            discounts = await sheets.get_discounts()
            if discount_code in discounts:
                percent = discounts[discount_code]["discount_percent"]
                max_discount = discounts[discount_code].get("max_discount", 0)
                discount = subtotal * (percent / 100)
                if max_discount > 0:
                    discount = min(discount, max_discount)
        
        total = subtotal - discount
        
        # Shipping cost for Italy (outside Perugia)
        shipping = 0
        if ctx.user_data.get("dest") == "Italy":
            # Calculate based on weight
            total_weight = sum(
                float(item.get("weight", "0").replace("g", "").replace("kg", "000") or 0) * item["qty"]
                for item in cart
            )
            if total_weight < 2000:
                shipping = 5.90
            elif total_weight < 5000:
                shipping = 8.90
            else:
                shipping = 12.90
            
            # Free shipping over 50€
            if subtotal >= 50:
                shipping = 0
        
        total += shipping
        
        # Update stock
        if not await sheets.update_stock(cart):
            await update.message.reply_text(
                "❗️ خطا در به‌روزرسانی موجودی. لطفاً دوباره امتحان کنید.\n"
                "Errore nell'aggiornamento dello stock. Riprova.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        # Create order
        order_id = str(uuid.uuid4())[:8].upper()
        order = Order(
            order_id=order_id,
            user_id=ctx.user_data.get("user_id", 0),
            handle=ctx.user_data.get("handle", "-"),
            name=ctx.user_data.get("name", ""),
            phone=ctx.user_data.get("phone", ""),
            address=f"{ctx.user_data.get('address', '')} | {ctx.user_data.get('postal', '')}",
            dest=ctx.user_data.get("dest", ""),
            items=[CartItem(**item) for item in cart],
            total=total,
            discount=discount,
            discount_code=discount_code,
            notes=ctx.user_data.get("notes", ""),
            status="preparing",
            created_at=dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        )
        
        # Save order
        if not await sheets.append_order(order):
            await update.message.reply_text(
                m("ERROR_SHEET"),
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        # Generate invoice
        try:
            invoice_buffer = await invoice_generator.generate(
                order_id, ctx.user_data, cart, total, discount
            )
            
            caption = (
                f"✅ <b>سفارش شما ثبت شد!</b>\n"
                f"<b>Il tuo ordine è stato registrato!</b>\n\n"
                f"📋 شماره سفارش / Ordine: <code>#{order_id}</code>\n"
                f"📍 مقصد / Destinazione: {ctx.user_data['dest']}\n"
                f"💶 مجموع / Totale: <b>{total:.2f}€</b>\n"
            )
            
            if discount > 0:
                caption += f"🎁 تخفیف / Sconto: -{discount:.2f}€\n"
            if shipping > 0:
                caption += f"🚚 ارسال / Spedizione: {shipping:.2f}€\n"
            
            caption += f"\n📞 به زودی با شما تماس می‌گیریم!\nTi contatteremo presto!"
            
            await update.message.reply_photo(
                photo=invoice_buffer,
                caption=caption,
                parse_mode="HTML",
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception as e:
            log.error(f"Invoice generation error: {e}")
            await update.message.reply_text(
                f"✅ سفارش #{order_id} ثبت شد!\n"
                f"Ordine #{order_id} registrato!\n\n"
                f"مجموع / Totale: {total:.2f}€",
                reply_markup=ReplyKeyboardRemove()
            )
        
        # Send promo message
        if promo := MSG.get("PROMO_AFTER_ORDER"):
            await update.message.reply_text(promo, disable_web_page_preview=True)
        
        # Notify admin
        admin_msg = (
            f"🆕 <b>سفارش جدید / Nuovo Ordine</b>\n\n"
            f"📋 #{order_id}\n"
            f"👤 {ctx.user_data['name']}\n"
            f"📞 {ctx.user_data['phone']}\n"
            f"📍 {ctx.user_data['dest']}\n"
            f"🏠 {ctx.user_data['address']}\n\n"
            f"<b>محصولات / Prodotti:</b>\n"
        )
        for item in cart:
            admin_msg += f"  ▫️ {item['qty']}× {item['fa']}\n"
        admin_msg += f"\n💶 <b>Total: {total:.2f}€</b>"
        if discount > 0:
            admin_msg += f"\n🎁 Discount: -{discount:.2f}€ ({discount_code})"
        
        try:
            invoice_buffer.seek(0)
            await notify_admin(admin_msg, invoice_buffer)
        except:
            await notify_admin(admin_msg)
        
        # Clear cart and abandoned carts
        ctx.user_data["cart"] = []
        try:
            await sheets.retry_operation(sheets.worksheets["abandoned_carts"].clear)
        except:
            pass
        
        log.info(f"Order {order_id} completed for user {ctx.user_data.get('user_id')}")
        return ConversationHandler.END
        
    except Exception as e:
        log.error(f"Error in confirm_order: {e}")
        await update.message.reply_text(
            "❗️ خطا در ثبت سفارش / Errore nell'ordine\n"
            "لطفاً دوباره امتحان کنید / Riprova",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

async def cancel_order(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Cancel order process"""
    try:
        # Keep cart but clear order data
        order_keys = ["name", "phone", "address", "postal", "discount_code", "notes", "dest"]
        for key in order_keys:
            ctx.user_data.pop(key, None)
        
        await update.message.reply_text(
            m("ORDER_CANCELLED"),
            reply_markup=ReplyKeyboardRemove()
        )
        
        # Send main menu
        await update.message.reply_text(
            m("WELCOME"),
            reply_markup=await kb_main(ctx),
            parse_mode="HTML"
        )
        
        return ConversationHandler.END
        
    except Exception as e:
        log.error(f"Error in cancel_order: {e}")
        return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

@rate_limit
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    try:
        user = update.effective_user
        ctx.user_data["user_id"] = user.id
        
        log.info(f"User {user.id} (@{user.username}) started bot")
        
        welcome = m("WELCOME").format(name=user.first_name)
        
        await update.message.reply_html(
            welcome,
            reply_markup=await kb_main(ctx)
        )
        
    except Exception as e:
        log.error(f"Error in cmd_start: {e}")
        await update.message.reply_text(
            "❗️ خطا در بارگذاری منو / Errore nel caricamento\n"
            "لطفاً دوباره /start کنید / Riprova con /start"
        )
        await notify_admin(f"⚠️ Error in /start: {e}")

@rate_limit
async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /search command"""
    try:
        query = " ".join(ctx.args).lower().strip()
        
        if not query or len(query) < 2:
            await update.message.reply_text(
                m("SEARCH_USAGE") + "\n\n"
                "<i>مثال / Esempio: /search زعفران</i>",
                parse_mode="HTML"
            )
            return
        
        products = await sheets.get_products()
        
        # Search in name, description, brand
        hits = []
        for pid, p in products.items():
            searchable = f"{p.fa} {p.it} {p.brand} {p.desc}".lower()
            
            # Direct match
            if query in searchable:
                hits.append((pid, p, 100))
                continue
            
            # Fuzzy match
            matches = get_close_matches(query, searchable.split(), n=1, cutoff=0.6)
            if matches:
                hits.append((pid, p, 60))
        
        # Sort by score
        hits.sort(key=lambda x: x[2], reverse=True)
        
        if not hits:
            await update.message.reply_text(
                f"🔍 نتیجه‌ای برای '{query}' یافت نشد\n"
                f"Nessun risultato per '{query}'"
            )
            return
        
        await update.message.reply_text(
            f"🔍 نتایج جستجو برای '{query}':\n"
            f"Risultati per '{query}':"
        )
        
        for pid, p, _ in hits[:5]:
            stock_status = "✅" if p.stock > 5 else "⚠️" if p.stock > 0 else "❌"
            rating = f"⭐ {p.rating:.1f}" if p.rating > 0 else ""
            
            cap = (
                f"<b>{p.fa} / {p.it}</b>\n"
                f"🏷 {p.brand}\n"
                f"📝 {p.desc[:100]}\n"
                f"💰 <b>{p.price}€</b> | {p.weight}\n"
                f"{stock_status} موجودی / Stock: {p.stock} {rating}"
            )
            
            btn = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "🛒 افزودن / Aggiungi",
                    callback_data=f"add_{pid}"
                )
            ]])
            
            if p.image_url:
                try:
                    await update.message.reply_photo(
                        p.image_url, 
                        caption=cap, 
                        reply_markup=btn,
                        parse_mode="HTML"
                    )
                except:
                    await update.message.reply_text(cap, reply_markup=btn, parse_mode="HTML")
            else:
                await update.message.reply_text(cap, reply_markup=btn, parse_mode="HTML")
        
    except Exception as e:
        log.error(f"Error in cmd_search: {e}")
        await update.message.reply_text("❗️ خطا در جستجو / Errore nella ricerca")

@rate_limit
async def cmd_cart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /cart command"""
    try:
        cart = ctx.user_data.get("cart", [])
        
        if not cart:
            await update.message.reply_text(
                m("CART_EMPTY"),
                reply_markup=await kb_main(ctx)
            )
            return
        
        await update.message.reply_text(
            fmt_cart(cart),
            reply_markup=kb_cart(cart),
            parse_mode="HTML"
        )
        
    except Exception as e:
        log.error(f"Error in cmd_cart: {e}")
        await update.message.reply_text("❗️ خطا / Errore")

@rate_limit
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    try:
        help_text = (
            "📚 <b>راهنمای بازارینو / Guida Bazarino</b>\n\n"
            "🛒 <b>خرید / Acquisti:</b>\n"
            "  • دسته‌بندی را انتخاب کنید\n"
            "  • محصول را به سبد اضافه کنید\n"
            "  • سفارش را تکمیل کنید\n\n"
            "📝 <b>دستورات / Comandi:</b>\n"
            "  /start - منوی اصلی\n"
            "  /search - جستجوی محصول\n"
            "  /cart - سبد خرید\n"
            "  /orders - سفارشات من\n"
            "  /about - درباره ما\n"
            "  /privacy - حریم خصوصی\n"
            "  /help - راهنما\n\n"
            "📞 <b>پشتیبانی / Supporto:</b>\n"
            "  @bazarino_support"
        )
        
        await update.message.reply_text(help_text, parse_mode="HTML")
        
    except Exception as e:
        log.error(f"Error in cmd_help: {e}")

@rate_limit
async def cmd_about(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /about command"""
    try:
        await update.message.reply_text(
            m("ABOUT_US"),
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        log.error(f"Error in cmd_about: {e}")

@rate_limit
async def cmd_privacy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /privacy command"""
    try:
        await update.message.reply_text(
            m("PRIVACY"),
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        log.error(f"Error in cmd_privacy: {e}")

@rate_limit
async def cmd_orders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /orders command - show user's order history"""
    try:
        user_id = update.effective_user.id
        orders = await sheets.get_user_orders(user_id)
        
        if not orders:
            await update.message.reply_text(
                "📦 هنوز سفارشی ثبت نکرده‌اید\n"
                "Non hai ancora effettuato ordini"
            )
            return
        
        text = "📦 <b>سفارشات شما / I tuoi ordini:</b>\n\n"
        
        for order in orders[:5]:
            status_emoji = {
                "preparing": "⏳",
                "shipped": "🚚",
                "delivered": "✅",
                "cancelled": "❌"
            }.get(order["status"], "❓")
            
            text += (
                f"{status_emoji} <code>#{order['order_id']}</code>\n"
                f"   📅 {order['created_at'][:10]}\n"
                f"   💰 {order['total']:.2f}€\n\n"
            )
        
        await update.message.reply_text(text, parse_mode="HTML")
        
    except Exception as e:
        log.error(f"Error in cmd_orders: {e}")
        await update.message.reply_text("❗️ خطا / Errore")

@rate_limit
async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /admin command - admin panel"""
    try:
        user_id = update.effective_user.id
        
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔️ دسترسی غیرمجاز / Accesso negato")
            return
        
        await update.message.reply_text(
            "🔧 <b>پنل مدیریت / Pannello Admin</b>",
            reply_markup=kb_admin(),
            parse_mode="HTML"
        )
        
    except Exception as e:
        log.error(f"Error in cmd_admin: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# PHOTO UPLOAD HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

@rate_limit
async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle photo uploads from users"""
    try:
        if not ctx.user_data.get("awaiting_photo"):
            return
        
        photo = update.message.photo[-1]
        
        if photo.file_size > MAX_PHOTO_SIZE:
            await update.message.reply_text(
                f"❌ حداکثر حجم فایل: {ENV['MAX_PHOTO_SIZE_MB']}MB\n"
                f"Dimensione massima: {ENV['MAX_PHOTO_SIZE_MB']}MB",
                reply_markup=await kb_main(ctx)
            )
            ctx.user_data["awaiting_photo"] = False
            return
        
        file = await photo.get_file()
        user = update.effective_user
        
        # Save to uploads sheet
        try:
            await sheets.retry_operation(
                sheets.worksheets["uploads"].append_row,
                [
                    dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    user.id,
                    f"@{user.username or '-'}",
                    file.file_id,
                    ctx.user_data.get("photo_note", "")
                ]
            )
        except Exception as e:
            log.error(f"Error saving upload: {e}")
        
        # Forward to admin
        await bot.send_photo(
            ADMIN_ID,
            file.file_id,
            caption=(
                f"📷 <b>تصویر جدید / Nuova foto</b>\n\n"
                f"👤 {user.first_name} (@{user.username or '-'})\n"
                f"🆔 <code>{user.id}</code>\n"
                f"📝 {ctx.user_data.get('photo_note', 'بدون توضیح')}"
            ),
            parse_mode="HTML"
        )
        
        await update.message.reply_text(
            "✅ تصویر شما ارسال شد!\nLa tua foto è stata inviata!",
            reply_markup=await kb_main(ctx)
        )
        
        ctx.user_data["awaiting_photo"] = False
        ctx.user_data["photo_note"] = ""
        
    except Exception as e:
        log.error(f"Error in handle_photo: {e}")
        await update.message.reply_text("❗️ خطا در آپلود / Errore upload")
        ctx.user_data["awaiting_photo"] = False

# ═══════════════════════════════════════════════════════════════════════════════
# CALLBACK QUERY ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

@rate_limit
async def router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Main callback query router"""
    try:
        q = update.callback_query
        data = q.data
        await q.answer()
        
        # ─── Navigation ───
        if data == "back":
            await safe_edit(q, m("WELCOME"), reply_markup=await kb_main(ctx))
            return
        
        if data == "ignore":
            return
        
        # ─── Categories ───
        if data.startswith("cat_"):
            cat = data[4:]
            await safe_edit(
                q, 
                EMOJI.get(cat, f"📦 {cat}"),
                reply_markup=await kb_category(cat, ctx)
            )
            return
        
        if data.startswith("back_cat_"):
            cat = data[9:]
            await safe_edit(
                q,
                EMOJI.get(cat, f"📦 {cat}"),
                reply_markup=await kb_category(cat, ctx)
            )
            return
        
        # ─── Product Display ───
        if data.startswith("show_"):
            pid = data[5:]
            products = await sheets.get_products()
            
            if pid not in products:
                await q.answer("❌ محصول یافت نشد / Prodotto non trovato", show_alert=True)
                return
            
            p = products[pid]
            stock_emoji = "✅" if p.stock > 5 else "⚠️" if p.stock > 0 else "❌"
            rating = f"⭐ {p.rating:.1f} ({p.rating_count})" if p.rating > 0 else "بدون امتیاز"
            
            cap = (
                f"<b>{p.fa}</b>\n"
                f"<i>{p.it}</i>\n\n"
                f"🏷 برند / Marca: {p.brand}\n"
                f"📝 {p.desc}\n\n"
                f"💰 قیمت / Prezzo: <b>{p.price}€</b>\n"
                f"⚖️ وزن / Peso: {p.weight}\n"
                f"{stock_emoji} موجودی / Stock: {p.stock}\n"
                f"📊 امتیاز / Valutazione: {rating}"
            )
            
            try:
                await q.message.delete()
            except:
                pass
            
            if p.image_url:
                try:
                    await ctx.bot.send_photo(
                        chat_id=q.message.chat.id,
                        photo=p.image_url,
                        caption=cap,
                        reply_markup=kb_product(pid, p),
                        parse_mode="HTML"
                    )
                    return
                except:
                    pass
            
            await ctx.bot.send_message(
                chat_id=q.message.chat.id,
                text=cap,
                reply_markup=kb_product(pid, p),
                parse_mode="HTML"
            )
            return
        
        # ─── Add to Cart ───
        if data.startswith("add_"):
            pid = data[4:]
            qty = ctx.user_data.get(f"add_qty_{pid}", 1)
            ok, msg = await add_to_cart(ctx, pid, qty, update)
            await q.answer(msg, show_alert=not ok)
            
            # Reset quantity
            ctx.user_data.pop(f"add_qty_{pid}", None)
            
            products = await sheets.get_products()
            if pid in products:
                cat = products[pid].cat
                await safe_edit(
                    q,
                    EMOJI.get(cat, cat),
                    reply_markup=await kb_category(cat, ctx)
                )
            return
        
        # ─── Product Quantity Selector ───
        if data.startswith("pinc_"):
            pid = data[5:]
            key = f"add_qty_{pid}"
            ctx.user_data[key] = ctx.user_data.get(key, 1) + 1
            await q.answer(f"تعداد: {ctx.user_data[key]}")
            return
        
        if data.startswith("pdec_"):
            pid = data[5:]
            key = f"add_qty_{pid}"
            current = ctx.user_data.get(key, 1)
            if current > 1:
                ctx.user_data[key] = current - 1
                await q.answer(f"تعداد: {ctx.user_data[key]}")
            return
        
        # ─── Cart Operations ───
        if data == "cart":
            cart = ctx.user_data.get("cart", [])
            if not cart:
                await safe_edit(q, m("CART_EMPTY"), reply_markup=await kb_main(ctx))
            else:
                await safe_edit(
                    q,
                    f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}",
                    reply_markup=kb_cart(cart)
                )
            return
        
        if data.startswith(("inc_", "dec_", "del_")):
            action = data[:3]
            pid = data[4:]
            ok, msg = await update_cart_item(ctx, pid, action)
            await q.answer(msg)
            
            cart = ctx.user_data.get("cart", [])
            if cart:
                await safe_edit(
                    q,
                    f"{m('CART_GUIDE')}\n\n{fmt_cart(cart)}",
                    reply_markup=kb_cart(cart)
                )
            else:
                await safe_edit(q, m("CART_EMPTY"), reply_markup=await kb_main(ctx))
            return
        
        if data == "clear_cart":
            await clear_cart(ctx)
            await q.answer("🗑 سبد پاک شد / Carrello svuotato")
            await safe_edit(q, m("CART_EMPTY"), reply_markup=await kb_main(ctx))
            return
        
        # ─── Destination Selection ───
        if data in ["order_perugia", "order_italy"]:
            ctx.user_data["dest"] = "Perugia" if data == "order_perugia" else "Italy"
            await q.answer(f"✅ {ctx.user_data['dest']}")
            cart = ctx.user_data.get("cart", [])
            await safe_edit(
                q,
                f"📍 مقصد: {ctx.user_data['dest']}\n\n{fmt_cart(cart)}",
                reply_markup=kb_cart(cart)
            )
            return
        
        # ─── Bestsellers ───
        if data == "bestsellers":
            products = await sheets.get_products()
            bestsellers = [
                (pid, p) for pid, p in products.items() 
                if p.is_bestseller and p.stock > 0
            ]
            
            if not bestsellers:
                await safe_edit(
                    q,
                    "🔥 محصول پرفروشی موجود نیست\nNessun prodotto più venduto",
                    reply_markup=await kb_main(ctx)
                )
                return
            
            rows = [[
                InlineKeyboardButton(
                    f"🔥 {p.fa} / {p.it}",
                    callback_data=f"show_{pid}"
                )
            ] for pid, p in bestsellers[:10]]
            rows.append([InlineKeyboardButton("🔙 برگشت / Indietro", callback_data="back")])
            
            await safe_edit(
                q,
                "🔥 <b>محصولات پرفروش / Più venduti</b>",
                reply_markup=InlineKeyboardMarkup(rows)
            )
            return
        
        # ─── Search ───
        if data == "search":
            await safe_edit(
                q,
                "🔍 برای جستجو از دستور زیر استفاده کنید:\n"
                "Per cercare usa il comando:\n\n"
                "<code>/search نام محصول</code>",
                reply_markup=await kb_main(ctx)
            )
            return
        
        # ─── My Orders ───
        if data == "my_orders":
            user_id = update.effective_user.id
            orders = await sheets.get_user_orders(user_id)
            
            if not orders:
                await safe_edit(
                    q,
                    "📦 سفارشی ثبت نشده\nNessun ordine",
                    reply_markup=await kb_main(ctx)
                )
                return
            
            text = "📦 <b>سفارشات شما / I tuoi ordini:</b>\n\n"
            for order in orders[:5]:
                status_emoji = {"preparing": "⏳", "shipped": "🚚", "delivered": "✅"}.get(order["status"], "❓")
                text += f"{status_emoji} <code>#{order['order_id']}</code> — {order['total']:.2f}€\n"
            
            await safe_edit(q, text, reply_markup=await kb_main(ctx))
            return
        
        # ─── Support ───
        if data == "support":
            await safe_edit(q, m("SUPPORT_MESSAGE"), reply_markup=kb_support())
            return
        
        if data == "upload_photo":
            ctx.user_data["awaiting_photo"] = True
            await safe_edit(
                q,
                "📷 لطفاً تصویر خود را ارسال کنید:\n"
                "Invia la tua foto:",
                reply_markup=kb_support()
            )
            return
        
        if data == "faq":
            faq_text = MSG.get("FAQ", "سوالات متداول در دسترس نیست")
            await safe_edit(q, faq_text, reply_markup=kb_support())
            return
        
        if data == "contact_info":
            await safe_edit(
                q,
                "📞 <b>تماس با ما / Contattaci:</b>\n\n"
                "📱 Telegram: @bazarino_support\n"
                "📧 Email: info@bazarino.it\n"
                "📍 Perugia, Italia",
                reply_markup=kb_support()
            )
            return
        
        # ─── Rating ───
        if data.startswith("rate_") and len(data) > 5:
            if data[5:].isdigit():
                # Rating value
                rating = int(data[5:])
                pid = ctx.user_data.get("rating_product")
                if pid:
                    await sheets.save_rating(update.effective_user.id, pid, rating, "")
                    await q.answer(f"✅ امتیاز ثبت شد: {'⭐' * rating}")
                    ctx.user_data.pop("rating_product", None)
            else:
                # Start rating
                pid = data[5:]
                ctx.user_data["rating_product"] = pid
                await safe_edit(
                    q,
                    "⭐ امتیاز خود را انتخاب کنید:\nSeleziona la tua valutazione:",
                    reply_markup=kb_rating()
                )
            return
        
        # ─── Admin Panel ───
        if data.startswith("admin_") and update.effective_user.id == ADMIN_ID:
            await handle_admin_callback(q, ctx, data)
            return
        
        # Checkout handled by ConversationHandler
        
    except Exception as e:
        log.error(f"Error in router: {e}")
        await notify_admin(f"⚠️ Router error: {e}")

async def handle_admin_callback(q, ctx: ContextTypes.DEFAULT_TYPE, data: str):
    """Handle admin panel callbacks"""
    try:
        if data == "admin_stats":
            stats = await sheets.get_statistics()
            text = (
                "📊 <b>آمار فروشگاه / Statistiche</b>\n\n"
                f"📦 کل سفارشات: {stats.get('total_orders', 0)}\n"
                f"💰 کل فروش: {stats.get('total_revenue', 0):.2f}€\n"
                f"📅 سفارشات امروز: {stats.get('today_orders', 0)}\n"
                f"⏳ در انتظار: {stats.get('pending_orders', 0)}\n"
                f"🛍 تعداد محصولات: {stats.get('total_products', 0)}\n"
                f"⚠️ موجودی کم: {len(stats.get('low_stock_products', []))}"
            )
            await safe_edit(q, text, reply_markup=kb_admin())
        
        elif data == "admin_pending":
            orders = await sheets.retry_operation(
                sheets.worksheets["orders"].get_all_records
            )
            pending = [o for o in orders if o.get("status") == "preparing"]
            
            if not pending:
                text = "✅ سفارش در انتظاری وجود ندارد"
            else:
                text = "⏳ <b>سفارشات در انتظار:</b>\n\n"
                seen = set()
                for o in pending[:10]:
                    oid = o.get("order_id", "")
                    if oid not in seen:
                        seen.add(oid)
                        text += f"▫️ #{oid} — {o.get('name', '-')}\n"
            
            await safe_edit(q, text, reply_markup=kb_admin())
        
        elif data == "admin_low_stock":
            products = await sheets.get_products()
            low_stock = [p for p in products.values() if p.stock <= LOW_STOCK_TH]
            
            if not low_stock:
                text = "✅ همه محصولات موجودی کافی دارند"
            else:
                text = "⚠️ <b>موجودی کم:</b>\n\n"
                for p in low_stock[:15]:
                    text += f"▫️ {p.fa}: {p.stock} عدد\n"
            
            await safe_edit(q, text, reply_markup=kb_admin())
        
        elif data == "admin_backup":
            await q.answer("📤 در حال ایجاد بکاپ...")
            await backup_sheets(ctx)
            await q.answer("✅ بکاپ ارسال شد", show_alert=True)
        
        elif data == "admin_refresh":
            await sheets.get_products(force_refresh=True)
            await q.answer("✅ کش رفرش شد", show_alert=True)
        
    except Exception as e:
        log.error(f"Error in admin callback: {e}")
        await q.answer("❗️ خطا", show_alert=True)

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ═══════════════════════════════════════════════════════════════════════════════

async def send_cart_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Send abandoned cart reminders"""
    try:
        records = await sheets.retry_operation(
            sheets.worksheets["abandoned_carts"].get_all_records
        )
        
        # Group by user, get latest
        user_carts = {}
        for r in records:
            try:
                user_id = int(r.get("user_id", 0))
                cart = json.loads(r.get("cart", "[]"))
                if user_id and cart:
                    user_carts[user_id] = cart
            except:
                continue
        
        sent = 0
        for user_id, cart in user_carts.items():
            try:
                await context.bot.send_message(
                    user_id,
                    f"🛒 سبد خرید شما منتظر شماست!\n"
                    f"Hai lasciato qualcosa nel carrello!\n\n"
                    f"{fmt_cart(cart)}\n\n"
                    f"👉 برای تکمیل: /start",
                    parse_mode="HTML"
                )
                sent += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                log.warning(f"Failed to send reminder to {user_id}: {e}")
        
        log.info(f"Sent {sent} cart reminders")
        
        # Clear old records
        await sheets.retry_operation(sheets.worksheets["abandoned_carts"].clear)
        
    except Exception as e:
        log.error(f"Error sending cart reminders: {e}")

async def check_order_status(context: ContextTypes.DEFAULT_TYPE):
    """Check and notify order status changes"""
    try:
        orders = await sheets.retry_operation(
            sheets.worksheets["orders"].get_all_records
        )
        
        for idx, order in enumerate(orders, start=2):
            try:
                if order.get("notified", "").upper() == "TRUE":
                    continue
                
                status = order.get("status", "")
                user_id = int(order.get("user_id", 0))
                order_id = order.get("order_id", "")
                
                if not user_id or not order_id:
                    continue
                
                if status in ["shipped", "delivered"]:
                    msg = {
                        "shipped": f"🚚 سفارش #{order_id} ارسال شد!\nIl tuo ordine #{order_id} è stato spedito!",
                        "delivered": f"✅ سفارش #{order_id} تحویل داده شد!\nOrdine #{order_id} consegnato!"
                    }[status]
                    
                    await context.bot.send_message(user_id, msg)
                    await sheets.retry_operation(
                        sheets.worksheets["orders"].update_cell, idx, 18, "TRUE"
                    )
                    log.info(f"Sent {status} notification for order {order_id}")
                    
            except Exception as e:
                log.warning(f"Error processing order notification: {e}")
                continue
        
    except Exception as e:
        log.error(f"Error checking order status: {e}")

async def backup_sheets(context: ContextTypes.DEFAULT_TYPE):
    """Create weekly backup of sheets"""
    try:
        sheet_keys = ["orders", "products", "discounts"]
        date_str = dt.datetime.utcnow().strftime("%Y%m%d")
        
        for key in sheet_keys:
            try:
                ws = sheets.worksheets.get(key)
                if not ws:
                    continue
                
                records = await sheets.retry_operation(ws.get_all_values)
                
                csv_content = "\n".join([",".join(str(c) for c in row) for row in records])
                csv_file = io.BytesIO(csv_content.encode("utf-8"))
                csv_file.name = f"{key}_backup_{date_str}.csv"
                
                await context.bot.send_document(
                    ADMIN_ID,
                    document=csv_file,
                    caption=f"📊 بکاپ {key} - {date_str}"
                )
                
            except Exception as e:
                log.error(f"Error backing up {key}: {e}")
        
        log.info("Weekly backup completed")
        
    except Exception as e:
        log.error(f"Error in backup: {e}")

async def daily_stats(context: ContextTypes.DEFAULT_TYPE):
    """Send daily statistics to admin"""
    try:
        stats = await sheets.get_statistics()
        
        msg = (
            f"📊 <b>گزارش روزانه / Report giornaliero</b>\n\n"
            f"📅 {dt.datetime.utcnow().strftime('%Y-%m-%d')}\n\n"
            f"📦 سفارشات امروز: {stats.get('today_orders', 0)}\n"
            f"💰 کل فروش: {stats.get('total_revenue', 0):.2f}€\n"
            f"⏳ در انتظار: {stats.get('pending_orders', 0)}\n"
            f"⚠️ موجودی کم: {len(stats.get('low_stock_products', []))}"
        )
        
        await context.bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
        
    except Exception as e:
        log.error(f"Error sending daily stats: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# APPLICATION SETUP
# ═══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application):
    """Post initialization hook"""
    try:
        # Set webhook
        webhook_url = f"{BASE_URL}/webhook/{WEBHOOK_SECRET}"
        await app.bot.set_webhook(webhook_url)
        log.info(f"Webhook set to {webhook_url}")
        
        # Set bot commands
        commands = [
            BotCommand("start", "🏠 منوی اصلی / Menu principale"),
            BotCommand("search", "🔍 جستجو / Cerca"),
            BotCommand("cart", "🛒 سبد خرید / Carrello"),
            BotCommand("orders", "📦 سفارشات من / I miei ordini"),
            BotCommand("help", "❓ راهنما / Aiuto"),
            BotCommand("about", "ℹ️ درباره ما / Chi siamo"),
            BotCommand("privacy", "🔒 حریم خصوصی / Privacy"),
        ]
        await app.bot.set_my_commands(commands)
        
    except Exception as e:
        log.error(f"Error in post_init: {e}")
        raise

async def post_shutdown(app: Application):
    """Post shutdown hook"""
    try:
        await app.bot.delete_webhook()
        log.info("Webhook deleted, shutting down")
    except Exception as e:
        log.error(f"Error in post_shutdown: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# FASTAPI APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

async def lifespan(app: FastAPI):
    """FastAPI lifespan manager"""
    global tg_app, bot
    
    try:
        # Initialize Google Sheets
        await sheets.initialize()
        
        # Build Telegram application
        persistence = PicklePersistence(filepath="bazarino_data.pickle")
        
        builder = (
            ApplicationBuilder()
            .token(TOKEN)
            .persistence(persistence)
            .post_init(post_init)
            .post_shutdown(post_shutdown)
        )
        
        tg_app = builder.build()
        bot = tg_app.bot
        
        await tg_app.initialize()
        
        # Setup job queue
        job_queue = tg_app.job_queue
        if job_queue:
            await job_queue.start()
            
            # Daily cart reminder at 18:00
            job_queue.run_daily(
                send_cart_reminder,
                time=dt.time(hour=18, minute=0),
                name="cart_reminder"
            )
            
            # Check order status every 10 minutes
            job_queue.run_repeating(
                check_order_status,
                interval=600,
                name="order_status"
            )
            
            # Weekly backup on Sunday at 00:00
            job_queue.run_daily(
                backup_sheets,
                time=dt.time(hour=0, minute=0),
                days=(6,),  # Sunday
                name="backup"
            )
            
            # Daily stats at 23:00
            job_queue.run_daily(
                daily_stats,
                time=dt.time(hour=23, minute=0),
                name="daily_stats"
            )
        
        # Register handlers
        # Commands
        tg_app.add_handler(CommandHandler("start", cmd_start))
        tg_app.add_handler(CommandHandler("search", cmd_search))
        tg_app.add_handler(CommandHandler("cart", cmd_cart))
        tg_app.add_handler(CommandHandler("orders", cmd_orders))
        tg_app.add_handler(CommandHandler("help", cmd_help))
        tg_app.add_handler(CommandHandler("about", cmd_about))
        tg_app.add_handler(CommandHandler("privacy", cmd_privacy))
        tg_app.add_handler(CommandHandler("admin", cmd_admin))
        
        # Photo handler
        tg_app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
        
        # Order conversation
        order_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(start_order, pattern="^checkout$")],
            states={
                ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_phone)],
                ASK_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_address)],
                ASK_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_postal)],
                ASK_POSTAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_discount)],
                ASK_DISCOUNT: [MessageHandler(filters.TEXT, ask_notes)],
                ASK_NOTES: [MessageHandler(filters.TEXT, confirm_order)],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_order),
                CommandHandler("start", cmd_start),
            ],
            per_message=False,
            per_chat=True,
        )
        tg_app.add_handler(order_conv)
        
        # Callback query router
        tg_app.add_handler(CallbackQueryHandler(router))
        
        log.info("Bot started successfully")
        await notify_admin("🟢 ربات بازارینو راه‌اندازی شد / Bot avviato")
        
        yield
        
        # Shutdown
        await notify_admin("🔴 ربات در حال خاموش شدن / Bot in arresto")
        await tg_app.shutdown()
        
    except Exception as e:
        log.error(f"Error in lifespan: {e}")
        raise

# Create FastAPI app
app = FastAPI(
    title="Bazarino Bot",
    description="Telegram bot for Iranian grocery shop in Italy",
    version="2.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "bot": "Bazarino",
        "version": "2.0.0",
        "timestamp": dt.datetime.utcnow().isoformat()
    }

@app.get("/health")
async def detailed_health():
    """Detailed health check"""
    try:
        products = await sheets.get_products()
        return {
            "status": "healthy",
            "products": len(products),
            "sheets_connected": True,
            "bot_running": tg_app is not None
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )

@app.post("/webhook/{secret}")
async def webhook(request: Request, secret: str):
    """Telegram webhook endpoint"""
    try:
        if secret != WEBHOOK_SECRET:
            log.warning("Invalid webhook secret received")
            raise HTTPException(status_code=403, detail="Invalid secret")
        
        data = await request.json()
        update = Update.de_json(data, bot)
        
        if not update:
            raise HTTPException(status_code=400, detail="Invalid update")
        
        await tg_app.process_update(update)
        return {"ok": True}
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Main entry point"""
    log.info(f"Starting Bazarino Bot on port {PORT}")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    main()
