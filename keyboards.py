import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from config import m, EMOJI
from g_sheets import get_products

cart_total = lambda c: sum(i["qty"] * i["price"] for i in c)
cart_count = lambda ctx: sum(i["qty"] for i in ctx.user_data.get("cart", []))

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
