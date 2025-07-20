import asyncio
import datetime as dt
import json
import logging
import uuid
from difflib import get_close_matches

from telegram import InlineKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import ContextTypes, ConversationHandler
from telegram.error import BadRequest, NetworkError

from config import m, ADMIN_ID, LOW_STOCK_TH
from g_sheets import (
    get_products,
    load_discounts,
    update_stock,
    retry_gspread,
    orders_ws,
    abandoned_cart_ws,
    uploads_ws,
    products_ws,
)
from invoice import generate_invoice
from keyboards import (
    kb_main,
    kb_category,
    kb_product,
    kb_cart,
    kb_support,
    cart_total,
)

log = logging.getLogger("bazarino")

# ───────────── Helpers
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

async def alert_admin(bot, pid, stock):
    if stock <= LOW_STOCK_TH and ADMIN_ID:
        for attempt in range(3):
            try:
                await bot.send_message(ADMIN_ID, f"⚠️ موجودی کم {stock}: {(await get_products())[pid]['fa']}")
                log.info(f"Low stock alert sent for {(await get_products())[pid]['fa']}")
                break
            except Exception as e:
                log.error(f"Alert fail attempt {attempt + 1}: {e}")
                await asyncio.sleep(1)

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
        await alert_admin(update.effective_message.bot, pid, stock)
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

async def increment_item(ctx, pid, update):
    return (await add_cart(ctx, pid, 1, update=update))[0]

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
                await ctx.bot.send_photo(ADMIN_ID, photo=invoice_buffer, caption="\n".join(msg))
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
            await ctx.bot.send_photo(
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
        if ADMIN_ID:
            await context.bot.send_message(ADMIN_ID, f"⚠️ خطا در بررسی وضعیت سفارشات: {e}")

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
        if ADMIN_ID:
            await context.bot.send_message(ADMIN_ID, f"⚠️ خطا در ایجاد بکاپ: {e}")

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
        if ADMIN_ID:
            await context.bot.send_message(ADMIN_ID, f"⚠️ خطا در ارسال یادآور سبد خرید: {e}")

# ───────────── /search
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
        if ADMIN_ID:
            await ctx.bot.send_message(ADMIN_ID, f"⚠️ خطا در /search: {e}")

# ───────────── Commands
async def cmd_start(u, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ctx.user_data["user_id"] = u.effective_user.id
        await u.message.reply_html(m("WELCOME"), reply_markup=await kb_main(ctx))
    except Exception as e:
        log.error(f"Error in cmd_start: {e}")
        await u.message.reply_text("❗️ خطایی در بارگذاری منو رخ داد. لطفاً بعداً امتحان کنید یا با پشتیبانی تماس بگیرید.\nErrore nel caricamento del menu. Riprova più tardi o contatta il supporto.")
        if ADMIN_ID:
            await ctx.bot.send_message(ADMIN_ID, f"⚠️ خطا در /start: {e}")
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
        ok = await increment_item(ctx, pid, update)
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

# ───────────── Router
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
        if ADMIN_ID:
            await ctx.bot.send_message(ADMIN_ID, f"⚠️ خطا در router: {e}")
