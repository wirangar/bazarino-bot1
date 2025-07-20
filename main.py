import asyncio
import datetime as dt
import logging
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI, Request, HTTPException
import uvicorn
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
    JobQueue,
    PicklePersistence,
)

from config import TOKEN, BASE_URL, WEBHOOK_SECRET, PORT, ADMIN_ID
from handlers import (
    cmd_start,
    cmd_search,
    cmd_about,
    cmd_privacy,
    handle_photo,
    start_order,
    ask_phone,
    ask_address,
    ask_postal,
    ask_discount,
    ask_notes,
    confirm_order,
    cancel_order,
    handle_cart_buttons,
    router,
    check_order_status,
    send_cart_reminder,
    backup_sheets,
    ASK_NAME,
    ASK_PHONE,
    ASK_ADDRESS,
    ASK_POSTAL,
    ASK_DISCOUNT,
    ASK_NOTES,
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("bazarino.log", maxBytes=5 * 1024 * 1024, backupCount=3),
    ],
)
log = logging.getLogger("bazarino")

tg_app = None
bot = None

async def post_init(app: Application):
    global bot
    bot = app.bot
    log.info("Application initialized")
    webhook_url = f"{BASE_URL}/webhook/{WEBHOOK_SECRET}"
    await app.bot.set_webhook(webhook_url)
    log.info(f"Webhook set to {webhook_url}")

async def post_shutdown(app: Application):
    log.info("Application shutting down")
    try:
        await app.bot.delete_webhook()
    except Exception as e:
        log.error(f"Failed to delete webhook: {e}")

async def lifespan(app: FastAPI):
    global tg_app
    try:
        persistence = PicklePersistence(filepath="bazarino_persistence.pickle")
        builder = (
            ApplicationBuilder()
            .token(TOKEN)
            .persistence(persistence)
            .post_init(post_init)
            .post_shutdown(post_shutdown)
        )
        tg_app = builder.build()

        # Start job queue
        if not tg_app.job_queue:
            tg_app.job_queue = JobQueue()
        await tg_app.initialize()
        await tg_app.job_queue.start()

        # --- Register handlers ---
        tg_app.add_handler(CommandHandler("start", cmd_start))
        tg_app.add_handler(CommandHandler("search", cmd_search))
        tg_app.add_handler(CommandHandler("about", cmd_about))
        tg_app.add_handler(CommandHandler("privacy", cmd_privacy))
        tg_app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

        conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(start_order, pattern="^checkout$")],
            states={
                ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_phone)],
                ASK_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_address)],
                ASK_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_postal)],
                ASK_POSTAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_discount)],
                ASK_DISCOUNT: [MessageHandler(filters.TEXT | filters.COMMAND, ask_notes)],
                ASK_NOTES: [MessageHandler(filters.TEXT | filters.COMMAND, confirm_order)],
            },
            fallbacks=[CommandHandler("cancel", cancel_order)],
            persistent=True,
            name="order_conversation",
        )
        tg_app.add_handler(conv_handler)
        tg_app.add_handler(CallbackQueryHandler(handle_cart_buttons, pattern=r"^(inc_|dec_|del_|add_)"))
        tg_app.add_handler(CallbackQueryHandler(router))

        # --- Schedule jobs ---
        tg_app.job_queue.run_daily(send_cart_reminder, time=dt.time(hour=18, minute=0))
        tg_app.job_queue.run_repeating(check_order_status, interval=600)
        tg_app.job_queue.run_daily(backup_sheets, time=dt.time(hour=0, minute=0))

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
    if secret != WEBHOOK_SECRET:
        log.error("Invalid webhook secret")
        raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        data = await req.json()
        update = Update.de_json(data, tg_app.bot)
        await tg_app.process_update(update)
        return {"ok": True}
    except Exception as e:
        log.error(f"Webhook error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def main():
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
