import html
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from great_expectation import run_checks

import os
import dotenv

dotenv.load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hello! I'm the Data Quality Bot.\n"
        "Send /check_quality to run data quality checks."
    )

async def check_quality(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Running data quality checks, please wait...")
    try:
        report = run_checks()
        
        safe_report = html.escape(report)
        formatted_message = f"<b>Data Quality Report</b>\n<pre>{safe_report}</pre>"
        
        await update.message.reply_text(
            formatted_message,
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error running checks: {str(e)}")

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check_quality", check_quality))
    print("Bot is running...")
    app.run_polling()