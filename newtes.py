
import os
import asyncio
import random
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ==============================
# STM PRO BOT - RANDOM TEST
# ==============================

BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")

SIGNAL_INTERVAL = 10       # সিগন্যালের পর ১০ সেকেন্ড অপেক্ষা
RESULT_DELAY = 15          # রেজাল্টের আগে ১৫ সেকেন্ড অপেক্ষা

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

running_tasks = {}
stats = {}


def get_stats(chat_id):
    if chat_id not in stats:
        stats[chat_id] = {
            "total": 0,
            "profit": 0,
            "loss": 0,
        }
    return stats[chat_id]


async def random_signal_loop(chat_id, bot):
    data = get_stats(chat_id)

    try:
        while True:
            # Random test signal
            direction = random.choice(["CALL 🟢", "PUT 🔴"])
            signal_id = data["total"] + 1
            now = datetime.now().strftime("%H:%M:%S")

            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "🧪 STM PRO BOT — TEST SIGNAL\n\n"
                    f"🔢 Signal: #{signal_id}\n"
                    f"📊 Direction: {direction}\n"
                    f"⏰ Time: {now}\n"
                    "⌛ Expiry: 1 minute\n\n"
                    "⚠️ Random test signal only"
                ),
            )

            # Simulated result, NOT a real market check
            await asyncio.sleep(RESULT_DELAY)

            result = random.choice(["PROFIT", "LOSS"])

            data["total"] += 1

            if result == "PROFIT":
                data["profit"] += 1
                result_text = "✅ PROFIT"
            else:
                data["loss"] += 1
                result_text = "❌ LOSS"

            total = data["total"]
            wins = data["profit"]
            losses = data["loss"]
            win_rate = wins / total * 100 if total else 0

            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "📋 STM PRO — TEST RESULT\n\n"
                    f"🔢 Signal: #{signal_id}\n"
                    f"📊 Direction: {direction}\n"
                    f"🏁 Result: {result_text}\n\n"
                    "📈 TEST STATISTICS\n"
                    f"Total: {total}\n"
                    f"Profit: {wins}\n"
                    f"Loss: {losses}\n"
                    f"Win rate: {win_rate:.1f}%\n\n"
                    "⚠️ This is a random simulation."
                ),
            )

            await asyncio.sleep(SIGNAL_INTERVAL)

    except asyncio.CancelledError:
        logger.info("Test signals stopped for chat %s", chat_id)
        raise

    except Exception:
        logger.exception("Signal loop failed for chat %s", chat_id)
        running_tasks.pop(chat_id, None)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if chat_id in running_tasks and not running_tasks[chat_id].done():
        await update.effective_message.reply_text(
            "🟢 Test signals are already running.\nUse /status to check."
        )
        return

    get_stats(chat_id)

    task = asyncio.create_task(
        random_signal_loop(chat_id, context.bot)
    )
    running_tasks[chat_id] = task

    await update.effective_message.reply_text(
        "🚀 STM PRO RANDOM TEST BOT\n\n"
        "✅ Random CALL / PUT signals\n"
        "✅ Random PROFIT / LOSS results\n"
        "✅ Automatic result messages\n\n"
        "Commands:\n"
        "/start - Start test signals\n"
        "/stop - Stop test signals\n"
        "/status - View statistics\n"
        "/reset - Reset statistics\n\n"
        "⚠️ Results are simulated, not market-based."
    )


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    task = running_tasks.pop(chat_id, None)

    if task and not task.done():
        task.cancel()
        await update.effective_message.reply_text(
            "⏹️ Test signals stopped."
        )
    else:
        await update.effective_message.reply_text(
            "The test bot is not running. Use /start."
        )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    data = get_stats(chat_id)

    total = data["total"]
    wins = data["profit"]
    losses = data["loss"]
    win_rate = wins / total * 100 if total else 0

    task = running_tasks.get(chat_id)
    is_running = task is not None and not task.done()

    await update.effective_message.reply_text(
        "📊 STM PRO TEST STATUS\n\n"
        f"Bot: {'🟢 RUNNING' if is_running else '🔴 STOPPED'}\n"
        f"Total signals completed: {total}\n"
        f"✅ Profit: {wins}\n"
        f"❌ Loss: {losses}\n"
        f"Win rate: {win_rate:.1f}%\n\n"
        "⚠️ Simulated statistics only."
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    stats[chat_id] = {
        "total": 0,
        "profit": 0,
        "loss": 0,
    }

    await update.effective_message.reply_text(
        "♻️ Test statistics reset to zero."
    )


def main():
    if not BOT_TOKEN:
        raise RuntimeError(
            "TG_BOT_TOKEN is missing. Set your Telegram bot token "
            "as an environment variable."
        )

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("reset", reset))

    logger.info("STM Pro Random Test Bot starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
