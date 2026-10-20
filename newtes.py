
import os
import time
import random
import asyncio
import logging
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from pyquotex.stable_api import Quotex

# ================ CONFIG ================

BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "8994179085:AAFO4KWMe4hW7j4ey8WKwTh-7PuTVPflR6E")
QX_EMAIL = os.getenv("QX_EMAIL", "janinakihoy00@gmail.com")
QX_PASSWORD = os.getenv("QX_PASSWORD", "71&@$@HwvIvw&$#")

EXPIRY = 60
RESULT_GRACE = 3

MARKETS = [
    "EURUSD_otc",
    "GBPUSD_otc",
    "USDJPY_otc",
    "AUDCAD_otc",
    "EURJPY_otc",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("STM")

qx = None
qx_lock = asyncio.Lock()

selected = {}
tasks = {}
stats = {}


def get_stats(chat_id):
    return stats.setdefault(
        chat_id,
        {"total": 0, "win": 0, "loss": 0, "draw": 0, "unavailable": 0},
    )


def market_keyboard(chat_id):
    chosen = selected.setdefault(chat_id, ["EURUSD_otc"])
    rows = []

    for market in MARKETS:
        prefix = "✅" if market in chosen else "▫️"
        rows.append([
            InlineKeyboardButton(
                f"{prefix} {market}",
                callback_data=f"market:{market}",
            )
        ])

    rows.append([
        InlineKeyboardButton("▶️ Start", callback_data="start"),
        InlineKeyboardButton("⏹ Stop", callback_data="stop"),
    ])
    rows.append([
        InlineKeyboardButton("📊 Status", callback_data="status"),
    ])

    return InlineKeyboardMarkup(rows)


async def connect_qx():
    global qx

    if not QX_EMAIL or not QX_PASSWORD:
        return False, "Set QX_EMAIL and QX_PASSWORD first."

    try:
        if qx is not None:
            try:
                if qx.check_connect():
                    return True, "Connected"
            except Exception:
                pass

        qx = Quotex(
            email=QX_EMAIL,
            password=QX_PASSWORD,
            lang="en",
        )
        qx.set_account_mode("PRACTICE")

        ok, reason = await qx.connect()
        return bool(ok), str(reason)

    except Exception as exc:
        log.exception("Quotex connection failed")
        return False, str(exc)


def normalize_candles(response):
    if isinstance(response, dict):
        response = response.get("data", [])

    if not isinstance(response, (list, tuple)):
        return []

    candles = []

    for item in response:
        if not isinstance(item, dict):
            continue

        try:
            candles.append({
                "time": int(float(item["time"])),
                "open": float(item["open"]),
                "close": float(item["close"]),
            })
        except (KeyError, TypeError, ValueError):
            continue

    return candles


async def get_exact_candle(market, candle_start):
    # Use a single QX request at a time.
    async with qx_lock:
        if qx is None or not qx.check_connect():
            ok, reason = await connect_qx()
            if not ok:
                raise ConnectionError(reason)

        response = await qx.get_candles(
            market,
            time.time(),
            900,
            60,
        )

    candles = normalize_candles(response)

    # Never substitute a different minute's candle.
    for candle in candles:
        if candle["time"] == candle_start:
            return candle

    return None


async def verify_result(market, direction, candle_start):
    # The selected candle closes at candle_start + 60.
    wait = candle_start + EXPIRY + RESULT_GRACE - time.time()
    if wait > 0:
        await asyncio.sleep(wait)

    for attempt in range(5):
        try:
            candle = await get_exact_candle(market, candle_start)

            if candle is not None:
                if candle["close"] > candle["open"]:
                    actual_direction = "CALL"
                elif candle["close"] < candle["open"]:
                    actual_direction = "PUT"
                else:
                    return "DRAW"

                return (
                    "WIN"
                    if actual_direction == direction
                    else "LOSS"
                )

        except Exception:
            log.exception("Could not verify %s candle", market)

        if attempt < 4:
            await asyncio.sleep(2)

    return "UNAVAILABLE"


async def signal_loop(chat_id, bot):
    data = get_stats(chat_id)

    try:
        while True:
            # Signal times: 04:20, 04:22, 04:24, ...
            # Align all signals to an even minute.
            now = time.time()
            current_minute = int(now // 60)
            next_even_minute = ((current_minute // 2) + 1) * 2
            signal_start = next_even_minute * 60

            await asyncio.sleep(max(0, signal_start - time.time()))

            markets = selected.get(chat_id, [])
            if not markets:
                await bot.send_message(
                    chat_id,
                    "⚠️ No market selected. Use /market.",
                )
                break

            # Record the precise start of this signal's candle.
            candle_start = int(time.time() // 60) * 60

            # If the scheduled start was missed by over 2 seconds,
            # skip instead of associating the signal with the wrong candle.
            if time.time() - candle_start > 2:
                continue

            market = random.choice(markets)
            direction = random.choice(["CALL", "PUT"])
            signal_id = data["total"] + data["unavailable"] + 1

            candle_time = datetime.fromtimestamp(
                candle_start
            ).strftime("%H:%M:%S")

            await bot.send_message(
                chat_id,
                "🚀 STM PRO | RANDOM SIGNAL\n\n"
                f"🔢 Signal: #{signal_id}\n"
                f"📊 Market: {market}\n"
                f"🎯 Direction: {'🟢 CALL' if direction == 'CALL' else '🔴 PUT'}\n"
                "⏱ Expiry: 1 minute\n"
                f"🕒 Candle start: {candle_time}\n\n"
                "📡 Checking this exact QX candle after 1 minute.",
            )

            # Wait for the exact signal candle to close and verify it.
            result = await verify_result(
                market,
                direction,
                candle_start,
            )

            if result == "UNAVAILABLE":
                data["unavailable"] += 1
                result_line = "⚠️ UNAVAILABLE — exact candle not found"
            else:
                data["total"] += 1
                data[result.lower()] += 1
                result_line = {
                    "WIN": "✅ WIN",
                    "LOSS": "❌ LOSS",
                    "DRAW": "➖ DRAW",
                }[result]

            total = data["total"]
            win_rate = data["win"] / total * 100 if total else 0

            await bot.send_message(
                chat_id,
                "📋 STM PRO | QX CANDLE RESULT\n\n"
                f"🔢 Signal: #{signal_id}\n"
                f"📊 Market: {market}\n"
                f"🎯 Signal: {direction}\n"
                f"🏁 Result: {result_line}\n"
                f"🕒 Candle: {candle_time} – "
                f"{datetime.fromtimestamp(candle_start + 60).strftime('%H:%M:%S')}\n\n"
                f"📈 Verified: {total}\n"
                f"✅ WIN: {data['win']}\n"
                f"❌ LOSS: {data['loss']}\n"
                f"➖ DRAW: {data['draw']}\n"
                f"⚠️ Unavailable: {data['unavailable']}\n"
                f"🎯 Win rate: {win_rate:.1f}%\n\n"
                "Result compares the random direction with the "
                "matching QX candle's open/close direction. "
                "It is not a trade receipt.",
            )

            # Next signal is on the next even-minute boundary.
            # For 04:20 signal, the next one is 04:22.

    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Signal loop failed")
        await bot.send_message(
            chat_id,
            "⚠️ Signal loop stopped. Check Termux logs.",
        )
    finally:
        tasks.pop(chat_id, None)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    selected.setdefault(chat_id, ["EURUSD_otc"])

    await update.effective_message.reply_text(
        "🚀 STM PRO QX TEST BOT\n\n"
        "Random CALL / PUT\n"
        "Exact 1-minute QX candle verification\n"
        "Signal schedule: every 2 minutes\n"
        "Result check: 1 minute after signal\n\n"
        "Choose your markets:",
        reply_markup=market_keyboard(chat_id),
    )


async def market_command(update, context):
    chat_id = update.effective_chat.id
    selected.setdefault(chat_id, ["EURUSD_otc"])

    await update.effective_message.reply_text(
        "Select one or more markets:",
        reply_markup=market_keyboard(chat_id),
    )


async def stop_task(chat_id):
    task = tasks.get(chat_id)
    if task and not task.done():
        task.cancel()
        return True
    return False


async def status_message(chat_id, bot):
    data = get_stats(chat_id)
    total = data["total"]
    rate = data["win"] / total * 100 if total else 0
    task = tasks.get(chat_id)

    await bot.send_message(
        chat_id,
        "📊 STM PRO STATUS\n\n"
        f"Running: {'YES' if task and not task.done() else 'NO'}\n"
        f"Markets: {', '.join(selected.get(chat_id, []))}\n"
        f"Verified: {total}\n"
        f"WIN: {data['win']}\n"
        f"LOSS: {data['loss']}\n"
        f"DRAW: {data['draw']}\n"
        f"Unavailable: {data['unavailable']}\n"
        f"Win rate: {rate:.1f}%",
    )


async def callback(update, context):
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    selected.setdefault(chat_id, ["EURUSD_otc"])
    data = get_stats(chat_id)
    action = query.data

    if action.startswith("market:"):
        market = action.split(":", 1)[1]
        chosen = selected[chat_id]

        if market in chosen:
            chosen.remove(market)
        else:
            chosen.append(market)

        await query.edit_message_reply_markup(
            reply_markup=market_keyboard(chat_id)
        )
        return

    if action == "start":
        if not selected[chat_id]:
            await query.message.reply_text(
                "Select at least one market first."
            )
            return

        task = tasks.get(chat_id)
        if task and not task.done():
            await query.message.reply_text("Already running.")
            return

        await query.message.reply_text("Connecting to QX...")
        ok, reason = await connect_qx()

        if not ok:
            await query.message.reply_text(
                f"QX connection failed: {reason}"
            )
            return

        tasks[chat_id] = asyncio.create_task(
            signal_loop(chat_id, context.bot)
        )
        await query.message.reply_text(
            "✅ Connected. Signals will follow the 2-minute schedule."
        )

    elif action == "stop":
        stopped = await stop_task(chat_id)
        await query.message.reply_text(
            "⏹ Stop requested." if stopped else "Not running."
        )

    elif action == "status":
        await status_message(chat_id, context.bot)


async def stop_command(update, context):
    stopped = await stop_task(update.effective_chat.id)
    await update.effective_message.reply_text(
        "⏹ Stop requested." if stopped else "Not running."
    )


async def status_command(update, context):
    await status_message(update.effective_chat.id, context.bot)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN is not set.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("market", market_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CallbackQueryHandler(callback))

    log.info("STM Pro starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
