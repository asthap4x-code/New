
import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from io import BytesIO

from PIL import Image, ImageDraw, ImageFont
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from pyquotex.stable_api import Quotex


# ============================================================
# STM PRO BOT - SINGLE FILE
# ============================================================

BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
CHANNEL_ID_TEXT = os.getenv("TG_CHANNEL_ID", "").strip()
QX_EMAIL = os.getenv("QX_EMAIL", "").strip()
QX_PASSWORD = os.getenv("QX_PASSWORD", "").strip()

try:
    CHANNEL_ID = int(CHANNEL_ID_TEXT)
except ValueError:
    CHANNEL_ID = 0

HISTORY_FILE = Path("stm_history.json")
STATE_FILE = Path("stm_state.json")

MARKETS = [
    "EURUSD_otc",
    "GBPUSD_otc",
    "USDJPY_otc",
    "AUDCAD_otc",
    "EURJPY_otc",
]

TIMEFRAME = 60
SIGNAL_LEAD_SECONDS = 7
MIN_PAYOUT = 80
MAX_CONSECUTIVE_LOSSES = 3

# Candle-spike filter: latest candle must not be
# unusually large compared with recent candles.
SPIKE_MULTIPLIER = 2.5

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("STM_PRO_BOT")

client = None
scanner_task = None
running = False
mtg_enabled = False
skip_next = set()
loss_streak = {}
market_busy = set()
last_signal_minute = {}
user_settings = {}


# ============================================================
# LOCAL STORAGE
# ============================================================

def load_json(path, default):
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        log.exception("Could not read %s", path)
    return default


def save_json(path, data):
    temp = path.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    temp.replace(path)


history = load_json(HISTORY_FILE, [])
saved_state = load_json(STATE_FILE, {})

loss_streak.update(saved_state.get("loss_streak", {}))
skip_next.update(saved_state.get("skip_next", []))


def persist_state():
    save_json(
        STATE_FILE,
        {
            "loss_streak": loss_streak,
            "skip_next": list(skip_next),
        },
    )


def record_result(item):
    history.append(item)
    save_json(HISTORY_FILE, history)


# ============================================================
# QUOTEX CONNECTION
# ============================================================

async def connect_quotex():
    global client

    client = Quotex(
        email=QX_EMAIL,
        password=QX_PASSWORD,
        lang="en",
    )

    result = await client.connect()

    if isinstance(result, tuple):
        connected = bool(result[0])
    else:
        connected = bool(result)

    if not connected:
        raise ConnectionError("Quotex connection failed")

    log.info("Quotex connected")


async def ensure_connection():
    global client

    if client is None:
        await connect_quotex()
        return

    try:
        check = getattr(client, "check_connect", None)
        if callable(check):
            result = check()
            if asyncio.iscoroutine(result):
                result = await result

            if result:
                return
    except Exception:
        log.warning("Connection check failed; reconnecting")

    await connect_quotex()


# ============================================================
# CANDLE DATA
# ============================================================

def as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_candle(candle):
    if not isinstance(candle, dict):
        return None

    try:
        timestamp = int(
            candle.get("from", candle.get("time", candle.get("timestamp")))
        )
        op = as_float(candle.get("open"))
        cl = as_float(candle.get("close"))
        hi = as_float(candle.get("high"))
        lo = as_float(candle.get("low"))

        if None in (op, cl, hi, lo):
            return None

        return {
            "time": timestamp,
            "open": op,
            "close": cl,
            "high": hi,
            "low": lo,
        }
    except (TypeError, ValueError):
        return None


async def get_candles(market, count=30):
    await ensure_connection()

    raw = await client.get_candles(
        market,
        int(time.time()),
        count * TIMEFRAME,
        TIMEFRAME,
    )

    if isinstance(raw, dict):
        raw = raw.get("candles", raw.get("data", []))

    if not isinstance(raw, (list, tuple)):
        return []

    result = []
    for item in raw:
        candle = normalize_candle(item)
        if candle:
            result.append(candle)

    result.sort(key=lambda x: x["time"])

    # Remove duplicate timestamps.
    unique = {}
    for candle in result:
        unique[candle["time"]] = candle

    return list(unique.values())


# ============================================================
# PAYOUT VERIFICATION
# ============================================================

def normalize_market_name(name):
    return str(name).lower().replace("/", "").replace("-", "").strip()


def find_market_payout(data, market):
    """
    Return a payout only when the API provides a value
    specifically matching the requested market.
    Never invent a payout.
    """
    wanted = normalize_market_name(market)

    if not isinstance(data, dict):
        return None

    for key, value in data.items():
        if normalize_market_name(key) != wanted:
            continue

        if isinstance(value, dict):
            for field in ("payout", "payment", "profit", "percent"):
                if field in value:
                    value = value[field]
                    break

        number = as_float(value)
        if number is None:
            return None

        if 0 < number <= 1:
            number *= 100

        if 0 < number <= 100:
            return number

    return None


async def get_verified_payout(market):
    """
    API return formats vary between PyQuotex versions.
    If the library does not return a market-specific payout,
    the market is skipped.
    """
    await ensure_connection()

    for method_name in ("get_payment", "get_payout"):
        method = getattr(client, method_name, None)

        if not callable(method):
            continue

        try:
            data = method()
            if asyncio.iscoroutine(data):
                data = await data

            payout = find_market_payout(data, market)
            if payout is not None:
                return payout

        except Exception:
            log.exception("Payout lookup failed for %s", market)

    return None


# ============================================================
# MARKET FILTERS AND SIGNAL DIRECTION
# ============================================================

def is_abnormal_market(candles):
    if len(candles) < 8:
        return True, "Not enough candle data"

    # Use completed candles only.
    completed = candles[:-1]
    if len(completed) < 6:
        return True, "Not enough completed candles"

    ranges = [
        max(c["high"] - c["low"], 0)
        for c in completed[-7:-1]
    ]

    latest_range = max(
        completed[-1]["high"] - completed[-1]["low"],
        0,
    )

    average_range = sum(ranges) / len(ranges) if ranges else 0

    if average_range <= 0:
        return True, "Invalid recent volatility"

    if latest_range > average_range * SPIKE_MULTIPLIER:
        return True, "Abnormal candle movement"

    return False, ""


def choose_direction(candles):
    """
    Simple momentum rule, not a guaranteed prediction:
    compare the last two completed candle bodies.
    """
    completed = candles[:-1]

    if len(completed) < 3:
        return None

    first = completed[-2]
    second = completed[-1]

    body1 = first["close"] - first["open"]
    body2 = second["close"] - second["open"]

    if body1 > 0 and body2 > 0:
        return "CALL"

    if body1 < 0 and body2 < 0:
        return "PUT"

    return None


# ============================================================
# CHART IMAGE WITH CENTER WATERMARK
# This is a generated candle chart, not a Quotex screenshot.
# ============================================================

def get_font(size, bold=False):
    candidates = (
        ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]
        if bold
        else ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    )

    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            pass

    return ImageFont.load_default()


def make_chart(market, candles, direction, target_time):
    width, height = 1000, 600
    image = Image.new("RGB", (width, height), "#101722")
    draw = ImageDraw.Draw(image)

    font = get_font(26, True)
    small = get_font(17)
    watermark_font = get_font(46, True)

    draw.text((35, 22), "STM PRO BOT", font=font, fill="#FFFFFF")
    draw.text(
        (35, 65),
        f"{market}  |  M1  |  {direction}",
        font=small,
        fill="#AFC0D6",
    )
    draw.text(
        (35, 94),
        "GENERATED CANDLE CHART - NOT A PLATFORM SCREENSHOT",
        font=small,
        fill="#8795A8",
    )

    chart = candles[-20:]
    if not chart:
        return image

    top, bottom = 150, 525
    left, right = 55, 945

    for i in range(6):
        y = top + i * (bottom - top) / 5
        draw.line((left, y, right, y), fill="#253345", width=1)

    low = min(c["low"] for c in chart)
    high = max(c["high"] for c in chart)
    span = max(high - low, 0.0000001)

    def y_price(price):
        return bottom - (price - low) / span * (bottom - top)

    step = (right - left) / len(chart)
    candle_width = max(7, int(step * 0.48))

    for index, candle in enumerate(chart):
        x = left + step * (index + 0.5)
        y_open = y_price(candle["open"])
        y_close = y_price(candle["close"])
        y_high = y_price(candle["high"])
        y_low = y_price(candle["low"])

        rising = candle["close"] >= candle["open"]
        color = "#20D6A0" if rising else "#FF5E75"

        draw.line((x, y_high, x, y_low), fill=color, width=2)
        draw.rectangle(
            (
                x - candle_width / 2,
                min(y_open, y_close),
                x + candle_width / 2,
                max(y_open, y_close) + 1,
            ),
            fill=color,
        )

    # Semi-transparent centered watermark.
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    box = overlay_draw.textbbox(
        (0, 0),
        "STM PRO BOT",
        font=watermark_font,
    )
    text_width = box[2] - box[0]
    text_height = box[3] - box[1]

    overlay_draw.text(
        (
            (width - text_width) / 2,
            (height - text_height) / 2,
        ),
        "STM PRO BOT",
        font=watermark_font,
        fill=(255, 255, 255, 80),
        stroke_width=1,
        stroke_fill=(0, 0, 0, 40),
    )

    image = Image.alpha_composite(image.convert("RGBA"), overlay)
    return image.convert("RGB")


async def send_chart(bot, market, candles, direction, target_time):
    image = make_chart(market, candles, direction, target_time)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)

    await bot.send_photo(
        chat_id=CHANNEL_ID,
        photo=buffer,
        caption=f"STM PRO BOT | {market}",
    )


# ============================================================
# TELEGRAM MESSAGES
# ============================================================

def format_time(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")


async def post_signal(bot, market, direction, payout, target_time, mtg=False):
    label = "MTG 1" if mtg else "NORMAL SIGNAL"

    text = (
        "🚀 STM PRO BOT\n\n"
        f"📊 Market: {market}\n"
        f"⏱ Timeframe: 1 Minute\n"
        f"🎯 Direction: {'🟢 CALL ⬆️' if direction == 'CALL' else '🔴 PUT ⬇️'}\n"
        f"💰 Verified payout: {payout:.0f}%\n"
        f"🕒 Target candle: {format_time(target_time)}\n"
        f"📌 Type: {label}\n\n"
        "⚠️ Signal only; not a guarantee of profit."
    )

    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
    )


async def post_result(
    bot,
    market,
    direction,
    payout,
    target_time,
    opening,
    closing,
    outcome,
    mtg=False,
):
    change = closing - opening
    label = "MTG 1 RESULT" if mtg else "SIGNAL RESULT"

    if outcome == "WIN":
        emoji = "✅"
    elif outcome == "LOSS":
        emoji = "❌"
    else:
        emoji = "➖"

    text = (
        f"📊 STM PRO BOT — {label}\n\n"
        f"Market: {market}\n"
        f"Direction: {direction}\n"
        f"Payout: {payout:.0f}%\n"
        f"Target candle: {format_time(target_time)}\n"
        f"Opening price: {opening:.8f}\n"
        f"Closing price: {closing:.8f}\n"
        f"Price change: {change:+.8f}\n\n"
        f"{emoji} Result: {outcome}"
    )

    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
    )


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def calculate_result(direction, opening, closing):
    if closing == opening:
        return "DRAW"

    if direction == "CALL":
        return "WIN" if closing > opening else "LOSS"

    return "WIN" if closing < opening else "LOSS"


async def wait_until(timestamp):
    while True:
        remaining = timestamp - time.time()
        if remaining <= 0:
            return
        await asyncio.sleep(min(remaining, 0.5))


async def evaluate_signal(
    app,
    market,
    direction,
    payout,
    target_time,
    mtg=False,
):
    """
    Evaluate the exact target candle by timestamp.
    Do not substitute a different candle if data is missing.
    """
    await wait_until(target_time + TIMEFRAME + 2)

    try:
        candles = await get_candles(market, 20)
        candle = next(
            (c for c in candles if c["time"] == target_time),
            None,
        )

        if candle is None:
            log.warning(
                "Target candle missing: %s at %s",
                market,
                target_time,
            )
            await app.bot.send_message(
                chat_id=CHANNEL_ID,
                text=(
                    "⚠️ STM PRO BOT\n"
                    f"Market: {market}\n"
                    f"Target: {format_time(target_time)}\n"
                    "Result unavailable: exact target candle not found."
                ),
            )
            return None

        opening = candle["open"]
        closing = candle["close"]
        outcome = calculate_result(direction, opening, closing)

        await post_result(
            app.bot,
            market,
            direction,
            payout,
            target_time,
            opening,
            closing,
            outcome,
            mtg=mtg,
        )

        record_result(
            {
                "market": market,
                "direction": direction,
                "payout": payout,
                "target_time": target_time,
                "open": opening,
                "close": closing,
                "result": outcome,
                "mtg": mtg,
            }
        )

        return outcome

    except Exception:
        log.exception("Result evaluation failed for %s", market)
        return None


# ============================================================
# MTG HANDLING
# ============================================================

async def process_market_signal(app, market, candles, payout, target_time):
    if market in market_busy:
        return

    market_busy.add(market)

    try:
        # Skip one signal after a failed MTG.
        if market in skip_next:
            skip_next.discard(market)
            persist_state()
            log.info("Skipping one signal for %s", market)
            return

        if loss_streak.get(market, 0) >= MAX_CONSECUTIVE_LOSSES:
            log.info("Market paused after loss streak: %s", market)
            return

        direction = choose_direction(candles)
        if direction is None:
            log.info("No clear direction for %s", market)
            return

        await post_signal(
            app.bot,
            market,
            direction,
            payout,
            target_time,
            mtg=False,
        )

        try:
            await send_chart(
                app.bot,
                market,
                candles,
                direction,
                target_time,
            )
        except Exception:
            log.exception("Chart generation/post failed")

        first_result = await evaluate_signal(
            app,
            market,
            direction,
            payout,
            target_time,
            mtg=False,
        )

        if first_result == "WIN":
            loss_streak[market] = 0
            persist_state()
            return

        if first_result == "DRAW":
            return

        if first_result != "LOSS":
            return

        loss_streak[market] = loss_streak.get(market, 0) + 1
        persist_state()

        # MTG is selected by the user in Telegram.
        if not mtg_enabled:
            return

        # Never issue an already-started candle as a fresh signal.
        # Schedule MTG for the next complete candle, seven seconds
        # before it begins.
        next_target = (int(time.time()) // TIMEFRAME + 2) * TIMEFRAME
        await wait_until(next_target - SIGNAL_LEAD_SECONDS)

        fresh_candles = await get_candles(market, 30)
        if len(fresh_candles) < 8:
            return

        abnormal, _reason = is_abnormal_market(fresh_candles)
        if abnormal:
            return

        mtg_payout = await get_verified_payout(market)
        if mtg_payout is None or mtg_payout < MIN_PAYOUT:
            return

        mtg_direction = choose_direction(fresh_candles)
        if mtg_direction is None:
            return

        await post_signal(
            app.bot,
            market,
            mtg_direction,
            mtg_payout,
            next_target,
            mtg=True,
        )

        try:
            await send_chart(
                app.bot,
                market,
                fresh_candles,
                mtg_direction,
                next_target,
            )
        except Exception:
            log.exception("MTG chart failed")

        mtg_result = await evaluate_signal(
            app,
            market,
            mtg_direction,
            mtg_payout,
            next_target,
            mtg=True,
        )

        if mtg_result == "WIN":
            loss_streak[market] = 0

        elif mtg_result == "LOSS":
            loss_streak[market] = loss_streak.get(market, 0) + 1
            skip_next.add(market)

        persist_state()

    except Exception:
        log.exception("Signal processing failed for %s", market)

    finally:
        market_busy.discard(market)


# ============================================================
# MAIN SCANNER
# ============================================================

async def scanner_loop(app):
    global running

    log.info("Scanner started")

    while running:
        try:
            # Scan at second 53, seven seconds before the next minute.
            now = time.time()
            minute_start = int(now // TIMEFRAME) * TIMEFRAME
            scan_time = minute_start + TIMEFRAME - SIGNAL_LEAD_SECONDS

            if now >= scan_time:
                scan_time += TIMEFRAME

            await wait_until(scan_time)

            if not running:
                break

            target_time = int(time.time() // TIMEFRAME + 1) * TIMEFRAME
            scan_minute = int(scan_time // TIMEFRAME)

            for market in MARKETS:
                if not running:
                    break

                if last_signal_minute.get(market) == scan_minute:
                    continue

                last_signal_minute[market] = scan_minute

                try:
                    if loss_streak.get(market, 0) >= MAX_CONSECUTIVE_LOSSES:
                        continue

                    payout = await get_verified_payout(market)
                    if payout is None:
                        log.info("No verified payout for %s; skipping", market)
                        continue

                    if payout < MIN_PAYOUT:
                        log.info(
                            "Payout below %s%%: %s = %s",
                            MIN_PAYOUT,
                            market,
                            payout,
                        )
                        continue

                    candles = await get_candles(market, 30)

                    if len(candles) < 8:
                        continue

                    # At scan time, the latest candle may be incomplete.
                    # Volatility filter uses completed candles.
                    abnormal, reason = is_abnormal_market(candles)
                    if abnormal:
                        log.info("Skipping %s: %s", market, reason)
                        continue

                    # Run result tracking separately so the scanner
                    # can keep its minute schedule.
                    asyncio.create_task(
                        process_market_signal(
                            app,
                            market,
                            candles,
                            payout,
                            target_time,
                        )
                    )

                except Exception:
                    log.exception("Scan failed for %s", market)

        except asyncio.CancelledError:
            break
        except Exception:
            log.exception("Scanner loop error")
            await asyncio.sleep(2)

    log.info("Scanner stopped")


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("YES — MTG 1 ON", callback_data="mtg_yes"),
            InlineKeyboardButton("NO — MTG 1 OFF", callback_data="mtg_no"),
        ],
    ]

    await update.effective_message.reply_text(
        "🚀 STM PRO BOT\n\n"
        "MTG 1 চালু রাখতে চাস?\n"
        "YES অথবা NO নির্বাচন কর।\n\n"
        "সিগন্যাল পেতে এরপর Start Scanner চাপতে হবে।",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    global mtg_enabled, running, scanner_task

    query = update.callback_query
    await query.answer()

    if query.data == "mtg_yes":
        mtg_enabled = True
    elif query.data == "mtg_no":
        mtg_enabled = False
    elif query.data == "scanner_start":
        if running:
            await query.edit_message_text("Scanner is already running.")
            return

        running = True
        scanner_task = asyncio.create_task(
            scanner_loop(context.application)
        )

        await query.edit_message_text(
            "✅ STM PRO BOT scanner started.\n"
            f"MTG 1: {'ON' if mtg_enabled else 'OFF'}\n"
            f"Minimum payout: {MIN_PAYOUT}%"
        )
        return

    elif query.data == "scanner_stop":
        running = False
        await query.edit_message_text("⏹ Scanner stop requested.")
        return

    keyboard = [
        [
            InlineKeyboardButton(
                "▶️ Start Scanner",
                callback_data="scanner_start",
            ),
        ],
    ]

    await query.edit_message_text(
        f"MTG 1: {'ON' if mtg_enabled else 'OFF'}\n\n"
        "নিচের বাটনে চাপ দিয়ে স্ক্যানার চালু কর।",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def startscan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    global running, scanner_task

    if running:
        await update.effective_message.reply_text(
            "Scanner is already running."
        )
        return

    running = True
    scanner_task = asyncio.create_task(
        scanner_loop(context.application)
    )

    await update.effective_message.reply_text(
        f"✅ Scanner started.\n"
        f"MTG 1: {'ON' if mtg_enabled else 'OFF'}"
    )


async def stop_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    global running
    running = False
    await update.effective_message.reply_text(
        "⏹ Scanner stop requested."
    )


async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    total = len(history)
    wins = sum(1 for h in history if h.get("result") == "WIN")
    losses = sum(1 for h in history if h.get("result") == "LOSS")
    draws = sum(1 for h in history if h.get("result") == "DRAW")

    await update.effective_message.reply_text(
        "📊 STM PRO BOT STATUS\n\n"
        f"Scanner: {'RUNNING' if running else 'STOPPED'}\n"
        f"MTG 1: {'ON' if mtg_enabled else 'OFF'}\n"
        f"Recorded results: {total}\n"
        f"WINS: {wins}\n"
        f"LOSSES: {losses}\n"
        f"DRAWS: {draws}\n"
        f"Minimum payout: {MIN_PAYOUT}%"
    )


async def history_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    recent = history[-10:]

    if not recent:
        await update.effective_message.reply_text(
            "এখনও কোনো রেজাল্ট হিস্টোরিতে নেই।"
        )
        return

    lines = ["📜 STM PRO BOT — Recent Results"]

    for item in reversed(recent):
        stamp = format_time(item.get("target_time", time.time()))
        lines.append(
            f"{stamp} | {item.get('market')} | "
            f"{item.get('direction')} | {item.get('result')}"
        )

    await update.effective_message.reply_text("\n".join(lines))


async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    log.error(
        "Telegram handler error",
        exc_info=context.error,
    )


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

async def post_init(app: Application):
    log.info("Telegram bot started")


async def post_shutdown(app: Application):
    global running

    running = False

    if client is not None:
        try:
            close_method = getattr(client, "close", None)
            if callable(close_method):
                result = close_method()
                if asyncio.iscoroutine(result):
                    await result
        except Exception:
            log.exception("Quotex close failed")


def main():
    if not BOT_TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN environment variable")

    if not CHANNEL_ID:
        raise SystemExit("Missing or invalid TG_CHANNEL_ID")

    if not QX_EMAIL or not QX_PASSWORD:
        raise SystemExit("Missing QX_EMAIL or QX_PASSWORD")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("startscan", startscan_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(error_handler)

    log.info("Starting STM PRO BOT")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
