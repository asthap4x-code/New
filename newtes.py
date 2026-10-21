
import asyncio
import json
import logging
import os
import time
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from pyquotex.stable_api import Quotex

load_dotenv()

# =========================================================
# STM PRO BOT - SINGLE FILE, NO SCREENSHOTS
# =========================================================

BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
CHANNEL_TEXT = os.getenv("TG_CHANNEL_ID", "").strip()
QX_EMAIL = os.getenv("QX_EMAIL", "").strip()
QX_PASSWORD = os.getenv("QX_PASSWORD", "").strip()

try:
    CHANNEL_ID = int(CHANNEL_TEXT)
except ValueError:
    CHANNEL_ID = 0

TIMEFRAME = 60
MIN_PAYOUT = 80
MIN_TREND_CANDLES = 3
MAX_TREND_CANDLES = 7
MAX_LOSSES = 3
SIGNAL_LEAD = 7
SPIKE_MULTIPLIER = 2.5
GAP_MULTIPLIER = 0.5

# Used only if the installed API cannot provide an asset list.
FALLBACK_MARKETS = [
    "EURUSD_otc",
    "GBPUSD_otc",
    "USDJPY_otc",
    "AUDCAD_otc",
    "EURJPY_otc",
]

BASE_DIR = Path(__file__).resolve().parent
HISTORY_FILE = BASE_DIR / "stm_history.json"
STATE_FILE = BASE_DIR / "stm_state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("STM_PRO")

qx = None
connected = False
running = False
scanner_task = None
mtg_enabled = False

available_markets = []
selected_markets = set()
market_busy = set()
last_processed_candle = {}
trend_state = {}
loss_streak = {}
skip_next = set()
active_users = set()


# =========================================================
# LOCAL STORAGE
# =========================================================

def read_json(path, default):
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        log.exception("Failed reading %s", path)
    return default


def write_json(path, value):
    temp = path.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
    temp.replace(path)


history = read_json(HISTORY_FILE, [])
saved = read_json(STATE_FILE, {})

loss_streak.update(saved.get("loss_streak", {}))
skip_next.update(saved.get("skip_next", []))
trend_state.update(saved.get("trend_state", {}))
selected_markets.update(saved.get("selected_markets", []))


def save_state():
    write_json(
        STATE_FILE,
        {
            "loss_streak": loss_streak,
            "skip_next": sorted(skip_next),
            "trend_state": trend_state,
            "selected_markets": sorted(selected_markets),
        },
    )


def save_result(record):
    history.append(record)
    write_json(HISTORY_FILE, history)


# =========================================================
# QUOTEX CONNECTION
# =========================================================

async def call_api(obj, method_name, *args, **kwargs):
    method = getattr(obj, method_name, None)
    if not callable(method):
        return None

    result = method(*args, **kwargs)
    if asyncio.iscoroutine(result):
        result = await result
    return result


async def connect():
    global qx, connected

    qx = Quotex(
        email=QX_EMAIL,
        password=QX_PASSWORD,
        lang="en",
    )

    result = await qx.connect()

    if isinstance(result, tuple):
        connected = bool(result[0])
    else:
        connected = bool(result)

    if not connected:
        raise ConnectionError("Quotex connection failed")

    log.info("Connected to Quotex")


async def ensure_connection():
    global connected

    if qx is None:
        await connect()
        return

    try:
        status = await call_api(qx, "check_connect")
        if status is not False and status is not None:
            connected = True
            return
    except Exception:
        log.warning("Connection check failed")

    connected = False
    await connect()


# =========================================================
# ASSET / MARKET DISCOVERY
# =========================================================

def clean_asset_name(value):
    return str(value).strip()


def extract_assets(data):
    result = []

    if isinstance(data, dict):
        for name, info in data.items():
            if isinstance(info, dict):
                if info.get("enabled") is False:
                    continue
                if info.get("is_open") is False:
                    continue
                if info.get("open") is False:
                    continue
            result.append(clean_asset_name(name))

    elif isinstance(data, (list, tuple, set)):
        for item in data:
            if isinstance(item, str):
                result.append(clean_asset_name(item))
            elif isinstance(item, dict):
                name = (
                    item.get("name")
                    or item.get("asset")
                    or item.get("symbol")
                )
                if not name:
                    continue
                if item.get("enabled") is False:
                    continue
                if item.get("is_open") is False:
                    continue
                if item.get("open") is False:
                    continue
                result.append(clean_asset_name(name))

    return sorted(set(x for x in result if x))


async def discover_markets():
    await ensure_connection()

    # Method names vary between PyQuotex versions.
    for method_name in (
        "get_all_assets",
        "get_available_asset",
        "get_assets",
    ):
        try:
            data = await call_api(qx, method_name)
            assets = extract_assets(data)
            if assets:
                log.info(
                    "Discovered %s assets via %s",
                    len(assets),
                    method_name,
                )
                return assets
        except Exception:
            log.exception("Asset discovery failed: %s", method_name)

    log.warning(
        "Asset discovery unavailable; using fallback list. "
        "Update PyQuotex adapter if needed."
    )
    return FALLBACK_MARKETS.copy()


# =========================================================
# CANDLE DATA
# =========================================================

def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_candle(item):
    if not isinstance(item, dict):
        return None

    try:
        stamp = int(
            item.get(
                "from",
                item.get("time", item.get("timestamp")),
            )
        )
        op = number(item.get("open"))
        cl = number(item.get("close"))
        hi = number(item.get("high"))
        lo = number(item.get("low"))

        if None in (op, cl, hi, lo):
            return None

        return {
            "time": stamp,
            "open": op,
            "close": cl,
            "high": hi,
            "low": lo,
        }
    except (TypeError, ValueError):
        return None


async def get_candles(market, count=30):
    await ensure_connection()

    raw = await call_api(
        qx,
        "get_candles",
        market,
        int(time.time()),
        count * TIMEFRAME,
        TIMEFRAME,
    )

    if isinstance(raw, dict):
        raw = raw.get("candles", raw.get("data", []))

    if not isinstance(raw, (list, tuple)):
        return []

    by_time = {}
    for item in raw:
        candle = normalize_candle(item)
        if candle:
            by_time[candle["time"]] = candle

    return sorted(by_time.values(), key=lambda x: x["time"])


# =========================================================
# VERIFIED PAYOUT
# =========================================================

def normalize_name(name):
    return (
        str(name)
        .lower()
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )


def parse_payout_value(value):
    if isinstance(value, dict):
        for field in ("payout", "payment", "profit", "percent"):
            if field in value:
                value = value[field]
                break

    value = number(value)
    if value is None:
        return None

    if 0 < value <= 1:
        value *= 100

    if 0 < value <= 100:
        return value

    return None


def market_payout_from_data(data, market):
    wanted = normalize_name(market)

    if not isinstance(data, dict):
        return None

    for name, value in data.items():
        if normalize_name(name) == wanted:
            return parse_payout_value(value)

    return None


async def get_payout(market):
    await ensure_connection()

    for method_name in ("get_payment", "get_payout"):
        try:
            data = await call_api(qx, method_name)
            payout = market_payout_from_data(data, market)
            if payout is not None:
                return payout
        except Exception:
            log.exception("Payout lookup failed for %s", market)

    # Do not invent payout if the API does not provide it.
    return None


# =========================================================
# TREND ENGINE
# =========================================================

def candle_direction(candle):
    if candle["close"] > candle["open"]:
        return "UP"
    if candle["close"] < candle["open"]:
        return "DOWN"
    return "FLAT"


def initialize_trend(candles):
    """
    Find at least 3 consecutive candles in the same direction,
    using at most the most recent 7 completed candles.
    """
    recent = candles[-MAX_TREND_CANDLES:]
    if len(recent) < MIN_TREND_CANDLES:
        return None, 0

    run_direction = None
    run_length = 0

    for candle in recent:
        direction = candle_direction(candle)

        if direction == "FLAT":
            run_direction = None
            run_length = 0
            continue

        if direction == run_direction:
            run_length += 1
        else:
            run_direction = direction
            run_length = 1

    if run_length >= MIN_TREND_CANDLES:
        return run_direction, run_length

    return None, 0


def update_trend(market, candles):
    """
    Trend rules:
    - Initial trend requires 3 consecutive same-direction candles.
    - A single opposite candle does NOT change the established trend.
    - Three consecutive opposite candles confirm a reversal.
    - Up to 7 recent candles are considered for initial detection.
    """
    completed = candles[:-1]
    if len(completed) < MIN_TREND_CANDLES:
        return None

    latest = completed[-1]
    candle_id = str(latest["time"])
    state = trend_state.get(market, {})

    if state.get("last_candle") == candle_id:
        return state.get("direction")

    direction = candle_direction(latest)

    if direction == "FLAT":
        state["last_candle"] = candle_id
        trend_state[market] = state
        save_state()
        return state.get("direction")

    current = state.get("direction")
    opposite_count = int(state.get("opposite_count", 0))

    if current not in ("UP", "DOWN"):
        initial, count = initialize_trend(completed[-MAX_TREND_CANDLES:])
        if initial:
            current = initial
            opposite_count = 0
        else:
            current = None

    elif direction == current:
        opposite_count = 0

    else:
        opposite_count += 1
        if opposite_count >= MIN_TREND_CANDLES:
            current = direction
            opposite_count = 0

    state["direction"] = current
    state["opposite_count"] = opposite_count
    state["last_candle"] = candle_id
    trend_state[market] = state
    save_state()

    return current


# =========================================================
# GAP / VOLATILITY FILTERS
# =========================================================

def filter_market(candles):
    """
    Filter abnormal movement and candle gaps.
    candles[-1] may be incomplete; use completed candles.
    """
    completed = candles[:-1]

    if len(completed) < 8:
        return False, "Insufficient candle data"

    recent = completed[-8:]
    ranges = [
        max(c["high"] - c["low"], 0)
        for c in recent[:-1]
    ]

    avg_range = sum(ranges) / len(ranges) if ranges else 0
    if avg_range <= 0:
        return False, "Invalid candle ranges"

    latest = recent[-1]
    latest_range = latest["high"] - latest["low"]

    if latest_range > avg_range * SPIKE_MULTIPLIER:
        return False, "Abnormally fast candle movement"

    previous = recent[-2]
    gap = abs(latest["open"] - previous["close"])

    if gap > avg_range * GAP_MULTIPLIER:
        return False, "Gap Up/Down detected"

    return True, ""


# =========================================================
# SIGNAL TIMING / TELEGRAM POSTS
# =========================================================

def fmt_time(stamp):
    return datetime.fromtimestamp(stamp).strftime("%H:%M:%S")


async def wait_until(stamp):
    while True:
        remaining = stamp - time.time()
        if remaining <= 0:
            return
        await asyncio.sleep(min(remaining, 0.5))


async def post_signal(
    app,
    market,
    direction,
    payout,
    target,
    is_mtg=False,
):
    text = (
        "🚀 STM PRO BOT\n\n"
        f"📊 Pair: {market}\n"
        f"📈 Trend: {direction}\n"
        f"🎯 Signal: {'CALL ⬆️' if direction == 'UP' else 'PUT ⬇️'}\n"
        f"💰 Payout: {payout:.0f}%\n"
        f"⏱ Timeframe: 1 Minute\n"
        f"🕒 Target candle: {fmt_time(target)}\n"
        f"📌 Mode: {'MTG 1' if is_mtg else 'NORMAL'}\n\n"
        "⚠️ Signals are not guaranteed profits."
    )
    await app.bot.send_message(chat_id=CHANNEL_ID, text=text)


async def post_result(
    app,
    market,
    direction,
    payout,
    target,
    opening,
    closing,
    result,
    is_mtg=False,
):
    change = closing - opening
    emoji = {
        "WIN": "✅",
        "LOSS": "❌",
        "DRAW": "➖",
        "UNKNOWN": "⚠️",
    }.get(result, "⚠️")

    text = (
        f"📊 STM PRO BOT — {'MTG 1' if is_mtg else 'RESULT'}\n\n"
        f"Pair: {market}\n"
        f"Direction: {direction}\n"
        f"Payout: {payout:.0f}%\n"
        f"Target: {fmt_time(target)}\n"
        f"Open: {opening:.8f}\n"
        f"Close: {closing:.8f}\n"
        f"Change: {change:+.8f}\n\n"
        f"{emoji} Result: {result}"
    )
    await app.bot.send_message(chat_id=CHANNEL_ID, text=text)


async def evaluate_signal(
    app,
    market,
    direction,
    payout,
    target,
    is_mtg=False,
):
    await wait_until(target + TIMEFRAME + 3)

    try:
        candles = await get_candles(market, 30)
        candle = next(
            (c for c in candles if c["time"] == target),
            None,
        )

        if candle is None:
            await app.bot.send_message(
                chat_id=CHANNEL_ID,
                text=(
                    "⚠️ STM PRO BOT\n"
                    f"{market}: exact target candle unavailable.\n"
                    "Result not guessed."
                ),
            )
            return None

        opening = candle["open"]
        closing = candle["close"]

        if closing == opening:
            result = "DRAW"
        elif direction == "UP":
            result = "WIN" if closing > opening else "LOSS"
        else:
            result = "WIN" if closing < opening else "LOSS"

        await post_result(
            app,
            market,
            direction,
            payout,
            target,
            opening,
            closing,
            result,
            is_mtg,
        )

        save_result({
            "market": market,
            "direction": direction,
            "payout": payout,
            "target": target,
            "open": opening,
            "close": closing,
            "result": result,
            "mtg": is_mtg,
        })

        return result

    except Exception:
        log.exception("Result check failed: %s", market)
        return None


# =========================================================
# MTG AND MARKET SIGNAL PROCESSING
# =========================================================

async def process_signal(app, market, candles, payout, target):
    if market in market_busy:
        return

    market_busy.add(market)

    try:
        if market in skip_next:
            skip_next.discard(market)
            save_state()
            log.info("Skipped one signal for %s", market)
            return

        if loss_streak.get(market, 0) >= MAX_LOSSES:
            return

        allowed, reason = filter_market(candles)
        if not allowed:
            log.info("%s skipped: %s", market, reason)
            return

        direction = update_trend(market, candles)
        if direction not in ("UP", "DOWN"):
            log.info("Trend not confirmed for %s", market)
            return

        await post_signal(
            app,
            market,
            direction,
            payout,
            target,
        )

        result = await evaluate_signal(
            app,
            market,
            direction,
            payout,
            target,
        )

        if result == "WIN":
            loss_streak[market] = 0
            save_state()
            return

        if result == "DRAW" or result is None:
            return

        if result != "LOSS":
            return

        loss_streak[market] = loss_streak.get(market, 0) + 1
        save_state()

        if not mtg_enabled:
            return

        # Send MTG only before a future complete candle.
        next_target = (int(time.time()) // TIMEFRAME + 2) * TIMEFRAME
        await wait_until(next_target - SIGNAL_LEAD)

        fresh = await get_candles(market, 30)
        if len(fresh) < 8:
            return

        allowed, _ = filter_market(fresh)
        if not allowed:
            return

        payout2 = await get_payout(market)
        if payout2 is None or payout2 < MIN_PAYOUT:
            return

        direction2 = update_trend(market, fresh)
        if direction2 not in ("UP", "DOWN"):
            return

        await post_signal(
            app,
            market,
            direction2,
            payout2,
            next_target,
            is_mtg=True,
        )

        result2 = await evaluate_signal(
            app,
            market,
            direction2,
            payout2,
            next_target,
            is_mtg=True,
        )

        if result2 == "WIN":
            loss_streak[market] = 0
        elif result2 == "LOSS":
            loss_streak[market] = loss_streak.get(market, 0) + 1
            skip_next.add(market)

        save_state()

    except Exception:
        log.exception("Signal processing failed for %s", market)

    finally:
        market_busy.discard(market)


# =========================================================
# SCANNER
# =========================================================

async def scanner_loop(app):
    global running

    log.info("Scanner started")

    while running:
        try:
            # Aim to scan 7 seconds before the next minute.
            now = time.time()
            current_minute = int(now // TIMEFRAME) * TIMEFRAME
            scan_at = current_minute + TIMEFRAME - SIGNAL_LEAD

            if now >= scan_at:
                scan_at += TIMEFRAME

            await wait_until(scan_at)

            if not running:
                break

            target = (int(time.time() // TIMEFRAME) + 1) * TIMEFRAME

            for market in list(selected_markets):
                if not running:
                    break

                if market in market_busy:
                    continue

                try:
                    candles = await get_candles(market, 30)
                    if len(candles) < 8:
                        continue

                    # Prevent duplicate processing for the same target.
                    if last_processed_candle.get(market) == target:
                        continue
                    last_processed_candle[market] = target

                    payout = await get_payout(market)
                    if payout is None:
                        log.info("No verified payout for %s", market)
                        continue

                    if payout < MIN_PAYOUT:
                        log.info("Payout below 80%%: %s", market)
                        continue

                    asyncio.create_task(
                        process_signal(
                            app,
                            market,
                            candles,
                            payout,
                            target,
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


# =========================================================
# TELEGRAM COMMANDS
# =========================================================

def market_keyboard(markets, page=0, page_size=20):
    start = page * page_size
    subset = markets[start:start + page_size]
    rows = []

    for market in subset:
        selected = market in selected_markets
        prefix = "✅ " if selected else "▫️ "
        rows.append([
            InlineKeyboardButton(
                prefix + market,
                callback_data="m:" + market,
            )
        ])

    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton(
                "⬅️ Previous",
                callback_data=f"page:{page-1}",
            )
        )
    if start + page_size < len(markets):
        nav.append(
            InlineKeyboardButton(
                "Next ➡️",
                callback_data=f"page:{page+1}",
            )
        )
    if nav:
        rows.append(nav)

    rows.append([
        InlineKeyboardButton("Select ALL", callback_data="select_all"),
        InlineKeyboardButton("Clear ALL", callback_data="clear_all"),
    ])
    rows.append([
        InlineKeyboardButton("▶️ Start Scanner", callback_data="scan_start"),
        InlineKeyboardButton("⏹ Stop", callback_data="scan_stop"),
    ])

    return InlineKeyboardMarkup(rows)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global mtg_enabled

    active_users.add(update.effective_user.id)

    keyboard = [
        [
            InlineKeyboardButton("YES — MTG 1", callback_data="mtg_yes"),
            InlineKeyboardButton("NO — MTG OFF", callback_data="mtg_no"),
        ],
    ]

    await update.effective_message.reply_text(
        "🚀 STM PRO BOT\n\n"
        "MTG 1 চালু করবে?\n"
        "YES অথবা NO নির্বাচন কর।",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def markets_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global available_markets

    if not available_markets:
        await update.effective_message.reply_text(
            "Market list loading..."
        )
        available_markets = await discover_markets()

    if not selected_markets:
        # Do not automatically select every asset.
        # The user must explicitly choose.
        pass

    await update.effective_message.reply_text(
        f"Available markets: {len(available_markets)}\n"
        "Tap to select or deselect a pair:",
        reply_markup=market_keyboard(available_markets),
    )


async def select_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not available_markets:
        await update.effective_message.reply_text(
            "Run /markets first."
        )
        return

    if not context.args:
        await update.effective_message.reply_text(
            "Usage: /select EURUSD_otc"
        )
        return

    wanted = " ".join(context.args).strip()
    match = next(
        (
            m for m in available_markets
            if normalize_name(m) == normalize_name(wanted)
        ),
        None,
    )

    if not match:
        await update.effective_message.reply_text(
            "Pair not found. Use /markets to see available pairs."
        )
        return

    if match in selected_markets:
        selected_markets.remove(match)
        message = f"Removed: {match}"
    else:
        selected_markets.add(match)
        message = f"Selected: {match}"

    save_state()
    await update.effective_message.reply_text(message)


async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    global mtg_enabled, running, scanner_task

    query = update.callback_query
    await query.answer()

    data = query.data

    if data == "mtg_yes":
        mtg_enabled = True
        await query.edit_message_text(
            "MTG 1: ON\n\nNow use /markets to choose pairs."
        )
        return

    if data == "mtg_no":
        mtg_enabled = False
        await query.edit_message_text(
            "MTG 1: OFF\n\nNow use /markets to choose pairs."
        )
        return

    if data == "select_all":
        selected_markets.update(available_markets)
        save_state()
        await query.edit_message_text(
            f"Selected {len(selected_markets)} markets.",
            reply_markup=market_keyboard(available_markets),
        )
        return

    if data == "clear_all":
        selected_markets.clear()
        save_state()
        await query.edit_message_text(
            "All markets cleared.",
            reply_markup=market_keyboard(available_markets),
        )
        return

    if data.startswith("page:"):
        page = int(data.split(":", 1)[1])
        await query.edit_message_text(
            "Select markets:",
            reply_markup=market_keyboard(available_markets, page),
        )
        return

    if data.startswith("m:"):
        market = data[2:]
        if market in selected_markets:
            selected_markets.remove(market)
        else:
            selected_markets.add(market)

        save_state()
        await query.edit_message_text(
            f"Selected markets: {len(selected_markets)}",
            reply_markup=market_keyboard(available_markets),
        )
        return

    if data == "scan_start":
        if not selected_markets:
            await query.edit_message_text(
                "No pairs selected. Use /markets first."
            )
            return

        if running:
            await query.edit_message_text("Scanner already running.")
            return

        running = True
        scanner_task = asyncio.create_task(
            scanner_loop(context.application)
        )

        await query.edit_message_text(
            "✅ Scanner started\n"
            f"Markets selected: {len(selected_markets)}\n"
            f"MTG 1: {'ON' if mtg_enabled else 'OFF'}\n"
            f"Minimum payout: {MIN_PAYOUT}%"
        )
        return

    if data == "scan_stop":
        running = False
        await query.edit_message_text("Scanner stop requested.")


async def startscan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    global running, scanner_task

    if not selected_markets:
        await update.effective_message.reply_text(
            "Select markets first using /markets."
        )
        return

    if running:
        await update.effective_message.reply_text(
            "Scanner already running."
        )
        return

    running = True
    scanner_task = asyncio.create_task(
        scanner_loop(context.application)
    )
    await update.effective_message.reply_text("Scanner started.")


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running
    running = False
    await update.effective_message.reply_text(
        "Scanner stop requested."
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wins = sum(1 for x in history if x.get("result") == "WIN")
    losses = sum(1 for x in history if x.get("result") == "LOSS")
    draws = sum(1 for x in history if x.get("result") == "DRAW")

    await update.effective_message.reply_text(
        "📊 STM PRO BOT STATUS\n\n"
        f"Scanner: {'RUNNING' if running else 'STOPPED'}\n"
        f"MTG 1: {'ON' if mtg_enabled else 'OFF'}\n"
        f"Selected pairs: {len(selected_markets)}\n"
        f"Results: {len(history)}\n"
        f"WIN: {wins}\nLOSS: {losses}\nDRAW: {draws}\n"
        f"Minimum payout: {MIN_PAYOUT}%"
    )


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    recent = history[-10:]

    if not recent:
        await update.effective_message.reply_text(
            "No recorded results yet."
        )
        return

    lines = ["📜 STM PRO BOT — RECENT RESULTS"]

    for item in reversed(recent):
        lines.append(
            f"{fmt_time(item.get('target', time.time()))} | "
            f"{item.get('market')} | "
            f"{item.get('direction')} | "
            f"{item.get('result')}"
        )

    await update.effective_message.reply_text("\n".join(lines))


async def post_init(app):
    global available_markets

    available_markets = await discover_markets()
    log.info("Market list loaded: %d", len(available_markets))


async def post_shutdown(app):
    global running, connected

    running = False

    if qx is not None:
        try:
            await call_api(qx, "close")
        except Exception:
            log.exception("Quotex shutdown failed")

    connected = False


async def error_handler(update, context):
    log.error("Telegram error: %s", context.error)


def main():
    if not BOT_TOKEN:
        raise SystemExit(
            "Missing TG_BOT_TOKEN. Check your .env file."
        )

    if not CHANNEL_ID:
        raise SystemExit(
            "Missing/invalid TG_CHANNEL_ID. Check your .env file."
        )

    if not QX_EMAIL or not QX_PASSWORD:
        raise SystemExit(
            "Missing QX_EMAIL or QX_PASSWORD in .env."
        )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("markets", markets_command))
    app.add_handler(CommandHandler("select", select_command))
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
