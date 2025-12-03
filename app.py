# ─────────────────────────────────────────────────────────────────────────────
# AutoTrade Bot — Full Version (MAINNET)
# Converted from TESTNET → MAINNET
# ─────────────────────────────────────────────────────────────────────────────

import os
import time
import uuid
import threading
import requests
import re
from flask import Flask, request, jsonify
from flask_cors import CORS
from binance.um_futures import UMFutures
from decimal import Decimal, getcontext, ROUND_DOWN
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from bson import ObjectId
from flask_caching import Cache
import atexit

# ──────────────────────────────────────────────
# BASE / GLOBALS
# ──────────────────────────────────────────────
getcontext().prec = 12
IST = ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(app)

# Small cache for polling endpoints
cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 5})

# ──────────────────────────────────────────────
# MongoDB Setup
# ──────────────────────────────────────────────
MONGO_URI = "mongodb+srv://netmanconnect:eDxdS7AkkimNGJdi@cluster0.exzvao3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["mttrader"]
status_logs = db["trades"]
predictions_collection = db["prediction"]
logs_collection = db["logs"]

# In-memory runtime registries
task_results = {}
cancel_flags = {}
auto_jobs = {}
active_trades = {}

# Background scheduler (IST)
scheduler = BackgroundScheduler({
    'apscheduler.timezone': 'Asia/Kolkata',
    'apscheduler.job_defaults.misfire_grace_time': 300,
    'apscheduler.job_defaults.max_instances': 1
})
scheduler.start()

# ──────────────────────────────────────────────
# Logging Helper → writes to mttrader.logs
# ──────────────────────────────────────────────
from flask import has_request_context, request as flask_request, g

def log_event(level: str, message: str, email: str = None, context: dict = None):
    """Store structured logs in MongoDB 'logs' collection."""
    try:
        if not email and has_request_context():
            try:
                if flask_request.is_json:
                    data = flask_request.get_json(silent=True)
                    if data and "email" in data:
                        email = data["email"]
            except Exception:
                pass
            if not email:
                email = flask_request.args.get("email")
            if not email:
                email = getattr(g, "email", None)

        entry = {
            "timestamp": datetime.now(IST).isoformat(),
            "level": str(level).upper(),
            "message": message,
            "email": email or "UNKNOWN",
            "context": context or {},
            "environment": "MAINNET"
        }

        logs_collection.insert_one(entry)
        app.logger.info(f"[{entry['level']}] {message} | email={entry['email']} | ctx={entry['context']}")

    except Exception as e:
        app.logger.error(f"Failed to write to Mongo logs: {e}")

log_event("STARTUP", "AutoTrade bot (MAINNET) started successfully.", email="SYSTEM")

def _on_shutdown():
    log_event("SHUTDOWN", "AutoTrade bot stopped gracefully.", email="SYSTEM")

atexit.register(_on_shutdown)

@app.errorhandler(Exception)
def _global_exception_handler(e):
    log_event("ERROR", f"Unhandled exception: {str(e)}", context={"type": type(e).__name__})
    return jsonify({"error": "Internal server error"}), 500

# ──────────────────────────────────────────────
# MAINNET CONFIGURATION
# ──────────────────────────────────────────────
# Binance USDT-M Futures mainnet endpoint
BINANCE_API_URL = "https://fapi.binance.com"

min_order_values = {
    'BTCUSDT': Decimal('5'),
    'ETHUSDT': Decimal('5'),
    'DEFAULT': Decimal('5')
}

current_prices = {
    'BTCUSDT': Decimal('0'),
    'ETHUSDT': Decimal('0')
}

# ──────────────────────────────────────────────
# NEW: Position and Leverage Management
# ──────────────────────────────────────────────

def get_position_info(client, symbol: str) -> dict:
    """Get current position information for a symbol."""
    try:
        positions = client.get_position_risk(symbol=symbol)
        for pos in positions:
            if pos['symbol'] == symbol:
                return {
                    'position_amt': Decimal(pos.get('positionAmt', '0')),
                    'leverage': int(pos.get('leverage', 1)),  # <-- FIXED (default to 1)
                    'entry_price': Decimal(pos.get('entryPrice', '0') if pos.get('entryPrice') != '0.0' else '0'),
                    'unrealized_pnl': Decimal(pos.get('unRealizedProfit', '0'))
                }

        return {'position_amt': Decimal('0'), 'leverage': 1, 'entry_price': Decimal('0'), 'unrealized_pnl': Decimal('0')}
    except Exception as e:
        app.logger.error(f"Failed to get position info for {symbol}: {e}")
        return {'position_amt': Decimal('0'), 'leverage': 1, 'entry_price': Decimal('0'), 'unrealized_pnl': Decimal('0')}


def safe_change_leverage(client, symbol: str, leverage: int, email: str = None) -> bool:
    """
    Safely change leverage with proper error handling.
    Returns True if successful, False if failed.
    """
    try:
        # First check if there's an open position
        position_info = get_position_info(client, symbol)
        
        if position_info['position_amt'] != 0:
            # Position exists - check if we're trying to reduce leverage
            current_leverage = position_info['leverage']
            
            if leverage < current_leverage:
                app.logger.warning(
                    f"Cannot reduce leverage from {current_leverage}x to {leverage}x "
                    f"with open position. Keeping current leverage."
                )
                log_event("WARNING", 
                    f"Leverage reduction skipped (open position exists): {current_leverage}x -> {leverage}x",
                    email=email,
                    context={"symbol": symbol, "position_amt": str(position_info['position_amt'])})
                return False
            elif leverage == current_leverage:
                app.logger.info(f"Leverage already set to {leverage}x for {symbol}")
                return True
        
        # Safe to change leverage (no position OR increasing leverage OR new position)
        client.change_leverage(symbol=symbol, leverage=leverage)
        app.logger.info(f"Successfully changed leverage to {leverage}x for {symbol}")
        log_event("INFO", f"Leverage changed to {leverage}x", email=email, context={"symbol": symbol})
        return True
        
    except Exception as e:
        error_str = str(e)
        
        # Handle specific Binance error codes
        if '-4161' in error_str:
            app.logger.warning(
                f"Binance error -4161: Cannot reduce leverage with open position for {symbol}. "
                f"Continuing with current leverage."
            )
            log_event("WARNING", 
                "Leverage change blocked by Binance (open position)", 
                email=email,
                context={"symbol": symbol, "requested_leverage": leverage, "error": "E-4161"})
            return False
        else:
            # Other errors should be logged and handled
            app.logger.error(f"Failed to change leverage for {symbol}: {e}")
            log_event("ERROR", f"Leverage change failed: {e}", email=email, context={"symbol": symbol})
            raise  # Re-raise for critical errors

# ──────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────
getcontext().prec = 18

def fetch_price_with_retries(client, symbol: str, retries: int = 3, delay: float = 1.0) -> Decimal:
    """Fetch ticker price with small retry window; updates current_prices cache."""
    for _ in range(retries):
        try:
            price = Decimal(client.ticker_price(symbol=symbol)["price"])
            current_prices[symbol] = price
            return price
        except Exception as e:
            app.logger.warning(f"Fetch price failed ({symbol}): {e}")
            time.sleep(delay)
    raise ConnectionError(f"Unable to fetch price for {symbol}")

def get_available_balance(client) -> Decimal:
    """Return available USDT balance from futures account."""
    for b in client.balance():
        if b['asset'] == 'USDT':
            return Decimal(b['availableBalance'])
    raise ValueError("USDT balance not found")

def get_symbol_precision_and_step_size(client, symbol: str) -> tuple:
    """Read exchange_info and return (decimals, step_size) for LOT_SIZE filter."""
    info = client.exchange_info()
    for s in info['symbols']:
        if s['symbol'] == symbol:
            for f in s['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = Decimal(f['stepSize'])
                    decimals = abs(step_size.normalize().as_tuple().exponent)
                    return decimals, step_size
    return 4, Decimal('0.0001')

def truncate_to_step(quantity: Decimal, step_size: Decimal) -> Decimal:
    """Floor quantity to exchange LOT_SIZE step."""
    return (quantity // step_size) * step_size

def ceil_to_step(quantity: Decimal, step_size: Decimal) -> Decimal:
    """Ceil quantity to the nearest LOT_SIZE step (so notional meets MIN_NOTIONAL)."""
    if step_size == 0:
        return quantity
    # Compute multiplier = ceil(quantity / step_size)
    multiplier = (quantity / step_size).to_integral_value(rounding=ROUND_DOWN)
    if multiplier * step_size < quantity:
        multiplier = multiplier + Decimal(1)
    return multiplier * step_size

def get_min_tradable_qty(client, symbol: str) -> Decimal:
    """Compute min tradable qty from min notional and current price (floored to step)."""
    symbol = symbol.upper()
    min_usd = min_order_values.get(symbol, min_order_values['DEFAULT'])
    current_price = current_prices.get(symbol, Decimal('0'))

    if current_price > 0:
        raw_qty = min_usd / current_price
        _, step_size = get_symbol_precision_and_step_size(client, symbol)
        return truncate_to_step(raw_qty, step_size)

    return Decimal('0.0001')

def calculate_trade_quantity(client, symbol: str, size_usdt: Decimal) -> tuple:
    """Returns (qty, adjusted_size, was_adjusted)"""
    price = fetch_price_with_retries(client, symbol)
    min_qty = get_min_tradable_qty(client, symbol)
    decimals, step_size = get_symbol_precision_and_step_size(client, symbol)

    quantity = size_usdt / price

    if quantity < min_qty:
        adjusted_usdt = price * min_qty
        return min_qty, adjusted_usdt, True

    quantity = truncate_to_step(quantity, step_size)

    if quantity <= 0:
        adjusted_usdt = price * step_size
        return step_size, adjusted_usdt, True

    return quantity, size_usdt, False

def fetch_min_notional_values():
    """Fetch MIN_NOTIONAL from exchangeInfo hourly for BTC/ETH."""
    try:
        res = requests.get(f'{BINANCE_API_URL}/fapi/v1/exchangeInfo', timeout=5)
        data = res.json()
        for symbol in ['BTCUSDT', 'ETHUSDT']:
            symbol_info = next((s for s in data['symbols'] if s['symbol'] == symbol), None)
            if symbol_info:
                filter_info = next((f for f in symbol_info['filters'] if f['filterType'] == 'MIN_NOTIONAL'), None)
                if filter_info:
                    min_order_values[symbol] = Decimal(filter_info['notional'])
                    app.logger.info(f"Updated min notional for {symbol}: {min_order_values[symbol]}")
    except Exception as e:
        app.logger.error(f"Failed to fetch min notional values (MAINNET): {e}")
        log_event("ERROR", "Failed to fetch min notional values (MAINNET)", context={"error": str(e)})

fetch_min_notional_values()
scheduler.add_job(fetch_min_notional_values, 'interval', hours=1)

def mark_failed(task_id: str, msg: str, email=None):
    """Mark a running trade task as FAILED (db + memory), and free active_trades[email]."""
    try:
        status_logs.update_one({"task_id": task_id}, {"$set": {
            "status": "FAILED", "error_message": msg, "end_time": datetime.now(IST)
        }})
    except Exception as e:
        app.logger.error(f"Failed to write FAILED status to Mongo: {e}")

    task_results[task_id] = {"status": "FAILED", "error_message": msg}
    if email:
        active_trades.pop(email, None)

    app.logger.error(f"{task_id} FAILED: {msg}")
    log_event("ERROR", f"Trade task failed: {msg}", email=email, context={"task_id": task_id})

def get_next_15min_interval() -> datetime:
    """Get next 15-minute boundary (IST)."""
    now = datetime.now(IST)
    minutes = now.minute
    next_interval_minutes = ((minutes // 15) + 1) * 15
    if next_interval_minutes >= 60:
        next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_time = now.replace(minute=next_interval_minutes, second=0, microsecond=0)
    return next_time

# ──────────────────────────────────────────────
# MongoDB Prediction Fetching
# ──────────────────────────────────────────────
def get_latest_prediction(symbol: str = "BTCUSDT") -> dict:
    try:
        prediction = predictions_collection.find_one({}, sort=[("updatedAt", -1)])
        if not prediction:
            app.logger.warning("No prediction found in database")
            return None
        app.logger.info(
            f"Prediction: direction={prediction.get('direction')} "
            f"conf={prediction.get('confidence')} "
            f"entry={prediction.get('entry')} tp1={prediction.get('tp1')} tp2={prediction.get('tp2')} tp3={prediction.get('tp3')} sl={prediction.get('sl')}"
        )
        return prediction
    except Exception as e:
        app.logger.error(f"Error fetching prediction from MongoDB: {e}")
        log_event("ERROR", "Error fetching prediction from Mongo", context={"error": str(e)})
        return None

def parse_prediction_direction(prediction: dict) -> str:
    """Return BUY/SELL based on LONG/SHORT; None if unknown."""
    if not prediction:
        return None
    direction = str(prediction.get("direction", "")).upper()
    if direction == "LONG":
        return "BUY"
    elif direction == "SHORT":
        return "SELL"
    app.logger.warning(f"Unknown direction value in prediction: {direction}")
    return None

# ──────────────────────────────────────────────
# FIXED: Enhanced Trading Logic with TP1/TP2/TP3
# ──────────────────────────────────────────────
def auto_trade_with_tps(
    client, symbol, qty, tp_levels, sl_price, leverage, task_id,
    callback_url=None, direction="BUY", email=None, entry_price_target=None
):
    """
    Enhanced trading loop with multiple take-profit levels and breakeven logic.
    FIXED: Proper leverage change handling with position checking.
    """
    try:
        # If an entry target is provided, wait up to 5 minutes
        if entry_price_target:
            app.logger.info(f"{task_id}: Waiting for entry price {entry_price_target} ({direction})")
            wait_count = 0
            max_wait = 300
            while wait_count < max_wait:
                current_price = fetch_price_with_retries(client, symbol)
                if (direction == "BUY" and current_price <= entry_price_target) or \
                   (direction == "SELL" and current_price >= entry_price_target):
                    break
                time.sleep(1)
                wait_count += 1

        entry_price = fetch_price_with_retries(client, symbol)
        
        # FIXED: Safe leverage change with error handling
        leverage_changed = safe_change_leverage(client, symbol, leverage, email)
        if not leverage_changed:
            app.logger.warning(f"{task_id}: Continuing with existing leverage due to open position")

        # Determine symbol precision and step
        decimals, step_size = get_symbol_precision_and_step_size(client, symbol)

        # Normalize qty to Decimal and floor to LOT_SIZE step
        try:
            qty = Decimal(qty)
        except Exception:
            return mark_failed(task_id, "Invalid quantity format", email=email)

        # Ensure order notional >= exchange minimum (100 USDT on Binance futures) for opening (non-reduce) orders
        MIN_NOTIONAL = Decimal('100')
        notional = qty * entry_price
        if notional < MIN_NOTIONAL:
            # compute minimum quantity to meet min notional
            min_qty = truncate_to_step((MIN_NOTIONAL / entry_price), step_size)
            if min_qty <= 0:
                return mark_failed(task_id, "Computed min order qty is zero; cannot place order", email=email)

            qty = min_qty
            app.logger.warning(f"{task_id}: Adjusted qty to meet min notional: qty={qty}, notional={qty * entry_price}")
            log_event("WARNING", f"Adjusted qty to meet min notional: {qty * entry_price}", email=email,
                      context={"task_id": task_id, "symbol": symbol})

        qty_str = "{0:.{1}f}".format(float(qty), decimals)

        # Open market position (fail-safe with proper logging)
        try:
            client.new_order(
                symbol=symbol,
                side=direction,
                type='MARKET',
                quantity=qty_str
            )
            app.logger.info(f"{task_id}: Entered {direction} {qty_str} {symbol} @ {entry_price}")
            log_event("INFO", f"Entered position {direction} {symbol}", email=email,
                      context={"task_id": task_id, "qty": qty_str, "entry": str(entry_price)})
        except Exception as e:
            app.logger.error(f"{task_id}: Open position failed: {e}")
            return mark_failed(task_id, str(e), email=email)

        # Record entry in DB
        try:
            status_logs.update_one(
                {"task_id": task_id},
                {"$set": {
                    "entry_price": str(entry_price),
                    "tp_levels": {k: {"price": str(v["price"]), "qty_pct": v["qty_pct"]} for k, v in tp_levels.items()},
                    "sl_price": str(sl_price)
                }}
            )
        except Exception as e:
            app.logger.error(f"{task_id}: Failed to write entry to Mongo: {e}")

    except Exception as e:
        log_event("ERROR", f"Open position failed: {e}", email=email, context={"task_id": task_id})
        return mark_failed(task_id, str(e), email=email)

    # Runtime loop for TP/SL
    remaining_qty = qty
    _, step_size = get_symbol_precision_and_step_size(client, symbol)
    tp_hits = []
    current_sl = sl_price
    breakeven_moved = False

    step = 0
    max_steps = 3600 

    while True:
        exit_reason = None
        exit_qty = Decimal(0)

        # fetch current price each loop
        try:
            price = fetch_price_with_retries(client, symbol)
        except Exception as e:
            app.logger.error(f"{task_id}: Price fetch failed inside loop: {e}")
            price = Decimal(0)

        # Check for cancel request
        if cancel_flags.get(task_id):
            exit_reason = "CANCELLED_BY_USER"
            exit_qty = remaining_qty
        else:
            # Iterate TP levels and close partial positions when hit
            for tp_name in ["tp1", "tp2", "tp3"]:
                if tp_name not in tp_levels or tp_name in tp_hits:
                    continue

                tp_data = tp_levels[tp_name]
                tp_price = tp_data["price"]
                tp_qty_pct = tp_data["qty_pct"]

                hit = (direction == "BUY" and price >= tp_price) or (direction == "SELL" and price <= tp_price)
                if not hit:
                    continue

                partial_qty = truncate_to_step(qty * Decimal(tp_qty_pct) / Decimal(100), step_size)
                if partial_qty > remaining_qty:
                    partial_qty = remaining_qty

                if partial_qty <= 0:
                    app.logger.warning(f"{task_id}: Computed partial_qty is zero, skipping {tp_name}")
                    continue

                try:
                    partial_qty_str = "{0:.{1}f}".format(float(partial_qty), decimals)
                    client.new_order(
                        symbol=symbol,
                        side="SELL" if direction == "BUY" else "BUY",
                        type='MARKET',
                        quantity=partial_qty_str,
                        reduceOnly=True
                    )

                    remaining_qty -= partial_qty
                    tp_hits.append(tp_name)
                    app.logger.info(f"{task_id}: {tp_name} HIT at {price}. Closed {partial_qty_str}, Remaining: {remaining_qty}")
                    log_event("INFO", f"{tp_name} hit", email=email,
                              context={"task_id": task_id, "price": str(price), "partial_qty": partial_qty_str})

                    # Move SL to breakeven after TP1
                    if tp_name == "tp1" and not breakeven_moved:
                        current_sl = entry_price
                        breakeven_moved = True
                        app.logger.info(f"{task_id}: Stop loss moved to breakeven at {entry_price}")

                    # DB TP hit
                    try:
                        status_logs.update_one(
                            {"task_id": task_id},
                            {"$push": {"tp_hits": {
                                "level": tp_name,
                                "price": str(price),
                                "quantity": str(partial_qty),
                                "time": datetime.now(IST).isoformat()
                            }}} 
                        )
                    except Exception as e:
                        app.logger.error(f"{task_id}: Failed to push TP hit to Mongo: {e}")

                    if remaining_qty <= step_size:
                        exit_reason = "ALL_TPS_COMPLETED"
                        exit_qty = Decimal(0)
                        break

                except Exception as e:
                    app.logger.error(f"{task_id}: Error closing {tp_name}: {e}")
                    log_event("ERROR", f"Error closing {tp_name}: {e}", email=email, context={"task_id": task_id})

            # Forced cutoff after max_steps
            if step >= max_steps and not exit_reason:
                exit_reason = "FORCED_EXIT"
                exit_qty = remaining_qty

        # Finalize on exit_reason
        if exit_reason:
            final_pnl_amt = Decimal(0)

            # Close remaining if any
            if exit_qty and exit_qty > step_size:
                try:
                    decimals_exit = 9 if symbol == 'BTCUSDT' else 4
                    exit_qty_str = "{0:.{1}f}".format(float(exit_qty), decimals_exit)
                    client.new_order(
                        symbol=symbol,
                        side="SELL" if direction == "BUY" else "BUY",
                        type='MARKET',
                        quantity=exit_qty_str,
                        reduceOnly=True
                    )
                    app.logger.info(f"{task_id}: Final exit {exit_qty_str} at {price}")
                except Exception as e:
                    app.logger.error(f"{task_id}: Final exit error: {e}")
                    log_event("ERROR", f"Final exit error: {e}", email=email, context={"task_id": task_id})

            # Compute pnl
            current_price = fetch_price_with_retries(client, symbol)
            if direction == "BUY":
                final_pnl_amt = (current_price - entry_price) * qty
                pnl_pct = ((current_price - entry_price) / entry_price * Decimal("100"))
            else:
                final_pnl_amt = (entry_price - current_price) * qty
                pnl_pct = ((entry_price - current_price) / entry_price * Decimal("100"))

            result = {
                "status": exit_reason,
                "symbol": symbol,
                "entry_price": str(entry_price),
                "exit_price": str(current_price),
                "pnl_amount": str(final_pnl_amt),
                "pnl_percent": str(pnl_pct),
                "tp_hits": tp_hits,
                "total_quantity": str(qty),
                "closed_quantity": str(qty - remaining_qty),
                "ledger_balance": "N/A"
            }

            task_results[task_id] = result
            try:
                status_logs.update_one({"task_id": task_id}, {"$set": {**result, "end_time": datetime.now(IST)}})
            except Exception as e:
                app.logger.error(f"{task_id}: Failed to write final result to Mongo: {e}")

            if email:
                active_trades.pop(email, None)

            if callback_url:
                try:
                    requests.post(callback_url, json={"task_id": task_id, **result}, timeout=10)
                except Exception:
                    pass

            log_event("INFO", f"Trade finished: {exit_reason}", email=email,
                      context={"task_id": task_id, "pnl": result["pnl_amount"], "pnl_pct": result["pnl_percent"]})

            cancel_flags.pop(task_id, None)
            break

        step += 1
        time.sleep(1)

# ──────────────────────────────────────────────
# Polling + Auto-Trade Scheduling Logic
# ──────────────────────────────────────────────

def poll_predict_and_trade(job_id: str):
    """Periodically polls MongoDB predictions and initiates trades if threshold met."""
    meta = auto_jobs[job_id]
    email = meta["trade_params"]["email"]
    symbol = meta["trade_params"].get("symbol", "BTCUSDT").upper()

    if meta["executed_count"] >= meta["max_trades"]:
        try:
            meta["job"].remove()
            meta["status"] = "COMPLETED"
            log_event("INFO", f"Auto-trade completed all {meta['max_trades']} trades", email)
        except Exception as e:
            log_event("ERROR", f"Job stop error: {e}", email)
        return

    if email in active_trades:
        log_event("INFO", f"{email} already has an active trade", email)
        return

    try:
        prediction = get_latest_prediction(symbol)
        if not prediction:
            log_event("WARNING", "No prediction found in DB", email)
            return

        direction = parse_prediction_direction(prediction)
        confidence = float(prediction.get("confidence", 0))
        log_event("INFO", f"Prediction dir={direction} conf={confidence}", email)

    except Exception as e:
        log_event("ERROR", f"Prediction fetch failed: {e}", email)
        return

    if direction and confidence >= meta["threshold"]:
        payload = meta["trade_params"].copy()
        payload["direction"] = direction

        if all(k in prediction for k in ["tp1", "tp2", "tp3", "sl"]):
            payload["use_tp_levels"] = True
            payload["tp1"] = prediction["tp1"]
            payload["tp2"] = prediction["tp2"]
            payload["tp3"] = prediction["tp3"]
            payload["sl"] = prediction["sl"]
            payload["entry_target"] = prediction.get("entry")

        try:
            r = requests.post("http://127.0.0.1:5002/trade", json=payload, timeout=15)
            app.logger.info(f"{job_id}: POST /trade → {r.status_code}")
            if r.status_code in (200, 202):
                meta["executed_count"] += 1
                meta["executed_trades"].append({
                    "timestamp": datetime.now(IST).isoformat(),
                    "direction": direction,
                    "confidence": confidence,
                    "prediction_time": prediction.get("updatedAtIST"),
                    "tp_levels_used": payload.get("use_tp_levels", False),
                    "trade_number": meta["executed_count"]
                })
                log_event("INFO", f"Trade {meta['executed_count']} executed", email)
            else:
                meta["job"].remove()
                meta["status"] = "FAILED"
                log_event("ERROR", "Trade API returned failure, stopping job", email,
                          {"status": r.status_code, "text": r.text})
        except Exception as e:
            try:
                meta["job"].remove()
            except Exception:
                pass
            meta["status"] = "FAILED"
            log_event("ERROR", f"Trade request failed: {e}", email)
    else:
        log_event("INFO", "Threshold not met or invalid direction", email)

# ──────────────────────────────────────────────
# Flask Routes — Main Trading Endpoints
# ──────────────────────────────────────────────

@app.route("/trade", methods=["POST"])
def start_trade():
    data = flask_request.get_json(force=True)
    required_fields = ["email", "symbol", "leverage", "api_key", "api_secret", "size_usdt"]
    for f in required_fields:
        if f not in data:
            return jsonify({"error": f"Missing required field: {f}"}), 400

    email = data["email"]
    if email in active_trades:
        return jsonify({"error": "You already have an active trade."}), 403

    try:
        # Use MAINNET base URL for client
        client = UMFutures(key=data["api_key"], secret=data["api_secret"], base_url=BINANCE_API_URL)
        size_usdt = Decimal(str(data["size_usdt"]))
        if size_usdt <= 0:
            return jsonify({"error": "Size must be positive"}), 400
        
        # Fetch symbol price first
        symbol_price = current_prices.get(
            data["symbol"],
            fetch_price_with_retries(client, data["symbol"])
        )
        
        # Calculate initial quantity and notional
        qty = size_usdt / symbol_price
        decimals, step = get_symbol_precision_and_step_size(client, data["symbol"])
        qty = truncate_to_step(qty, step)
        notional = qty * symbol_price
        adjusted_size = size_usdt
        was_adjusted = False

        # Adjust if notional is below minimum
        if notional < Decimal('100'):
            min_qty = ceil_to_step(Decimal('100') / symbol_price, step)
            min_notional = min_qty * symbol_price

            # Verify adjusted notional meets minimum
            if min_notional < Decimal('100'):
                msg = f"Cannot place order: required notional ${min_notional:.2f} still below $100 minimum for {data['symbol']}"
                log_event("ERROR", msg, data["email"])
                stop_jobs_on_failure(data["email"], msg)
                return jsonify({"error": msg}), 400

            qty = min_qty
            adjusted_size = min_notional
            was_adjusted = True

            app.logger.warning(
                f"Adjusted qty to meet Binance min notional: qty={qty}, notional={adjusted_size}"
            )


        bal = get_available_balance(client)
        required = adjusted_size / Decimal(data["leverage"])
        if bal < required:
            msg = f"Insufficient balance: need {required:.2f}, have {bal:.2f}"
            
            log_event("ERROR", msg, email)
            # Stop user's auto-trade jobs on failure
            stop_jobs_on_failure(email, msg)
            return jsonify({"error": msg}), 400

        task_id = str(uuid.uuid4())
        task_results[task_id] = {"status": "RUNNING"}
        cancel_flags[task_id] = False
        active_trades[email] = task_id

        use_tp_levels = data.get("use_tp_levels", False)
        status_logs.insert_one({
            "task_id": task_id,
            "email": email,
            "symbol": data["symbol"],
            "size_usdt": str(adjusted_size),
            "quantity": str(qty),
            "leverage": int(data["leverage"]),
            "status": "RUNNING",
            "start_time": datetime.now(IST),
            "size_adjusted": was_adjusted,
            "requested_size": str(size_usdt),
            "use_tp_levels": use_tp_levels,
            "tp_hits": [],
            "environment": "MAINNET"
        })

        direction = data.get("direction", "BUY").upper()

        # TP-based trade
        if use_tp_levels and all(k in data for k in ["tp1", "tp2", "tp3", "sl"]):
            tp_levels = {
                "tp1": {"price": Decimal(str(data["tp1"])), "qty_pct": 33},
                "tp2": {"price": Decimal(str(data["tp2"])), "qty_pct": 33},
                "tp3": {"price": Decimal(str(data["tp3"])), "qty_pct": 34}
            }
            sl_price = Decimal(str(data["sl"]))
            entry_target = Decimal(str(data["entry_target"])) if data.get("entry_target") else None

            threading.Thread(
                target=auto_trade_with_tps,
                args=(client, data["symbol"], qty, tp_levels, sl_price,
                      int(data["leverage"]), task_id, data.get("callback_url"),
                      direction, email, entry_target),
                daemon=True
            ).start()

            log_event("INFO", "Started TP_LEVELS trade", email, {"task_id": task_id})
            return jsonify({
                "task_id": task_id,
                "status": "STARTED",
                "trade_type": "TP_LEVELS",
                "symbol": data["symbol"],
                "environment": "MAINNET"
            }), 202

        # Simple P/L trade
        if "profit_percent" not in data or "loss_percent" not in data:
            return jsonify({"error": "Missing profit_percent or loss_percent"}), 400

        from threading import Thread
        def simple_auto_trade():
            try:
                entry_price = fetch_price_with_retries(client, data["symbol"])
                
                # FIXED: Use safe leverage change
                safe_change_leverage(client, data["symbol"], int(data["leverage"]), email)
                
                decimals_local = 9 if data["symbol"] == 'BTCUSDT' else 4
                qty_str = f"{float(qty):.{decimals_local}f}"

                client.new_order(symbol=data["symbol"], side=direction, type='MARKET', quantity=qty_str)
                profit_pct = Decimal(str(data["profit_percent"]))
                loss_pct = Decimal(str(data["loss_percent"]))

                while True:
                    if cancel_flags.get(task_id):
                        exit_reason = "CANCELLED_BY_USER"
                        break
                    price = fetch_price_with_retries(client, data["symbol"])
                    pnl_pct = ((price - entry_price) / entry_price * Decimal("100")) if direction == "BUY" else ((entry_price - price) / entry_price * Decimal("100"))
                    if pnl_pct >= profit_pct:
                        exit_reason = "COMPLETED"; break
                    if pnl_pct <= -loss_pct:
                        exit_reason = "STOPPED_LOSS"; break
                    time.sleep(1)

                client.new_order(symbol=data["symbol"], side="SELL" if direction == "BUY" else "BUY",
                                 type='MARKET', quantity=qty_str, reduceOnly=True)
                price = fetch_price_with_retries(client, data["symbol"])
                pnl_amt = (price - entry_price) * Decimal(qty_str) if direction == "BUY" else (entry_price - price) * Decimal(qty_str)
                pnl_pct = ((price - entry_price) / entry_price * Decimal("100")) if direction == "BUY" else ((entry_price - price) / entry_price * Decimal("100"))
                result = {"status": exit_reason, "pnl_amount": str(pnl_amt), "pnl_percent": str(pnl_pct)}
                task_results[task_id] = result
                status_logs.update_one({"task_id": task_id}, {"$set": {**result, "end_time": datetime.now(IST)}})
                if email:
                    active_trades.pop(email, None)
                log_event("INFO", f"Simple trade finished: {exit_reason}", email, {"pnl": str(pnl_pct)})
            except Exception as e:
                mark_failed(task_id, str(e), email=email)

        Thread(target=simple_auto_trade, daemon=True).start()
        return jsonify({
            "task_id": task_id,
            "status": "STARTED",
            "trade_type": "SIMPLE",
            "symbol": data["symbol"],
            "environment": "MAINNET"
        }), 202

    except Exception as e:
        log_event("ERROR", f"Trade init failed: {e}", email)
        return jsonify({"error": "Trade initialization failed", "details": str(e)}), 502

# ──────────────────────────────────────────────
# Auto-Trade Job Scheduling Endpoints
# ──────────────────────────────────────────────

@app.route("/start_auto_trade", methods=["POST"])
def start_auto_trade():
    data = flask_request.get_json(force=True)
    required_fields = ["email", "symbol", "size_usdt", "leverage", "api_key", "api_secret", "threshold"]
    for f in required_fields:
        if f not in data:
            return jsonify({"error": f"Missing field: {f}"}), 400

    max_trades = int(data.get("max_trades", 10))
    if max_trades <= 0:
        return jsonify({"error": "max_trades must be > 0"}), 400

    trade_params = {
        "email": data["email"],
        "symbol": data["symbol"],
        "size_usdt": data["size_usdt"],
        "leverage": data["leverage"],
        "api_key": data["api_key"],
        "api_secret": data["api_secret"]
    }
    if "profit_percent" in data: trade_params["profit_percent"] = str(data["profit_percent"])
    if "loss_percent" in data: trade_params["loss_percent"] = str(data["loss_percent"])

    try:
        threshold = float(data["threshold"])
        if threshold <= 0 or threshold > 1:
            return jsonify({"error": "Threshold must be between 0 and 1"}), 400
    except Exception as e:
        return jsonify({"error": f"Invalid threshold: {e}"}), 400

    job_id = str(uuid.uuid4())
    next_run = get_next_15min_interval()

    job = scheduler.add_job(
        poll_predict_and_trade,
        trigger="cron",
        minute="0,15,30,45",
        next_run_time=next_run,
        args=[job_id],
        id=job_id
    )

    auto_jobs[job_id] = {
        "job": job,
        "threshold": threshold,
        "trade_params": trade_params,
        "last_prediction": None,
        "executed_trades": [],
        "max_trades": max_trades,
        "executed_count": 0,
        "status": "ACTIVE"
    }

    log_event("INFO", "Started auto-trade job", data["email"], {"job_id": job_id})
    return jsonify({
        "job_id": job_id,
        "message": f"Auto-trade started successfully ({max_trades} trades).",
        "max_trades": max_trades,
        "next_run_time": job.next_run_time.isoformat(),
        "interval": "Every 15 minutes (:00, :15, :30, :45)",
        "symbol": data["symbol"]
    }), 202


@app.route("/auto_trade_status/<job_id>", methods=["GET"])
def auto_trade_status(job_id):
    meta = auto_jobs.get(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "job_id": job_id,
        "status": meta.get("status", "ACTIVE"),
        "symbol": meta["trade_params"]["symbol"],
        "executed_count": meta["executed_count"],
        "max_trades": meta["max_trades"],
        "remaining_trades": meta["max_trades"] - meta["executed_count"],
        "next_run_time": meta["job"].next_run_time.isoformat() if meta["job"].next_run_time else None,
        "threshold": meta["threshold"]
    })

@app.route("/auto_trade_status_by_email", methods=["POST"])
@cache.cached(timeout=5, query_string=False)
def auto_trade_status_by_email():
    """Return all jobs for an email, cached 5s"""
    data = flask_request.get_json(force=True)
    email = data.get("email")
    if not email:
        return jsonify({"error": "Missing field: email"}), 400

    results = [{
        "job_id": job_id,
        "symbol": meta["trade_params"]["symbol"],
        "status": meta.get("status", "ACTIVE"),
        "executed_count": meta["executed_count"],
        "max_trades": meta["max_trades"],
        "remaining_trades": meta["max_trades"] - meta["executed_count"],
        "next_run_time": meta["job"].next_run_time.isoformat() if meta["job"].next_run_time else None,
        "threshold": meta["threshold"]
    } for job_id, meta in auto_jobs.items() if meta["trade_params"].get("email") == email]

    if not results:
        return jsonify({"email": email, "jobs": [], "message": "No jobs found"}), 404
    return jsonify({"email": email, "jobs": results, "count": len(results)}), 200


# ──────────────────────────────────────────────
# Trade Status & Cancel Operations
# ──────────────────────────────────────────────

@app.route("/status/<task_id>", methods=["GET"])
def get_status(task_id):
    result = task_results.get(task_id)
    if not result:
        return jsonify({"error": "Task not found"}), 404
    db_entry = status_logs.find_one({"task_id": task_id})
    if db_entry:
        result.update({
            "start_time": db_entry.get("start_time"),
            "end_time": db_entry.get("end_time"),
            "symbol": db_entry.get("symbol"),
            "leverage": db_entry.get("leverage"),
            "size_usdt": db_entry.get("size_usdt"),
            "tp_hits": db_entry.get("tp_hits", [])
        })
    return jsonify(result)

@app.route("/cancel/<task_id>", methods=["POST"])
def cancel_trade(task_id):
    if task_id not in task_results:
        return jsonify({"error": "Task not found"}), 404
    if task_results[task_id].get("status") != "RUNNING":
        return jsonify({"error": "Cannot cancel non-running trade"}), 400
    cancel_flags[task_id] = True
    status_logs.update_one({"task_id": task_id}, {"$set": {"status": "CANCELLED", "cancel_time": datetime.now(IST)}})
    log_event("INFO", "Cancel requested", None, {"task_id": task_id})
    return jsonify({"task_id": task_id, "status": "CANCEL_REQUESTED"}), 202

def stop_auto_trade_jobs_by_email(email: str):
    stopped_jobs = []
    for job_id, meta in list(auto_jobs.items()):
        if meta["trade_params"]["email"] == email:
            try:
                meta["job"].remove()
                meta["status"] = "STOPPED_BY_USER"
                stopped_jobs.append(job_id)
            except Exception as e:
                log_event("ERROR", f"Failed to stop job {job_id}: {e}", email)
    return stopped_jobs

@app.route("/cancel_by_email", methods=["POST"])
def cancel_by_email():
    data = flask_request.get_json(force=True)
    email = data.get("email")
    if not email:
        return jsonify({"error": "Missing email"}), 400

    docs = status_logs.find({"email": email, "status": "RUNNING"}, {"task_id": 1})
    tids = [d["task_id"] for d in docs]
    for t in tids: cancel_flags[t] = True
    status_logs.update_many({"task_id": {"$in": tids}},
                            {"$set": {"status": "CANCELLED", "cancel_time": datetime.now(IST)}})

    stopped_job_ids = stop_auto_trade_jobs_by_email(email)
    if email in active_trades:
        active_trades.pop(email)
    log_event("INFO", f"Cancelled {len(tids)} trades", email)
    return jsonify({
        "email": email,
        "cancelled_trades": tids,
        "stopped_auto_trade_jobs": stopped_job_ids,
        "message": f"Cancelled {len(tids)} trades and stopped {len(stopped_job_ids)} jobs"
    }), 202

# ──────────────────────────────────────────────
# Emergency Stop, Price, Balance, and Prediction APIs
# ──────────────────────────────────────────────

@app.route("/emergency_stop", methods=["POST"])
def emergency_stop():
    running_tasks = list(active_trades.values())
    for task_id in running_tasks:
        cancel_flags[task_id] = True
        status_logs.update_one(
            {"task_id": task_id},
            {"$set": {"status": "EMERGENCY_STOPPED", "end_time": datetime.now(IST)}}
        )

    stopped_jobs = []
    for job_id, meta in list(auto_jobs.items()):
        try:
            meta["job"].remove()
            meta["status"] = "EMERGENCY_STOPPED"
            stopped_jobs.append(job_id)
        except Exception as e:
            log_event("ERROR", f"Failed to remove job {job_id}: {e}")

    active_trades.clear()
    log_event("WARNING", "Emergency stop triggered", None, {"cancelled_trades": running_tasks, "stopped_jobs": stopped_jobs})
    return jsonify({
        "message": "Emergency stop executed successfully",
        "cancelled_trades": running_tasks,
        "stopped_jobs": stopped_jobs,
        "timestamp": datetime.now(IST).isoformat()
    }), 200


@app.route("/price", methods=["GET"])
def price():
    sym = flask_request.args.get("symbol", "BTCUSDT").upper()
    try:
        res = requests.get(f"{BINANCE_API_URL}/fapi/v1/ticker/price?symbol={sym}", timeout=5)
        res.raise_for_status()
        price_val = Decimal(res.json()["price"])
        current_prices[sym] = price_val
        return jsonify({
            "symbol": sym,
            "price": str(price_val),
            "timestamp": datetime.now(IST).isoformat(),
            "environment": "MAINNET"
        }), 200
    except Exception as e:
        log_event("ERROR", f"Price fetch failed: {e}", None, {"symbol": sym})
        return jsonify({"error": f"Failed to fetch price for {sym}", "details": str(e)}), 500


@app.route("/balance", methods=["POST"])
def get_balance():
    data = flask_request.get_json()
    if not data.get("api_key") or not data.get("api_secret"):
        return jsonify({"error": "Missing API credentials"}), 400
    try:
        client = UMFutures(key=data["api_key"], secret=data["api_secret"], base_url=BINANCE_API_URL)
        balance = get_available_balance(client)
        return jsonify({
            "asset": "USDT",
            "available_balance": str(balance),
            "timestamp": datetime.now(IST).isoformat(),
            "environment": "MAINNET"
        }), 200
    except Exception as e:
        log_event("ERROR", f"Balance fetch failed: {e}")
        return jsonify({"error": "Failed to fetch balance", "details": str(e)}), 500


@app.route("/current_prediction", methods=["GET"])
def current_prediction():
    symbol = flask_request.args.get("symbol", "BTCUSDT").upper()
    try:
        prediction = get_latest_prediction(symbol)
        if not prediction:
            return jsonify({"error": "No prediction available", "symbol": symbol}), 404
        if "_id" in prediction:
            prediction["_id"] = str(prediction["_id"])
        return jsonify({
            "symbol": symbol,
            "prediction": {
                "direction": prediction.get("direction"),
                "entry": prediction.get("entry"),
                "tp1": prediction.get("tp1"),
                "tp2": prediction.get("tp2"),
                "tp3": prediction.get("tp3"),
                "sl": prediction.get("sl"),
                "confidence": prediction.get("confidence"),
                "updatedAtIST": prediction.get("updatedAtIST")
            },
            "timestamp": datetime.now(IST).isoformat(),
            "environment": "MAINNET"
        }), 200
    except Exception as e:
        log_event("ERROR", f"Prediction fetch failed: {e}")
        return jsonify({"error": "Failed to fetch prediction", "details": str(e)}), 500


@app.route("/stop_auto_trade/<job_id>", methods=["POST"])
def stop_auto_trade(job_id):
    meta = auto_jobs.get(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    try:
        meta["job"].remove()
        meta["status"] = "STOPPED_BY_USER"
        log_event("INFO", "Auto-trade job stopped manually", meta["trade_params"]["email"], {"job_id": job_id})
        return jsonify({
            "job_id": job_id,
            "message": "Auto-trade stopped successfully",
            "executed_trades": meta["executed_count"],
            "max_trades": meta["max_trades"],
            "timestamp": datetime.now(IST).isoformat()
        }), 200
    except Exception as e:
        log_event("ERROR", f"Failed to stop auto-trade: {e}", None, {"job_id": job_id})
        return jsonify({"error": "Failed to stop auto-trade job", "details": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Quick status check"""
    return jsonify({
        "status": "healthy",
        "active_trades": len(active_trades),
        "active_auto_jobs": len(auto_jobs),
        "timestamp": datetime.now(IST).isoformat(),
        "environment": "MAINNET"
    }), 200


# ──────────────────────────────────────────────
# Global Fail-Safe: stop auto-trade job on any trade or balance error
# ──────────────────────────────────────────────

def stop_jobs_on_failure(email: str, error_message: str):
    """Stops all jobs for given email when a trade fails due to error or balance issue"""
    stopped = []
    for job_id, meta in list(auto_jobs.items()):
        if meta["trade_params"]["email"] == email:
            try:
                meta["job"].remove()
                meta["status"] = "FAILED"
                stopped.append(job_id)
            except Exception:
                pass
    log_event("ERROR", f"Stopped jobs due to trade failure: {error_message}", email, {"jobs": stopped})
    return stopped


# Inject into existing failure path
_old_mark_failed = mark_failed
def mark_failed_with_job_stop(task_id: str, msg: str, email=None):
    """Override: also stop all jobs of user"""
    _old_mark_failed(task_id, msg, email)
    if email:
        stop_jobs_on_failure(email, msg)

# Re-assign globally
mark_failed = mark_failed_with_job_stop

# ──────────────────────────────────────────────
# Final Section — Server Entry Point
# ──────────────────────────────────────────────

log_event("STARTUP", "All routes, schedulers, and fail-safes loaded successfully.", email="SYSTEM")

for job in scheduler.get_jobs():
    app.logger.info(f"Loaded job: {job.id} next={job.next_run_time}")

# ──────────────────────────────────────────────
# Main Runner
# ──────────────────────────────────────────────
if __name__ == "__main__":
    try:
        fetch_min_notional_values()
        log_event("INFO", "Initial exchangeInfo sync completed (MAINNET).")
    except Exception as e:
        log_event("ERROR", f"Initial minNotional fetch failed (MAINNET): {e}")

    port = int(os.getenv("PORT", 5002))
    app.logger.info(f"Starting Flask app (MAINNET) on 0.0.0.0:{port}")
    log_event("STARTUP", f"Flask app (MAINNET) listening on port {port}", email="SYSTEM")

    app.run(debug=False, host="0.0.0.0", port=port)
