import os
import time
import uuid
import threading
import requests
import re
from flask import Flask, request, jsonify
from flask_cors import CORS
from binance.um_futures import UMFutures
from decimal import Decimal, getcontext ,  ROUND_DOWN
from datetime import datetime, date
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

getcontext().prec = 12
IST = ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(app)

# MongoDB Setup
MONGO_URI = "mongodb+srv://netmanconnect:eDxdS7AkkimNGJdi@cluster0.exzvao3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["mttrader"]
status_logs = db["trades"]
daily_stats = db["daily_stats"]
daily_limit_config = db["daily_limit"]

task_results = {}
cancel_flags = {}
auto_jobs = {}
active_trades = {}  # Track active trades by email

scheduler = BackgroundScheduler({
    'apscheduler.timezone': 'Asia/Kolkata',
    'apscheduler.job_defaults.misfire_grace_time': 300,
    'apscheduler.job_defaults.max_instances': 1
})
scheduler.start()

BINANCE_TESTNET_URL = "https://testnet.binancefuture.com"

# Minimum trade notional values (will be updated dynamically)
min_order_values = {
    'BTCUSDT': Decimal('9'),
    'ETHUSDT': Decimal('9'),
    'DEFAULT': Decimal('9')
}

current_prices = {
    'BTCUSDT': Decimal('0'),
    'ETHUSDT': Decimal('0')
}

# ── Utility Functions ──────────────────────────────
def get_profit_loss_limits():
    config = daily_limit_config.find_one()
    if not config:
        return Decimal("3.0"), Decimal("2.0")  # fallback if not present
    profit = Decimal(str(config.get("profit", 3)))
    loss = Decimal(str(config.get("loss", 2)))
    return profit, loss



getcontext().prec = 18  # Ensures high precision for financial calculations

def fetch_price_with_retries(client, symbol: str, retries: int = 3, delay: float = 1.0) -> Decimal:
    for _ in range(retries):
        try:
            price = Decimal(client.ticker_price(symbol=symbol)["price"])
            current_prices[symbol] = price
            return price
        except Exception as e:
            app.logger.warning(f"Fetch price failed: {e}")
            time.sleep(delay)
    raise ConnectionError(f"Unable to fetch price for {symbol}")

def get_available_balance(client) -> Decimal:
    for b in client.balance():
        if b['asset'] == 'USDT':
            return Decimal(b['availableBalance'])
    raise ValueError("USDT balance not found")

def get_symbol_precision_and_step_size(client, symbol: str) -> tuple:
    """
    Fetch the correct quantity precision and stepSize for the given trading pair from Binance.
    Returns: (decimals, step_size)
    """
    info = client.exchange_info()
    for s in info['symbols']:
        if s['symbol'] == symbol:
            for f in s['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = Decimal(f['stepSize'])
                    decimals = abs(step_size.normalize().as_tuple().exponent)
                    return decimals, step_size
    return 4, Decimal('0.0001')  # Fallback default

def truncate_to_step(quantity: Decimal, step_size: Decimal) -> Decimal:
    """
    Truncate quantity down to the nearest multiple of step size.
    """
    return (quantity // step_size) * step_size

def get_min_tradable_qty(client, symbol: str) -> Decimal:
    """
    Get minimum tradable quantity based on min notional and price.
    """
    symbol = symbol.upper()
    min_usd = min_order_values.get(symbol, min_order_values['DEFAULT'])
    current_price = current_prices.get(symbol, Decimal('0'))

    if current_price > 0:
        raw_qty = min_usd / current_price
        _, step_size = get_symbol_precision_and_step_size(client, symbol)
        return truncate_to_step(raw_qty, step_size)

    return Decimal('0.0001')  # fallback

def calculate_trade_quantity(client, symbol: str, size_usdt: Decimal) -> tuple:
    """
    Calculate the quantity to trade for a given USDT size.
    Returns: (valid_quantity, adjusted_usdt_size, was_adjusted)
    """
    price = fetch_price_with_retries(client, symbol)
    min_qty = get_min_tradable_qty(client, symbol)
    decimals, step_size = get_symbol_precision_and_step_size(client, symbol)

    # Raw quantity based on desired trade size
    quantity = size_usdt / price

    # If too small, adjust up to minimum
    if quantity < min_qty:
        adjusted_usdt = price * min_qty
        return min_qty, adjusted_usdt, True

    # Truncate quantity to allowed step
    quantity = truncate_to_step(quantity, step_size)

    # Final guard (just in case)
    if quantity <= 0:
        adjusted_usdt = price * step_size
        return step_size, adjusted_usdt, True

    return quantity, size_usdt, False



def update_daily_stats(email: str, pnl_pct: Decimal):
    today = date.today().isoformat()
    profit_limit, loss_limit = get_profit_loss_limits()

    doc = daily_stats.find_one({"email": email, "date": today})
    if not doc:
        doc = {"email": email, "date": today, "total_profit_pct": 0.0, "total_loss_pct": 0.0, "is_locked": False}

    if pnl_pct > 0:
        doc["total_profit_pct"] += float(pnl_pct)
    else:
        doc["total_loss_pct"] += abs(float(pnl_pct))

    if doc["total_profit_pct"] >= float(profit_limit) or doc["total_loss_pct"] >= float(loss_limit):
        doc["is_locked"] = True

    daily_stats.update_one({"email": email, "date": today}, {"$set": doc}, upsert=True)

def reset_daily_locks():
    today = date.today().isoformat()
    daily_stats.update_many(
        {"date": {"$ne": today}},
        {"$set": {
            "total_profit_pct": 0.0,
            "total_loss_pct": 0.0,
            "is_locked": False,
            "date": today
        }}
    )
    app.logger.info("Daily stats reset for all users")

def fetch_min_notional_values():
    """Fetch minimum notional values from Binance"""
    try:
        res = requests.get('https://testnet.binancefuture.com/fapi/v1/exchangeInfo', timeout=5)
        data = res.json()
        
        for symbol in ['BTCUSDT', 'ETHUSDT']:
            symbol_info = next((s for s in data['symbols'] if s['symbol'] == symbol), None)
            if symbol_info:
                filter_info = next((f for f in symbol_info['filters'] if f['filterType'] == 'MIN_NOTIONAL'), None)
                if filter_info:
                    min_order_values[symbol] = Decimal(filter_info['notional'])
                    app.logger.info(f"Updated min notional for {symbol}: {min_order_values[symbol]}")
    except Exception as e:
        app.logger.error(f"Failed to fetch min notional values: {e}")

# Initialize min notional values and set up periodic refresh
fetch_min_notional_values()
scheduler.add_job(fetch_min_notional_values, 'interval', hours=1)
scheduler.add_job(reset_daily_locks, trigger="cron", hour=0, minute=0)

def is_user_locked(email: str) -> bool:
    today = date.today().isoformat()
    doc = daily_stats.find_one({"email": email, "date": today})
    if not doc:
        daily_stats.insert_one({
            "email": email, "date": today,
            "total_profit_pct": 0.0, "total_loss_pct": 0.0, "is_locked": False
        })
        return False
    return doc.get("is_locked", False)

def mark_failed(task_id: str, msg: str, email=None):
    status_logs.update_one({"task_id": task_id}, {"$set": {
        "status": "FAILED", "error_message": msg, "end_time": datetime.now(IST)
    }})
    task_results[task_id] = {"status": "FAILED", "error_message": msg}
    if email:
        active_trades.pop(email, None)
    app.logger.error(f"{task_id} FAILED: {msg}")

# ── Trading Logic ──────────────────────────────
def auto_trade(client, symbol, qty, profit_pct, loss_pct, leverage, task_id, callback_url=None, direction="BUY", email=None):
    try:
        entry_price = fetch_price_with_retries(client, symbol)
        client.change_leverage(symbol=symbol, leverage=leverage)
        
        # Format quantity with proper precision
        decimals = 9 if symbol == 'BTCUSDT' else 4
        qty_str = "{0:.{1}f}".format(float(qty), decimals)
        
        client.new_order(
            symbol=symbol,
            side=direction,
            type='MARKET',
            quantity=qty_str
        )
        app.logger.info(f"{task_id}: Entered {direction} {qty_str} {symbol} @ {entry_price}")
    except Exception as e:
        return mark_failed(task_id, str(e), email=email)

    step = 0
    max_steps = 3600000
    while True:
        if cancel_flags.get(task_id):
            exit_reason = "CANCELLED_BY_USER"
        else:
            try:
                price = fetch_price_with_retries(client, symbol)
            except Exception as e:
                return mark_failed(task_id, str(e), email=email)
            pnl_pct = ((price - entry_price) / entry_price * Decimal("100")) if direction == "BUY" else ((entry_price - price) / entry_price * Decimal("100"))
            exit_reason = None
            if pnl_pct >= profit_pct:
                exit_reason = "COMPLETED"
            elif pnl_pct <= -loss_pct:
                exit_reason = "STOPPED_LOSS"
            elif step >= max_steps:
                exit_reason = "FORCED_EXIT"

        if exit_reason:
            try:
                client.new_order(
                    symbol=symbol,
                    side="SELL" if direction == "BUY" else "BUY",
                    type='MARKET',
                    quantity=qty_str,
                    reduceOnly=True
                )
            except Exception as e:
                return mark_failed(task_id, f"Exit error: {e}", email=email)

            pnl_amt = (price - entry_price) * Decimal(qty_str) if direction == "BUY" else (entry_price - price) * Decimal(qty_str)
            result = {
                "status": exit_reason,
                "symbol": symbol,
                "entry_price": str(entry_price),
                "exit_price": str(price),
                "pnl_amount": str(pnl_amt),
                "pnl_percent": str(pnl_pct),
                "ledger_balance": "N/A"
            }
            task_results[task_id] = result
            status_logs.update_one({"task_id": task_id}, {"$set": {**result, "end_time": datetime.now(IST)}})
            if email:
                update_daily_stats(email, pnl_pct)
                active_trades.pop(email, None)
            if callback_url:
                try: 
                    requests.post(callback_url, json={"task_id": task_id, **result}, timeout=10)
                except: 
                    pass
            cancel_flags.pop(task_id, None)
            break

        step += 1
        time.sleep(1)

def poll_predict_and_trade(job_id: str):
    meta = auto_jobs[job_id]
    email = meta["trade_params"]["email"]
    symbol = meta["trade_params"].get("symbol", "BTCUSDT").upper()

    if is_user_locked(email):
        app.logger.info(f"{job_id}: User {email} is locked for the day.")
        return

    if email in active_trades:
        app.logger.info(f"{job_id}: User {email} already has an active trade.")
        return

    try:
        if symbol == "BTCUSDT":
            predict_url = "http://31.97.206.221:5000/predict"
        elif symbol == "ETHUSDT":
            predict_url = "http://31.97.206.221:5001/predict"
        else:
            app.logger.warning(f"{job_id}: Unsupported symbol: {symbol}")
            return

        resp = requests.get(predict_url, timeout=10)
        resp.raise_for_status()
        pred = resp.json()
    except Exception as e:
        app.logger.error(f"{job_id} predict fetch failed: {e}")
        return

    meta["last_prediction"] = pred
    sig = pred.get("signal", "")
    conf = pred.get("confidence", 0)
    text = re.sub(r'[^A-Z]', '', sig.upper())
    direction = "BUY" if "BUY" in text else "SELL" if "SELL" in text else None
    app.logger.info(f"{job_id}: signal='{sig}' sanitized='{text}', confidence={conf}")

    if not meta.get("first_signal_skipped", False):
        app.logger.info(f"{job_id}: First signal skipped.")
        meta["first_signal_skipped"] = True
        return

    if direction and conf >= meta["threshold"]:
        payload = meta["trade_params"].copy()
        payload["direction"] = direction
        try:
            r = requests.post("http://127.0.0.1:5001/trade", json=payload, timeout=10)
            app.logger.info(f"{job_id}: POST /trade → {r.status_code} {r.text}")
            if r.status_code in (200, 202):
                meta["executed_trades"].append(datetime.now(IST).isoformat())
        except Exception as e:
            app.logger.error(f"{job_id}: trade request failed: {e}")

# ── Flask Endpoints ──────────────────────────────
@app.route("/trade", methods=["POST"])
def start_trade():
    data = request.get_json(force=True)
    
    # Validate required fields
    required_fields = ["email", "symbol", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret", "size_usdt"]
    for f in required_fields:
        if f not in data:
            return jsonify({"error": f"Missing required field: {f}"}), 400

    # Check user trading limits
    if is_user_locked(data["email"]):
        return jsonify({"error": "Daily trading limit reached. Try again tomorrow."}), 403
    if data["email"] in active_trades:
        return jsonify({"error": "You have an active trade. Please wait for it to complete."}), 403

    try:
        client = UMFutures(key=data["api_key"], secret=data["api_secret"], base_url=BINANCE_TESTNET_URL)
        
        # Calculate quantity based on USDT size
        size_usdt = Decimal(str(data["size_usdt"]))
        if size_usdt <= 0:
            return jsonify({"error": "Size must be positive"}), 400
            
        qty, adjusted_size, was_adjusted = calculate_trade_quantity(client, data["symbol"], size_usdt)
        
        # Check balance
        bal = get_available_balance(client)
        required = adjusted_size / Decimal(data["leverage"])
        if bal < required:
            return jsonify({
                "error": f"Insufficient balance. Need {required:.2f} USDT, available {bal:.2f}",
                "required": str(required),
                "available": str(bal)
            }), 400

        # Prepare trade task
        task_id = str(uuid.uuid4())
        task_results[task_id] = {"status": "RUNNING"}
        cancel_flags[task_id] = False
        active_trades[data["email"]] = task_id
        
        status_logs.insert_one({
            "task_id": task_id,
            "email": data["email"],
            "symbol": data["symbol"],
            "size_usdt": str(adjusted_size),
            "quantity": str(qty),
            "profit_pct": str(data["profit_percent"]),
            "loss_pct": str(data["loss_percent"]),
            "leverage": int(data["leverage"]),
            "status": "RUNNING",
            "start_time": datetime.now(IST),
            "size_adjusted": was_adjusted,
            "requested_size": str(size_usdt)
        })

        threading.Thread(
            target=auto_trade,
            args=(
                client,
                data["symbol"],
                qty,
                Decimal(str(data["profit_percent"])),
                Decimal(str(data["loss_percent"])),
                int(data["leverage"]),
                task_id,
                data.get("callback_url"),
                data.get("direction", "BUY").upper(),
                data["email"]
            ),
            daemon=True
        ).start()

        return jsonify({
            "task_id": task_id,
            "status": "STARTED",
            "symbol": data["symbol"],
            "size_usdt": str(adjusted_size),
            "quantity": str(qty),
            "leverage": int(data["leverage"]),
            "size_adjusted": was_adjusted,
            "message": "Trade started successfully"
        }), 202

    except Exception as e:
        app.logger.error(f"Trade failed: {str(e)}")
        return jsonify({
            "error": "Trade initialization failed",
            "details": str(e)
        }), 502

@app.route("/start_auto_trade", methods=["POST"])
def start_auto_trade():
    data = request.get_json(force=True)

    required_fields = [
        "email", "symbol", "size_usdt", "profit_percent", "loss_percent",
        "leverage", "api_key", "api_secret", "threshold"
    ]
    
    for f in required_fields:
        if f not in data:
            return jsonify({"error": f"Missing required field: {f}"}), 400

    # Validate percentages
    try:
        profit_pct = Decimal(str(data["profit_percent"]))
        loss_pct = Decimal(str(data["loss_percent"]))
        if profit_pct <= 0 or loss_pct <= 0:
            return jsonify({"error": "Profit and loss percentages must be positive"}), 400
    except Exception as e:
        return jsonify({"error": f"Invalid profit/loss percentage: {str(e)}"}), 400

    if is_user_locked(data["email"]):
        return jsonify({"error": "Daily trading limit reached. Try again tomorrow."}), 403

    trade_params = {
        "email": data["email"],
        "symbol": data["symbol"],
        "size_usdt": data["size_usdt"],
        "profit_percent": str(profit_pct),
        "loss_percent": str(loss_pct),
        "leverage": data["leverage"],
        "api_key": data["api_key"],
        "api_secret": data["api_secret"]
    }

    try:
        threshold = float(data["threshold"])
        if threshold <= 0 or threshold > 1:
            return jsonify({"error": "Threshold must be between 0 and 1"}), 400
    except Exception as e:
        return jsonify({"error": f"Invalid threshold: {str(e)}"}), 400

    job_id = str(uuid.uuid4())
    job = scheduler.add_job(
        poll_predict_and_trade,
        trigger="interval",
        minutes=15,
        next_run_time=datetime.now(IST),
        args=[job_id],
        id=job_id
    )

    auto_jobs[job_id] = {
        "job": job,
        "threshold": threshold,
        "trade_params": trade_params,
        "last_prediction": None,
        "executed_trades": [],
        "first_signal_skipped": False
    }

    return jsonify({
        "job_id": job_id,
        "message": "Auto-trade started successfully",
        "next_run_time": job.next_run_time.isoformat(),
        "interval_minutes": 15,
        "symbol": data["symbol"]
    }), 202

@app.route("/auto_trade_status/<job_id>", methods=["GET"])
def auto_trade_status(job_id):
    meta = auto_jobs.get(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    
    return jsonify({
        "job_id": job_id,
        "status": "ACTIVE" if meta["job"].next_run_time else "INACTIVE",
        "symbol": meta["trade_params"]["symbol"],
        "last_prediction": meta["last_prediction"],
        "executed_trades": meta["executed_trades"],
        "next_run_time": meta["job"].next_run_time.isoformat() if meta["job"].next_run_time else None,
        "threshold": meta["threshold"]
    })

@app.route("/auto_trade_status_by_email", methods=["POST"])
def auto_trade_status_by_email():
    data = request.get_json(force=True)
    email = data.get("email")
    if not email:
        return jsonify({"error": "Missing field: email"}), 400

    results = []
    for job_id, meta in auto_jobs.items():
        if meta["trade_params"].get("email") == email:
            results.append({
                "job_id": job_id,
                "symbol": meta["trade_params"]["symbol"],
                "status": "ACTIVE" if meta["job"].next_run_time else "INACTIVE",
                "last_prediction": meta["last_prediction"],
                "executed_trades": meta["executed_trades"],
                "next_run_time": meta["job"].next_run_time.isoformat() if meta["job"].next_run_time else None,
                "threshold": meta["threshold"]
            })

    if not results:
        return jsonify({
            "email": email,
            "jobs": [],
            "message": "No active auto-trade jobs found"
        }), 404

    return jsonify({
        "email": email,
        "jobs": results,
        "count": len(results)
    }), 200

@app.route("/status/<task_id>", methods=["GET"])
def get_status(task_id):
    result = task_results.get(task_id)
    if not result:
        return jsonify({"error": "Task not found"}), 404
    
    # Add additional info from database if available
    db_entry = status_logs.find_one({"task_id": task_id})
    if db_entry:
        result.update({
            "start_time": db_entry.get("start_time"),
            "end_time": db_entry.get("end_time"),
            "symbol": db_entry.get("symbol"),
            "leverage": db_entry.get("leverage"),
            "size_usdt": db_entry.get("size_usdt"),
            "size_adjusted": db_entry.get("size_adjusted", False),
            "requested_size": db_entry.get("requested_size")
        })
    
    return jsonify(result)

@app.route("/cancel/<task_id>", methods=["POST"])
def cancel_trade(task_id):
    if task_id not in task_results:
        return jsonify({"error": "Task not found"}), 404
        
    if task_results[task_id].get("status") != "RUNNING":
        return jsonify({"error": "Cannot cancel - trade is not running"}), 400
        
    cancel_flags[task_id] = True
    status_logs.update_one(
        {"task_id": task_id},
        {"$set": {
            "status": "CANCELLED",
            "cancel_time": datetime.now(IST)
        }}
    )
    return jsonify({
        "task_id": task_id,
        "status": "CANCEL_REQUESTED",
        "message": "Cancel request received"
    }), 202

def stop_auto_trade_jobs_by_email(email: str):
    stopped_jobs = []
    for job_id, meta in list(auto_jobs.items()):
        if meta["trade_params"]["email"] == email:
            try:
                meta["job"].remove()
                stopped_jobs.append(job_id)
                auto_jobs.pop(job_id, None)
            except Exception as e:
                app.logger.error(f"Failed to remove job {job_id}: {e}")
    return stopped_jobs

@app.route("/cancel_by_email", methods=["POST"])
def cancel_by_email():
    data = request.get_json(force=True)
    email = data.get("email")
    if not email:
        return jsonify({"error": "Missing field: email"}), 400

    # Cancel running trades
    docs = status_logs.find({"email": email, "status": "RUNNING"}, {"task_id": 1})
    tids = [d["task_id"] for d in docs]
    for t in tids:
        cancel_flags[t] = True
    status_logs.update_many(
        {"task_id": {"$in": tids}},
        {"$set": {
            "status": "CANCELLED",
            "cancel_time": datetime.now(IST)
        }}
    )

    # Stop auto-trade background jobs
    stopped_job_ids = stop_auto_trade_jobs_by_email(email)

    # Remove from active trades
    if email in active_trades:
        active_trades.pop(email)

    return jsonify({
        "email": email,
        "cancelled_trades": tids,
        "stopped_auto_trade_jobs": stopped_job_ids,
        "message": f"Cancelled {len(tids)} trades and stopped {len(stopped_job_ids)} auto-trade jobs"
    }), 202

@app.route("/emergency_stop", methods=["POST"])
def emergency_stop():
    # 1. Cancel all running trades
    running_tasks = list(active_trades.values())
    for task_id in running_tasks:
        cancel_flags[task_id] = True
        status_logs.update_one(
            {"task_id": task_id},
            {"$set": {
                "status": "EMERGENCY_STOPPED",
                "end_time": datetime.now(IST)
            }}
        )

    # 2. Stop all auto-trade background jobs
    stopped_jobs = []
    for job_id, meta in list(auto_jobs.items()):
        try:
            meta["job"].remove()
            stopped_jobs.append(job_id)
        except Exception as e:
            app.logger.error(f"Failed to remove job {job_id}: {e}")
    auto_jobs.clear()

    # 3. Clear active_trades
    active_trades.clear()

    return jsonify({
        "message": "Emergency stop executed successfully",
        "cancelled_trades": running_tasks,
        "stopped_jobs": stopped_jobs,
        "timestamp": datetime.now(IST).isoformat()
    }), 200

@app.route("/price", methods=["GET"])
def price():
    sym = request.args.get("symbol", "BTCUSDT").upper()
    try:
        res = requests.get(
            f"https://testnet.binance.vision/api/v3/ticker/price?symbol={sym}",
            timeout=5
        )
        res.raise_for_status()
        price = Decimal(res.json()["price"])
        current_prices[sym] = price  # Update current price
        return jsonify({
            "symbol": sym,
            "price": str(price),
            "timestamp": datetime.now(IST).isoformat()
        })
    except Exception as e:
        app.logger.error(f"Price fetch failed for {sym}: {e}")
        return jsonify({
            "error": "Price fetch failed",
            "symbol": sym,
            "details": str(e)
        }), 500

@app.route("/balance", methods=["POST"])
def get_balance():
    data = request.get_json()
    if not data.get("api_key") or not data.get("api_secret"):
        return jsonify({"error": "Missing API credentials"}), 400

    try:
        client = UMFutures(
            key=data["api_key"],
            secret=data["api_secret"],
            base_url=BINANCE_TESTNET_URL
        )
        balance = get_available_balance(client)
        return jsonify({
            "asset": "USDT",
            "available_balance": str(balance),
            "timestamp": datetime.now(IST).isoformat()
        }), 200
    except Exception as e:
        app.logger.error(f"Balance fetch failed: {e}")
        return jsonify({
            "error": "Failed to fetch balance",
            "details": str(e)
        }), 500

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.getenv("PORT", 5001)))