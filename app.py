import os
import re
import time
import uuid
import threading
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from binance.um_futures import UMFutures  # type: ignore
from decimal import Decimal, getcontext, ROUND_DOWN
from datetime import datetime, date
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

getcontext().prec = 12
IST = ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(app)

MONGO_URI = os.getenv("MONGO_URI", "your-mongo-uri-here")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["mttrader"]
status_logs = db["trades"]
daily_stats = db["daily_stats"]

task_results = {}
cancel_flags = {}
auto_jobs = {}

scheduler = BackgroundScheduler()
scheduler.start()

BINANCE_TESTNET_URL = "https://testnet.binancefuture.com"

# ── Helpers ──────────────────────────────────────────────

def fetch_price_with_retries(client, symbol: str, retries: int = 3, delay: float = 1.0) -> Decimal:
    for attempt in range(1, retries + 1):
        try:
            data = client.ticker_price(symbol=symbol)
            return Decimal(data["price"])
        except Exception as e:
            app.logger.warning(f"Fetch price failed ({attempt}/{retries}): {e}")
            time.sleep(delay)
    raise ConnectionError(f"Failed to fetch price for {symbol}")

def get_available_balance(client) -> Decimal:
    for item in client.balance():
        if item['asset'] == 'USDT':
            return Decimal(item['availableBalance'])
    raise ValueError("USDT balance not found")

def get_symbol_filters(client, symbol: str):
    info = client.exchange_info()
    for s in info['symbols']:
        if s['symbol'] == symbol:
            for f in s['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step = Decimal(f['stepSize'])
                    minq = Decimal(f['minQty'])
                    prec = abs(step.normalize().as_tuple().exponent)
                    return {"step_size": step, "min_qty": minq, "precision": prec}
    raise ValueError(f"No filters for {symbol}")

def round_quantity(qty: Decimal, precision: int) -> Decimal:
    fmt = '1.' + '0'*precision
    return qty.quantize(Decimal(fmt), rounding=ROUND_DOWN)

def mark_failed(task_id: str, msg: str):
    status_logs.update_one({"task_id": task_id},
                          {"$set": {"status": "FAILED", "error_message": msg, "end_time": datetime.now(IST)}})
    task_results[task_id] = {"status": "FAILED", "error_message": msg}
    app.logger.error(f"Trade {task_id} FAILED: {msg}")

def update_daily_stats(email: str, pnl_pct: Decimal):
    today = date.today().isoformat()
    doc = daily_stats.find_one({"email": email, "date": today})
    if not doc:
        doc = {"email": email, "date": today, "total_profit_pct": 0.0, "total_loss_pct": 0.0, "is_locked": False}
    if pnl_pct > 0:
        doc["total_profit_pct"] += float(pnl_pct)
    else:
        doc["total_loss_pct"] += abs(float(pnl_pct))
    if doc["total_profit_pct"] >= 3.0 or doc["total_loss_pct"] >= 2.0:
        doc["is_locked"] = True
    daily_stats.update_one({"email": email, "date": today}, {"$set": doc}, upsert=True)

def reset_daily_locks():
    today = date.today().isoformat()
    daily_stats.update_many({"date": {"$ne": today}},
                            {"$set": {"total_profit_pct": 0.0, "total_loss_pct": 0.0, "is_locked": False, "date": today}})

scheduler.add_job(func=reset_daily_locks, trigger="cron", hour=0, minute=0)

# ── Core Trading Function ───────────────────────────────

def auto_trade(client, symbol, qty, profit_pct, loss_pct, leverage, task_id, callback_url=None, direction="BUY", email=None):
    try:
        entry_price = fetch_price_with_retries(client, symbol)
    except Exception as e:
        return mark_failed(task_id, str(e))

    try:
        client.change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        return mark_failed(task_id, f"Leverage error: {e}")

    entry_side = direction.upper()
    exit_side = "SELL" if entry_side == "BUY" else "BUY"

    try:
        client.new_order(symbol=symbol, side=entry_side, type='MARKET', quantity=str(qty))
    except Exception as e:
        return mark_failed(task_id, f"{entry_side} failed: {e}")

    step = 0
    max_steps = 3600000

    while True:
        if cancel_flags.get(task_id):
            exit_reason = "CANCELLED_BY_USER"
        else:
            try:
                price = fetch_price_with_retries(client, symbol)
            except Exception as e:
                return mark_failed(task_id, str(e))

            pnl_pct = ((price - entry_price) / entry_price * Decimal("100")) if entry_side == "BUY" else ((entry_price - price) / entry_price * Decimal("100"))
            exit_reason = None
            if pnl_pct >= profit_pct:
                exit_reason = "COMPLETED"
            elif pnl_pct <= -loss_pct:
                exit_reason = "STOPPED_LOSS"
            elif step >= max_steps:
                exit_reason = "FORCED_EXIT"

        if exit_reason:
            try:
                client.new_order(symbol=symbol, side=exit_side, type='MARKET', quantity=str(qty), reduceOnly=True)
            except Exception as e:
                return mark_failed(task_id, f"{exit_side} error: {e}")

            pnl_amt = (price - entry_price) * qty if entry_side == "BUY" else (entry_price - price) * qty
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
            status_logs.update_one({"task_id": task_id},
                                   {"$set": {**result, "end_time": datetime.now(IST)}})

            if email:
                update_daily_stats(email, pnl_pct)

            if callback_url:
                try:
                    requests.post(callback_url, json={"task_id": task_id, **result}, timeout=10)
                except:
                    pass

            cancel_flags.pop(task_id, None)
            break

        step += 1
        time.sleep(1)

# ── Auto‑Trade Worker ─────────────────────────────────────

def poll_predict_and_trade(job_id: str):
    meta = auto_jobs[job_id]
    resp = requests.get("http://13.203.202.106:5000/predict")
    resp.raise_for_status()
    pred = resp.json()
    meta["last_prediction"] = pred.copy()

    if pred.get("confidence", 0) >= meta["threshold"]:
        raw_signal = pred.get("signal", "")
        text = re.sub(r'[^A-Z]', '', raw_signal.upper())
        direction = "BUY" if "BUY" in text else "SELL" if "SELL" in text else None
        if not direction:
            app.logger.warning(f"Unknown sanitized signal: {raw_signal} -> {text}")
            return
        payload = meta["trade_params"].copy()
        payload["direction"] = direction
        r = requests.post("http://13.203.202.106:5001/trade", json=payload)
        if r.status_code in (200, 202):
            meta["executed_trades"].append(datetime.now(IST).isoformat())

# ── API Endpoints ─────────────────────────────────────────

@app.route("/trade", methods=["POST"])
def start_trade():
    data = request.get_json(force=True)
    required = ["email", "symbol", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret"]
    for f in required:
        if f not in data:
            return jsonify({"error": f"Missing field: {f}"}), 400

    today = date.today().isoformat()
    doc = daily_stats.find_one({"email": data["email"], "date": today})
    if doc and doc.get("is_locked"):
        return jsonify({"error": "Trading blocked for today (daily limit hit)"}), 403

    try:
        client = UMFutures(key=data["api_key"], secret=data["api_secret"], base_url=BINANCE_TESTNET_URL)
        ep = fetch_price_with_retries(client, data["symbol"])
        bal = get_available_balance(client)
        filt = get_symbol_filters(client, data["symbol"])
        raw = bal * int(data["leverage"]) / ep * Decimal("0.95")
        qty = round_quantity(raw, filt["precision"])
        if qty < filt["min_qty"]:
            return jsonify({"error": f"qty {qty} below min"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    task_id = str(uuid.uuid4())
    task_results[task_id] = {"status": "RUNNING"}
    cancel_flags[task_id] = False
    status_logs.insert_one({
        "task_id": task_id, "email": data["email"], "symbol": data["symbol"],
        "quantity": str(qty), "profit_pct": str(data["profit_percent"]),
        "loss_pct": str(data["loss_percent"]), "leverage": int(data["leverage"]),
        "entry_price": str(ep), "status": "RUNNING", "start_time": datetime.now(IST)
    })

    threading.Thread(target=auto_trade,
                     args=(
                         client, data["symbol"], qty,
                         Decimal(str(data["profit_percent"])),
                         Decimal(str(data["loss_percent"])),
                         int(data["leverage"]),
                         task_id, data.get("callback_url"),
                         data.get("direction", "BUY").upper(), data["email"]
                     ),
                     daemon=True).start()

    return jsonify({
        "task_id": task_id,
        "status": "STARTED",
        "entry_price": str(ep),
        "quantity": str(qty),
        "leverage": int(data["leverage"])
    }), 202

@app.route("/status/<task_id>", methods=["GET"])
def get_status(task_id):
    if task_id not in task_results:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task_results[task_id]), 200

@app.route("/cancel/<task_id>", methods=["POST"])
def cancel_trade(task_id):
    if task_id not in task_results:
        return jsonify({"error": "Task not found"}), 404
    if task_results[task_id]["status"] != "RUNNING":
        return jsonify({"error": "Cannot cancel", "message": f"Already {task_results[task_id]['status']}"}), 400
    cancel_flags[task_id] = True
    return jsonify({"task_id": task_id, "status": "CANCEL_REQUESTED"}), 202

@app.route("/cancel_by_email", methods=["POST"])
def cancel_by_email():
    data = request.get_json(force=True)
    email = data.get("email")
    if not email:
        return jsonify({"error": "Missing field: email"}), 400
    docs = status_logs.find({"email": email, "status": "RUNNING"}, {"task_id": 1})
    tids = [d["task_id"] for d in docs]
    for t in tids:
        cancel_flags[t] = True
    status_logs.update_many(
        {"task_id": {"$in": tids}},
        {"$set": {"status": "CANCEL_REQUESTED", "cancel_time": datetime.now(IST)}}
    )
    return jsonify({"email": email, "cancelled": tids, "message": f"Requested cancellation for {len(tids)}"}), 202

@app.route("/start_auto_trade", methods=["POST"])
def start_auto_trade():
    d = request.get_json(force=True)
    required = ["email", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret", "threshold", "symbol"]
    for f in required:
        if f not in d:
            return jsonify({"error": f"Missing field: {f}"}), 400
    trade_params = {k: d[k] for k in required if k != "threshold"}
    threshold = float(d["threshold"])
    job_id = str(uuid.uuid4())
    job = scheduler.add_job(func=poll_predict_and_trade, trigger="interval", minutes=15,
                             next_run_time=datetime.now(IST), args=[job_id], id=job_id)
    auto_jobs[job_id] = {"job": job, "threshold": threshold, "trade_params": trade_params,
                         "last_prediction": None, "executed_trades": []}
    return jsonify({"job_id": job_id, "message": "Auto-trade started",
                    "next_run_time": job.next_run_time.isoformat()}), 202

@app.route("/auto_trade_status/<job_id>", methods=["GET"])
def auto_trade_status(job_id):
    meta = auto_jobs.get(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"job_id": job_id,
                    "next_run_time": meta["job"].next_run_time.isoformat(),
                    "last_prediction": meta["last_prediction"],
                    "executed_trades": meta["executed_trades"]}), 200

@app.route("/price", methods=["GET"])
def get_price():
    sym = request.args.get("symbol", "BTCUSDT").upper()
    try:
        resp = requests.get(f"https://testnet.binance.vision/api/v3/ticker/price?symbol={sym}")
        data = resp.json()
        price = Decimal(data["price"])
        return jsonify({"symbol": sym, "price": str(price), "timestamp": datetime.now(IST).isoformat()}), 200
    except Exception:
        return jsonify({"error": "Price fetch failed"}), 500

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.getenv("PORT", 5001)))
