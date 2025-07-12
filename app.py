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

# ── Utility Functions ──────────────────────────────

def fetch_price_with_retries(client, symbol: str, retries: int = 3, delay: float = 1.0) -> Decimal:
    for _ in range(retries):
        try:
            return Decimal(client.ticker_price(symbol=symbol)["price"])
        except Exception as e:
            app.logger.warning(f"Fetch price failed: {e}")
            time.sleep(delay)
    raise ConnectionError(f"Unable to fetch price for {symbol}")

def get_available_balance(client) -> Decimal:
    for b in client.balance():
        if b['asset'] == 'USDT':
            return Decimal(b['availableBalance'])
    raise ValueError("USDT balance not found")

def get_symbol_filters(client, symbol: str):
    info = client.exchange_info()
    for s in info.get('symbols', []):
        if s['symbol'] == symbol:
            for f in s.get('filters', []):
                if f['filterType'] == 'LOT_SIZE':
                    step = Decimal(f['stepSize'])
                    min_qty = Decimal(f['minQty'])
                    prec = abs(step.normalize().as_tuple().exponent)
                    return {"step_size": step, "min_qty": min_qty, "precision": prec}
    raise ValueError(f"No filters for {symbol}")

def round_quantity(qty: Decimal, precision: int) -> Decimal:
    fmt = '1.' + '0' * precision
    return qty.quantize(Decimal(fmt), rounding=ROUND_DOWN)

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
        client.new_order(symbol=symbol, side=direction, type='MARKET', quantity=str(qty))
        app.logger.info(f"{task_id}: Entered {direction} {qty}@{entry_price}")
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
                client.new_order(symbol=symbol, side="SELL" if direction == "BUY" else "BUY", type='MARKET', quantity=str(qty), reduceOnly=True)
            except Exception as e:
                return mark_failed(task_id, f"Exit error: {e}", email=email)

            pnl_amt = (price - entry_price) * qty if direction == "BUY" else (entry_price - price) * qty
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
                try: requests.post(callback_url, json={"task_id": task_id, **result}, timeout=10)
                except: pass
            cancel_flags.pop(task_id, None)
            break

        step += 1
        time.sleep(1)

def poll_predict_and_trade(job_id: str):
    meta = auto_jobs[job_id]
    email = meta["trade_params"]["email"]

    if is_user_locked(email):
        app.logger.info(f"{job_id}: User {email} is locked for the day.")
        return
    if email in active_trades:
        app.logger.info(f"{job_id}: User {email} already has an active trade.")
        return

    try:
        resp = requests.get("http://13.203.202.106:5000/predict", timeout=10)
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

    if direction and conf >= meta["threshold"]:
        payload = meta["trade_params"].copy()
        payload["direction"] = direction
        payload["quantity"] = payload.get("quantity") or None
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
    for f in ["email", "symbol", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret"]:
        if f not in data:
            return jsonify({"error": f"Missing field: {f}"}), 400

    if is_user_locked(data["email"]):
        return jsonify({"error": "You have hit the bot's profit/loss threshold for today."}), 403
    if data["email"] in active_trades:
        return jsonify({"error": "You already have an active trade. Please wait until it completes."}), 403

    try:
        client = UMFutures(key=data["api_key"], secret=data["api_secret"], base_url=BINANCE_TESTNET_URL)
        ep = fetch_price_with_retries(client, data["symbol"])
        bal = get_available_balance(client)
        filt = get_symbol_filters(client, data["symbol"])
        raw_qty = bal * int(data["leverage"]) / ep * Decimal("0.95")
        qty = round_quantity(raw_qty, filt["precision"])
        if qty < filt["min_qty"]:
            return jsonify({"error": f"Quantity {qty} below minimum"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    task_id = str(uuid.uuid4())
    task_results[task_id] = {"status": "RUNNING"}
    cancel_flags[task_id] = False
    active_trades[data["email"]] = task_id
    status_logs.insert_one({
        "task_id": task_id, "email": data["email"], "symbol": data["symbol"],
        "quantity": str(qty), "profit_pct": str(data["profit_percent"]),
        "loss_pct": str(data["loss_percent"]), "leverage": int(data["leverage"]),
        "entry_price": str(ep), "status": "RUNNING", "start_time": datetime.now(IST)
    })

    threading.Thread(target=auto_trade, args=(
        client, data["symbol"], qty,
        Decimal(str(data["profit_percent"])),
        Decimal(str(data["loss_percent"])),
        int(data["leverage"]), task_id, data.get("callback_url"),
        data.get("direction", "BUY").upper(), data["email"]
    ), daemon=True).start()

    return jsonify({"task_id": task_id, "status": "STARTED", "entry_price": str(ep), "quantity": str(qty), "leverage": int(data["leverage"])}), 202

@app.route("/start_auto_trade", methods=["POST"])
def start_auto_trade():
    d = request.get_json(force=True)
    for f in ["email", "symbol", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret", "threshold"]:
        if f not in d:
            return jsonify({"error": f"Missing field: {f}"}), 400

    if is_user_locked(d["email"]):
        return jsonify({"error": "Daily limit exceeded"}), 403

    trade_params = {k: d[k] for k in ["email", "symbol", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret"]}
    threshold = float(d["threshold"])
    job_id = str(uuid.uuid4())
    job = scheduler.add_job(poll_predict_and_trade, trigger="interval",
                            minutes=15, next_run_time=datetime.now(IST),
                            args=[job_id], id=job_id)
    auto_jobs[job_id] = {"job": job, "threshold": threshold, "trade_params": trade_params,
                         "last_prediction": None, "executed_trades": []}
    return jsonify({"job_id": job_id, "message": "Auto-trade started", "next_run_time": job.next_run_time.isoformat()}), 202

@app.route("/auto_trade_status/<job_id>", methods=["GET"])
def auto_trade_status(job_id):
    meta = auto_jobs.get(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "job_id": job_id,
        "last_prediction": meta["last_prediction"],
        "executed_trades": meta["executed_trades"],
        "next_run_time": meta["job"].next_run_time.isoformat()
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
                "last_prediction": meta["last_prediction"],
                "executed_trades": meta["executed_trades"],
                "next_run_time": meta["job"].next_run_time.isoformat()
            })

    if not results:
        return jsonify({"email": email, "jobs": [], "message": "No active auto-trade jobs found"}), 404

    return jsonify({"email": email, "jobs": results}), 200

@app.route("/status/<task_id>", methods=["GET"])
def get_status(task_id):
    return jsonify(task_results.get(task_id, {"error": "Not found"}))

@app.route("/cancel/<task_id>", methods=["POST"])
def cancel_trade(task_id):
    if task_results.get(task_id, {}).get("status") != "RUNNING":
        return jsonify({"error": "Cannot cancel"}), 400
    cancel_flags[task_id] = True
    return jsonify({"task_id": task_id, "status": "CANCEL_REQUESTED"}), 202
def stop_auto_trade_jobs_by_email(email: str):
    stopped_jobs = []
    for job_id, meta in list(auto_jobs.items()):
        if meta["trade_params"]["email"] == email:
            try:
                meta["job"].remove()
            except Exception as e:
                app.logger.error(f"Failed to remove job {job_id}: {e}")
            auto_jobs.pop(job_id, None)
            stopped_jobs.append(job_id)
    return stopped_jobs

@app.route("/cancel_by_email", methods=["POST"])
def cancel_by_email():
    email = request.get_json(force=True).get("email")
    if not email:
        return jsonify({"error": "Missing field: email"}), 400

    # Cancel running trades
    docs = status_logs.find({"email": email, "status": "RUNNING"}, {"task_id": 1})
    tids = [d["task_id"] for d in docs]
    for t in tids:
        cancel_flags[t] = True
    status_logs.update_many({"task_id": {"$in": tids}}, {"$set": {
        "status": "CANCEL_REQUESTED", "cancel_time": datetime.now(IST)
    }})

    # Stop auto-trade background jobs
    stopped_job_ids = stop_auto_trade_jobs_by_email(email)

    return jsonify({
        "email": email,
        "cancelled_trades": tids,
        "stopped_auto_trade_jobs": stopped_job_ids,
        "message": f"Cancelled {len(tids)} trades and stopped {len(stopped_job_ids)} auto-trade jobs"
    }), 202


@app.route("/price", methods=["GET"])
def price():
    sym = request.args.get("symbol", "BTCUSDT").upper()
    try:
        res = requests.get(f"https://testnet.binance.vision/api/v3/ticker/price?symbol={sym}")
        return jsonify({"symbol": sym, "price": res.json()["price"], "timestamp": datetime.now(IST).isoformat()})
    except:
        return jsonify({"error": "Price fetch failed"}), 500

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.getenv("PORT", 5001)))
