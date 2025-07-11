import os
import time
import uuid
import threading
import requests
from flask import Flask, request, jsonify 
from flask_cors import CORS
from binance.um_futures import UMFutures        # type: ignore
from binance.error import ClientError           # type: ignore
from decimal import Decimal, getcontext, ROUND_DOWN
from datetime import datetime
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

getcontext().prec = 12
IST = ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(app)

# MongoDB setup
MONGO_URI = "mongodb+srv://netmanconnect:eDxdS7AkkimNGJdi@cluster0.exzvao3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["mttrader"]
status_logs = db["trades"]

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
    for s in client.exchange_info()['symbols']:
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
        {"$set": {"status": "FAILED","error_message": msg,"end_time": datetime.now(IST)}})
    task_results[task_id] = {"status": "FAILED", "error_message": msg}
    app.logger.error(f"Trade {task_id} FAILED: {msg}")

# ── Core Trading ─────────────────────────────────────────
def auto_trade(client, symbol, qty, profit_pct, loss_pct, leverage, task_id, callback_url=None, max_steps=3600000):
    try:
        entry_price = fetch_price_with_retries(client, symbol)
    except Exception as e:
        return mark_failed(task_id, str(e))

    try:
        client.change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        return mark_failed(task_id, f"Leverage error: {e}")

    try:
        client.new_order(symbol=symbol, side='BUY', type='MARKET', quantity=str(qty))
    except Exception as e:
        return mark_failed(task_id, f"Buy failed: {e}")

    step = 0
    while True:
        if cancel_flags.get(task_id):
            exit_reason = "CANCELLED_BY_USER"
        else:
            try:
                price = fetch_price_with_retries(client, symbol)
            except Exception as e:
                return mark_failed(task_id, str(e))

            pnl_pct = (price - entry_price) / entry_price * Decimal("100")
            exit_reason = None
            if pnl_pct >= profit_pct:
                exit_reason = "COMPLETED"
            elif pnl_pct <= -loss_pct:
                exit_reason = "STOPPED_LOSS"
            elif step >= max_steps:
                exit_reason = "FORCED_EXIT"

        if exit_reason:
            try:
                client.new_order(symbol=symbol, side='SELL', type='MARKET',
                                 quantity=str(qty), reduceOnly=True)
            except Exception as e:
                return mark_failed(task_id, f"Sell error: {e}")

            pnl_amt = (price - entry_price) * qty
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
            if callback_url:
                try: requests.post(callback_url, json={"task_id":task_id,**result},timeout=10)
                except: pass
            cancel_flags.pop(task_id, None)
            break

        step += 1
        time.sleep(1)

# ── Auto-Trade Worker ───────────────────────────────────
def poll_predict_and_trade(job_id: str):
    meta = auto_jobs[job_id]
    resp = requests.get("http://13.203.202.106:5000/predict")
    resp.raise_for_status()
    pred = resp.json()
    meta["last_prediction"] = pred.copy()

    if pred.get("confidence", 0) >= meta["threshold"]:
        payload = meta["trade_params"].copy()
        payload["symbol"] = pred["signal"]
        r = requests.post("http://localhost:5000/trade", json=payload)
        if r.status_code in (200, 202):
            meta["executed_trades"].append(datetime.now(IST).isoformat())

# ── API Endpoints ───────────────────────────────────────
@app.route("/trade", methods=["POST"])
def start_trade():
    data = request.get_json(force=True)
    for f in ["email", "symbol", "quantity", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret"]:
        if f not in data:
            return jsonify({"error": f"Missing field: {f}"}), 400
    try:
        client = UMFutures(
            key=data["api_key"],
            secret=data["api_secret"],
            base_url=BINANCE_TESTNET_URL
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 401

    symbol, lev = data["symbol"], int(data["leverage"])
    profit_pct = Decimal(str(data["profit_percent"]))
    loss_pct = Decimal(str(data["loss_percent"]))
    callback = data.get("callback_url")

    try:
        ep = fetch_price_with_retries(client, symbol)
        bal = get_available_balance(client)
        filt = get_symbol_filters(client, symbol)
        raw = bal * lev / ep * Decimal("0.95")
        qty = round_quantity(raw, filt["precision"])
        if qty < filt["min_qty"]:
            return jsonify({"error": f"qty {qty} below min"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    task_id = str(uuid.uuid4())
    task_results[task_id] = {"status": "RUNNING"}
    cancel_flags[task_id] = False
    status_logs.insert_one({
        "task_id": task_id, "email": data["email"], "symbol": symbol,
        "quantity": str(qty), "profit_pct": str(profit_pct),
        "loss_pct": str(loss_pct), "leverage": lev,
        "entry_price": str(ep), "status": "RUNNING", "start_time": datetime.now(IST)
    })

    threading.Thread(target=auto_trade,
                     args=(client, symbol, qty, profit_pct, loss_pct, lev, task_id, callback),
                     daemon=True).start()

    return jsonify({
        "task_id": task_id, "status": "STARTED",
        "entry_price": str(ep), "quantity": str(qty), "leverage": lev
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
    if not tids:
        return jsonify({"email": email, "cancelled": [], "message": "No running trades"}), 200

    for t in tids:
        cancel_flags[t] = True
    status_logs.update_many({"task_id": {"$in": tids}},
                            {"$set": {"status": "CANCEL_REQUESTED", "cancel_time": datetime.now(IST)}})

    return jsonify({"email": email, "cancelled": tids,
                    "message": f"Requested cancellation for {len(tids)}"}), 202

@app.route("/start_auto_trade", methods=["POST"])
def start_auto_trade():
    d = request.get_json(force=True)
    missing = [f for f in ("email", "quantity", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret", "threshold") if f not in d]
    if missing:
        return jsonify({"error": f"Missing: {', '.join(missing)}"}), 400

    trade_params = {k: d[k] for k in ("email", "quantity", "profit_percent", "loss_percent", "leverage", "api_key", "api_secret")}
    threshold = float(d["threshold"])
    job_id = str(uuid.uuid4())

    job = scheduler.add_job(func=poll_predict_and_trade, trigger="interval",
                            minutes=15, next_run_time=datetime.now(IST), args=[job_id], id=job_id)

    auto_jobs[job_id] = {
        "job": job,
        "threshold": threshold,
        "trade_params": trade_params,
        "last_prediction": None,
        "executed_trades": []
    }

    return jsonify({"job_id": job_id,
                    "message": "Auto-trade started",
                    "next_run_time": job.next_run_time.isoformat()}), 202

@app.route("/auto_trade_status/<job_id>", methods=["GET"])
def auto_trade_status(job_id):
    meta = auto_jobs.get(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "job_id": job_id,
        "next_run_time": meta["job"].next_run_time.isoformat(),
        "last_prediction": meta["last_prediction"],
        "executed_trades": meta["executed_trades"]
    }), 200

@app.route("/price", methods=["GET"])
def get_price():
    sym = request.args.get("symbol", "BTCUSDT").upper()
    try:
        data = requests.get(f"https://testnet.binance.vision/api/v3/ticker/price?symbol={sym}").json()
        price = Decimal(data["price"])
        return jsonify({"symbol": sym, "price": str(price), "timestamp": datetime.now(IST).isoformat()}), 200
    except:
        return jsonify({"error": "Price fetch failed"}), 500

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5001)))
