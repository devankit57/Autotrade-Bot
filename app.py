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

getcontext().prec = 12
IST = ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(app)

# MongoDB Setup
MONGO_URI = "mongodb+srv://netmanconnect:eDxdS7AkkimNGJdi@cluster0.exzvao3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["mttrader"]
status_logs = db["trades"]
predictions_collection = db["prediction"]

task_results = {}
cancel_flags = {}
auto_jobs = {}
active_trades = {}

scheduler = BackgroundScheduler({
    'apscheduler.timezone': 'Asia/Kolkata',
    'apscheduler.job_defaults.misfire_grace_time': 300,
    'apscheduler.job_defaults.max_instances': 1
})
scheduler.start()

# ═══════════════════════════════════════════════════════════════
# MAINNET CONFIGURATION
# ═══════════════════════════════════════════════════════════════
BINANCE_MAINNET_URL = "https://fapi.binance.com"
BINANCE_MAINNET_API_URL = "https://fapi.binance.com"

min_order_values = {
    'BTCUSDT': Decimal('10'),
    'ETHUSDT': Decimal('10'),
    'DEFAULT': Decimal('10')
}

current_prices = {
    'BTCUSDT': Decimal('0'),
    'ETHUSDT': Decimal('0')
}

# ── Utility Functions ──────────────────────────────
getcontext().prec = 18

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
    return (quantity // step_size) * step_size

def get_min_tradable_qty(client, symbol: str) -> Decimal:
    symbol = symbol.upper()
    min_usd = min_order_values.get(symbol, min_order_values['DEFAULT'])
    current_price = current_prices.get(symbol, Decimal('0'))

    if current_price > 0:
        raw_qty = min_usd / current_price
        _, step_size = get_symbol_precision_and_step_size(client, symbol)
        return truncate_to_step(raw_qty, step_size)

    return Decimal('0.0001')

def calculate_trade_quantity(client, symbol: str, size_usdt: Decimal) -> tuple:
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
    """Fetch minimum notional values from Binance MAINNET"""
    try:
        res = requests.get(f'{BINANCE_MAINNET_API_URL}/fapi/v1/exchangeInfo', timeout=5)
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

fetch_min_notional_values()
scheduler.add_job(fetch_min_notional_values, 'interval', hours=1)

def mark_failed(task_id: str, msg: str, email=None):
    status_logs.update_one({"task_id": task_id}, {"$set": {
        "status": "FAILED", "error_message": msg, "end_time": datetime.now(IST)
    }})
    task_results[task_id] = {"status": "FAILED", "error_message": msg}
    if email:
        active_trades.pop(email, None)
    app.logger.error(f"{task_id} FAILED: {msg}")

def get_next_15min_interval() -> datetime:
    """Get the next perfect 15-minute interval (e.g., 9:00, 9:15, 9:30)"""
    now = datetime.now(IST)
    
    # Round up to next 15-minute mark
    minutes = now.minute
    next_interval_minutes = ((minutes // 15) + 1) * 15
    
    if next_interval_minutes >= 60:
        next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_time = now.replace(minute=next_interval_minutes, second=0, microsecond=0)
    
    return next_time

# ── MongoDB Prediction Fetching ──────────────────────────────
def get_latest_prediction(symbol: str = "BTCUSDT") -> dict:
    try:
        prediction = predictions_collection.find_one(
            {},
            sort=[("updatedAt", -1)]
        )
        
        if not prediction:
            app.logger.warning("No prediction found in database")
            return None
            
        app.logger.info(f"Retrieved prediction: direction={prediction.get('direction')}, confidence={prediction.get('confidence')}")
        return prediction
        
    except Exception as e:
        app.logger.error(f"Error fetching prediction from MongoDB: {e}")
        return None

def parse_prediction_direction(prediction: dict) -> str:
    if not prediction:
        return None
        
    direction = prediction.get("direction", "").upper()
    
    if direction == "LONG":
        return "BUY"
    elif direction == "SHORT":
        return "SELL"
    else:
        app.logger.warning(f"Unknown direction: {direction}")
        return None

# ── Enhanced Trading Logic with TP1/TP2/TP3 ──────────────────────────────
def auto_trade_with_tps(client, symbol, qty, tp_levels, sl_price, leverage, task_id, 
                        callback_url=None, direction="BUY", email=None, entry_price_target=None):
    """
    Enhanced trading with multiple take-profit levels and trailing stop
    tp_levels: dict like {"tp1": {"price": Decimal, "qty_pct": 33}, "tp2": {...}, "tp3": {...}}
    """
    try:
        # Wait for entry price if specified
        if entry_price_target:
            app.logger.info(f"{task_id}: Waiting for entry price {entry_price_target}")
            max_wait = 300  # 5 minutes max wait
            wait_count = 0
            while wait_count < max_wait:
                current_price = fetch_price_with_retries(client, symbol)
                if direction == "BUY" and current_price <= entry_price_target:
                    break
                elif direction == "SELL" and current_price >= entry_price_target:
                    break
                time.sleep(1)
                wait_count += 1
        
        entry_price = fetch_price_with_retries(client, symbol)
        client.change_leverage(symbol=symbol, leverage=leverage)
        
        decimals = 9 if symbol == 'BTCUSDT' else 4
        qty_str = "{0:.{1}f}".format(float(qty), decimals)
        
        client.new_order(
            symbol=symbol,
            side=direction,
            type='MARKET',
            quantity=qty_str
        )
        app.logger.info(f"{task_id}: Entered {direction} {qty_str} {symbol} @ {entry_price}")
        
        # Update status log with TP levels
        status_logs.update_one(
            {"task_id": task_id},
            {"$set": {
                "entry_price": str(entry_price),
                "tp_levels": {k: {"price": str(v["price"]), "qty_pct": v["qty_pct"]} for k, v in tp_levels.items()},
                "sl_price": str(sl_price)
            }}
        )
        
    except Exception as e:
        return mark_failed(task_id, str(e), email=email)

    # Track remaining quantity and TP hits
    remaining_qty = qty
    _, step_size = get_symbol_precision_and_step_size(client, symbol)
    tp_hits = []
    current_sl = sl_price
    breakeven_moved = False
    
    step = 0
    max_steps = 86400  # 24 hours max
    
    while True:
        if cancel_flags.get(task_id):
            exit_reason = "CANCELLED_BY_USER"
            exit_qty = remaining_qty
        else:
            try:
                price = fetch_price_with_retries(client, symbol)
            except Exception as e:
                return mark_failed(task_id, str(e), email=email)
            
            exit_reason = None
            exit_qty = None
            
            # Check stop loss
            if direction == "BUY":
                if price <= current_sl:
                    exit_reason = "STOPPED_LOSS"
                    exit_qty = remaining_qty
            else:  # SELL
                if price >= current_sl:
                    exit_reason = "STOPPED_LOSS"
                    exit_qty = remaining_qty
            
            # Check TP levels in order
            if not exit_reason:
                for tp_name in ["tp1", "tp2", "tp3"]:
                    if tp_name not in tp_levels or tp_name in tp_hits:
                        continue
                    
                    tp_data = tp_levels[tp_name]
                    tp_price = tp_data["price"]
                    tp_qty_pct = tp_data["qty_pct"]
                    
                    hit = False
                    if direction == "BUY" and price >= tp_price:
                        hit = True
                    elif direction == "SELL" and price <= tp_price:
                        hit = True
                    
                    if hit:
                        # Calculate partial exit quantity
                        partial_qty = truncate_to_step(qty * Decimal(tp_qty_pct) / Decimal(100), step_size)
                        
                        if partial_qty > remaining_qty:
                            partial_qty = remaining_qty
                        
                        if partial_qty > 0:
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
                                
                                # Move SL to breakeven after TP1
                                if tp_name == "tp1" and not breakeven_moved:
                                    current_sl = entry_price
                                    breakeven_moved = True
                                    app.logger.info(f"{task_id}: Stop loss moved to breakeven at {entry_price}")
                                
                                # Update database
                                status_logs.update_one(
                                    {"task_id": task_id},
                                    {"$push": {"tp_hits": {
                                        "level": tp_name,
                                        "price": str(price),
                                        "quantity": str(partial_qty),
                                        "time": datetime.now(IST).isoformat()
                                    }}}
                                )
                                
                                # If all quantity closed, exit
                                if remaining_qty <= step_size:
                                    exit_reason = "ALL_TPS_COMPLETED"
                                    exit_qty = Decimal(0)
                                    break
                                
                            except Exception as e:
                                app.logger.error(f"{task_id}: Error closing {tp_name}: {e}")
            
            # Forced exit after max time
            if step >= max_steps and not exit_reason:
                exit_reason = "FORCED_EXIT"
                exit_qty = remaining_qty

        # Final exit
        if exit_reason:
            final_pnl_amt = Decimal(0)
            
            # Close remaining position if any
            if exit_qty and exit_qty > step_size:
                try:
                    exit_qty_str = "{0:.{1}f}".format(float(exit_qty), decimals)
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
            
            # Calculate total PnL
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
            status_logs.update_one({"task_id": task_id}, {"$set": {**result, "end_time": datetime.now(IST)}})
            
            if email:
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

# ── Prediction Polling with TP mechanism ──────────────────────────────
def poll_predict_and_trade(job_id: str):
    meta = auto_jobs[job_id]
    email = meta["trade_params"]["email"]
    symbol = meta["trade_params"].get("symbol", "BTCUSDT").upper()
    
    # Check if we've reached the trade limit
    if meta["executed_count"] >= meta["max_trades"]:
        app.logger.info(f"{job_id}: Max trades ({meta['max_trades']}) reached. Stopping job.")
        try:
            meta["job"].remove()
            meta["status"] = "COMPLETED"
            app.logger.info(f"{job_id}: Job stopped after completing {meta['executed_count']} trades")
        except Exception as e:
            app.logger.error(f"{job_id}: Error stopping job: {e}")
        return

    if email in active_trades:
        app.logger.info(f"{job_id}: User {email} already has an active trade.")
        return

    try:
        prediction = get_latest_prediction(symbol)
        
        if not prediction:
            app.logger.warning(f"{job_id}: No prediction available in database")
            return
        
        # Store last prediction
        meta["last_prediction"] = {
            "direction": prediction.get("direction"),
            "confidence": prediction.get("confidence"),
            "entry": prediction.get("entry"),
            "tp1": prediction.get("tp1"),
            "tp2": prediction.get("tp2"),
            "tp3": prediction.get("tp3"),
            "sl": prediction.get("sl"),
            "updatedAt": prediction.get("updatedAtIST")
        }
        
        direction = parse_prediction_direction(prediction)
        confidence = float(prediction.get("confidence", 0))
        
        app.logger.info(f"{job_id}: MongoDB prediction - direction={direction}, confidence={confidence}")
        
    except Exception as e:
        app.logger.error(f"{job_id} prediction fetch failed: {e}")
        return

    # Skip first signal logic
    if not meta.get("first_signal_skipped", False):
        app.logger.info(f"{job_id}: First signal skipped.")
        meta["first_signal_skipped"] = True
        return

    # Check if confidence meets threshold and direction is valid
    if direction and confidence >= meta["threshold"]:
        payload = meta["trade_params"].copy()
        payload["direction"] = direction
        
        # Add TP levels from prediction
        if all(k in prediction for k in ["tp1", "tp2", "tp3", "sl"]):
            payload["use_tp_levels"] = True
            payload["tp1"] = prediction.get("tp1")
            payload["tp2"] = prediction.get("tp2")
            payload["tp3"] = prediction.get("tp3")
            payload["sl"] = prediction.get("sl")
            payload["entry_target"] = prediction.get("entry")
        
        try:
            r = requests.post("http://127.0.0.1:5002/trade", json=payload, timeout=10)
            app.logger.info(f"{job_id}: POST /trade → {r.status_code} {r.text}")
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
                app.logger.info(f"{job_id}: Trade {meta['executed_count']}/{meta['max_trades']} executed")
        except Exception as e:
            app.logger.error(f"{job_id}: trade request failed: {e}")
    else:
        app.logger.info(f"{job_id}: Threshold not met or no valid direction")

# ── Flask Endpoints ──────────────────────────────
@app.route("/trade", methods=["POST"])
def start_trade():
    data = request.get_json(force=True)
    
    required_fields = ["email", "symbol", "leverage", "api_key", "api_secret", "size_usdt"]
    for f in required_fields:
        if f not in data:
            return jsonify({"error": f"Missing required field: {f}"}), 400

    if data["email"] in active_trades:
        return jsonify({"error": "You have an active trade. Please wait for it to complete."}), 403

    try:
        client = UMFutures(key=data["api_key"], secret=data["api_secret"], base_url=BINANCE_MAINNET_URL)
        
        size_usdt = Decimal(str(data["size_usdt"]))
        if size_usdt <= 0:
            return jsonify({"error": "Size must be positive"}), 400
            
        qty, adjusted_size, was_adjusted = calculate_trade_quantity(client, data["symbol"], size_usdt)
        
        bal = get_available_balance(client)
        required = adjusted_size / Decimal(data["leverage"])
        if bal < required:
            return jsonify({
                "error": f"Insufficient balance. Need {required:.2f} USDT, available {bal:.2f}",
                "required": str(required),
                "available": str(bal)
            }), 400

        task_id = str(uuid.uuid4())
        task_results[task_id] = {"status": "RUNNING"}
        cancel_flags[task_id] = False
        active_trades[data["email"]] = task_id
        
        # Check if using TP levels or simple profit/loss
        use_tp_levels = data.get("use_tp_levels", False)
        
        status_logs.insert_one({
            "task_id": task_id,
            "email": data["email"],
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
        
        if use_tp_levels and all(k in data for k in ["tp1", "tp2", "tp3", "sl"]):
            # Use TP1/TP2/TP3 mechanism
            tp_levels = {
                "tp1": {"price": Decimal(str(data["tp1"])), "qty_pct": 33},
                "tp2": {"price": Decimal(str(data["tp2"])), "qty_pct": 33},
                "tp3": {"price": Decimal(str(data["tp3"])), "qty_pct": 34}
            }
            sl_price = Decimal(str(data["sl"]))
            entry_target = Decimal(str(data["entry_target"])) if data.get("entry_target") else None
            
            threading.Thread(
                target=auto_trade_with_tps,
                args=(client, data["symbol"], qty, tp_levels, sl_price, int(data["leverage"]), 
                      task_id, data.get("callback_url"), direction, data["email"], entry_target),
                daemon=True
            ).start()
            
            return jsonify({
                "task_id": task_id,
                "status": "STARTED",
                "symbol": data["symbol"],
                "size_usdt": str(adjusted_size),
                "quantity": str(qty),
                "leverage": int(data["leverage"]),
                "trade_type": "TP_LEVELS",
                "environment": "MAINNET",
                "tp_levels": {"tp1": str(data["tp1"]), "tp2": str(data["tp2"]), "tp3": str(data["tp3"])},
                "sl": str(data["sl"]),
                "message": "MAINNET trade started with TP1/TP2/TP3 mechanism"
            }), 202
        else:
            # Fallback to simple profit/loss mechanism
            if "profit_percent" not in data or "loss_percent" not in data:
                return jsonify({"error": "Missing profit_percent or loss_percent for simple trade"}), 400
            
            from threading import Thread
            
            def simple_auto_trade():
                try:
                    entry_price = fetch_price_with_retries(client, data["symbol"])
                    client.change_leverage(symbol=data["symbol"], leverage=int(data["leverage"]))
                    
                    decimals = 9 if data["symbol"] == 'BTCUSDT' else 4
                    qty_str = "{0:.{1}f}".format(float(qty), decimals)
                    
                    client.new_order(symbol=data["symbol"], side=direction, type='MARKET', quantity=qty_str)
                    
                    profit_pct = Decimal(str(data["profit_percent"]))
                    loss_pct = Decimal(str(data["loss_percent"]))
                    
                    while True:
                        if cancel_flags.get(task_id):
                            exit_reason = "CANCELLED_BY_USER"
                        else:
                            price = fetch_price_with_retries(client, data["symbol"])
                            pnl_pct = ((price - entry_price) / entry_price * Decimal("100")) if direction == "BUY" else ((entry_price - price) / entry_price * Decimal("100"))
                            
                            if pnl_pct >= profit_pct:
                                exit_reason = "COMPLETED"
                            elif pnl_pct <= -loss_pct:
                                exit_reason = "STOPPED_LOSS"
                            else:
                                time.sleep(1)
                                continue
                        
                        client.new_order(symbol=data["symbol"], side="SELL" if direction == "BUY" else "BUY", 
                                       type='MARKET', quantity=qty_str, reduceOnly=True)
                        
                        price = fetch_price_with_retries(client, data["symbol"])
                        pnl_amt = (price - entry_price) * Decimal(qty_str) if direction == "BUY" else (entry_price - price) * Decimal(qty_str)
                        pnl_pct = ((price - entry_price) / entry_price * Decimal("100")) if direction == "BUY" else ((entry_price - price) / entry_price * Decimal("100"))
                        
                        result = {"status": exit_reason, "pnl_amount": str(pnl_amt), "pnl_percent": str(pnl_pct)}
                        task_results[task_id] = result
                        status_logs.update_one({"task_id": task_id}, {"$set": {**result, "end_time": datetime.now(IST)}})
                        
                        if data["email"]:
                            active_trades.pop(data["email"], None)
                        break
                        
                except Exception as e:
                    mark_failed(task_id, str(e), email=data["email"])
            
            Thread(target=simple_auto_trade, daemon=True).start()
            
            return jsonify({
                "task_id": task_id,
                "status": "STARTED",
                "symbol": data["symbol"],
                "trade_type": "SIMPLE",
                "environment": "MAINNET",
                "message": "MAINNET trade started with simple profit/loss"
            }), 202

    except Exception as e:
        app.logger.error(f"MAINNET Trade failed: {str(e)}")
        return jsonify({"error": "Trade initialization failed", "details": str(e)}), 502

@app.route("/start_auto_trade", methods=["POST"])
def start_auto_trade():
    data = request.get_json(force=True)

    required_fields = [
        "email", "symbol", "size_usdt", "leverage", "api_key", "api_secret", "threshold"
    ]
    
    for f in required_fields:
        if f not in data:
            return jsonify({"error": f"Missing required field: {f}"}), 400

    # Get max_trades parameter (default to 10)
    max_trades = int(data.get("max_trades", 10))
    if max_trades <= 0:
        return jsonify({"error": "max_trades must be greater than 0"}), 400

    trade_params = {
        "email": data["email"],
        "symbol": data["symbol"],
        "size_usdt": data["size_usdt"],
        "leverage": data["leverage"],
        "api_key": data["api_key"],
        "api_secret": data["api_secret"]
    }
    
    # Add optional profit/loss for simple mode
    if "profit_percent" in data:
        trade_params["profit_percent"] = str(data["profit_percent"])
    if "loss_percent" in data:
        trade_params["loss_percent"] = str(data["loss_percent"])

    try:
        threshold = float(data["threshold"])
        if threshold <= 0 or threshold > 1:
            return jsonify({"error": "Threshold must be between 0 and 1"}), 400
    except Exception as e:
        return jsonify({"error": f"Invalid threshold: {str(e)}"}), 400

    job_id = str(uuid.uuid4())
    
    # Calculate next perfect 15-minute interval
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
        "first_signal_skipped": False,
        "max_trades": max_trades,
        "executed_count": 0,
        "status": "ACTIVE"
    }

    return jsonify({
        "job_id": job_id,
        "message": f"Auto-trade started successfully. Will execute {max_trades} trades then stop.",
        "max_trades": max_trades,
        "next_run_time": job.next_run_time.isoformat(),
        "interval": "Every 15 minutes (:00, :15, :30, :45)",
        "symbol": data["symbol"],
        "environment": "MAINNET"
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
        "last_prediction": meta["last_prediction"],
        "executed_trades": meta["executed_trades"],
        "executed_count": meta["executed_count"],
        "max_trades": meta["max_trades"],
        "remaining_trades": meta["max_trades"] - meta["executed_count"],
        "next_run_time": meta["job"].next_run_time.isoformat() if meta["job"].next_run_time else None,
        "threshold": meta["threshold"],
        "environment": "MAINNET"
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
                "status": meta.get("status", "ACTIVE"),
                "last_prediction": meta["last_prediction"],
                "executed_trades": meta["executed_trades"],
                "executed_count": meta["executed_count"],
                "max_trades": meta["max_trades"],
                "remaining_trades": meta["max_trades"] - meta["executed_count"],
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
        "count": len(results),
        "environment": "MAINNET"
    }), 200

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
            "size_adjusted": db_entry.get("size_adjusted", False),
            "requested_size": db_entry.get("requested_size"),
            "use_tp_levels": db_entry.get("use_tp_levels", False),
            "tp_hits": db_entry.get("tp_hits", []),
            "environment": db_entry.get("environment", "MAINNET")
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
                meta["status"] = "STOPPED_BY_USER"
                stopped_jobs.append(job_id)
            except Exception as e:
                app.logger.error(f"Failed to remove job {job_id}: {e}")
    return stopped_jobs

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
        {"$set": {
            "status": "CANCELLED",
            "cancel_time": datetime.now(IST)
        }}
    )

    stopped_job_ids = stop_auto_trade_jobs_by_email(email)

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

    stopped_jobs = []
    for job_id, meta in list(auto_jobs.items()):
        try:
            meta["job"].remove()
            meta["status"] = "EMERGENCY_STOPPED"
            stopped_jobs.append(job_id)
        except Exception as e:
            app.logger.error(f"Failed to remove job {job_id}: {e}")

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
            f"{BINANCE_MAINNET_API_URL}/fapi/v1/ticker/price?symbol={sym}",
            timeout=5
        )
        res.raise_for_status()
        price = Decimal(res.json()["price"])
        current_prices[sym] = price
        return jsonify({
            "symbol": sym,
            "price": str(price),
            "timestamp": datetime.now(IST).isoformat(),
            "environment": "MAINNET"
        })
    except Exception as e:
        app.logger.error(f"MAINNET Price fetch failed for {sym}: {e}")
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
            base_url=BINANCE_MAINNET_URL
        )
        balance = get_available_balance(client)
        return jsonify({
            "asset": "USDT",
            "available_balance": str(balance),
            "timestamp": datetime.now(IST).isoformat(),
            "environment": "MAINNET"
        }), 200
    except Exception as e:
        app.logger.error(f"MAINNET Balance fetch failed: {e}")
        return jsonify({
            "error": "Failed to fetch balance",
            "details": str(e)
        }), 500

@app.route("/current_prediction", methods=["GET"])
def current_prediction():
    """Get the latest prediction from MongoDB"""
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    
    try:
        prediction = get_latest_prediction(symbol)
        
        if not prediction:
            return jsonify({
                "error": "No prediction available",
                "symbol": symbol
            }), 404
        
        if '_id' in prediction:
            prediction['_id'] = str(prediction['_id'])
        
        return jsonify({
            "symbol": symbol,
            "prediction": {
                "direction": prediction.get("direction"),
                "entry": prediction.get("entry"),
                "tp": prediction.get("tp"),
                "tp1": prediction.get("tp1"),
                "tp2": prediction.get("tp2"),
                "tp3": prediction.get("tp3"),
                "sl": prediction.get("sl"),
                "dynamic_sl": prediction.get("dynamic_sl"),
                "confidence": prediction.get("confidence"),
                "net_profit_est": prediction.get("net_profit_est"),
                "net_loss_est": prediction.get("net_loss_est"),
                "reasoning": prediction.get("reasoning"),
                "currentPrice": prediction.get("currentPrice"),
                "lastAnalysisTime": prediction.get("lastAnalysisTime"),
                "updatedAtIST": prediction.get("updatedAtIST"),
                "createdAt": prediction.get("createdAt"),
                "updatedAt": prediction.get("updatedAt")
            },
            "timestamp": datetime.now(IST).isoformat(),
            "environment": "MAINNET"
        }), 200
        
    except Exception as e:
        app.logger.error(f"Failed to fetch prediction: {e}")
        return jsonify({
            "error": "Failed to fetch prediction",
            "details": str(e)
        }), 500

@app.route("/stop_auto_trade/<job_id>", methods=["POST"])
def stop_auto_trade(job_id):
    """Stop a specific auto-trade job"""
    meta = auto_jobs.get(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    
    try:
        meta["job"].remove()
        meta["status"] = "STOPPED_BY_USER"
        
        return jsonify({
            "job_id": job_id,
            "message": "Auto-trade job stopped successfully",
            "executed_trades": meta["executed_count"],
            "max_trades": meta["max_trades"],
            "timestamp": datetime.now(IST).isoformat()
        }), 200
    except Exception as e:
        app.logger.error(f"Failed to stop job {job_id}: {e}")
        return jsonify({
            "error": "Failed to stop auto-trade job",
            "details": str(e)
        }), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "active_trades": len(active_trades),
        "active_auto_jobs": len(auto_jobs),
        "timestamp": datetime.now(IST).isoformat(),
        "environment": "MAINNET"
    }), 200

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.getenv("PORT", 5002)))
