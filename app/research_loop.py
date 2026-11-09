"""Small-watchlist research scoring and paper-only position state machine.

All market inputs are provider OHLCV rows. This module does not submit orders,
connect to broker accounts, or synthesize market prices.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from uuid import uuid4

import pandas as pd


DB_PATH = Path(__file__).resolve().parents[1] / "data" / "research_paper.sqlite3"
INITIAL_CASH = 1_000_000.0
MAX_POSITION_FRACTION = 0.10
MAX_GROSS_FRACTION = 0.30
ENTRY_SCORE_THRESHOLD = 60.0


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS paper_account (
            id INTEGER PRIMARY KEY CHECK(id=1), cash REAL NOT NULL,
            realized_pnl REAL NOT NULL DEFAULT 0, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS paper_positions (
            symbol TEXT PRIMARY KEY, quantity INTEGER NOT NULL,
            avg_price REAL NOT NULL, opened_at TEXT NOT NULL,
            scan_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS research_latest (
            symbol TEXT PRIMARY KEY, scan_id TEXT NOT NULL,
            scanned_at TEXT NOT NULL, payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS research_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
            event_type TEXT NOT NULL, symbol TEXT, event_time TEXT NOT NULL,
            payload_json TEXT NOT NULL, previous_hash TEXT NOT NULL,
            event_hash TEXT UNIQUE NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_research_events_time
            ON research_events(event_time, id);
    """)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("INSERT OR IGNORE INTO paper_account(id,cash,realized_pnl,updated_at) VALUES(1,?,?,?)",
                 (INITIAL_CASH, 0.0, now))
    return conn


def _canonical(value: dict) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _append_event(conn: sqlite3.Connection, event_type: str, symbol: str | None, payload: dict) -> dict:
    event_time = datetime.now(timezone.utc).isoformat()
    event_id = str(uuid4())
    payload_json = _canonical(payload)
    previous_hash = conn.execute("SELECT event_hash FROM research_events ORDER BY id DESC LIMIT 1").fetchone()
    previous_hash = previous_hash[0] if previous_hash else "0" * 64
    content = _canonical({"event_id": event_id, "event_type": event_type, "symbol": symbol,
                          "event_time": event_time, "payload": payload,
                          "previous_hash": previous_hash})
    event_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    conn.execute("INSERT INTO research_events(event_id,event_type,symbol,event_time,payload_json,previous_hash,event_hash) VALUES(?,?,?,?,?,?,?)",
                 (event_id, event_type, symbol, event_time, payload_json, previous_hash, event_hash))
    return {"event_id": event_id, "event_type": event_type, "symbol": symbol,
            "event_time": event_time, "payload": payload, "previous_hash": previous_hash,
            "event_hash": event_hash}


def record_research_event(event_type: str, symbol: str | None, payload: dict) -> dict:
    """Append a real research-stage event to the tamper-evident chain."""
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        event = _append_event(conn, event_type, symbol, payload)
        conn.commit()
        return event
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def score_daily_bars(symbol: str, frame: pd.DataFrame, source: dict, progress=None) -> dict:
    """Calculate a transparent 0-100 research score and a separate data/risk gate."""
    required = {"date", "open", "high", "low", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"行情字段缺失：{', '.join(sorted(missing))}")
    data = frame.copy().sort_values("date").dropna(subset=["date"])
    for column in ("open", "high", "low", "close", "volume"):
        data[column] = pd.to_numeric(data[column], errors="coerce")
    numeric = data[["open", "high", "low", "close", "volume"]].to_numpy(dtype=float)
    if not all(math.isfinite(float(value)) for value in numeric.ravel()):
        raise ValueError("日线 OHLCV 含空值或非有限值")
    if (data[["open", "high", "low", "close"]] <= 0).any().any() or (data["volume"] < 0).any():
        raise ValueError("日线价格或成交量超出有效范围")
    if (data["high"] < data["low"]).any():
        raise ValueError("日线最高价低于最低价")
    if len(data) < 30:
        raise ValueError("有效日线不足 30 根，不能评分")
    close = data["close"].astype(float)
    volume = data["volume"].astype(float)
    returns = close.pct_change().replace([float("inf"), -float("inf")], float("nan")).dropna()
    last_close = float(close.iloc[-1])
    ma5, ma20 = float(close.tail(5).mean()), float(close.tail(20).mean())
    ret5 = float(close.iloc[-1] / close.iloc[-6] - 1)
    ret20 = float(close.iloc[-1] / close.iloc[-21] - 1)
    vol20 = float(returns.tail(20).std(ddof=1) * math.sqrt(244)) if len(returns.tail(20)) > 1 else 1.0
    prev_volume = float(volume.iloc[-25:-5].mean())
    volume_ratio = float(volume.tail(5).mean() / prev_volume) if prev_volume > 0 else None
    recent_high = float(close.tail(20).max())
    drawdown20 = float(last_close / recent_high - 1) if recent_high else 0.0
    latest_date = pd.Timestamp(data["date"].iloc[-1]).date().isoformat()
    features = {"bar_date": latest_date, "rows": int(len(data)),
                "return_5d": round(ret5, 6), "return_20d": round(ret20, 6),
                "ma5": round(ma5, 4), "ma20": round(ma20, 4),
                "annualized_volatility_20d": round(vol20, 6),
                "volume_ratio_5d_to_prev20d": round(volume_ratio, 4) if volume_ratio is not None else None,
                "drawdown_from_20d_high": round(drawdown20, 6)}
    age_days = (datetime.now().date() - pd.Timestamp(latest_date).date()).days
    stale = str(source.get("cache_status", "")).startswith("stale")
    score_available = not stale and 0 <= age_days <= 7
    event_source = {**source, "availability": "fresh" if score_available else "stale",
                    "bar_date": latest_date, "age_days": age_days}
    if progress:
        progress("FEATURES_GENERATED", {"features": features, "source": event_source})
    components = {
        "trend_vs_ma20": 25.0 if last_close > ma20 else 0.0,
        "short_trend": 15.0 if ma5 > ma20 else 0.0,
        "momentum_20d": max(0.0, min(25.0, 12.5 + ret20 * 100.0)),
        "volume_confirmation": max(0.0, min(15.0, ((volume_ratio or 0.0) - 0.5) * 10.0)),
        "volatility_quality": 10.0 if vol20 <= 0.30 else 5.0 if vol20 <= 0.50 else 0.0,
        "drawdown_quality": 10.0 if drawdown20 >= -0.08 else 5.0 if drawdown20 >= -0.15 else 0.0,
    }
    score = round(sum(components.values()), 2)
    rounded_components = {key: round(value, 2) for key, value in components.items()}
    risk_reasons = []
    if last_close <= 0 or not math.isfinite(last_close):
        risk_reasons.append("收盘价无效")
    if age_days > 7:
        risk_reasons.append("最新日线超过 7 个自然日")
    if age_days < 0:
        risk_reasons.append("行情日期晚于当前日期")
    if stale:
        risk_reasons.append("行情使用过期缓存")
    if vol20 > 0.80 or not math.isfinite(vol20):
        risk_reasons.append("年化波动率超过 80% 或不可计算")
    if float(volume.iloc[-1]) <= 0:
        risk_reasons.append("最新日成交量无效")
    if progress:
        if score_available:
            progress("SCORE_COMPUTED", {"score": score, "score_threshold": ENTRY_SCORE_THRESHOLD,
                                        "score_components": rounded_components})
        else:
            progress("SCORE_SKIPPED", {"reason": "行情缓存过期或最新日线超过 7 天/日期异常；不输出评分",
                                       "availability": "stale", "bar_date": latest_date,
                                       "age_days": age_days})
        progress("RISK_GATE_DECIDED", {"risk_gate": "ALLOW" if not risk_reasons else "BLOCK",
                                       "risk_reasons": risk_reasons,
                                       "rules": {"max_bar_age_days": 7, "max_annualized_volatility": 0.80,
                                                 "require_positive_close": True,
                                                 "require_positive_latest_volume": True,
                                                 "reject_stale_cache": True}})
    return {
        "symbol": symbol, "price": round(last_close, 4) if score_available else None,
        "score": score if score_available else None,
        "score_threshold": ENTRY_SCORE_THRESHOLD,
        "score_components": rounded_components if score_available else {},
        "features": features,
        "risk_gate": "ALLOW" if not risk_reasons else "BLOCK",
        "risk_reasons": risk_reasons,
        "data": {"provider": source.get("provider"), "retrieved_at": source.get("retrieved_at"),
                 "cache_status": source.get("cache_status"), "snapshot_sha256": source.get("snapshot_sha256"),
                 "snapshot_file": source.get("snapshot_file"),
                 "availability": "fresh" if score_available else "stale",
                 "bar_date": latest_date, "age_days": age_days},
        "paper_state": "FLAT",
    }


def _scan_data_unavailable(scan: dict) -> bool:
    data = scan.get("data") or {}
    cache_status = str(data.get("cache_status") or "")
    if cache_status.startswith("stale") or cache_status == "unavailable" or data.get("availability") in {"stale", "unavailable"}:
        return True
    bar_date = data.get("bar_date") or (scan.get("features") or {}).get("bar_date")
    if bar_date:
        try:
            age_days = (datetime.now().date() - pd.Timestamp(bar_date).date()).days
        except Exception:
            return True
        return age_days < 0 or age_days > 7
    return False


def save_scan_result(result: dict) -> dict:
    scan_id = str(uuid4())
    result = {**result, "scan_id": scan_id, "scanned_at": datetime.now(timezone.utc).isoformat()}
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        position = conn.execute("SELECT symbol,quantity,avg_price,opened_at FROM paper_positions WHERE symbol=?",
                                (result["symbol"],)).fetchone()
        result["paper_state"] = ("PAPER_OPEN" if position else
                                 "RISK_BLOCKED" if result["risk_gate"] == "BLOCK" else
                                 "CANDIDATE" if result.get("score") is not None and
                                 float(result["score"]) >= ENTRY_SCORE_THRESHOLD else "FLAT")
        result["paper_position"] = dict(position) if position else None
        event = _append_event(conn, "SCAN_AUDIT_PERSISTED", result["symbol"],
                              {**result, "source": result.get("data"), "audit_id": result["scan_id"]})
        result["audit_event_id"] = event["event_id"]
        result["audit_hash"] = event["event_hash"]
        conn.execute("INSERT INTO research_latest(symbol,scan_id,scanned_at,payload_json) VALUES(?,?,?,?) "
                      "ON CONFLICT(symbol) DO UPDATE SET scan_id=excluded.scan_id,scanned_at=excluded.scanned_at,payload_json=excluded.payload_json",
                      (result["symbol"], scan_id, result["scanned_at"], _canonical(result)))
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_scan_failure(symbol: str, message: str, batch_id: str | None = None,
                      source: dict | None = None) -> dict:
    now, scan_id = datetime.now(timezone.utc).isoformat(), str(uuid4())
    reason = message[:240]
    blocked_result = {"symbol": symbol, "scan_id": scan_id, "scanned_at": now,
                      "price": None, "score": None, "score_threshold": ENTRY_SCORE_THRESHOLD,
                      "score_components": {}, "features": {}, "risk_gate": "BLOCK",
                      "risk_reasons": [reason], "data": {"provider": None, "retrieved_at": None,
                                                            "cache_status": "unavailable", "snapshot_sha256": None,
                                                            "snapshot_file": None},
                      "paper_state": "RISK_BLOCKED", "execution": "real_data_only",
                      "batch_id": batch_id}
    blocked_result["data"].update(source or {})
    blocked_result["data"]["availability"] = "unavailable"
    event = {"reason": reason, "execution": "real_data_only", "batch_id": batch_id,
             "source": blocked_result["data"], "availability": "unavailable"}
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        saved = _append_event(conn, "WATCHLIST_SCAN_FAILED", symbol, {**event, **blocked_result})
        blocked_result["audit_event_id"] = saved["event_id"]
        blocked_result["audit_hash"] = saved["event_hash"]
        conn.execute("INSERT INTO research_latest(symbol,scan_id,scanned_at,payload_json) VALUES(?,?,?,?) "
                     "ON CONFLICT(symbol) DO UPDATE SET scan_id=excluded.scan_id,scanned_at=excluded.scanned_at,payload_json=excluded.payload_json",
                     (symbol, scan_id, now, _canonical(blocked_result)))
        conn.commit()
        return saved
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def latest_results() -> list[dict]:
    conn = _connect()
    try:
        rows = conn.execute("SELECT payload_json FROM research_latest ORDER BY scanned_at DESC LIMIT 10").fetchall()
        positions = {row["symbol"]: row for row in conn.execute("SELECT symbol,quantity,avg_price,opened_at FROM paper_positions")}
    finally:
        conn.close()
    results = [json.loads(row[0]) for row in rows]
    for item in results:
        if _scan_data_unavailable(item):
            item.setdefault("data", {})["availability"] = "stale"
            item["price"] = None
            item["score"] = None
            item["score_components"] = {}
            item["risk_gate"] = "BLOCK"
            reasons = item.setdefault("risk_reasons", [])
            if "行情过期或不可用" not in reasons:
                reasons.append("行情过期或不可用；评分与纸面动作已禁用")
        pos = positions.get(item["symbol"])
        item["paper_state"] = "PAPER_OPEN" if pos else ("RISK_BLOCKED" if item["risk_gate"] == "BLOCK" else "CANDIDATE" if item.get("score") is not None and item["score"] >= ENTRY_SCORE_THRESHOLD else "FLAT")
        item["paper_position"] = dict(pos) if pos else None
    return results


def paper_action(symbol: str, action: str, quantity: int | None = None) -> dict:
    if action not in {"open", "close"}:
        raise ValueError("动作只支持 open 或 close")
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        latest = conn.execute("SELECT payload_json FROM research_latest WHERE symbol=?", (symbol,)).fetchone()
        if latest is None:
            raise ValueError("请先对该股票完成真实日线扫描")
        scan = json.loads(latest[0])
        account = conn.execute("SELECT cash,realized_pnl FROM paper_account WHERE id=1").fetchone()
        position = conn.execute("SELECT quantity,avg_price FROM paper_positions WHERE symbol=?", (symbol,)).fetchone()
        now = datetime.now(timezone.utc).isoformat()
        if action == "open":
            if position:
                raise ValueError("该标的已有纸面持仓")
            if _scan_data_unavailable(scan):
                raise ValueError("最新行情过期或不可用；不能创建纸面持仓")
            if scan["risk_gate"] != "ALLOW":
                raise ValueError("独立风控门禁阻断，不能创建纸面持仓")
            if scan.get("score") is None or float(scan["score"]) < ENTRY_SCORE_THRESHOLD:
                raise ValueError(f"评分低于纸面研究阈值 {ENTRY_SCORE_THRESHOLD:.0f}")
            price = float(scan["price"])
            if not math.isfinite(price) or price <= 0:
                raise ValueError("最新真实行情不可用；不能创建纸面持仓")
            max_notional = min(INITIAL_CASH * MAX_POSITION_FRACTION, float(account["cash"]))
            if quantity is None:
                quantity = int(max_notional / price / 100) * 100
            if quantity < 100 or quantity % 100:
                raise ValueError("纸面数量必须是至少 100 股的整手数量")
            notional = quantity * price
            if notional > max_notional + 1e-6:
                raise ValueError("单标的纸面敞口超过初始资金 10% 或可用现金")
            total_positions = conn.execute("SELECT COALESCE(SUM(quantity*avg_price),0) FROM paper_positions").fetchone()[0]
            if total_positions + notional > INITIAL_CASH * MAX_GROSS_FRACTION + 1e-6:
                raise ValueError("组合纸面总敞口超过初始资金 30%")
            conn.execute("UPDATE paper_account SET cash=cash-?,updated_at=? WHERE id=1", (notional, now))
            conn.execute("INSERT INTO paper_positions(symbol,quantity,avg_price,opened_at,scan_id) VALUES(?,?,?,?,?)",
                         (symbol, quantity, price, now, scan["scan_id"]))
            event = _append_event(conn, "PAPER_POSITION_OPENED", symbol,
                                  {"scan_id": scan["scan_id"], "quantity": quantity, "price": price,
                                   "notional": round(notional, 4), "risk_gate": scan["risk_gate"],
                                   "score": scan["score"], "source": scan.get("data"),
                                   "bar_date": scan.get("features", {}).get("bar_date"),
                                   "retrieved_at": scan.get("data", {}).get("retrieved_at"),
                                   "snapshot_sha256": scan.get("data", {}).get("snapshot_sha256"),
                                   "execution": "paper_only"})
        else:
            if not position:
                raise ValueError("该标的没有可平仓的纸面持仓")
            if _scan_data_unavailable(scan):
                raise ValueError("最新行情来自过期缓存或不可用；不能用该价格平仓")
            if scan.get("price") is None or not math.isfinite(float(scan["price"])) or float(scan["price"]) <= 0:
                raise ValueError("最新真实行情不可用；不能用模拟价格平仓")
            price = float(scan["price"])
            qty = int(position["quantity"])
            proceeds = qty * price
            realized = (price - float(position["avg_price"])) * qty
            conn.execute("UPDATE paper_account SET cash=cash+?,realized_pnl=realized_pnl+?,updated_at=? WHERE id=1",
                         (proceeds, realized, now))
            conn.execute("DELETE FROM paper_positions WHERE symbol=?", (symbol,))
            event = _append_event(conn, "PAPER_POSITION_CLOSED", symbol,
                                  {"scan_id": scan["scan_id"], "quantity": qty, "price": price,
                                   "realized_pnl": round(realized, 4), "source": scan.get("data"),
                                   "bar_date": scan.get("features", {}).get("bar_date"),
                                   "retrieved_at": scan.get("data", {}).get("retrieved_at"),
                                   "snapshot_sha256": scan.get("data", {}).get("snapshot_sha256"),
                                   "execution": "paper_only"})
        conn.commit()
        return {"status": "paper_only", "action": action, "event": event,
                "state": paper_state()}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def paper_state() -> dict:
    conn = _connect()
    try:
        account = conn.execute("SELECT cash,realized_pnl,updated_at FROM paper_account WHERE id=1").fetchone()
        positions = [dict(row) for row in conn.execute("SELECT * FROM paper_positions ORDER BY symbol")]
        events = [dict(row) for row in conn.execute("SELECT event_id,event_type,symbol,event_time,payload_json,previous_hash,event_hash FROM research_events ORDER BY id DESC LIMIT 100")]
        all_events = conn.execute("SELECT event_id,event_type,symbol,event_time,payload_json,previous_hash,event_hash FROM research_events ORDER BY id").fetchall()
    finally:
        conn.close()
    latest_prices = {item["symbol"]: (None if _scan_data_unavailable(item) else item.get("price"))
                     for item in latest_results()}
    previous_hash = "0" * 64
    chain_valid = True
    for row in all_events:
        payload = json.loads(row["payload_json"])
        content = _canonical({"event_id": row["event_id"], "event_type": row["event_type"],
                              "symbol": row["symbol"], "event_time": row["event_time"],
                              "payload": payload, "previous_hash": row["previous_hash"]})
        expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if row["previous_hash"] != previous_hash or row["event_hash"] != expected:
            chain_valid = False
            break
        previous_hash = row["event_hash"]
    position_value = 0.0
    used_cost_basis = False
    for position in positions:
        mark = latest_prices.get(position["symbol"])
        try:
            mark = float(mark)
            if not math.isfinite(mark) or mark <= 0:
                raise ValueError
        except (TypeError, ValueError):
            # When the provider is unavailable, report the last persisted cost
            # basis instead of inventing a mark or failing the whole account view.
            mark = float(position["avg_price"])
            used_cost_basis = True
        position_value += int(position["quantity"]) * mark
    equity = float(account["cash"]) + position_value
    return {"execution": "paper_only", "cash": round(float(account["cash"]), 2),
            "realized_pnl": round(float(account["realized_pnl"]), 2),
            "position_value": round(position_value, 2), "equity": round(equity, 2),
            "valuation_basis": "cost_basis_fallback" if used_cost_basis else "latest_real_scan",
            "positions": positions, "events": [dict(e, payload=json.loads(e.pop("payload_json"))) for e in events],
            "audit_chain": {"event_count": len(all_events), "valid": chain_valid,
                            "head_hash": previous_hash}}


def recent_events(limit: int = 100, batch_id: str | None = None, after: int = 0) -> dict:
    conn = _connect()
    try:
        if batch_id:
            rows = conn.execute("SELECT id,event_id,event_type,symbol,event_time,payload_json,previous_hash,event_hash "
                                "FROM research_events WHERE id>? AND payload_json LIKE ? "
                                "ORDER BY id ASC LIMIT ?", (after, f'%"batch_id":"{batch_id}"%', limit)).fetchall()
        else:
            rows = conn.execute("SELECT id,event_id,event_type,symbol,event_time,payload_json,previous_hash,event_hash "
                                "FROM research_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    finally:
        conn.close()
    all_events = [{"cursor": row["id"], "event_id": row["event_id"], "event_type": row["event_type"],
               "symbol": row["symbol"], "event_time": row["event_time"],
               "payload": json.loads(row["payload_json"]),
               "previous_hash": row["previous_hash"], "event_hash": row["event_hash"]}
              for row in rows]
    events = [event for event in all_events
              if batch_id is None or event["payload"].get("batch_id") == batch_id]
    return {"events": events, "count": len(events),
            "cursor": max((event["cursor"] for event in all_events), default=after),
            "audit_chain": paper_state()["audit_chain"]}
