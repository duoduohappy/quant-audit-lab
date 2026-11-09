from datetime import date, datetime, timedelta, timezone
import csv
import math
import json
import hashlib
import os
import sqlite3
import threading
import time
from collections import deque
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator
from dotenv import load_dotenv
import pandas as pd

from app.audit import run_audit
from app.providers import akshare_provider as market_provider
from app.research_loop import (ENTRY_SCORE_THRESHOLD, latest_results, paper_action,
                               paper_state, save_scan_failure, save_scan_result,
                               score_daily_bars)

ROOT = Path(__file__).parent
load_dotenv(ROOT.parent / ".env")
app = FastAPI(title="Quant Audit Lab", version="0.1.0", description="A-share backtest trust audit")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")

_PROCESS_STARTED = time.monotonic()
_OPS_LOCK = threading.Lock()
_OPS = {"requests": 0, "active": 0, "last_request_at": None,
        "last_latency_ms": None, "last_status": None}
_QUOTE_LOCK = threading.Lock()
_QUOTE_CACHE: dict[str, dict] = {}
_QUOTE_HISTORY: dict[str, deque] = {}
_KLINE_LOCK = threading.Lock()
_KLINE_CACHE: dict[str, dict] = {}
_QUOTE_HISTORY_MAX_SAMPLES = 720
_QUOTE_TTL_SECONDS = max(10, int(os.getenv("LIVE_QUOTE_TTL_SECONDS", "10")))
_QUOTE_FAILURE_UNTIL = 0.0
_QUOTE_STATS = {"requests": 0, "provider_successes": 0, "provider_failures": 0,
                "cache_hits": 0, "last_success_at": None, "last_error": None}
_BOOK_HISTORY_DB = ROOT.parent / "data" / "orderbook_history.sqlite3"
_RUN_PROGRESS_LOCK = threading.Lock()
_RUN_PROGRESS: dict[str, dict] = {}


def _book_history_connection():
    _BOOK_HISTORY_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_BOOK_HISTORY_DB, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS orderbook_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        provider TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        source_timestamp TEXT,
        stale INTEGER NOT NULL,
        price REAL NOT NULL,
        bids_json TEXT NOT NULL,
        asks_json TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_book_symbol_observed ON orderbook_snapshots(symbol, observed_at)")
    return conn


def _save_orderbook_snapshot(payload: dict):
    """Persist only an actual upstream response, not a cache replay."""
    with _book_history_connection() as conn:
        conn.execute(
            "INSERT INTO orderbook_snapshots(symbol, provider, observed_at, source_timestamp, stale, price, bids_json, asks_json) VALUES(?,?,?,?,?,?,?,?)",
            (payload["symbol"], payload["provider"], payload["observed_at"], payload["source_timestamp"],
             int(payload["stale"]), payload["price"], json.dumps(payload["bids"]), json.dumps(payload["asks"]))
        )


def _history_datetime(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"{field} 不是有效日期时间。") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
    return parsed.astimezone(timezone.utc)


def _publish_run_progress(progress_id: str | None, stage: str, state: str,
                          message: str, details: dict | None = None):
    if not progress_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    with _RUN_PROGRESS_LOCK:
        expired = [key for key, value in _RUN_PROGRESS.items()
                   if (datetime.now(timezone.utc) - datetime.fromisoformat(value["updated_at"])).total_seconds() > 3600]
        for key in expired:
            _RUN_PROGRESS.pop(key, None)
        record = _RUN_PROGRESS.setdefault(progress_id, {"events": [], "status": "running"})
        event = {"seq": len(record["events"]) + 1, "stage": stage, "state": state,
                 "message": message, "updated_at": now, "details": details or {}}
        record["events"].append(event)
        record["status"] = "failed" if state == "failed" else "complete" if stage == "complete" else "running"
        record["updated_at"] = now


@app.middleware("http")
async def collect_runtime_metrics(request, call_next):
    started = time.monotonic()
    with _OPS_LOCK:
        _OPS["requests"] += 1
        _OPS["active"] += 1
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        with _OPS_LOCK:
            _OPS["active"] = max(0, _OPS["active"] - 1)
            _OPS["last_request_at"] = datetime.now(timezone.utc).isoformat()
            _OPS["last_latency_ms"] = round((time.monotonic() - started) * 1000, 1)
            _OPS["last_status"] = status


def _latest_market_cache():
    cache_dir = market_provider.CACHE_DIR
    files = list(cache_dir.glob("*.json")) if cache_dir.exists() else []
    if not files:
        return None, []
    meta_file = max(files, key=lambda path: path.stat().st_mtime)
    try:
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        csv_file = meta_file.with_suffix(".csv")
        tape = []
        if csv_file.is_file():
            with csv_file.open("r", encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))[-48:]
            for row in rows:
                try:
                    tape.append({"date": row.get("date"), "close": float(row["close"]),
                                 "volume": float(row.get("volume") or 0)})
                except (TypeError, ValueError, KeyError):
                    continue
        age = max(0, int(time.time() - meta_file.stat().st_mtime))
        return {"symbol": meta.get("symbol"), "provider": meta.get("provider"),
                "retrieved_at": meta.get("retrieved_at"), "bar_date": meta.get("end"),
                "rows": meta.get("rows"), "sha256": meta.get("snapshot_sha256"),
                "cache_status": meta.get("cache_status"), "cache_age_seconds": age,
                "cache_ttl_seconds": meta.get("cache_ttl_seconds", market_provider.CACHE_TTL_SECONDS),
                "fallback_used": meta.get("fallback_used", False),
                "provider_attempts": meta.get("provider_attempts", [])}, tape
    except (OSError, ValueError, json.JSONDecodeError):
        return None, []


def _latest_run_path():
    run_dir = ROOT.parent / "data" / "runs"
    files = list(run_dir.glob("*.json")) if run_dir.exists() else []
    if not files:
        return None
    def created_at(path):
        try:
            return json.loads(path.read_text(encoding="utf-8")).get("created_at", "")
        except (OSError, ValueError, json.JSONDecodeError):
            return ""
    return max(files, key=created_at)


def _latest_run():
    latest = _latest_run_path()
    if latest is None:
        return None
    try:
        run = json.loads(latest.read_text(encoding="utf-8"))
        return {"run_id": run.get("run_id"), "created_at": run.get("created_at"),
                "symbol": run.get("provenance", {}).get("symbol"),
                "provider": run.get("provenance", {}).get("provider"),
                "rows": run.get("provenance", {}).get("rows"),
                "gate": run.get("summary", {}).get("gate"),
                "max_nav_gap": run.get("summary", {}).get("max_abs_nav_gap"),
                "fill_mismatches": run.get("summary", {}).get("fill_mismatch_count"),
                "snapshot_sha256": run.get("provenance", {}).get("snapshot_sha256"),
                "agent_mode": run.get("agent", {}).get("mode"),
                "strategy_source": run.get("config", {}).get("strategy_source"),
                "fast_window": run.get("config", {}).get("fast_window"),
                "slow_window": run.get("config", {}).get("slow_window"),
                "framebt_version": run.get("config", {}).get("framebt", {}).get("version"),
                "framebt_commit": run.get("config", {}).get("framebt", {}).get("source_commit"),
                "event_fills": run.get("summary", {}).get("event_fill_count", 0),
                "vector_fills": run.get("summary", {}).get("vector_fill_count", 0),
                "framebt_fills": run.get("summary", {}).get("framebt_fill_count", 0),
                "human_review": run.get("human_review"),
                "equity_tail": run.get("daily", [])[-48:]}
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _read_run_artifact(run_id: UUID) -> dict:
    artifact = ROOT.parent / "data" / "runs" / f"{run_id}.json"
    if not artifact.is_file():
        raise HTTPException(status_code=404, detail="找不到这次审计运行记录。")
    try:
        return json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="审计运行记录无法读取。") from exc


class AuditRequest(BaseModel):
    symbol: str = "600519"
    start_date: date = Field(default_factory=lambda: date.today() - timedelta(days=365 * 3))
    end_date: date = Field(default_factory=date.today)
    fast_window: int = Field(10, ge=2, le=100)
    slow_window: int = Field(30, ge=3, le=200)
    initial_cash: float = Field(1_000_000, gt=0)
    slippage_bps: float = Field(5, ge=0, le=100)
    commission_bps: float = Field(2.5, ge=0, le=100)
    use_llm: bool = False
    progress_id: str | None = Field(None, min_length=8, max_length=80)

    @model_validator(mode="after")
    def validate_date_range(self):
        if self.start_date >= self.end_date:
            raise ValueError("开始日期必须早于结束日期。")
        if self.end_date > date.today():
            raise ValueError("结束日期不能晚于今天。")
        return self


class ReviewDecision(BaseModel):
    decision: Literal["approve", "block"]
    reason: str = Field(min_length=3, max_length=500)
    reviewer: str = Field(min_length=1, max_length=120)
    due_at: str = Field(min_length=1, max_length=40)

    @field_validator("reason", "reviewer", "due_at")
    @classmethod
    def normalize_required_review_fields(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("复核原因、复核人和截止时间均为必填项")
        return value

    @field_validator("reason")
    @classmethod
    def validate_review_reason_length(cls, value: str) -> str:
        if len(value) < 3:
            raise ValueError("复核原因至少需要 3 个字符")
        return value

    @field_validator("due_at")
    @classmethod
    def validate_review_deadline(cls, value: str) -> str:
        try:
            datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("截止时间必须是有效的 ISO 日期时间") from exc
        return value


class FactorScore(BaseModel):
    date: date
    score: float


class FactorReplayRequest(AuditRequest):
    factor_name: str = Field(min_length=2, max_length=120)
    experiment_id: str = Field(min_length=1, max_length=160)
    threshold: float = 0.0
    scores: list[FactorScore] = Field(min_length=1, max_length=10000)

    @field_validator("threshold")
    @classmethod
    def finite_threshold(cls, value):
        if not math.isfinite(value):
            raise ValueError("持仓阈值必须是有限数值。")
        return value

    @field_validator("scores")
    @classmethod
    def unique_finite_scores(cls, rows):
        days = [row.date for row in rows]
        if len(days) != len(set(days)):
            raise ValueError("每个交易日只能有一个因子分数。")
        if any(not math.isfinite(row.score) for row in rows):
            raise ValueError("因子分数必须是有限数值。")
        return rows


class ResearchScanRequest(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=10)

    @field_validator("symbols")
    @classmethod
    def validate_watchlist_symbols(cls, values):
        symbols = [str(value).strip() for value in values]
        if any(len(symbol) != 6 or not symbol.isdigit() or symbol[0] not in "036" for symbol in symbols):
            raise ValueError("自选池仅支持沪深市场 0/3/6 开头的 6 位股票代码")
        if len(set(symbols)) != len(symbols):
            raise ValueError("自选池代码不能重复")
        return symbols


class PaperTransitionRequest(BaseModel):
    quantity: int | None = Field(None, ge=100, le=1_000_000)

    @field_validator("quantity")
    @classmethod
    def validate_lot_quantity(cls, value):
        if value is not None and value % 100:
            raise ValueError("纸面数量必须是 100 股的整数倍")
        return value


@app.get("/")
def home():
    return FileResponse(ROOT / "static" / "index.html")


@app.get("/api/health")
def health():
    return {"status":"ok", "service":"quant-audit-lab", "market_data":"AkShare public daily bars",
            "llm_configured": bool(os.getenv("LLM_API_KEY")),
            "uptime_seconds": int(time.monotonic() - _PROCESS_STARTED)}


@app.get("/api/telemetry")
def telemetry():
    """Read operational state and the latest cached daily-bar snapshot without upstream calls."""
    market, tape = _latest_market_cache()
    latest_run = _latest_run()
    now = time.time()
    with _OPS_LOCK:
        ops = dict(_OPS)
    last_provider_call = market_provider._last_provider_call
    retry_after = market_provider._retry_after
    with _QUOTE_LOCK:
        quote_stats = dict(_QUOTE_STATS)
        quote_cache = {symbol: {"price": entry["payload"].get("price"),
                                "source_timestamp": entry["payload"].get("source_timestamp"),
                                "provider": entry["payload"].get("provider"),
                                "stale": entry["payload"].get("stale"),
                                "source_age_seconds": entry["payload"].get("source_age_seconds"),
                                "last_error": quote_stats.get("last_error"),
                                "age_seconds": max(0, int(time.time() - entry["_cached_epoch"])),
                                "history_samples": len(_QUOTE_HISTORY.get(symbol, ())),
                                "history_latest_at": (_QUOTE_HISTORY[symbol][-1].get("observed_at")
                                                      if _QUOTE_HISTORY.get(symbol) else None)}
                       for symbol, entry in _QUOTE_CACHE.items()}
    return {
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "service": {"status": "online", "uptime_seconds": int(time.monotonic() - _PROCESS_STARTED),
                    "requests_total": ops["requests"], "active_requests": ops["active"],
                    "last_request_at": ops["last_request_at"], "last_latency_ms": ops["last_latency_ms"],
                    "last_status": ops["last_status"]},
        "market": {"status": "snapshot_available" if market else "waiting_for_first_fetch",
                   "cache_ttl_seconds": market_provider.CACHE_TTL_SECONDS,
                   "min_provider_interval_seconds": market_provider.MIN_PROVIDER_INTERVAL_SECONDS,
                   "failure_cooldown_seconds": market_provider.FAILURE_COOLDOWN_SECONDS,
                   "retry_cooldown_seconds": max(0, int(retry_after - now)),
                   "seconds_since_provider_call": int(now - last_provider_call) if last_provider_call else None,
                   "snapshot": market, "tape": tape},
        "live_quote": {"provider": "Tencent Finance quote feed",
                       "status": "cooldown" if _QUOTE_FAILURE_UNTIL > now else
                                  "available" if quote_cache else "not_requested",
                       "cache_ttl_seconds": _QUOTE_TTL_SECONDS,
                       "history_max_samples": _QUOTE_HISTORY_MAX_SAMPLES,
                       "history_storage": "process_memory",
                       "history_note": "报价采样仅在当前服务进程内保留；进程重启后清空。",
                       "cooldown_seconds": max(0, int(_QUOTE_FAILURE_UNTIL - now)),
                       "stats": quote_stats, "symbols": quote_cache},
        "latest_run": latest_run,
        "llm_configured": bool(os.getenv("LLM_API_KEY")),
    }


@app.get("/api/runs/progress/{progress_id}")
def run_progress(progress_id: str, after: int = 0):
    """Return actual backend stage events for a running audit request."""
    with _RUN_PROGRESS_LOCK:
        record = _RUN_PROGRESS.get(progress_id)
        if not record:
            raise HTTPException(status_code=404, detail="没有找到该运行进度；任务可能已过期或服务已重启。")
        return {"progress_id": progress_id, "status": record["status"],
                "events": [event for event in record["events"] if event["seq"] > after],
                "updated_at": record["updated_at"]}


@app.get("/api/runs/latest")
def latest_run_artifact():
    """Return the complete most recent saved audit so its review can resume after refresh."""
    latest = _latest_run_path()
    if latest is None:
        # An empty workspace is a normal first-visit state, not an HTTP error.
        # This avoids noisy console errors while preserving an explicit empty state.
        return {"status": "empty", "run": None}
    try:
        return json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="最近一次审计运行记录无法读取。") from exc


@app.get("/api/runs/{run_id}")
def get_run_artifact(run_id: UUID):
    """Fetch a saved run artifact by stable ID, including any recorded human decision."""
    return _read_run_artifact(run_id)


@app.get("/api/market/quote/{symbol}")
def market_quote(symbol: str):
    """Return a timestamped Tencent quote and five-level order book through a throttled cache."""
    global _QUOTE_FAILURE_UNTIL
    if len(symbol) != 6 or not symbol.isdigit() or symbol[0] not in "036":
        raise HTTPException(status_code=422, detail="仅支持沪深市场 0/3/6 开头的 6 位股票代码。")
    now = time.time()
    with _QUOTE_LOCK:
        cached = _QUOTE_CACHE.get(symbol)
        _QUOTE_STATS["requests"] += 1
        if cached and now - cached["_cached_epoch"] < _QUOTE_TTL_SECONDS:
            _QUOTE_STATS["cache_hits"] += 1
            return {**cached["payload"], "history": list(_QUOTE_HISTORY.get(symbol, ())),
                    "history_max_samples": _QUOTE_HISTORY_MAX_SAMPLES,
                    "history_storage": "process_memory", "cache_hit": True,
                    "cache_age_seconds": int(now - cached["_cached_epoch"]),
                    "refresh_after_seconds": max(0, int(_QUOTE_TTL_SECONDS - (now - cached["_cached_epoch"]))) }
        if now < _QUOTE_FAILURE_UNTIL:
            if cached:
                return {**cached["payload"], "history": list(_QUOTE_HISTORY.get(symbol, ())),
                        "history_max_samples": _QUOTE_HISTORY_MAX_SAMPLES,
                        "history_storage": "process_memory", "cache_hit": True, "stale": True,
                        "cache_age_seconds": max(0, int(now - cached["_cached_epoch"])),
                        "refresh_after_seconds": int(_QUOTE_FAILURE_UNTIL - now),
                        "warning": "行情源处于故障冷却期，返回最近成功快照。"}
            raise HTTPException(status_code=503, detail=f"行情源故障冷却中，请 {int(_QUOTE_FAILURE_UNTIL-now)} 秒后重试。")
        try:
            frame = market_provider.fetch_quote(symbol)
            values = dict(zip(frame["item"], frame["value"]))
            def number(key):
                try:
                    value = float(values[key])
                    return value if math.isfinite(value) else None
                except (KeyError, TypeError, ValueError):
                    return None
            price = number("最新")
            if price is None or not math.isfinite(price) or price <= 0:
                raise ValueError("行情源没有返回有效现价。")
            raw_timestamp = values.get("行情时间")
            source_time = None
            if raw_timestamp:
                try:
                    source_time = datetime.strptime(str(raw_timestamp), "%Y%m%d%H%M%S").replace(
                        tzinfo=timezone(timedelta(hours=8)))
                except ValueError:
                    source_time = None
            age_seconds = max(0, int((datetime.now(timezone.utc) - source_time.astimezone(timezone.utc)).total_seconds())) if source_time else None
            is_stale = age_seconds is None or age_seconds > 600
            payload = {
                "symbol": symbol, "price": price, "change": number("涨跌"),
                "change_pct": number("涨幅"), "volume_lots": number("总手"),
                "amount": number("金额"), "average_price": number("均价"),
                "open": number("今开"), "high": number("最高"), "low": number("最低"),
                "previous_close": number("昨收"),
                "bids": [{"price": number(f"buy_{i}"), "volume": number(f"buy_{i}_vol")} for i in range(1, 6)],
                "asks": [{"price": number(f"sell_{i}"), "volume": number(f"sell_{i}_vol")} for i in range(1, 6)],
                "provider": "Tencent Finance quote feed",
                "source_timestamp": source_time.isoformat() if source_time else None,
                "observed_at": datetime.now(timezone.utc).isoformat(), "source_age_seconds": age_seconds,
                "stale": is_stale, "freshness": "LIVE" if not is_stale else "STALE_OR_UNVERIFIED",
            }
            _QUOTE_CACHE[symbol] = {"payload": payload, "_cached_epoch": now}
            try:
                _save_orderbook_snapshot(payload)
                payload["orderbook_persisted"] = True
            except sqlite3.Error as history_exc:
                payload["orderbook_persisted"] = False
                payload["orderbook_persistence_warning"] = f"盘口历史未落盘：{type(history_exc).__name__}"
            history = _QUOTE_HISTORY.setdefault(symbol, deque(maxlen=_QUOTE_HISTORY_MAX_SAMPLES))
            source_key = payload["source_timestamp"] or payload["observed_at"]
            if not history or history[-1].get("source_key") != source_key:
                history.append({"source_key": source_key, "source_timestamp": payload["source_timestamp"],
                                "observed_at": payload["observed_at"], "price": price})
            _QUOTE_FAILURE_UNTIL = 0.0
            _QUOTE_STATS["provider_successes"] += 1
            _QUOTE_STATS["last_success_at"] = payload["observed_at"]
            _QUOTE_STATS["last_error"] = None
            return {**payload, "history": list(history),
                    "history_max_samples": _QUOTE_HISTORY_MAX_SAMPLES,
                    "history_storage": "process_memory", "cache_hit": False, "cache_age_seconds": 0,
                    "refresh_after_seconds": _QUOTE_TTL_SECONDS}
        except Exception as exc:
            _QUOTE_FAILURE_UNTIL = time.time() + 30
            _QUOTE_STATS["provider_failures"] += 1
            _QUOTE_STATS["last_error"] = f"{type(exc).__name__}: {str(exc)[:220]}"
            if cached:
                return {**cached["payload"], "history": list(_QUOTE_HISTORY.get(symbol, ())),
                        "history_max_samples": _QUOTE_HISTORY_MAX_SAMPLES,
                        "history_storage": "process_memory", "cache_hit": True, "stale": True,
                        "cache_age_seconds": max(0, int(now - cached["_cached_epoch"])),
                        "refresh_after_seconds": 0, "warning": f"行情源暂不可用：{type(exc).__name__}"}
            raise HTTPException(status_code=502, detail=f"实时行情暂不可用：{type(exc).__name__}: {str(exc)[:220]}") from exc


@app.get("/api/market/kline/{symbol}")
def market_kline(symbol: str, period: str = Query("1d"), start: str = Query(...), end: str = Query(...)):
    """Serve genuine Eastmoney/AkShare OHLCV bars; never synthesize missing candles."""
    if len(symbol) != 6 or not symbol.isdigit() or symbol[0] not in "036":
        raise HTTPException(status_code=422, detail="仅支持沪深市场 0/3/6 开头的 6 位股票代码。")
    allowed = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "60m": "60", "1d": "daily", "1w": "weekly", "1mo": "monthly"}
    if period not in allowed:
        raise HTTPException(status_code=422, detail="周期仅支持 1/5/15/30/60 分钟、日 K、周 K、月 K。")
    try:
        start_dt, end_dt = datetime.fromisoformat(start), datetime.fromisoformat(end)
        # HTML date inputs submit date-only values. Treat the end date as the
        # whole local trading day, rather than midnight at its start.
        if len(end.strip()) == 10:
            end_dt = end_dt.replace(hour=23, minute=59, second=59)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="时间范围必须为 ISO 日期或日期时间。") from exc
    if start_dt > end_dt:
        raise HTTPException(status_code=422, detail="开始时间不能晚于结束时间。")
    intraday = period in {"1m", "5m", "15m", "30m", "60m"}
    max_days = 5 if period == "1m" else 30 if intraday else 3650
    if (end_dt - start_dt).total_seconds() > max_days * 86400:
        raise HTTPException(status_code=422, detail=f"{period} 周期单次最多查询 {max_days} 天。")
    key = f"{symbol}:{period}:{start}:{end}"
    now = time.time()
    with _KLINE_LOCK:
        cached = _KLINE_CACHE.get(key)
        ttl = 15 if intraday else 300
        if cached and now - cached["cached_epoch"] < ttl:
            return {**cached["payload"], "cache_hit": True, "cache_age_seconds": int(now-cached["cached_epoch"])}
        try:
            if intraday:
                start_arg = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                end_arg = end_dt.strftime("%Y-%m-%d %H:%M:%S")
                raw = market_provider.ak.stock_zh_a_hist_min_em(
                    symbol=symbol, start_date=start_arg, end_date=end_arg,
                    period=allowed[period], adjust="")
                provider = "AkShare/Eastmoney minute bars"
                retrieved_at = datetime.now(timezone.utc).isoformat()
                columns = {"时间": "time", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount"}
                bars_frame = raw.rename(columns=columns).copy()
                if "time" not in bars_frame or bars_frame.empty:
                    raise ValueError("分钟线数据源没有返回有效 K 线。")
                for col in ("volume", "amount"):
                    if col not in bars_frame:
                        bars_frame[col] = 0
                bars_frame["time"] = pd.to_datetime(bars_frame["time"], errors="coerce")
                for col in ("open", "high", "low", "close", "volume", "amount"):
                    if col in bars_frame:
                        bars_frame[col] = pd.to_numeric(bars_frame[col], errors="coerce")
                bars_frame = bars_frame.dropna(subset=["time", "open", "high", "low", "close"])
                bars_frame = bars_frame.loc[bars_frame.time.between(start_dt, end_dt)].sort_values("time")
                bars = [{**{k: (v.isoformat(sep=" ") if k == "time" else (float(v) if pd.notna(v) else 0)) for k,v in row.items()}} for row in bars_frame[[c for c in ("time","open","high","low","close","volume","amount") if c in bars_frame]].to_dict("records")]
            else:
                fetch_start, fetch_end = start_dt.date(), end_dt.date()
                if (fetch_end-fetch_start).days < 40:
                    fetch_start -= timedelta(days=45)
                    fetch_end += timedelta(days=1)
                raw, meta = market_provider.fetch_daily(symbol, fetch_start.isoformat(), fetch_end.isoformat())
                for col in ("volume", "amount"):
                    if col not in raw:
                        raw[col] = 0
                raw = raw.loc[raw.date.between(pd.Timestamp(start_dt.date()), pd.Timestamp(end_dt.date()))].copy()
                if period == "1w":
                    raw["bucket"] = raw.date.dt.to_period("W-FRI")
                elif period == "1mo":
                    raw["bucket"] = raw.date.dt.to_period("M")
                else:
                    raw["bucket"] = raw.date.dt.to_period("D")
                grouped = raw.groupby("bucket", sort=True).agg(date=("date","last"),open=("open","first"),high=("high","max"),low=("low","min"),close=("close","last"),volume=("volume","sum"),amount=("amount","sum")).reset_index(drop=True)
                bars = [{"time":row.date.strftime("%Y-%m-%d"),"open":float(row.open),"high":float(row.high),"low":float(row.low),"close":float(row.close),"volume":float(row.volume or 0),"amount":float(row.amount or 0)} for row in grouped.itertuples()]
                provider, retrieved_at = meta.get("provider"), meta.get("retrieved_at")
            if not bars:
                raise ValueError("所选时间范围没有行情 K 线。")
            payload = {"symbol":symbol,"period":period,"provider":provider,"retrieved_at":retrieved_at,
                       "source_timestamp":bars[-1].get("time"),
                       "bars":bars,"count":len(bars),"stale":False,"data_kind":"upstream_ohlcv",
                       "notice":"分钟线由东方财富公开行情接口返回，可能存在服务商延迟；现价独立来自腾讯报价源。" if intraday else "周/月线由带来源记录的真实日线 OHLCV 聚合。"}
            _KLINE_CACHE[key] = {"payload":payload,"cached_epoch":now}
            return {**payload,"cache_hit":False,"cache_age_seconds":0}
        except HTTPException:
            raise
        except Exception as exc:
            if cached:
                return {**cached["payload"],"stale":True,"cache_hit":True,"cache_age_seconds":int(now-cached["cached_epoch"]),"warning":f"上游刷新失败，返回缓存：{type(exc).__name__}"}
            raise HTTPException(status_code=502,detail=f"真实 K 线获取失败：{type(exc).__name__}: {str(exc)[:180]}") from exc


@app.get("/api/market/book/{symbol}/history")
def orderbook_history(symbol: str, start: str = Query(...), end: str = Query(...),
                      limit: int = Query(500, ge=1, le=2000)):
    """Query locally captured real top-five snapshots by observation time (China local time if no zone)."""
    if len(symbol) != 6 or not symbol.isdigit() or symbol[0] not in "036":
        raise HTTPException(status_code=422, detail="仅支持沪深市场 0/3/6 开头的 6 位股票代码。")
    start_dt, end_dt = _history_datetime(start, "start"), _history_datetime(end, "end")
    if start_dt > end_dt:
        raise HTTPException(status_code=422, detail="开始时间不能晚于结束时间。")
    if (end_dt - start_dt).total_seconds() > 31 * 86400:
        raise HTTPException(status_code=422, detail="单次查询最多覆盖 31 天。")
    start_iso, end_iso = start_dt.isoformat(), end_dt.isoformat()
    with _book_history_connection() as conn:
        earliest = conn.execute("SELECT MIN(observed_at) FROM orderbook_snapshots WHERE symbol=?", (symbol,)).fetchone()[0]
        rows = conn.execute(
            "SELECT id, symbol, provider, observed_at, source_timestamp, stale, price, bids_json, asks_json "
            "FROM orderbook_snapshots WHERE symbol=? AND observed_at>=? AND observed_at<=? "
            "ORDER BY observed_at ASC LIMIT ?", (symbol, start_iso, end_iso, limit)
        ).fetchall()
    snapshots = [{"id": row["id"], "symbol": row["symbol"], "provider": row["provider"],
                  "observed_at": row["observed_at"], "source_timestamp": row["source_timestamp"],
                  "stale": bool(row["stale"]), "price": row["price"],
                  "bids": json.loads(row["bids_json"]), "asks": json.loads(row["asks_json"])} for row in rows]
    return {"symbol": symbol, "start": start_iso, "end": end_iso, "count": len(snapshots),
            "limit": limit, "capture_started_at": earliest, "storage": "sqlite_local_observations",
            "notice": "仅包含本服务启动采集后实际观察到的公开五档快照；不提供采集前的历史盘口。",
            "snapshots": snapshots}


@app.post("/api/research/scan")
def research_scan(request: ResearchScanRequest):
    """Score at most ten user-selected symbols from real daily bars; no price synthesis."""
    batch_id = str(uuid4())
    end = date.today() - timedelta(days=1)  # Exclude today's potentially incomplete candle.
    start = end - timedelta(days=120)
    results, failures = [], []
    for symbol in request.symbols:
        try:
            bars, source = market_provider.fetch_daily(symbol, start.isoformat(), end.isoformat())
            result = score_daily_bars(symbol, bars, source)
            result["batch_id"] = batch_id
            results.append(save_scan_result(result))
        except Exception as exc:
            reason = f"{type(exc).__name__}: {str(exc)[:180]}"
            save_scan_failure(symbol, reason)
            failures.append({"symbol": symbol, "status": "unavailable", "reason": reason})
    return {"batch_id": batch_id, "requested": len(request.symbols),
            "succeeded": len(results), "failed": len(failures),
            "score_threshold": ENTRY_SCORE_THRESHOLD, "results": results,
            "failures": failures, "execution": "research_only",
            "notice": "只扫描用户选择的少量标的；评分来自真实日线确定性特征，不构成投资建议或全市场覆盖。"}


@app.get("/api/research/latest")
def research_latest():
    results = latest_results()
    return {"results": results, "count": len(results), "execution": "research_only"}


@app.get("/api/research/events")
def research_events(limit: int = Query(100, ge=1, le=500)):
    from app.research_loop import recent_events
    return recent_events(limit)


@app.get("/api/paper/state")
def get_paper_state():
    return paper_state()


@app.post("/api/paper/{symbol}/{action}")
def transition_paper_position(symbol: str, action: Literal["open", "close"], request: PaperTransitionRequest):
    if len(symbol) != 6 or not symbol.isdigit() or symbol[0] not in "036":
        raise HTTPException(status_code=422, detail="仅支持沪深市场 0/3/6 开头的 6 位股票代码。")
    try:
        return paper_action(symbol, action, request.quantity)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/audit")
def audit(request: AuditRequest):
    progress_id = request.progress_id
    _publish_run_progress(progress_id, "snapshot", "started", "向行情源请求所选标的日线快照")
    current_stage = {"value": "snapshot"}
    def publish(event):
        current_stage["value"] = event["stage"]
        _publish_run_progress(progress_id, event["stage"], event["state"], event["message"], event.get("details"))
    try:
        result = run_audit(request.model_dump(mode="json"), progress_callback=publish)
        _publish_run_progress(progress_id, "complete", "completed", "审计产物已写入，可进行人工复核",
                              {"run_id": result["run_id"], "gate": result["summary"]["gate"]})
        return result
    except ValueError as exc:
        _publish_run_progress(progress_id, current_stage["value"], "failed", str(exc))
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _publish_run_progress(progress_id, current_stage["value"], "failed", f"{type(exc).__name__}: {exc}")
        raise HTTPException(status_code=502, detail=f"行情获取或审计失败：{type(exc).__name__}: {exc}") from exc


@app.post("/api/factor-replay")
def factor_replay(request: FactorReplayRequest):
    """Replay normalized point-in-time RD-Agent factor scores; generated code is never run here."""
    if any(row.date > request.end_date for row in request.scores):
        raise HTTPException(status_code=422, detail="因子分数日期不能晚于所选回测结束日。")
    rows = sorted(({"date": row.date.isoformat(), "score": row.score} for row in request.scores),
                  key=lambda row: row["date"])
    score_map = {row["date"]: row["score"] for row in rows}
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    signal_dir = ROOT.parent / "data" / "factor_signals"
    signal_dir.mkdir(parents=True, exist_ok=True)
    signal_file = signal_dir / f"{digest}.json"
    if not signal_file.exists():
        signal_file.write_text(json.dumps({"factor_name": request.factor_name,
                                           "experiment_id": request.experiment_id,
                                           "scores": rows}, ensure_ascii=False, indent=2),
                              encoding="utf-8")
    candidate = {"source_project": "microsoft/RD-Agent", "experiment_id": request.experiment_id,
                 "factor_name": request.factor_name, "threshold": request.threshold,
                 "score_count": len(rows), "signal_sha256": digest,
                 "signal_snapshot_file": str(signal_file.relative_to(ROOT.parent))}
    progress_id = request.progress_id
    current_stage = {"value": "snapshot"}
    def publish(event):
        current_stage["value"] = event["stage"]
        _publish_run_progress(progress_id, event["stage"], event["state"], event["message"], event.get("details"))
    try:
        _publish_run_progress(progress_id, "snapshot", "started", "校验行情区间与外部因子分数")
        result = run_audit(request.model_dump(mode="json"), signal_scores=score_map,
                           research_candidate=candidate, progress_callback=publish)
        _publish_run_progress(progress_id, "complete", "completed", "因子回放与审计产物已写入",
                              {"run_id": result["run_id"], "gate": result["summary"]["gate"]})
        return result
    except ValueError as exc:
        _publish_run_progress(progress_id, current_stage["value"], "failed", str(exc))
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _publish_run_progress(progress_id, current_stage["value"], "failed", f"{type(exc).__name__}: {exc}")
        raise HTTPException(status_code=502, detail=f"因子信号回测失败：{type(exc).__name__}: {exc}") from exc


@app.post("/api/runs/{run_id}/decision")
def record_decision(run_id: UUID, decision: ReviewDecision):
    """Persist a human sign-off; approval is rejected unless deterministic checks passed."""
    artifact = ROOT.parent / "data" / "runs" / f"{run_id}.json"
    record = _read_run_artifact(run_id)
    if decision.decision == "approve" and record.get("summary", {}).get("gate") != "PASS":
        raise HTTPException(status_code=409, detail="审计门禁未通过，不能批准策略版本。")
    record["human_review"] = {"decision": decision.decision, "reason": decision.reason.strip(),
                              "reviewer": decision.reviewer.strip(), "due_at": decision.due_at.strip(),
                              "reviewed_at": datetime.now(timezone.utc).isoformat()}
    trace = record.setdefault("spider_trace", {})
    events = trace.setdefault("events", [])
    if not any(event.get("type") == "HUMAN_REVIEW_" + decision.decision.upper() for event in events):
        bars = record.get("market_bars") or []
        last_bar = bars[-1] if bars else {}
        events.append({"type": "HUMAN_REVIEW_" + decision.decision.upper(),
                       "label": "研究员人工复核决定已持久化", "time": last_bar.get("time"),
                       "bar_index": len(bars) - 1 if bars else None,
                       "value": last_bar.get("close"), "decision": decision.decision,
                       "reason": decision.reason.strip(), "reviewer": decision.reviewer.strip(),
                       "due_at": decision.due_at.strip(), "reviewed_at": record["human_review"]["reviewed_at"]})
    temp = artifact.with_suffix(".tmp")
    temp.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(artifact)
    return {"run_id": str(run_id), "human_review": record["human_review"], "artifact_file": record["artifact_file"]}


