from datetime import date, datetime, timedelta, timezone
import csv
import math
import json
import hashlib
import os
import threading
import time
from pathlib import Path
from typing import Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator
from dotenv import load_dotenv

from app.audit import run_audit
from app.providers import akshare_provider as market_provider

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
_QUOTE_TTL_SECONDS = max(10, int(os.getenv("LIVE_QUOTE_TTL_SECONDS", "20")))
_QUOTE_FAILURE_UNTIL = 0.0
_QUOTE_STATS = {"requests": 0, "provider_successes": 0, "provider_failures": 0,
                "cache_hits": 0, "last_success_at": None, "last_error": None}


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


def _latest_run():
    run_dir = ROOT.parent / "data" / "runs"
    files = list(run_dir.glob("*.json")) if run_dir.exists() else []
    if not files:
        return None
    try:
        run = json.loads(max(files, key=lambda path: path.stat().st_mtime).read_text(encoding="utf-8"))
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
                                "age_seconds": max(0, int(time.time() - entry["_cached_epoch"]))}
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
                       "cooldown_seconds": max(0, int(_QUOTE_FAILURE_UNTIL - now)),
                       "stats": quote_stats, "symbols": quote_cache},
        "latest_run": latest_run,
        "llm_configured": bool(os.getenv("LLM_API_KEY")),
    }


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
            return {**cached["payload"], "cache_hit": True,
                    "cache_age_seconds": int(now - cached["_cached_epoch"]),
                    "refresh_after_seconds": max(0, int(_QUOTE_TTL_SECONDS - (now - cached["_cached_epoch"]))) }
        if now < _QUOTE_FAILURE_UNTIL:
            if cached:
                return {**cached["payload"], "cache_hit": True, "stale": True,
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
            _QUOTE_FAILURE_UNTIL = 0.0
            _QUOTE_STATS["provider_successes"] += 1
            _QUOTE_STATS["last_success_at"] = payload["observed_at"]
            _QUOTE_STATS["last_error"] = None
            return {**payload, "cache_hit": False, "cache_age_seconds": 0,
                    "refresh_after_seconds": _QUOTE_TTL_SECONDS}
        except Exception as exc:
            _QUOTE_FAILURE_UNTIL = time.time() + 30
            _QUOTE_STATS["provider_failures"] += 1
            _QUOTE_STATS["last_error"] = f"{type(exc).__name__}: {str(exc)[:220]}"
            if cached:
                return {**cached["payload"], "cache_hit": True, "stale": True,
                        "cache_age_seconds": max(0, int(now - cached["_cached_epoch"])),
                        "refresh_after_seconds": 0, "warning": f"行情源暂不可用：{type(exc).__name__}"}
            raise HTTPException(status_code=502, detail=f"实时行情暂不可用：{type(exc).__name__}: {str(exc)[:220]}") from exc


@app.post("/api/audit")
def audit(request: AuditRequest):
    try:
        return run_audit(request.model_dump(mode="json"))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
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
    try:
        return run_audit(request.model_dump(mode="json"), signal_scores=score_map,
                         research_candidate=candidate)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"因子信号回测失败：{type(exc).__name__}: {exc}") from exc


@app.post("/api/runs/{run_id}/decision")
def record_decision(run_id: UUID, decision: ReviewDecision):
    """Persist a human sign-off; approval is rejected unless deterministic checks passed."""
    artifact = ROOT.parent / "data" / "runs" / f"{run_id}.json"
    if not artifact.is_file():
        raise HTTPException(status_code=404, detail="找不到这次审计运行记录。")
    record = json.loads(artifact.read_text(encoding="utf-8"))
    if decision.decision == "approve" and record.get("summary", {}).get("gate") != "PASS":
        raise HTTPException(status_code=409, detail="审计门禁未通过，不能批准策略版本。")
    record["human_review"] = {"decision": decision.decision, "reason": decision.reason.strip(),
                              "reviewed_at": datetime.now(timezone.utc).isoformat()}
    temp = artifact.with_suffix(".tmp")
    temp.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(artifact)
    return {"run_id": str(run_id), "human_review": record["human_review"], "artifact_file": record["artifact_file"]}

