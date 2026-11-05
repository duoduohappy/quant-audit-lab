from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

import akshare as ak
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = ROOT / "data" / "market_cache"
SNAPSHOT_DIR = ROOT / "data" / "snapshots"
CACHE_TTL_SECONDS = max(60, int(os.getenv("MARKET_CACHE_TTL_SECONDS", "21600")))
MIN_PROVIDER_INTERVAL_SECONDS = max(0.0, float(os.getenv("MARKET_MIN_REQUEST_INTERVAL_SECONDS", "2")))
FAILURE_COOLDOWN_SECONDS = max(10, int(os.getenv("MARKET_FAILURE_COOLDOWN_SECONDS", "90")))

# A single lock prevents concurrent audit requests from stampeding the same public
# endpoints. Render currently runs one web process; filesystem cache survives within
# that instance and is refreshed automatically after its TTL.
_provider_lock = threading.Lock()
_last_provider_call = 0.0
_retry_after = 0.0
_last_quote_call = 0.0


def _date_key(value: str) -> str:
    return pd.Timestamp(value).strftime("%Y%m%d")


def _cache_key(symbol: str, start: str, end: str) -> str:
    return hashlib.sha256(f"{symbol}|{start}|{end}|unadjusted-daily-v1".encode()).hexdigest()


def _read_cache(key: str) -> tuple[pd.DataFrame, dict] | None:
    data_path, meta_path = CACHE_DIR / f"{key}.csv", CACHE_DIR / f"{key}.json"
    if not data_path.is_file() or not meta_path.is_file():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        df = pd.read_csv(data_path, parse_dates=["date"])
        if df.empty or len(df) < 30:
            return None
        age = max(0.0, time.time() - data_path.stat().st_mtime)
        meta["cache_age_seconds"] = int(age)
        meta["cache_status"] = "fresh" if age <= CACHE_TTL_SECONDS else "stale_fallback"
        return df, meta
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        return None


def _write_cache(key: str, df: pd.DataFrame, meta: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data_path, meta_path = CACHE_DIR / f"{key}.csv", CACHE_DIR / f"{key}.json"
    data_tmp, meta_tmp = data_path.with_suffix(".csv.tmp"), meta_path.with_suffix(".json.tmp")
    data_tmp.write_text(df.to_csv(index=False, date_format="%Y-%m-%d", float_format="%.8f"), encoding="utf-8")
    meta_tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    data_tmp.replace(data_path)
    meta_tmp.replace(meta_path)


def _normalize(raw: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        raise ValueError("数据源返回空行情")
    mapping = {"日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
               "成交量": "volume", "涨跌幅": "pct_change"}
    df = raw.rename(columns=mapping).copy()
    required = ["date", "open", "high", "low", "close"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"行情字段缺失: {', '.join(missing)}")
    df = df[required + [c for c in ("volume", "pct_change", "amount") if c in df.columns]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "pct_change", "amount"):
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df = (df.dropna(subset=required)
          .loc[lambda frame: frame.date.between(pd.Timestamp(start), pd.Timestamp(end))]
          .sort_values("date").drop_duplicates("date").reset_index(drop=True))
    if len(df) < 30:
        raise ValueError(f"有效日线仅 {len(df)} 条，至少需要 30 条")
    return df


def _symbol_with_exchange(symbol: str) -> str:
    if symbol.startswith(("5", "6", "9")):
        return f"sh{symbol}"
    if symbol.startswith(("4", "8")):
        return f"bj{symbol}"
    return f"sz{symbol}"


def fetch_quote(symbol: str) -> pd.DataFrame:
    """Fetch one Tencent live quote and five-level book; retain the source timestamp."""
    global _last_quote_call, _last_provider_call
    with _provider_lock:
        delay = MIN_PROVIDER_INTERVAL_SECONDS - (time.time() - max(_last_quote_call, _last_provider_call))
        if delay > 0:
            time.sleep(delay)
        _last_quote_call = _last_provider_call = time.time()
        prefix = "sh" if symbol.startswith("6") else "sz"
        url = f"https://qt.gtimg.cn/q={prefix}{symbol}"
        response = requests.get(url, timeout=(3, 8), headers={
            "User-Agent": "Mozilla/5.0", "Referer": "https://finance.qq.com/"})
        response.raise_for_status()
        text = response.content.decode("gbk", errors="replace")
        if '="' not in text:
            raise ValueError("Tencent quote response did not contain a quote payload")
        fields = text.split('="', 1)[1].rsplit('"', 1)[0].split("~")
        if len(fields) < 36 or fields[2] != symbol:
            raise ValueError("Tencent quote response is incomplete or for a different symbol")

        def numeric(index):
            try:
                value = float(fields[index])
                return value if pd.notna(value) else None
            except (IndexError, TypeError, ValueError):
                return None

        previous = numeric(4)
        price = numeric(3)
        amount = None
        try:
            amount = float(fields[35].split("/")[2])
        except (IndexError, TypeError, ValueError):
            pass
        values = {
            "最新": price, "昨收": previous, "今开": numeric(5), "总手": numeric(6),
            "金额": amount, "涨跌": numeric(31), "涨幅": numeric(32),
            "最高": numeric(33), "最低": numeric(34),
            "均价": amount / (numeric(6) * 100) if amount and numeric(6) else None,
            "行情时间": fields[30] or None,
        }
        for level in range(5):
            bid_price, bid_lots = 9 + level * 2, 10 + level * 2
            ask_price, ask_lots = 19 + level * 2, 20 + level * 2
            values[f"buy_{level + 1}"] = numeric(bid_price)
            values[f"buy_{level + 1}_vol"] = numeric(bid_lots)
            values[f"sell_{level + 1}"] = numeric(ask_price)
            values[f"sell_{level + 1}_vol"] = numeric(ask_lots)
        frame = pd.DataFrame.from_dict(values, orient="index", columns=["value"]).reset_index()
        frame.columns = ["item", "value"]
        return frame


def _fetch_provider(provider: str, symbol: str, start: str, end: str) -> pd.DataFrame:
    start_arg, end_arg = _date_key(start), _date_key(end)
    if provider == "AkShare/Eastmoney":
        raw = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_arg,
                                 end_date=end_arg, adjust="")
    elif provider == "AkShare/Tencent":
        raw = ak.stock_zh_a_hist_tx(symbol=_symbol_with_exchange(symbol), start_date=start_arg,
                                    end_date=end_arg, adjust="")
    else:
        raw = ak.stock_zh_a_daily(symbol=_symbol_with_exchange(symbol), start_date=start_arg,
                                  end_date=end_arg, adjust="")
    return _normalize(raw, start, end)


def _save_snapshot(symbol: str, df: pd.DataFrame) -> tuple[str, str]:
    canonical = df.to_csv(index=False, date_format="%Y-%m-%d", float_format="%.8f").encode()
    digest = hashlib.sha256(canonical).hexdigest()
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = SNAPSHOT_DIR / f"{symbol}_{digest[:16]}.csv"
    if not path.exists():
        path.write_bytes(canonical)
    return digest, str(path.relative_to(ROOT))


def fetch_daily(symbol: str, start: str, end: str) -> tuple[pd.DataFrame, dict]:
    """Fetch auditable daily bars with cache, serialized requests, and source fallback.

    Fresh exact-range cache avoids all upstream calls. On an upstream outage, a prior
    exact-range snapshot is returned with an explicit stale marker and original
    provenance. Provider order is Eastmoney, Tencent, then Sina via AkShare.
    """
    global _last_provider_call, _retry_after
    key = _cache_key(symbol, start, end)
    cached = _read_cache(key)
    if cached and cached[1].get("cache_status") == "fresh":
        df, meta = cached
        return df, {**meta, "provider": f"{meta['provider']}（缓存命中）", "cache_status": "fresh_hit"}

    with _provider_lock:
        # Re-check after acquiring the lock; another request may have populated cache.
        cached = _read_cache(key)
        if cached and cached[1].get("cache_status") == "fresh":
            df, meta = cached
            return df, {**meta, "provider": f"{meta['provider']}（缓存命中）", "cache_status": "fresh_hit"}

        now = time.time()
        if now < _retry_after:
            if cached:
                df, meta = cached
                return df, {**meta, "provider": f"{meta['provider']}（过期缓存回退）",
                            "cache_status": "stale_fallback",
                            "fallback_reason": "上游请求处于短暂冷却期"}
            raise ValueError("行情源正处于限流冷却期，请稍后重试；系统已停止重复请求以避免延长封禁。")

        errors: list[str] = []
        for provider in ("AkShare/Eastmoney", "AkShare/Tencent", "AkShare/Sina"):
            delay = MIN_PROVIDER_INTERVAL_SECONDS - (time.time() - _last_provider_call)
            if delay > 0:
                time.sleep(delay)
            _last_provider_call = time.time()
            try:
                df = _fetch_provider(provider, symbol, start, end)
                digest, snapshot_file = _save_snapshot(symbol, df)
                display_provider = provider if provider == "AkShare/Eastmoney" else f"{provider}（备用源）"
                meta = {"provider": display_provider, "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        "symbol": symbol, "rows": len(df), "start": df.date.min().strftime("%Y-%m-%d"),
                        "end": df.date.max().strftime("%Y-%m-%d"), "snapshot_sha256": digest,
                        "snapshot_file": snapshot_file, "cache_status": "miss_refreshed",
                        "cache_ttl_seconds": CACHE_TTL_SECONDS,
                        "fallback_used": provider != "AkShare/Eastmoney",
                        "provider_attempts": errors + [f"{provider}:ok"]}
                _write_cache(key, df, meta)
                _retry_after = 0.0
                return df, meta
            except Exception as exc:
                errors.append(f"{provider}:{type(exc).__name__}")

        _retry_after = time.time() + FAILURE_COOLDOWN_SECONDS
        if cached:
            df, meta = cached
            return df, {**meta, "provider": f"{meta['provider']}（过期缓存回退）",
                        "cache_status": "stale_fallback",
                        "fallback_reason": "所有实时行情源暂不可用，已回退到上次成功快照",
                        "provider_attempts": errors}
        raise ValueError("实时行情源暂不可用（东方财富、腾讯、新浪均未成功），当前没有可用缓存；已启用短暂冷却以避免重复限流。请稍后重试。")

