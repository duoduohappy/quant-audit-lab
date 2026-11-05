from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import akshare as ak
import pandas as pd


def fetch_daily(symbol: str, start: str, end: str) -> tuple[pd.DataFrame, dict]:
    """Fetch public A-share daily bars and return normalized, provenance-tagged data."""
    raw = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start.replace("-", ""),
                             end_date=end.replace("-", ""), adjust="")
    if raw is None or raw.empty:
        raise ValueError("数据源未返回行情，请检查股票代码、日期范围或 AkShare 上游状态。")
    mapping = {"日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
               "成交量": "volume", "涨跌幅": "pct_change"}
    df = raw.rename(columns=mapping)
    required = ["date", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"行情字段缺失: {', '.join(missing)}")
    df = df[required + [c for c in ("volume", "pct_change") if c in df.columns]].copy()
    df["date"] = pd.to_datetime(df["date"])
    for col in ("open", "high", "low", "close", "volume", "pct_change"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=required).sort_values("date").drop_duplicates("date").reset_index(drop=True)
    if len(df) < 30:
        raise ValueError("有效日线少于 30 条，无法运行当前均线策略。")
    canonical = df.to_csv(index=False, date_format="%Y-%m-%d", float_format="%.8f").encode()
    snapshot_hash = hashlib.sha256(canonical).hexdigest()
    snapshot_dir = Path(__file__).resolve().parents[2] / "data" / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"{symbol}_{snapshot_hash[:16]}.csv"
    if not snapshot_path.exists():
        snapshot_path.write_bytes(canonical)
    meta = {"provider": "AkShare (stock_zh_a_hist)", "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol, "rows": len(df), "start": df.date.min().strftime("%Y-%m-%d"),
            "end": df.date.max().strftime("%Y-%m-%d"), "snapshot_sha256": snapshot_hash,
            "snapshot_file": str(snapshot_path.relative_to(snapshot_path.parents[2]))}
    return df, meta
