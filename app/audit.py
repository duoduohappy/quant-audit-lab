from __future__ import annotations

import uuid
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from app.agent.explainer import explain
from app.engines.ledger import event_engine, make_signals, vector_engine
from app.engines.framebt_adapter import run_framebt
from app.providers.akshare_provider import fetch_daily


def _metrics(curve: pd.DataFrame) -> dict:
    values = curve.equity.astype(float)
    returns = values.pct_change().dropna()
    peak = values.cummax()
    dd = values / peak - 1
    elapsed = max(1, len(values))
    cagr = float((values.iloc[-1] / values.iloc[0]) ** (252 / elapsed) - 1) if values.iloc[0] else 0
    return {"total_return": float(values.iloc[-1] / values.iloc[0] - 1), "cagr": cagr,
            "volatility": float(returns.std(ddof=1) * np.sqrt(252)) if len(returns) > 1 else 0,
            "max_drawdown": float(dd.min()) if len(dd) else 0,
            "sharpe": float(returns.mean() / returns.std(ddof=1) * np.sqrt(252)) if len(returns)>1 and returns.std(ddof=1) else 0}


def run_audit(payload: dict, *, signal_scores: dict[str, float] | None = None,
              research_candidate: dict | None = None) -> dict:
    symbol = payload["symbol"].strip()
    fast, slow = int(payload["fast_window"]), int(payload["slow_window"])
    if not symbol.isdigit() or len(symbol) != 6:
        raise ValueError("股票代码请输入 6 位数字，例如 600519。")
    if fast < 2 or slow <= fast or slow > 200:
        raise ValueError("均线参数必须满足 2 ≤ 快线 < 慢线 ≤ 200。")
    df, provenance = fetch_daily(symbol, payload["start_date"], payload["end_date"])
    if len(df) < slow + 2:
        raise ValueError(f"当前日期区间仅有 {len(df)} 个交易日，至少需要 {slow + 2} 条日线完成慢均线预热与执行。")
    if signal_scores is None:
        signals = make_signals(df, fast, slow)
    else:
        threshold = float(research_candidate["threshold"])
        normalized_dates = df.date.dt.strftime("%Y-%m-%d")
        market_dates = set(normalized_dates)
        if any(day > normalized_dates.iloc[-1] for day in signal_scores):
            raise ValueError("因子分数包含行情区间之后的日期，拒绝未来数据。")
        missing = sorted(set(signal_scores) - market_dates)
        if not (set(signal_scores) & market_dates):
            raise ValueError("Agent 因子分数与所选行情区间没有重叠日期。")
        # Scores are point-in-time values as of that day's close. Forward-fill only
        # after their own dates; the event and framebt engines execute on a later bar.
        last_state = 0
        states = []
        for day in normalized_dates:
            if day in signal_scores:
                last_state = int(float(signal_scores[day]) >= threshold)
            states.append(last_state)
        signals = pd.Series(states, index=df.index, dtype="int64")
        research_candidate = {**research_candidate,
                              "overlap_dates": len(set(signal_scores) - set(missing)),
                              "unmatched_dates": len(missing),
                              "execution": "score observed at close; target state executes from next session"}
    cash, slip, commission = float(payload["initial_cash"]), float(payload["slippage_bps"]), float(payload["commission_bps"])
    event_curve, event_fills = event_engine(df, signals, symbol, cash, slip, commission)
    vector_curve, vector_fills = vector_engine(df, signals, symbol, cash, slip, commission)
    framebt_curve, framebt_fills, framebt_meta = run_framebt(
        df, symbol, fast, slow, cash, slip, commission,
        target_states=signals.astype(int).tolist() if signal_scores is not None else None)
    daily = event_curve.merge(vector_curve, on="date", suffixes=("_event", "_vector"))
    daily = daily.merge(framebt_curve.rename(columns={"equity": "equity_framebt"}), on="date", how="left")
    daily["gap"] = daily.equity_event - daily.equity_vector
    daily["abs_gap"] = daily.gap.abs()
    fill_diff = int(len(event_fills) != len(vector_fills))
    if not fill_diff and not event_fills.empty:
        cols = ["date", "side", "shares"]
        fill_diff = int((event_fills[cols].reset_index(drop=True) != vector_fills[cols].reset_index(drop=True)).any(axis=1).sum())
    max_gap = float(daily.abs_gap.max()) if len(daily) else 0.0
    divergent = int((daily.abs_gap > .01).sum())
    gate = "PASS" if max_gap <= .01 and fill_diff == 0 else "BLOCK"
    summary = {"max_abs_nav_gap": max_gap, "divergent_days": divergent, "fill_mismatch_count": fill_diff,
               "gate": gate, "event_metrics": _metrics(event_curve), "vector_metrics": _metrics(vector_curve),
               "framebt_metrics": _metrics(framebt_curve),
               "event_fill_count": len(event_fills), "vector_fill_count": len(vector_fills),
               "framebt_fill_count": len(framebt_fills)}
    evidence = [{"id": f"NAV-{r.date.replace('-', '')}", "type": "daily_nav", "date": r.date,
                 "event_equity": round(float(r.equity_event), 4), "vector_equity": round(float(r.equity_vector), 4),
                 "gap": round(float(r.gap), 6)} for r in daily.nlargest(8, "abs_gap").itertuples()]
    if not evidence:
        evidence = [{"id":"DATA-EMPTY","type":"warning","detail":"无重叠的可审计权益序列"}]
    fill_records = event_fills.to_dict(orient="records") if len(event_fills) else []
    output = {"run_id": str(uuid.uuid4()), "created_at": datetime.now(timezone.utc).isoformat(),
              "provenance": provenance, "config": {"symbol":symbol,"fast_window":fast,"slow_window":slow,
              "initial_cash":cash,"slippage_bps":slip,"commission_bps":commission,
              "execution":"signal at close, next session open","limit_model":"approximate board-based daily limit",
              "sell_tax_rate":0.0005,"framebt":framebt_meta,
              "strategy_source": "RD-Agent(Q) score handoff" if research_candidate else "local Dual MA"}, "summary":summary,
              "daily": [{"date":r.date,"event":round(float(r.equity_event),2),"vector":round(float(r.equity_vector),2),
                         "framebt":round(float(r.equity_framebt),2) if pd.notna(r.equity_framebt) else None,
                         "gap":round(float(r.gap),4)} for r in daily.itertuples()],
              "fills":fill_records, "framebt_fills":framebt_fills.to_dict(orient="records"), "evidence":evidence,
              "limitations":["使用未复权日线；分红送转、ST 特殊涨跌幅、停牌撮合与交易所规则细节未完整建模。",
                             "framebt 已实际运行，但它与项目内账本的成交手数、成本和涨跌停假设可能不同；净值只作交叉观察，不将其差异误判为账本故障。",
                             "两套项目内账本的一致性门禁不代表策略有效；历史回测不代表未来收益。"]}
    output["agent"] = explain(output, enabled=bool(payload.get("use_llm", False)))
    if research_candidate:
        output["research_candidate"] = research_candidate
    run_dir = Path(__file__).resolve().parents[1] / "data" / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)
    output["artifact_file"] = f"data/runs/{output['run_id']}.json"
    artifact = run_dir / f"{output['run_id']}.json"
    artifact.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    return output
