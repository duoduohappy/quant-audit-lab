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


def _spider_research_trace(df: pd.DataFrame, signals: pd.Series, fills: pd.DataFrame,
                           provenance: dict, fast: int, slow: int, gate: str,
                           max_gap: float, fill_diff: int) -> tuple[list[dict], dict]:
    """Build a compact, deterministic trace from the exact bars/signals/fills being audited."""
    bars = df.reset_index(drop=True)
    close = bars["close"].astype(float)
    volume = bars["volume"].astype(float) if "volume" in bars else pd.Series(np.nan, index=bars.index)
    fast_ma, slow_ma = close.rolling(fast, min_periods=fast).mean(), close.rolling(slow, min_periods=slow).mean()
    prior_high = bars["high"].astype(float).shift(1).rolling(20, min_periods=20).max()
    prior_volume = volume.shift(1).rolling(20, min_periods=10).mean()
    fill_by_date: dict[str, list[dict]] = {}
    for fill in fills.to_dict(orient="records") if len(fills) else []:
        fill_by_date.setdefault(str(fill["date"]), []).append(fill)

    rows, events = [], []
    for i, row in bars.iterrows():
        day = row.date.strftime("%Y-%m-%d")
        fma = float(fast_ma.iloc[i]) if pd.notna(fast_ma.iloc[i]) else None
        sma = float(slow_ma.iloc[i]) if pd.notna(slow_ma.iloc[i]) else None
        prior_fma = float(fast_ma.iloc[i-1]) if i and pd.notna(fast_ma.iloc[i-1]) else None
        prior_sma = float(slow_ma.iloc[i-1]) if i and pd.notna(slow_ma.iloc[i-1]) else None
        state = int(signals.iloc[i])
        features: list[dict] = []
        if fma is not None and sma is not None and prior_fma is not None and prior_sma is not None:
            if prior_fma <= prior_sma and fma > sma:
                features.append({"type": "MA_CROSS_UP", "label": "快线上穿慢线", "value": round((fma + sma) / 2, 6)})
            elif prior_fma >= prior_sma and fma < sma:
                features.append({"type": "MA_CROSS_DOWN", "label": "快线下穿慢线", "value": round((fma + sma) / 2, 6)})
        if pd.notna(prior_high.iloc[i]) and float(row.close) > float(prior_high.iloc[i]):
            features.append({"type": "BREAKOUT_20D", "label": "收盘突破此前20日高点", "value": float(row.close),
                             "reference": float(prior_high.iloc[i])})
        vmean = float(prior_volume.iloc[i]) if pd.notna(prior_volume.iloc[i]) else None
        current_volume = float(volume.iloc[i]) if pd.notna(volume.iloc[i]) else None
        vol_ratio = current_volume / vmean if vmean and current_volume is not None else None
        if vol_ratio is not None and vol_ratio >= 1.5:
            features.append({"type": "VOLUME_SURGE_20D", "label": "成交量达到此前20日均量的1.5倍",
                             "value": float(row.close), "volume_ratio": round(vol_ratio, 3),
                             "volume": current_volume, "prior_volume_mean": vmean})
        if i and int(signals.iloc[i-1]) != state:
            features.append({"type": "STRATEGY_ENTRY" if state else "STRATEGY_EXIT",
                             "label": "双均线策略目标仓位切换", "value": float(row.close),
                             "execution": "信号收盘确认；下一交易日开盘执行"})
        day_fills = fill_by_date.get(day, [])
        for fill in day_fills:
            features.append({"type": "FILL_" + str(fill["side"]), "label": "事件账本实际成交",
                             "price": float(fill["price"]), "shares": int(fill["shares"]),
                             "value": float(fill["price"]), "fee": float(fill["fee"])})
        # Prefer the execution fact, then the strategy decision that caused it.
        # This makes the spider's leg land on a real fill price whenever one exists.
        primary = next((f for f in features if f["type"].startswith("FILL_")), None)
        primary = primary or next((f for f in features if f["type"].startswith("STRATEGY_")), None)
        primary = primary or next((f for f in features if f["type"].startswith("MA_CROSS")), None)
        primary = primary or next((f for f in features if f["type"] == "BREAKOUT_20D"), None)
        primary = primary or next((f for f in features if f["type"] == "VOLUME_SURGE_20D"), None)
        row_volume = float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else 0.0
        rows.append({"time": day, "open": float(row.open), "high": float(row.high), "low": float(row.low),
                     "close": float(row.close), "volume": row_volume,
                     "fast_ma": fma, "slow_ma": sma, "strategy_state": state,
                     "features": features, "capture_feature": primary["type"] if primary else "CLOSE"})
        for feature in features:
            events.append({"time": day, "bar_index": i, "close": float(row.close),
                           "fast_ma": fma, "slow_ma": sma, **feature})

    ohlc_valid = bool(((bars.high >= bars[["open", "close", "low"]].max(axis=1)) &
                       (bars.low <= bars[["open", "close", "high"]].min(axis=1))).all())
    trace = {"version": "2", "source_sha256": provenance.get("snapshot_sha256"),
             "bar_count": len(rows), "signal_count": int((signals.diff().fillna(0) != 0).sum()),
             "fill_count": int(len(fills)), "feature_count": len(events),
             "checks": {"ohlc_invariants": "PASS" if ohlc_valid else "FAIL",
                        "finite_close": "PASS" if np.isfinite(close).all() else "FAIL",
                        "ledger_gate": gate, "max_nav_gap": round(max_gap, 6),
                        "fill_mismatches": fill_diff},
             "method": "确定性规则扫描真实 OHLCV 与本次策略信号；不调用 LLM，不生成订单。",
             "events": events}
    return rows, trace


def run_audit(payload: dict, *, signal_scores: dict[str, float] | None = None,
              research_candidate: dict | None = None, progress_callback=None) -> dict:
    def progress(stage: str, state: str, message: str, **details):
        if progress_callback:
            progress_callback({"stage": stage, "state": state, "message": message, "details": details})

    symbol = payload["symbol"].strip()
    fast, slow = int(payload["fast_window"]), int(payload["slow_window"])
    if not symbol.isdigit() or len(symbol) != 6:
        raise ValueError("股票代码请输入 6 位数字，例如 600519。")
    if fast < 2 or slow <= fast or slow > 200:
        raise ValueError("均线参数必须满足 2 ≤ 快线 < 慢线 ≤ 200。")
    progress("snapshot", "started", "向行情源请求标的日线并校验快照")
    df, provenance = fetch_daily(symbol, payload["start_date"], payload["end_date"])
    progress("snapshot", "completed", "日线快照已载入", rows=len(df), provider=provenance.get("provider"),
             snapshot_sha256=provenance.get("snapshot_sha256"))
    if len(df) < slow + 2:
        raise ValueError(f"当前日期区间仅有 {len(df)} 个交易日，至少需要 {slow + 2} 条日线完成慢均线预热与执行。")
    if signal_scores is None:
        progress("signal", "started", "根据收盘价与移动均线计算策略状态", fast_window=fast, slow_window=slow)
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
        progress("signal", "started", "导入的 point-in-time 因子分数已对齐行情交易日",
                 factor_name=research_candidate.get("factor_name"), score_count=len(signal_scores))
    progress("signal", "completed", "策略目标状态序列已生成", signal_days=len(signals))
    cash, slip, commission = float(payload["initial_cash"]), float(payload["slippage_bps"]), float(payload["commission_bps"])
    progress("event_ledger", "started", "执行逐日事件账本")
    event_curve, event_fills = event_engine(df, signals, symbol, cash, slip, commission)
    progress("event_ledger", "completed", "事件账本执行完成", fills=len(event_fills))
    progress("vector_ledger", "started", "执行独立向量账本")
    vector_curve, vector_fills = vector_engine(df, signals, symbol, cash, slip, commission)
    progress("vector_ledger", "completed", "向量账本执行完成", fills=len(vector_fills))
    progress("framebt", "started", "向 framebt 适配器提交同一行情快照")
    framebt_curve, framebt_fills, framebt_meta = run_framebt(
        df, symbol, fast, slow, cash, slip, commission,
        target_states=signals.astype(int).tolist() if signal_scores is not None else None)
    progress("framebt", "completed", "framebt 回测完成", fills=len(framebt_fills),
             version=framebt_meta.get("version"), source_commit=framebt_meta.get("source_commit"))
    progress("reconciliation", "started", "对齐逐日净值与成交记录")
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
    progress("reconciliation", "completed", "一致性门禁已计算", gate=gate, max_nav_gap=max_gap,
             fill_mismatches=fill_diff, divergent_days=divergent)
    progress("risk_gate", "started", "依据账本净值差与逐笔成交差异评估晋级门禁")
    progress("risk_gate", "completed", "确定性门禁结论已生成；不代表策略收益或投资风险评价",
             gate=gate, approval_allowed=gate == "PASS", max_nav_gap=max_gap, fill_mismatches=fill_diff)
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
    market_bars, spider_trace = _spider_research_trace(
        df, signals, event_fills, provenance, fast, slow, gate, max_gap, fill_diff)
    trace_time = market_bars[-1]["time"]
    trace_index = len(market_bars) - 1
    spider_trace["events"].extend([
        {"type": "RECONCILIATION_" + gate, "label": "双账本一致性核对完成", "time": trace_time,
         "bar_index": trace_index, "value": market_bars[-1]["close"], "max_nav_gap": max_gap,
         "fill_mismatches": fill_diff},
        {"type": "RISK_GATE_" + gate, "label": "确定性风险门禁结论", "time": trace_time,
         "bar_index": trace_index, "value": market_bars[-1]["close"], "gate": gate},
        {"type": "EVIDENCE_READY", "label": "审计证据包已生成", "time": trace_time,
         "bar_index": trace_index, "value": market_bars[-1]["close"], "evidence_count": len(evidence)},
    ])
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
              "market_bars": market_bars, "spider_trace": spider_trace,
              "limitations":["使用未复权日线；分红送转、ST 特殊涨跌幅、停牌撮合与交易所规则细节未完整建模。",
                             "framebt 已实际运行，但它与项目内账本的成交手数、成本和涨跌停假设可能不同；净值只作交叉观察，不将其差异误判为账本故障。",
                             "两套项目内账本的一致性门禁不代表策略有效；历史回测不代表未来收益。"]}
    progress("evidence_agent", "started", "根据审计证据生成规则解释" +
             ("与 LLM 摘要" if payload.get("use_llm", False) else "；未启用 LLM"))
    output["agent"] = explain(output, enabled=bool(payload.get("use_llm", False)))
    if research_candidate:
        output["research_candidate"] = research_candidate
    run_dir = Path(__file__).resolve().parents[1] / "data" / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)
    output["artifact_file"] = f"data/runs/{output['run_id']}.json"
    artifact = run_dir / f"{output['run_id']}.json"
    artifact.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    progress("evidence_agent", "completed", "证据解释与运行产物已保存", agent_mode=output["agent"].get("mode"))
    return output

