from __future__ import annotations

import pandas as pd


def run_framebt(df: pd.DataFrame, symbol: str, fast: int, slow: int, initial_cash: float,
                slippage_bps: float, commission_bps: float,
                target_states: list[int] | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Run the pinned upstream framebt engine on the exact fetched daily-bar snapshot."""
    from framebt import (Engine, Bar, BarFeed, StandardAshareRules, StandardCost,
                         FrameStrategy, Order, OrderSide, OrderType)
    from framebt.strategies import DualMAStrategy

    # Use a monotonically increasing logical trading-day clock. Keep real dates in a
    # side map so the engine does not infer timezone semantics from vendor strings.
    dates = [pd.Timestamp(value).strftime("%Y-%m-%d") for value in df.date]
    bars = []
    for idx, row in enumerate(df.itertuples(index=False)):
        volume = getattr(row, "volume", 0)
        amount = getattr(row, "amount", 0)
        bars.append(Bar(timestamp=idx, symbol=symbol, open=float(row.open), high=float(row.high),
                        low=float(row.low), close=float(row.close),
                        volume=0.0 if pd.isna(volume) else float(volume),
                        amount=0.0 if pd.isna(amount) else float(amount), sequence_id=idx))
    feed = BarFeed(bars)
    limit = .30 if symbol.startswith(("4", "8")) else .20 if symbol.startswith(("300", "301", "688")) else .10
    strategy = DualMAStrategy(symbol, short=fast, long=slow)
    if target_states is not None:
        if len(target_states) != len(bars):
            raise ValueError("外部 Agent 信号长度必须与行情交易日数量一致。")

        class ImportedSignalStrategy(FrameStrategy):
            def __init__(self):
                self.last_target = 0

            def on_frame(self, frame, token, bus, host) -> list:
                desired = target_states[frame.frame_id] if frame.frame_id < len(target_states) else 0
                changed = desired != self.last_target
                self.last_target = desired
                if not changed:
                    return []
                current = host.portfolio.position(symbol).net_size()
                if desired and current <= 0 and host.portfolio.cash > 0:
                    return [Order(side=OrderSide.BUY, symbol=symbol, order_type=OrderType.MARKET,
                                  budget=host.portfolio.cash)]
                if not desired and current > 0:
                    return [Order(side=OrderSide.SELL, symbol=symbol, size=current,
                                  order_type=OrderType.MARKET)]
                return []

        strategy = ImportedSignalStrategy()
    result = Engine(feed, strategy, initial_cash=initial_cash,
                    settlement_cycle=1,
                    cost_model=StandardCost(commission=commission_bps / 10_000,
                                            min_commission=5.0, stamp_tax=.0005,
                                            slippage="proportional", k=slippage_bps / 10_000,
                                            transfer_fee=0.0),
                    rules=StandardAshareRules(limit=limit, settlement_cycle=1)).run()
    curve = pd.DataFrame([
        {"date": dates[min(max(int(frame_id), 0), len(dates) - 1)], "equity": float(equity)}
        for frame_id, equity in result.equity_curve
    ])
    fills = pd.DataFrame([
        {"date": dates[min(max(int(fill.timestamp), 0), len(dates) - 1)],
         "side": fill.side.value.upper(), "shares": int(fill.size), "price": round(float(fill.price), 4),
         "fee": round(float(fill.cost), 2), "reason": fill.reason or "framebt execution"}
        for fill in result.fills
    ], columns=["date", "side", "shares", "price", "fee", "reason"])
    return curve, fills, {"engine": "framebt", "version": "1.0.0",
                          "source_commit": "0c0cdfcd994ae45714491622a5df587e46a1ca0c",
                          "strategy": "imported RD-Agent signal" if target_states is not None else "DualMAStrategy",
                          "rule_limit": limit, "settlement_cycle": "T+1",
                          "signal_execution": "close signal, next frame open (engine delay pipeline)"}
