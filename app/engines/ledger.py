from __future__ import annotations

import pandas as pd


def make_signals(df: pd.DataFrame, fast: int, slow: int) -> pd.Series:
    fast_ma = df.close.rolling(fast, min_periods=fast).mean()
    slow_ma = df.close.rolling(slow, min_periods=slow).mean()
    return (fast_ma > slow_ma).fillna(False).astype(int)


def _limit_rate(symbol: str) -> float:
    if symbol.startswith(("300", "301", "688")):
        return .20
    if symbol.startswith(("4", "8")):
        return .30
    return .10


def _blocked(row, side: str, limit_rate: float) -> bool:
    # Approximate exchange daily limit from the previous close. ST and IPO special rules
    # are not inferable reliably from these bars and are disclosed as a model limitation.
    previous = row.get("prev_close")
    if pd.isna(previous) or not previous:
        return False
    threshold = limit_rate - .001
    if side == "buy":
        return row.close >= previous * (1 + threshold) and abs(row.high-row.low) < previous * .002
    return row.close <= previous * (1 - threshold) and abs(row.high-row.low) < previous * .002


def event_engine(df: pd.DataFrame, signals: pd.Series, symbol: str, initial_cash: float,
                 slippage_bps: float, commission_bps: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Event-by-event cash/lot ledger; signal at close, execute next open, T+1 lots."""
    cash, shares = initial_cash, 0
    lots: list[list[int]] = []  # [buy_index, shares]
    fills = []
    curve = [{"date": df.iloc[0].date.strftime("%Y-%m-%d"), "equity": initial_cash,
              "cash": initial_cash, "shares": 0}]
    limit = _limit_rate(symbol)
    for i in range(1, len(df)):
        row = df.iloc[i]
        desired = int(signals.iloc[i-1])
        current = 1 if shares else 0
        side = "buy" if desired and not current else "sell" if current and not desired else ""
        if side and not _blocked(row, side, limit):
            px = float(row.open) * (1 + slippage_bps / 10000 * (1 if side == "buy" else -1))
            if side == "buy":
                quantity = int(cash // (px * (1 + commission_bps / 10000)) // 100) * 100
                if quantity:
                    fee = max(5.0, quantity * px * commission_bps / 10000)
                    while quantity and quantity * px + fee > cash:
                        quantity -= 100
                        fee = max(5.0, quantity * px * commission_bps / 10000) if quantity else 0
                    if quantity:
                        cash -= quantity * px + fee
                        shares += quantity
                        lots.append([i, quantity])
                        fills.append({"date": row.date.strftime("%Y-%m-%d"), "side": "BUY", "shares": quantity,
                                      "price": round(px, 4), "fee": round(fee, 2), "reason": "MA state changed"})
            else:
                sellable = sum(q for bought, q in lots if bought < i)
                quantity = min(shares, sellable) // 100 * 100
                if quantity:
                    fee = max(5.0, quantity * px * commission_bps / 10000) + quantity * px * .0005
                    cash += quantity * px - fee
                    shares -= quantity
                    remaining = quantity
                    for lot in lots:
                        take = min(lot[1], remaining) if lot[0] < i else 0
                        lot[1] -= take
                        remaining -= take
                    lots = [lot for lot in lots if lot[1] > 0]
                    fills.append({"date": row.date.strftime("%Y-%m-%d"), "side": "SELL", "shares": quantity,
                                  "price": round(px, 4), "fee": round(fee, 2), "reason": "MA state changed"})
        equity = cash + shares * float(row.close)
        curve.append({"date": row.date.strftime("%Y-%m-%d"), "equity": equity, "cash": cash, "shares": shares})
    return pd.DataFrame(curve), pd.DataFrame(fills)


def vector_engine(df: pd.DataFrame, signals: pd.Series, symbol: str, initial_cash: float,
                  slippage_bps: float, commission_bps: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Batch-oriented target-state replay with separate array-based cash/share accounting."""
    prices = df.reset_index(drop=True)
    target = signals.shift(1).fillna(0).astype(int).to_numpy()
    cash, position = float(initial_cash), 0
    equity_rows = [{"date": prices.iloc[0].date.strftime("%Y-%m-%d"), "equity": initial_cash,
                    "cash": initial_cash, "shares": 0}]
    fill_rows = []
    lots: list[tuple[int, int]] = []
    cap = _limit_rate(symbol)
    for i in range(1, len(prices)):
        desired = target[i]
        pxrow = prices.iloc[i]
        side = 1 if desired and position == 0 else -1 if position and not desired else 0
        prev = prices.close.iloc[i-1]
        flat_range = abs(pxrow.high-pxrow.low) < prev*.002
        at_limit = flat_range and ((side == 1 and pxrow.close >= prev*(1+cap-.001)) or
                                   (side == -1 and pxrow.close <= prev*(1-cap+.001)))
        if side and not at_limit:
            execution = float(pxrow.open) * (1 + side * slippage_bps / 10000)
            if side > 0:
                qty = int(cash / (execution*(1+commission_bps/10000)) / 100)*100
                fee = max(5.0, qty*execution*commission_bps/10000) if qty else 0
                while qty and qty*execution+fee > cash:
                    qty -= 100
                    fee = max(5.0, qty*execution*commission_bps/10000) if qty else 0
                if qty:
                    cash -= qty*execution+fee
                    position += qty
                    lots.append((i, qty))
                    fill_rows.append({"date": pxrow.date.strftime("%Y-%m-%d"), "side": "BUY", "shares": qty,
                                      "price": round(execution,4), "fee": round(fee,2), "reason":"MA state changed"})
            else:
                eligible = sum(q for bought,q in lots if bought < i)
                qty = min(position, eligible)//100*100
                if qty:
                    fee = max(5.0, qty*execution*commission_bps/10000)+qty*execution*.0005
                    cash += qty*execution-fee
                    position -= qty
                    left=qty
                    updated=[]
                    for bought, lotqty in lots:
                        cut=min(left,lotqty) if bought<i else 0
                        left-=cut
                        if lotqty-cut: updated.append((bought,lotqty-cut))
                    lots=updated
                    fill_rows.append({"date":pxrow.date.strftime("%Y-%m-%d"),"side":"SELL","shares":qty,
                                      "price":round(execution,4),"fee":round(fee,2),"reason":"MA state changed"})
        equity_rows.append({"date":pxrow.date.strftime("%Y-%m-%d"),"equity":cash+position*float(pxrow.close),
                            "cash":cash,"shares":position})
    return pd.DataFrame(equity_rows), pd.DataFrame(fill_rows)
