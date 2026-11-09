from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from app import research_loop as loop


def rising_daily_bars(rows: int = 70) -> pd.DataFrame:
    days = []
    day = date.today() - timedelta(days=1)
    while len(days) < rows:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    days.reverse()
    close = [10.0 * (1.003 ** index) for index in range(rows)]
    return pd.DataFrame({
        "date": pd.to_datetime(days), "open": close,
        "high": [value * 1.01 for value in close],
        "low": [value * 0.99 for value in close], "close": close,
        "volume": [100_000.0] * rows,
    })


class ResearchLoopTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.original_db_path = loop.DB_PATH
        loop.DB_PATH = Path(self.tempdir.name) / "paper.sqlite3"
        self.source = {
            "provider": "unit-test-fixture", "retrieved_at": "2026-11-09T00:00:00+00:00",
            "cache_status": "miss_refreshed", "snapshot_sha256": "a" * 64,
            "snapshot_file": "fixture.csv",
        }
        self.bars = rising_daily_bars()

    def tearDown(self):
        loop.DB_PATH = self.original_db_path
        self.tempdir.cleanup()

    def test_scoring_is_explainable_and_stale_data_is_blocked(self):
        result = loop.score_daily_bars("600519", self.bars, self.source)
        self.assertEqual(result["risk_gate"], "ALLOW")
        self.assertGreaterEqual(result["score"], loop.ENTRY_SCORE_THRESHOLD)
        self.assertAlmostEqual(sum(result["score_components"].values()), result["score"])

        stale = loop.score_daily_bars("600519", self.bars,
                                      {**self.source, "cache_status": "stale_fallback"})
        self.assertEqual(stale["risk_gate"], "BLOCK")
        self.assertIn("行情使用过期缓存", stale["risk_reasons"])

    def test_paper_state_and_audit_chain_survive_provider_failure(self):
        result = loop.score_daily_bars("600519", self.bars, self.source)
        loop.save_scan_result(result)
        opened = loop.paper_action("600519", "open")
        self.assertEqual(opened["status"], "paper_only")
        self.assertEqual(opened["state"]["positions"][0]["quantity"] % 100, 0)

        loop.save_scan_failure("600519", "provider unavailable")
        state = loop.paper_state()
        self.assertEqual(state["valuation_basis"], "cost_basis_fallback")
        self.assertTrue(state["audit_chain"]["valid"])
        with self.assertRaisesRegex(ValueError, "行情来自过期缓存或不可用"):
            loop.paper_action("600519", "close")

        loop.save_scan_result(loop.score_daily_bars("600519", self.bars, self.source))
        closed = loop.paper_action("600519", "close")
        self.assertEqual(closed["status"], "paper_only")
        self.assertEqual(closed["state"]["positions"], [])
        self.assertTrue(closed["state"]["audit_chain"]["valid"])


if __name__ == "__main__":
    unittest.main()
