from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import date
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert RD-Agent/Qlib point-in-time scores to the Quant Audit Lab handoff format.")
    parser.add_argument("input_csv", type=Path, help="CSV with date,score columns")
    parser.add_argument("output_json", type=Path)
    parser.add_argument("--factor", required=True, help="Research factor name")
    parser.add_argument("--experiment", required=True, help="RD-Agent experiment/run identifier")
    args = parser.parse_args()

    scores = []
    seen = set()
    with args.input_csv.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames or not {"date", "score"}.issubset(reader.fieldnames):
            raise SystemExit("Input CSV must contain columns: date,score")
        for line, row in enumerate(reader, start=2):
            try:
                day = date.fromisoformat(row["date"][:10]).isoformat()
                score = float(row["score"])
            except (TypeError, ValueError) as exc:
                raise SystemExit(f"Invalid date/score at CSV line {line}: {exc}") from exc
            if not math.isfinite(score):
                raise SystemExit(f"Score must be finite at CSV line {line}")
            if day in seen:
                raise SystemExit(f"Duplicate score date: {day}")
            seen.add(day)
            scores.append({"date": day, "score": score})

    if not scores:
        raise SystemExit("CSV contains no score rows")
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps({"source": "microsoft/RD-Agent", "factor_name": args.factor,
                                            "experiment_id": args.experiment,
                                            "scores": scores}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Exported {len(scores)} dated scores to {args.output_json}")


if __name__ == "__main__":
    main()
