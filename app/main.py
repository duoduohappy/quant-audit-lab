from datetime import date, datetime, timedelta, timezone
import math
import json
import hashlib
import os
from pathlib import Path
from typing import Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv

from app.audit import run_audit

ROOT = Path(__file__).parent
load_dotenv(ROOT.parent / ".env")
app = FastAPI(title="Quant Audit Lab", version="0.1.0", description="A-share backtest trust audit")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")


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
            "llm_configured": bool(os.getenv("LLM_API_KEY"))}


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
