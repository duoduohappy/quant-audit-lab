# Open-source integrations

These are runtime integrations, not copied source trees. `upstream/` is only a local inspection area and is excluded from Git.

| Component | Integrated use | License / pin |
|---|---|---|
| [framebt](https://github.com/LisonEvf/framebt) | A-share causal backtest engine, built-in dual-MA strategy, imported point-in-time signal strategy, T+1/limit rules, costs, fills and equity curve | MIT; commit `0c0cdfcd994ae45714491622a5df587e46a1ca0c`; installed from the pinned Git URL in `requirements.txt` |
| [LangGraph](https://github.com/langchain-ai/langgraph) | StateGraph orchestration for deterministic evidence summary and optional LLM explanation node | MIT; bounded by `requirements.txt` and lockable for a release |
| [Microsoft RD-Agent(Q)](https://github.com/microsoft/RD-Agent) | External factor/model research worker. Its dated factor scores enter the `/api/factor-replay` handoff, then pass through framebt, internal ledger comparison and human review | MIT; runs separately using RD-Agent's Qlib/Docker environment; this app does not execute generated code |

## RD-Agent(Q) runtime boundary

Microsoft [RD-Agent(Q)](https://github.com/microsoft/RD-Agent) is integrated through a **score handoff**, not as an in-process package. Run its native `fin_factor` or `fin_quant` workflow in its own Qlib/Docker setup, export a factor score table as `date,score` CSV, and convert it with `scripts/rdagent_scores_to_json.py`. Import the JSON from the dashboard. The score artifact is SHA-256 addressed under `data/factor_signals/`; factor/experiment identifiers are also saved with the run. A threshold converts each dated score into a target position; signal decisions are taken at that date's close and passed to the next-session execution pipelines.

The RD-Agent research loop itself is not launched by this dashboard and its internal cost cannot be capped from this app. Its own docs describe an evolving loop (default `evolving_n=10`) and require a separate data/model setup. Keep that worker explicitly opt-in and configure a low iteration budget in its own environment. The app never runs generated Python; it accepts only finite, dated numeric scores. This is a safer, testable interface while the full Qlib output adapter and matching universe are still to be validated.

## Integration boundary

- The adapters use one fetched and hashed AkShare snapshot as the input boundary.
- `framebt` produces an independent backtest curve and fill ledger.
- `POST /api/factor-replay` accepts a normalized RD-Agent score artifact, hashes it, converts scores to target states, then runs the same three backtest/audit views.
- The app's event and vector ledgers remain separate implementations and are compared with each other for the current audit gate.
- The three engines can disagree because share sizing, fills, commissions and assumptions are not yet normalized. Do not describe the `framebt` difference as a framework bug or treat internal agreement as evidence of alpha.
- LangGraph owns orchestration only. Computed metrics and approve/block decisions stay deterministic; an LLM can only explain a small evidence packet.
- This handoff currently supports one A-share symbol and binary target exposure. It does not reproduce RD-Agent's cross-sectional portfolio weighting or claim to validate its native Qlib results.
