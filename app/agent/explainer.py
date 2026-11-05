from __future__ import annotations

import os
from typing import TypedDict


class AgentState(TypedDict, total=False):
    audit: dict
    enabled: bool
    result: dict


def _rule_summary(state: AgentState) -> AgentState:
    audit = state["audit"]
    evidence = audit["evidence"][:4]
    summary = audit["summary"]
    if summary["max_abs_nav_gap"] <= .01:
        text = ("两套项目内账本在本次快照下的最大日度权益差异不超过 1 分钱。"
                "可进入人工复核；framebt 曲线用于外部交叉观察，不纳入相同口径门禁。")
    else:
        text = (f"发现权益口径差异：最大绝对差额为 {summary['max_abs_nav_gap']:.2f} 元，"
                f"涉及 {summary['divergent_days']} 个交易日。先核对逐笔成交与约束假设。")
    result = {"mode": "rules", "text": text,
              "evidence_ids": [item["id"] for item in evidence]}
    if state.get("enabled") and not all(os.getenv(name) for name in (
            "LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL")):
        result["fallback_reason"] = "LLM 未配置，已回退到零 Token 规则摘要"
    return {"result": result}


def _should_call_llm(state: AgentState) -> str:
    configured = all(os.getenv(name) for name in ("LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL"))
    return "llm" if state.get("enabled") and configured else "end"


def _llm_summary(state: AgentState) -> AgentState:
    # One bounded generation per explicit user request. Numerical calculation and gate
    # remain deterministic; only a small, already-computed evidence packet is sent.
    try:
        from openai import OpenAI
        audit = state["audit"]
        response = OpenAI(api_key=os.environ["LLM_API_KEY"],
                          base_url=os.environ["LLM_BASE_URL"]).chat.completions.create(
            model=os.environ["LLM_MODEL"], temperature=0, max_tokens=240,
            messages=[
                {"role": "system", "content": "你是量化回测审计助手。只解释输入证据，不重算数字、不提供买卖建议、不批准策略。指出项目内账本门禁与 framebt 交叉参考的区别。"},
                {"role": "user", "content": str({"summary": audit["summary"],
                                                    "framebt": audit["config"]["framebt"],
                                                    "evidence": audit["evidence"][:4]})},
            ])
        text = response.choices[0].message.content or state["result"]["text"]
        return {"result": {**state["result"], "mode": "langgraph+llm", "text": text}}
    except Exception as exc:
        return {"result": {**state["result"], "fallback_reason": type(exc).__name__}}


def explain(audit: dict, enabled: bool = False) -> dict:
    """LangGraph orchestrates a deterministic gate and optional bounded explainer node."""
    try:
        from langgraph.graph import END, START, StateGraph

        graph = StateGraph(AgentState)
        graph.add_node("rule_summary", _rule_summary)
        graph.add_node("llm_summary", _llm_summary)
        graph.add_edge(START, "rule_summary")
        graph.add_conditional_edges("rule_summary", _should_call_llm,
                                    {"llm": "llm_summary", "end": END})
        graph.add_edge("llm_summary", END)
        return graph.compile().invoke({"audit": audit, "enabled": enabled})["result"]
    except ImportError:
        # Keep the zero-token path operational if an installation is missing its optional
        # orchestration package. The pinned app dependency normally uses the graph above.
        state = _rule_summary({"audit": audit, "enabled": enabled})
        if enabled:
            state["result"]["fallback_reason"] = "LangGraph is not installed"
        return state["result"]
