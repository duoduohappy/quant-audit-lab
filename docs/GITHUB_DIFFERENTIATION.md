# GitHub 查重范围与差异定位

查重采用“核心问题与端到端工作流相同”作为标准。本轮针对已知相邻仓库与 GitHub 公开搜索结果，没有发现同时覆盖“ A 股双账本复算、逐日/逐笔差异审计、证据约束解释、策略晋级门禁”的同一项目。公开仓库搜索无法证明全球范围内绝对不存在，故这是一项有边界的检索结论。

| 项目 | 已覆盖方向 | 本项目的差异 |
|---|---|---|
| [framebt](https://github.com/LisonEvf/framebt) | A 股确定性回测、T+1/费用/限价、单引擎独立复算 | 本项目固定版本直接作为回测内核；产品闭环补充多实现核对、审计证据、图形工作台和人工门禁 |
| [StockPro](https://github.com/Shadowell/StockPro) | A 股研究、因子/回测/模拟运营和仪表盘 | 本项目聚焦回测实现可信度与可复核差异证据 |
| [Trade_pilot](https://github.com/Jerrycyborg/Trade_pilot) | 交易后归因与执行质量诊断 | 本项目审计的是策略回测的账本语义和实现差异 |
| [FactorZen](https://github.com/rookiewu417/FactorZen) | 因子研究、组合风险、归因和模拟执行 | 本项目的核心闭环是双实现重放、审计证据与晋级门禁 |
| [LLM_QUANT_FACTORY](https://github.com/khakhasshi/LLM_QUANT_FACTORY) | 多 Agent 因子研究和量化工作流 | 当前产品以可复算回测审计为核心，LangGraph Agent 只解释证据；后续可将因子研究侧车接入此闭环，但当前没有集成它的代码 |

当前实际复用的开源实现包括 MIT 许可的 `framebt`（固定 commit）以及 LangGraph 工作流编排库。GitHub 定向查重不构成“全网绝对不存在”的证明；差异化应该基于新增的组合闭环与数据证据，而不是声称底层算法原创。

更深入的项目定位见架构文档。查重是截至 2026-11-05 的定向公开检索快照；在对外宣称“独一无二”前仍应再次查询 GitHub。
