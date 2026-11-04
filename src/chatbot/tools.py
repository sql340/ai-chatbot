from __future__ import annotations

from dataclasses import dataclass, field

from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, tool

from .knowledge import KnowledgeProvider, KnowledgeProviderError
from .models import OutboundItem


@dataclass(slots=True)
class AgentContext:
    tenant_id: str
    user_id: str
    session_id: str
    channel: str
    outbox: list[OutboundItem] = field(default_factory=list)
    handoff_requested: bool = False


def build_agent_tools(knowledge: KnowledgeProvider | None, top_k: int = 4) -> list[BaseTool]:
    tools: list[BaseTool] = [send_card, ask_choice, handoff_to_human]
    if knowledge is not None:
        tools.insert(0, _build_search_knowledge_tool(knowledge, top_k))
    return tools


def _build_search_knowledge_tool(knowledge: KnowledgeProvider, top_k: int) -> BaseTool:
    @tool("search_knowledge")
    async def search_knowledge(query: str, runtime: ToolRuntime[AgentContext]) -> str:
        """检索当前租户的业务知识库，返回可用于回答的政策、流程、产品等事实。"""

        try:
            hits = await knowledge.search(runtime.context.tenant_id, query, top_k=top_k)
        except KnowledgeProviderError as exc:
            return f"知识库暂时不可用：{exc}. 请告知用户稍后重试或转人工，不要编造事实。"

        if not hits:
            return "没有检索到相关业务知识。不要猜测；请向用户确认关键信息，或转人工。"

        sections: list[str] = []
        for index, hit in enumerate(hits, start=1):
            location = hit.source
            if hit.page_number is not None:
                location += f", page {hit.page_number}"
            sections.append(f"[{index}] {hit.content}\n来源：{location}")
        return "\n\n".join(sections)

    return search_knowledge


@tool("send_card")
def send_card(
    title: str,
    body: str,
    runtime: ToolRuntime[AgentContext],
    buttons: list[str] | None = None,
) -> str:
    """向用户发送一张卡片，适合展示订单、账单、工单或操作结果。"""

    payload = {
        "title": title.strip(),
        "body": body.strip(),
        "buttons": [item.strip() for item in (buttons or []) if item.strip()][:4],
    }
    runtime.context.outbox.append(OutboundItem(kind="card", payload=payload))
    return "卡片已加入待发送队列。回复时不要再完整复述卡片内容。"


@tool("ask_choice")
def ask_choice(
    question: str,
    options: list[str],
    runtime: ToolRuntime[AgentContext],
    allow_multiple: bool = False,
) -> str:
    """向用户发送选择器，适合让用户在少量明确选项中选择。"""

    cleaned = [item.strip() for item in options if item.strip()]
    if not 2 <= len(cleaned) <= 6:
        return "选择器需要 2 到 6 个选项。请调整后重试，或直接用一句话询问用户。"
    runtime.context.outbox.append(
        OutboundItem(
            kind="select",
            payload={
                "question": question.strip(),
                "options": [{"label": item, "value": item} for item in cleaned],
                "allow_multiple": allow_multiple,
            },
        )
    )
    return "选择器已加入待发送队列。回复时只用一句话引导用户选择。"


@tool("handoff_to_human")
def handoff_to_human(reason: str, runtime: ToolRuntime[AgentContext]) -> str:
    """停止自动回复并转接真人客服，用于红线、投诉、高风险或无法处理的问题。"""

    runtime.context.handoff_requested = True
    runtime.context.outbox.append(
        OutboundItem(
            kind="handoff",
            text="我帮你转接人工同事，请稍等。",
            payload={"reason": reason.strip()},
        )
    )
    return "已加入转人工队列。现在只用一句话告诉用户正在转接，不要继续提供结论。"


__all__ = ["AgentContext", "build_agent_tools"]

