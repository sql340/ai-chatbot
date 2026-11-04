from __future__ import annotations

from deepagents import create_deep_agent
from deepagents.backends import StateBackend
from langchain_openai import ChatOpenAI
from langgraph.graph.state import CompiledStateGraph

from .config import Settings
from .knowledge import KnowledgeProvider
from .prompts import SYSTEM_PROMPT
from .tools import AgentContext, build_agent_tools


def build_model(settings: Settings) -> ChatOpenAI:
    return ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.llm_api_key or "not-configured",
        base_url=settings.llm_base_url,
        temperature=settings.llm_temperature,
        max_completion_tokens=settings.llm_max_tokens,
        timeout=settings.llm_timeout,
        max_retries=settings.llm_max_retries,
        use_responses_api=False,
        streaming=True,
    )


def build_agent(
    settings: Settings,
    knowledge: KnowledgeProvider | None,
) -> CompiledStateGraph:
    """Build the DeepAgent graph once at service startup.

    Building once keeps the tool list and system prompt stable, which is a
    prerequisite for provider-side prefix/KV cache hits.
    """

    return create_deep_agent(
        model=build_model(settings),
        tools=build_agent_tools(knowledge, top_k=settings.rag_top_k),
        system_prompt=SYSTEM_PROMPT,
        context_schema=AgentContext,
        backend=StateBackend(),
        name="customer_service",
    )


__all__ = ["build_agent", "build_model"]

