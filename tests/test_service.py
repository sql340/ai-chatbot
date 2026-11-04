from __future__ import annotations

from typing import Any

import pytest
from langchain_core.messages import AIMessage

from chatbot.config import Settings
from chatbot.locks import build_lock_manager
from chatbot.models import ChatRequest, OutboundItem
from chatbot.service import ChatService, SessionClosedError, SessionForbiddenError
from chatbot.store import SessionNotFoundError, build_session_store


class FakeAgent:
    def __init__(self) -> None:
        self.calls = 0

    async def ainvoke(self, state: dict[str, Any], *, context: Any, config: Any) -> dict[str, Any]:
        self.calls += 1
        context.outbox.append(
            OutboundItem(kind="card", payload={"title": "订单", "body": "已找到订单"})
        )
        answer = AIMessage(content="已经帮你查到了订单。\n\n还需要我帮你处理什么？")
        return {"messages": [*state["messages"], answer]}

    async def astream(
        self,
        state: dict[str, Any],
        *,
        context: Any,
        config: Any,
        stream_mode: Any,
    ):
        self.calls += 1
        for token in ["已经", "查到。"]:
            yield "messages", (AIMessage(content=token), {})
        context.outbox.append(OutboundItem(kind="select", payload={"question": "选一个"}))
        yield "values", {
            "messages": [*state["messages"], AIMessage(content="已经查到。\n\n选一个继续。")]
        }


def settings(**overrides: Any) -> Settings:
    base = {
        "SESSION_BACKEND": "memory",
        "LLM_API_KEY": "sk-test",
        "MAX_TURNS": 3,
        "SESSION_LOCK_WAIT_SECONDS": 0.05,
    }
    base.update(overrides)
    return Settings(**base)


def build_service(agent: FakeAgent, config: Settings | None = None) -> ChatService:
    config = config or settings()
    return ChatService(
        settings=config,
        agent=agent,  # type: ignore[arg-type]
        store=build_session_store(config),
        locks=build_lock_manager(config),
    )


@pytest.mark.asyncio
async def test_chat_persists_and_isolates_sessions() -> None:
    service = build_service(FakeAgent())
    request = ChatRequest(tenant_id="acme", user_id="u1", message="帮我查订单")
    first = await service.chat(request)

    assert first.turn_no == 1
    assert first.messages[0].kind == "text"
    assert any(item.kind == "card" for item in first.messages)

    second = await service.chat(
        ChatRequest(
            tenant_id="acme",
            user_id="u1",
            session_id=first.session_id,
            message="继续",
        )
    )
    assert second.turn_no == 2

    stored = await service.get_session("acme", "u1", first.session_id)
    assert stored.turn_count == 2
    assert [message.role for message in stored.messages] == ["user", "assistant", "user", "assistant"]

    with pytest.raises(SessionNotFoundError):
        await service.get_session("other", "u1", first.session_id)
    with pytest.raises(SessionForbiddenError):
        await service.get_session("acme", "u2", first.session_id)


@pytest.mark.asyncio
async def test_max_turns_hands_off() -> None:
    service = build_service(FakeAgent(), settings(MAX_TURNS=1))
    result = await service.chat(
        ChatRequest(tenant_id="acme", user_id="u1", message="你好")
    )
    assert result.status == "handed_off"
    assert result.messages[-1].kind == "handoff"

    with pytest.raises(SessionClosedError):
        await service.chat(
            ChatRequest(
                tenant_id="acme",
                user_id="u1",
                session_id=result.session_id,
                message="继续",
            )
        )


@pytest.mark.asyncio
async def test_stream_emits_delta_and_structured_actions() -> None:
    service = build_service(FakeAgent())
    request = ChatRequest(tenant_id="acme", user_id="u1", message="查一下")
    events = [event async for event in service.stream(request)]
    names = [name for name, _data in events]
    assert names[0] == "session"
    assert "delta" in names
    assert "message" in names
    assert "action" in names
    assert names[-1] == "done"

