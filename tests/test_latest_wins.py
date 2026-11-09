from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage

from chatbot.channels.base import InboundMessage
from chatbot.channels.dispatcher import ChannelDispatcher
from chatbot.channels.state import build_channel_binding_store, build_event_deduplicator
from chatbot.config import Settings
from chatbot.locks import build_lock_manager
from chatbot.models import ChatRequest, ChatResponse, OutboundItem
from chatbot.service import ChatService, GenerationSupersededError
from chatbot.store import build_session_store


class BlockingAgent:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls: list[str] = []

    async def ainvoke(self, state: dict[str, Any], *, context: Any, config: Any) -> dict[str, Any]:
        text = state["messages"][-1].content
        self.calls.append(text)
        if text == "旧问题":
            self.started.set()
            await self.release.wait()
        return {"messages": [*state["messages"], AIMessage(content=f"回答：{text}")]}

    async def astream(self, *args: Any, **kwargs: Any):
        raise AssertionError("stream is not used in this test")


class BlockingStreamAgent:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def ainvoke(self, state: dict[str, Any], *, context: Any, config: Any) -> dict[str, Any]:
        return {"messages": [*state["messages"], AIMessage(content="新回答")]}

    async def astream(
        self,
        state: dict[str, Any],
        *,
        context: Any,
        config: Any,
        stream_mode: Any,
    ):
        self.started.set()
        yield "messages", (AIMessage(content="旧"), {})
        await self.release.wait()
        yield "messages", (AIMessage(content="回答"), {})
        yield "values", {"messages": [AIMessage(content="旧回答")]}


def build_service(agent: Any) -> ChatService:
    settings = Settings(
        SESSION_BACKEND="memory",
        LLM_API_KEY="sk-test",
        MAX_TURNS=10,
        SESSION_LOCK_WAIT_SECONDS=0.2,
    )
    return ChatService(
        settings=settings,
        agent=agent,
        store=build_session_store(settings),
        locks=build_lock_manager(settings),
    )


@pytest.mark.asyncio
async def test_latest_message_wins_and_old_reply_is_discarded() -> None:
    agent = BlockingAgent()
    service = build_service(agent)
    session = await service.create_session(tenant_id="acme", user_id="u1", channel="web")

    old_request = ChatRequest(
        tenant_id="acme",
        user_id="u1",
        session_id=session.id,
        message="旧问题",
    )
    old_task = asyncio.create_task(service.chat(old_request))
    await asyncio.wait_for(agent.started.wait(), timeout=1)

    new_request = ChatRequest(
        tenant_id="acme",
        user_id="u1",
        session_id=session.id,
        message="新问题",
    )
    new_response = await service.chat(new_request)

    with pytest.raises(GenerationSupersededError):
        await old_task

    stored = await service.get_session("acme", "u1", session.id)
    assert new_response.epoch == 2
    assert not await service.is_current("acme", session.id, 1)
    assert await service.is_current("acme", session.id, 2)
    assert stored.active_epoch == 2
    assert stored.turn_count == 2
    assert [message.content for message in stored.messages] == [
        "旧问题",
        "新问题",
        "回答：新问题",
    ]


@pytest.mark.asyncio
async def test_stream_generation_is_superseded_before_commit() -> None:
    agent = BlockingStreamAgent()
    service = build_service(agent)
    session = await service.create_session(tenant_id="acme", user_id="u1", channel="web")

    async def consume_old_stream() -> list[tuple[str, dict[str, Any]]]:
        request = ChatRequest(
            tenant_id="acme",
            user_id="u1",
            session_id=session.id,
            message="旧问题",
        )
        return [event async for event in service.stream(request)]

    old_task = asyncio.create_task(consume_old_stream())
    await asyncio.wait_for(agent.started.wait(), timeout=1)

    await service.chat(
        ChatRequest(
            tenant_id="acme",
            user_id="u1",
            session_id=session.id,
            message="新问题",
        )
    )
    agent.release.set()
    events = await asyncio.wait_for(old_task, timeout=1)

    assert any(
        name == "error" and data.get("code") == "superseded" for name, data in events
    )
    stored = await service.get_session("acme", "u1", session.id)
    assert [message.content for message in stored.messages] == [
        "旧问题",
        "新问题",
        "新回答",
    ]


@pytest.mark.asyncio
async def test_channel_concurrent_messages_share_one_session() -> None:
    service = build_service(BlockingAgent())
    settings = service.settings
    dispatcher = ChannelDispatcher(
        service=service,
        bindings=build_channel_binding_store(settings),
        deduplicator=build_event_deduplicator(settings),
        settings=settings,
    )

    first, second = await asyncio.gather(
        dispatcher.handle(
            InboundMessage(
                channel="web",
                tenant_id="acme",
                user_id="u1",
                session_key="u1",
                text="你好",
                event_id="event-1",
            )
        ),
        dispatcher.handle(
            InboundMessage(
                channel="web",
                tenant_id="acme",
                user_id="u1",
                session_key="u1",
                text="继续",
                event_id="event-2",
            )
        ),
    )

    assert first is not None
    assert second is not None
    assert first.session_id == second.session_id
    stored = await service.get_session("acme", "u1", first.session_id)
    assert stored.turn_count == 2
    assert [message.role for message in stored.messages].count("user") == 2


@pytest.mark.asyncio
async def test_channel_drops_stale_response_before_send() -> None:
    service = build_service(BlockingAgent())
    settings = service.settings
    dispatcher = ChannelDispatcher(
        service=service,
        bindings=build_channel_binding_store(settings),
        deduplicator=build_event_deduplicator(settings),
        settings=settings,
    )
    adapter = AsyncMock()
    session = await service.create_session(tenant_id="acme", user_id="u1", channel="web")
    inbound = InboundMessage(
        channel="web",
        tenant_id="acme",
        user_id="u1",
        session_key="u1",
        text="你好",
        event_id="event-stale",
    )
    dispatcher.handle = AsyncMock(  # type: ignore[method-assign]
        return_value=ChatResponse(
            session_id=session.id,
            turn_no=1,
            epoch=1,
            status="active",
            messages=[OutboundItem(kind="text", text="旧回复")],
        )
    )

    await dispatcher.process_and_send(adapter, inbound)

    adapter.send.assert_not_awaited()
