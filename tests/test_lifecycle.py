from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage

from chatbot.api import create_app
from chatbot.channels.base import InboundMessage
from chatbot.channels.dispatcher import ChannelDispatcher
from chatbot.channels.state import build_channel_binding_store, build_event_deduplicator
from chatbot.config import Settings
from chatbot.lifecycle import InvalidSessionTransitionError, transition_session
from chatbot.locks import build_lock_manager
from chatbot.models import (
    ChatRequest,
    SatisfactionRequest,
    SessionRecord,
    StoredMessage,
)
from chatbot.service import ChatService, SessionClosedError, SessionForbiddenError
from chatbot.store import build_session_store


class LifecycleAgent:
    def __init__(self, *, resolve_first: bool = False) -> None:
        self.resolve_first = resolve_first
        self.calls = 0

    async def ainvoke(self, state: dict[str, Any], *, context: Any, config: Any) -> dict[str, Any]:
        self.calls += 1
        if self.resolve_first:
            self.resolve_first = False
            context.resolution_requested = True
            context.resolution_summary = "已确认订单状态并告知用户。"
        return {"messages": [*state["messages"], AIMessage(content="已经处理好了。")]}

    async def astream(self, *args: Any, **kwargs: Any):
        yield "values", {"messages": [AIMessage(content="已经处理好了。")]}


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "SESSION_BACKEND": "memory",
        "LLM_API_KEY": "sk-test",
        "MAX_TURNS": 3,
        "LIFECYCLE_SWEEP_INTERVAL_SECONDS": 0,
    }
    values.update(overrides)
    return Settings(**values)


def make_service(agent: LifecycleAgent, **overrides: Any) -> ChatService:
    settings = make_settings(**overrides)
    return ChatService(
        settings=settings,
        agent=agent,  # type: ignore[arg-type]
        store=build_session_store(settings),
        locks=build_lock_manager(settings),
    )


def test_transition_matrix_and_legacy_status() -> None:
    session = SessionRecord(
        id="s1",
        tenant_id="acme",
        user_id="u1",
        channel="web",
        status="handed_off",  # type: ignore[arg-type]
    )
    assert session.status == "handoff_pending"

    assert transition_session(session, "human_active", reason="agent_joined")
    assert session.status == "human_active"
    assert transition_session(session, "solved", reason="resolved")
    assert transition_session(session, "active", reason="reopened")
    assert session.reopened_count == 1
    assert session.solved_at is None
    assert transition_session(session, "closed", reason="user_ended")

    with pytest.raises(InvalidSessionTransitionError):
        transition_session(session, "active")


@pytest.mark.asyncio
async def test_human_owned_message_is_recorded_without_agent_call() -> None:
    agent = LifecycleAgent()
    service = make_service(agent)
    session = await service.create_session(tenant_id="acme", user_id="u1", channel="web")
    session.status = "human_active"
    await service.store.save_if_revision(session, session.revision)

    response = await service.chat(
        ChatRequest(
            tenant_id="acme",
            user_id="u1",
            session_id=session.id,
            message="我再补充一个订单号",
        )
    )

    assert response.status == "human_active"
    assert response.messages[0].kind == "handoff"
    assert agent.calls == 0
    stored = await service.get_session("acme", "u1", session.id)
    assert stored.messages[-2].content == "我再补充一个订单号"
    assert stored.messages[-1].role == "assistant"


@pytest.mark.asyncio
async def test_resolution_prompts_for_satisfaction_and_high_score_closes() -> None:
    service = make_service(LifecycleAgent(resolve_first=True))
    response = await service.chat(
        ChatRequest(tenant_id="acme", user_id="u1", message="订单查到了吗")
    )

    assert response.status == "solved"
    assert any(
        item.kind == "select" and item.payload.get("kind") == "satisfaction"
        for item in response.messages
    )
    stored = await service.get_session("acme", "u1", response.session_id)
    assert stored.resolution_summary == "已确认订单状态并告知用户。"
    assert stored.satisfaction_prompted_at is not None

    submitted = await service.submit_satisfaction(
        SatisfactionRequest(tenant_id="acme", user_id="u1", score=5),
        response.session_id,
    )
    assert submitted.status == "closed"
    assert submitted.satisfaction is not None
    assert submitted.satisfaction.score == 5

    repeated = await service.submit_satisfaction(
        SatisfactionRequest(tenant_id="acme", user_id="u1", score=1),
        response.session_id,
    )
    assert repeated.satisfaction is not None
    assert repeated.satisfaction.score == 5


@pytest.mark.asyncio
async def test_low_satisfaction_hands_off_and_later_messages_do_not_call_agent() -> None:
    agent = LifecycleAgent(resolve_first=True)
    service = make_service(agent)
    resolved = await service.chat(
        ChatRequest(tenant_id="acme", user_id="u1", message="处理一下")
    )

    submitted = await service.submit_satisfaction(
        SatisfactionRequest(tenant_id="acme", user_id="u1", score=1),
        resolved.session_id,
    )
    assert submitted.status == "handoff_pending"
    assert submitted.status_reason == "low_satisfaction"
    assert submitted.handoff_reason == "low_satisfaction"

    follow_up = await service.chat(
        ChatRequest(
            tenant_id="acme",
            user_id="u1",
            session_id=resolved.session_id,
            message="我还没解决",
        )
    )
    assert follow_up.status == "handoff_pending"
    assert agent.calls == 1


@pytest.mark.asyncio
async def test_numeric_reply_to_satisfaction_prompt_is_consumed_as_csat() -> None:
    service = make_service(LifecycleAgent(resolve_first=True))
    resolved = await service.chat(
        ChatRequest(tenant_id="acme", user_id="u1", message="好了")
    )

    response = await service.chat(
        ChatRequest(
            tenant_id="acme",
            user_id="u1",
            session_id=resolved.session_id,
            message="5",
        )
    )

    assert response.status == "closed"
    stored = await service.get_session("acme", "u1", resolved.session_id)
    assert stored.satisfaction is not None
    assert stored.satisfaction.score == 5
    assert [message.content for message in stored.messages][-2:] == [
        "5",
        "谢谢你的评分，很高兴这次能帮到你。",
    ]


@pytest.mark.asyncio
async def test_solved_session_reopens_within_window() -> None:
    agent = LifecycleAgent(resolve_first=True)
    service = make_service(agent)
    resolved = await service.chat(
        ChatRequest(tenant_id="acme", user_id="u1", message="第一件事")
    )

    reopened = await service.chat(
        ChatRequest(
            tenant_id="acme",
            user_id="u1",
            session_id=resolved.session_id,
            message="我还有另一个问题",
        )
    )

    assert reopened.status == "active"
    assert agent.calls == 2
    stored = await service.get_session("acme", "u1", resolved.session_id)
    assert stored.reopened_count == 1
    assert stored.solved_at is None
    assert stored.satisfaction_prompted_at is None


@pytest.mark.asyncio
async def test_solved_session_closes_after_reopen_window() -> None:
    service = make_service(LifecycleAgent(resolve_first=True))
    resolved = await service.chat(
        ChatRequest(tenant_id="acme", user_id="u1", message="已经解决")
    )
    stored = await service.get_session("acme", "u1", resolved.session_id)
    stored.solved_at = datetime.now(UTC) - timedelta(
        seconds=service.settings.reopen_window_seconds + 1
    )
    assert await service.store.save_if_revision(stored, stored.revision)

    with pytest.raises(SessionClosedError):
        await service.chat(
            ChatRequest(
                tenant_id="acme",
                user_id="u1",
                session_id=resolved.session_id,
                message="还在吗",
            )
        )

    closed = await service.get_session("acme", "u1", resolved.session_id)
    assert closed.status == "closed"
    assert closed.close_reason == "solved_timeout"


@pytest.mark.asyncio
async def test_lifecycle_sweep_closes_idle_and_expired_solved_sessions() -> None:
    service = make_service(LifecycleAgent(), IDLE_CLOSE_SECONDS=60, SOLVED_CLOSE_SECONDS=120)
    active = await service.create_session(tenant_id="acme", user_id="u1", channel="web")
    active.last_activity_at = datetime.now(UTC) - timedelta(seconds=61)
    assert await service.store.save_if_revision(active, active.revision)

    solved = await service.create_session(tenant_id="acme", user_id="u2", channel="web")
    solved.status = "solved"
    solved.solved_at = datetime.now(UTC) - timedelta(seconds=121)
    solved.satisfaction_prompted_at = solved.solved_at
    assert await service.store.save_if_revision(solved, solved.revision)

    result = await service.sweep_lifecycle()

    assert result == {"idle_timeout": 1, "solved_timeout": 1}
    assert (await service.get_session("acme", "u1", active.id)).close_reason == "idle_timeout"
    assert (await service.get_session("acme", "u2", solved.id)).close_reason == "solved_timeout"


@pytest.mark.asyncio
async def test_manual_close_is_idempotent_and_user_scoped() -> None:
    service = make_service(LifecycleAgent())
    session = await service.create_session(tenant_id="acme", user_id="u1", channel="web")

    with pytest.raises(SessionForbiddenError):
        await service.close_session("acme", "u2", session.id, "user_ended")

    closed = await service.close_session("acme", "u1", session.id, "user_ended")
    repeated = await service.close_session("acme", "u1", session.id, "user_ended")
    assert closed.status == "closed"
    assert closed.close_reason == "user_ended"
    assert repeated.updated_at == closed.updated_at


def test_lifecycle_api_close_and_satisfaction() -> None:
    settings = make_settings()
    service = ChatService(
        settings=settings,
        agent=LifecycleAgent(resolve_first=True),  # type: ignore[arg-type]
        store=build_session_store(settings),
        locks=build_lock_manager(settings),
    )
    app = create_app(settings=settings, service=service)

    with TestClient(app) as client:
        resolved = client.post(
            "/v1/chat",
            json={"tenant_id": "acme", "user_id": "u1", "message": "已经处理好了"},
        ).json()
        assert resolved["status"] == "solved"

        scored = client.post(
            f"/v1/sessions/{resolved['session_id']}/satisfaction",
            json={"tenant_id": "acme", "user_id": "u1", "score": 4},
        )
        assert scored.status_code == 200
        assert scored.json()["status"] == "closed"
        assert scored.json()["satisfaction"]["score"] == 4

        active = client.post(
            "/v1/chat",
            json={"tenant_id": "acme", "user_id": "u2", "message": "还没解决"},
        ).json()
        unavailable = client.post(
            f"/v1/sessions/{active['session_id']}/satisfaction",
            json={"tenant_id": "acme", "user_id": "u2", "score": 5},
        )
        assert unavailable.status_code == 409

        closed = client.post(
            f"/v1/sessions/{active['session_id']}/close",
            json={"tenant_id": "acme", "user_id": "u2", "reason": "user_ended"},
        )
        assert closed.status_code == 200
        assert closed.json()["status"] == "closed"
        assert closed.json()["status_reason"] == "user_ended"


@pytest.mark.asyncio
async def test_channel_reuses_human_owned_session_and_links_new_session_to_closed_parent() -> None:
    agent = LifecycleAgent()
    service = make_service(agent)
    settings = service.settings
    bindings = build_channel_binding_store(settings)
    dispatcher = ChannelDispatcher(
        service=service,
        bindings=bindings,
        deduplicator=build_event_deduplicator(settings),
        settings=settings,
    )
    inbound = InboundMessage(
        channel="web",
        tenant_id="acme",
        user_id="u1",
        session_key="u1",
        text="补充信息",
        event_id="event-human",
    )

    human_session = await service.create_session(
        tenant_id="acme", user_id="u1", channel="web"
    )
    human_session.status = "handoff_pending"
    assert await service.store.save_if_revision(human_session, human_session.revision)
    await bindings.set("acme", "web", "u1", human_session.id)

    human_response = await dispatcher.handle(inbound)
    assert human_response is not None
    assert human_response.session_id == human_session.id
    assert human_response.status == "handoff_pending"
    assert agent.calls == 0

    await service.close_session("acme", "u1", human_session.id, "human_ended")
    new_response = await dispatcher.handle(
        replace(inbound, text="重新开一个会话", event_id="event-new")
    )
    assert new_response is not None
    assert new_response.session_id != human_session.id
    creation = await service.get_session("acme", "u1", new_response.session_id)
    assert creation.parent_session_id == human_session.id
    assert agent.calls == 1


def test_session_record_deserializes_legacy_status() -> None:
    session = SessionRecord.model_validate(
        {
            "id": "legacy",
            "tenant_id": "acme",
            "user_id": "u1",
            "channel": "web",
            "status": "handed_off",
            "messages": [StoredMessage(role="user", content="你好").model_dump()],
        }
    )
    assert session.status == "handoff_pending"
