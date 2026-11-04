from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from chatbot.api import create_app
from chatbot.config import Settings
from chatbot.locks import build_lock_manager
from chatbot.service import ChatService
from chatbot.store import build_session_store


class FakeAgent:
    async def ainvoke(self, state: dict[str, Any], *, context: Any, config: Any) -> dict[str, Any]:
        from langchain_core.messages import AIMessage

        return {"messages": [*state["messages"], AIMessage(content="你好\n\n有什么可以帮你？")]}

    async def astream(self, *args: Any, **kwargs: Any):
        from langchain_core.messages import AIMessage

        yield "values", {"messages": [AIMessage(content="你好\n\n有什么可以帮你？")]}


def test_chat_endpoint_and_stream() -> None:
    settings = Settings(SESSION_BACKEND="memory", LLM_API_KEY="sk-test")
    service = ChatService(
        settings=settings,
        agent=FakeAgent(),  # type: ignore[arg-type]
        store=build_session_store(settings),
        locks=build_lock_manager(settings),
    )
    app = create_app(settings=settings, service=service)

    with TestClient(app) as client:
        response = client.post(
            "/v1/chat",
            json={"tenant_id": "acme", "user_id": "u1", "message": "你好"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["turn_no"] == 1
        assert len(payload["messages"]) == 2

        with client.stream(
            "POST",
            "/v1/chat/stream",
            json={"tenant_id": "acme", "user_id": "u1", "message": "再来一次"},
        ) as stream_response:
            body = "".join(stream_response.iter_text())
        assert "event: session" in body
        assert "event: message" in body
        assert "event: done" in body

