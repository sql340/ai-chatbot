from __future__ import annotations

import hashlib
import json
from typing import Any
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage

from chatbot.api import create_app
from chatbot.config import Settings
from chatbot.locks import build_lock_manager
from chatbot.service import ChatService
from chatbot.store import build_session_store


class FakeAgent:
    def __init__(self) -> None:
        self.calls = 0

    async def ainvoke(self, state: dict[str, Any], *, context: Any, config: Any) -> dict[str, Any]:
        self.calls += 1
        return {"messages": [*state["messages"], AIMessage(content="你好。\n\n有什么可以帮你？")]}


def build_app(agent: FakeAgent, **overrides: Any) -> tuple[Any, ChatService]:
    settings = Settings(
        SESSION_BACKEND="memory",
        LLM_API_KEY="sk-test",
        **overrides,
    )
    service = ChatService(
        settings=settings,
        agent=agent,  # type: ignore[arg-type]
        store=build_session_store(settings),
        locks=build_lock_manager(settings),
    )
    return create_app(settings=settings, service=service), service


def feishu_signature(body: bytes, timestamp: str, nonce: str, encrypt_key: str) -> str:
    return hashlib.sha256(
        f"{timestamp}{nonce}{encrypt_key}".encode() + body
    ).hexdigest()


def test_web_channel_reuses_session_and_serves_frontend() -> None:
    app, _service = build_app(FakeAgent())
    with TestClient(app) as client:
        first = client.post(
            "/v1/channels/web/events",
            json={"tenant_id": "acme", "user_id": "u1", "message": "你好"},
        )
        assert first.status_code == 200
        first_payload = first.json()
        assert first_payload["turn_no"] == 1
        assert first_payload["epoch"] == 1

        second = client.post(
            "/v1/channels/web/events",
            json={"tenant_id": "acme", "user_id": "u1", "message": "继续"},
        )
        assert second.status_code == 200
        assert second.json()["session_id"] == first_payload["session_id"]
        assert second.json()["turn_no"] == 2
        assert second.json()["epoch"] == 2

        forbidden = client.post(
            "/v1/channels/web/events",
            json={
                "tenant_id": "acme",
                "user_id": "u2",
                "session_id": first_payload["session_id"],
                "message": "换个人继续",
            },
        )
        assert forbidden.status_code == 403

        invalid = client.post(
            "/v1/channels/web/events",
            json={"tenant_id": "acme", "user_id": "u1"},
        )
        assert invalid.status_code == 400

        page = client.get("/")
        assert page.status_code == 200
        assert "在线支持台" in page.text
        assert client.get("/static/app.js").status_code == 200
        assert client.get("/static/styles.css").status_code == 200


def test_feishu_challenge_signature_and_duplicate_event() -> None:
    encrypt_key = "encrypt-key"
    timestamp, nonce = "1700000000", "nonce"
    challenge_body = json.dumps(
        {"type": "url_verification", "challenge": "challenge-1"}
    ).encode()
    signature = feishu_signature(challenge_body, timestamp, nonce, encrypt_key)
    agent = FakeAgent()
    app, _service = build_app(
        agent,
        FEISHU_ENABLED=True,
        FEISHU_APP_ID="app",
        FEISHU_APP_SECRET="secret",
        FEISHU_ENCRYPT_KEY=encrypt_key,
    )

    with TestClient(app) as client:
        rejected = client.post("/v1/channels/feishu/events", content=challenge_body)
        assert rejected.status_code == 403

        challenge = client.post(
            "/v1/channels/feishu/events",
            content=challenge_body,
            headers={
                "x-lark-request-timestamp": timestamp,
                "x-lark-request-nonce": nonce,
                "x-lark-signature": signature,
            },
        )
        assert challenge.status_code == 200
        assert challenge.json() == {"challenge": "challenge-1"}

        adapter = app.state.channels.get("feishu")
        assert adapter is not None
        adapter.send = AsyncMock()
        event_body = json.dumps(
            {
                "header": {"event_type": "im.message.receive_v1", "event_id": "ev-1"},
                "event": {
                    "sender": {"sender_id": {"open_id": "ou_1"}},
                    "message": {
                        "message_type": "text",
                        "content": json.dumps({"text": "你好"}),
                        "chat_id": "oc_1",
                        "chat_type": "p2p",
                        "message_id": "om_1",
                    },
                },
            }
        ).encode()
        event_signature = feishu_signature(event_body, timestamp, nonce, encrypt_key)
        headers = {
            "x-lark-request-timestamp": timestamp,
            "x-lark-request-nonce": nonce,
            "x-lark-signature": event_signature,
        }

        first = client.post("/v1/channels/feishu/events", content=event_body, headers=headers)
        second = client.post("/v1/channels/feishu/events", content=event_body, headers=headers)

        assert first.status_code == 200
        assert second.status_code == 200
        assert agent.calls == 1
        assert adapter.send.await_count == 1


def test_disabled_channel_returns_404() -> None:
    app, _service = build_app(FakeAgent(), FEISHU_ENABLED=False)
    with TestClient(app) as client:
        response = client.post("/v1/channels/feishu/events", json={})
        assert response.status_code == 404
