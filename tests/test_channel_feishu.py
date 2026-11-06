from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from chatbot.channels.base import ChannelHttpRequest
from chatbot.channels.feishu import FeishuAdapter
from chatbot.config import Settings
from chatbot.models import ChatResponse, OutboundItem


def feishu_settings(**overrides) -> Settings:
    values = {
        "SESSION_BACKEND": "memory",
        "FEISHU_ENABLED": True,
        "FEISHU_APP_ID": "app",
        "FEISHU_APP_SECRET": "secret",
        "FEISHU_TENANT_ID": "acme",
        "FEISHU_ENCRYPT_KEY": "",
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.asyncio
async def test_feishu_url_verification() -> None:
    adapter = FeishuAdapter(feishu_settings())
    body = json.dumps({"type": "url_verification", "challenge": "challenge-1"}).encode()
    result = await adapter.receive(ChannelHttpRequest("POST", {}, {}, body))
    assert result.response == {"challenge": "challenge-1"}
    assert result.message is None


@pytest.mark.asyncio
async def test_feishu_text_and_card_action_events() -> None:
    adapter = FeishuAdapter(feishu_settings())
    text_event = {
        "header": {"event_type": "im.message.receive_v1", "event_id": "ev-1"},
        "event": {
            "sender": {"sender_id": {"open_id": "ou_1"}},
            "message": {
                "message_type": "text",
                "content": json.dumps({"text": "@_user_1 你好"}),
                "chat_id": "oc_1",
                "chat_type": "p2p",
                "message_id": "om_1",
            },
        },
    }
    result = await adapter.receive(
        ChannelHttpRequest("POST", {}, {}, json.dumps(text_event).encode())
    )
    assert result.message is not None
    assert result.message.text == "你好"
    assert result.message.session_key == "oc_1"
    assert result.message.event_id == "ev-1"

    card_event = {
        "header": {"event_type": "card.action.trigger", "event_id": "ev-2"},
        "event": {
            "operator": {"operator_id": {"open_id": "ou_1"}},
            "action": {
                "tag": "button",
                "value": {
                    "action": "select",
                    "value": "查物流",
                    "session_id": "session-1",
                },
            },
            "context": {"open_chat_id": "oc_1", "open_message_id": "om_1"},
        },
    }
    result = await adapter.receive(
        ChannelHttpRequest("POST", {}, {}, json.dumps(card_event).encode())
    )
    assert result.message is not None
    assert result.message.text == "我选择了：查物流"
    assert result.message.session_id == "session-1"


@pytest.mark.asyncio
async def test_feishu_signature_verification() -> None:
    encrypt_key = "encrypt-key"
    body = b'{"type":"url_verification","challenge":"ok"}'
    timestamp = "1700000000"
    nonce = "nonce"
    signature = hashlib.sha256(
        f"{timestamp}{nonce}{encrypt_key}".encode() + body
    ).hexdigest()
    adapter = FeishuAdapter(feishu_settings(FEISHU_ENCRYPT_KEY=encrypt_key))
    result = await adapter.receive(
        ChannelHttpRequest(
            "POST",
            {
                "x-lark-request-timestamp": timestamp,
                "x-lark-request-nonce": nonce,
                "x-lark-signature": signature,
            },
            {},
            body,
        )
    )
    assert result.response == {"challenge": "ok"}


@pytest.mark.asyncio
async def test_feishu_send_text_and_select_card() -> None:
    captured: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
            )
        captured.append(json.loads(request.content))
        return httpx.Response(200, json={"code": 0, "data": {"message_id": "om_out"}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(feishu_settings(), client=client)
    inbound = await _inbound_text(adapter)
    await adapter.send(
        inbound,
        ChatResponse(
            session_id="session-1",
            turn_no=1,
            status="active",
            messages=[
                OutboundItem(kind="text", text="你好"),
                OutboundItem(
                    kind="select",
                    payload={
                        "question": "选一个",
                        "options": [{"label": "查物流", "value": "logistics"}],
                    },
                ),
            ],
        ),
    )
    await client.aclose()

    assert captured[0]["msg_type"] == "text"
    assert captured[1]["msg_type"] == "interactive"
    card = json.loads(str(captured[1]["content"]))
    assert card["header"]["title"]["content"] == "选一个"
    assert card["elements"][1]["actions"][0]["value"]["session_id"] == "session-1"


async def _inbound_text(adapter: FeishuAdapter):
    event = {
        "header": {"event_type": "im.message.receive_v1", "event_id": "ev"},
        "event": {
            "sender": {"sender_id": {"open_id": "ou_1"}},
            "message": {
                "message_type": "text",
                "content": json.dumps({"text": "hi"}),
                "chat_id": "oc_1",
                "chat_type": "p2p",
                "message_id": "om_1",
            },
        },
    }
    result = await adapter.receive(
        ChannelHttpRequest("POST", {}, {}, json.dumps(event).encode())
    )
    assert result.message is not None
    return result.message

