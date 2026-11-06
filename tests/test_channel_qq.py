from __future__ import annotations

import json

import httpx
import pytest

from chatbot.channels.base import ChannelHttpRequest
from chatbot.channels.qq import QQOneBotAdapter
from chatbot.config import Settings
from chatbot.models import ChatResponse, OutboundItem


def qq_settings(**overrides) -> Settings:
    values = {
        "SESSION_BACKEND": "memory",
        "QQ_ENABLED": True,
        "QQ_TENANT_ID": "acme",
        "QQ_ONEBOT_API_BASE_URL": "http://onebot.local",
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.asyncio
async def test_qq_parse_group_message_and_send() -> None:
    captured: list[tuple[httpx.URL, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request.url, json.loads(request.content)))
        return httpx.Response(200, json={"status": "ok", "retcode": 0})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = QQOneBotAdapter(qq_settings(), client=client)
    event = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10000,
        "user_id": 12345,
        "group_id": 67890,
        "message_id": 99,
        "message": [{"type": "text", "data": {"text": "查订单"}}],
    }
    result = await adapter.receive(
        ChannelHttpRequest("POST", {}, {}, json.dumps(event).encode())
    )
    assert result.message is not None
    assert result.message.text == "查订单"
    assert result.message.session_key == "group:67890:12345"

    await adapter.send(
        result.message,
        ChatResponse(
            session_id="s1",
            turn_no=1,
            status="active",
            messages=[OutboundItem(kind="text", text="收到")],
        ),
    )
    await client.aclose()
    assert captured[0][0].path == "/send_group_msg"
    assert captured[0][1]["group_id"] == 67890
    assert captured[0][1]["message"] == "收到"

