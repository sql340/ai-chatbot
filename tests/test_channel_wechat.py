from __future__ import annotations

import base64
import hashlib
import struct

import httpx
import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from chatbot.channels.base import ChannelHttpRequest
from chatbot.channels.wechat import WeChatOfficialAccountAdapter
from chatbot.config import Settings
from chatbot.models import ChatResponse, OutboundItem


def wechat_settings(**overrides) -> Settings:
    values = {
        "SESSION_BACKEND": "memory",
        "WECHAT_ENABLED": True,
        "WECHAT_TENANT_ID": "acme",
        "WECHAT_TOKEN": "token",
        "WECHAT_APP_ID": "appid",
        "WECHAT_APP_SECRET": "secret",
        "WECHAT_API_BASE_URL": "https://wechat.local",
    }
    values.update(overrides)
    return Settings(**values)


def signature(token: str, timestamp: str, nonce: str) -> str:
    return hashlib.sha1("".join(sorted([token, timestamp, nonce])).encode()).hexdigest()


def encrypted_signature(token: str, timestamp: str, nonce: str, encrypted: str) -> str:
    return hashlib.sha1(
        "".join(sorted([token, timestamp, nonce, encrypted])).encode()
    ).hexdigest()


def wechat_encrypt(message: str, app_id: str, encoding_key: str) -> str:
    key = base64.b64decode(encoding_key + "=")
    raw = b"0123456789abcdef" + struct.pack("!I", len(message.encode())) + message.encode()
    raw += app_id.encode()
    padding = 32 - len(raw) % 32
    raw += bytes([padding]) * padding
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(raw) + encryptor.finalize()).decode()


@pytest.mark.asyncio
async def test_wechat_get_verification_and_text_callback() -> None:
    adapter = WeChatOfficialAccountAdapter(wechat_settings())
    timestamp, nonce = "1700000000", "nonce"
    query = {
        "signature": signature("token", timestamp, nonce),
        "timestamp": timestamp,
        "nonce": nonce,
        "echostr": "echo-ok",
    }
    result = await adapter.receive(ChannelHttpRequest("GET", {}, query, b""))
    assert result.response == "echo-ok"

    xml = """<xml>
      <ToUserName>gh_1</ToUserName>
      <FromUserName>openid-1</FromUserName>
      <CreateTime>1700000000</CreateTime>
      <MsgType>text</MsgType>
      <Content>你好</Content>
      <MsgId>123</MsgId>
    </xml>""".encode()
    result = await adapter.receive(ChannelHttpRequest("POST", {}, query, xml))
    assert result.message is not None
    assert result.message.text == "你好"
    assert result.message.user_id == "openid-1"


@pytest.mark.asyncio
async def test_wechat_send_custom_message() -> None:
    captured: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/cgi-bin/token":
            return httpx.Response(200, json={"access_token": "token", "expires_in": 7200})
        captured.append(
            {"path": request.url.path, "params": dict(request.url.params), "json": _json(request)}
        )
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = WeChatOfficialAccountAdapter(wechat_settings(), client=client)
    from chatbot.channels.base import InboundMessage

    inbound = InboundMessage(
        channel="wechat",
        tenant_id="acme",
        user_id="openid-1",
        session_key="openid-1",
        text="你好",
        reply={"open_id": "openid-1"},
    )
    await adapter.send(
        inbound,
        ChatResponse(
            session_id="s1",
            turn_no=1,
            status="active",
            messages=[OutboundItem(kind="text", text="收到")],
        ),
    )
    await client.aclose()
    assert captured[0]["path"] == "/cgi-bin/message/custom/send"
    assert captured[0]["params"]["access_token"] == "token"
    assert captured[0]["json"]["touser"] == "openid-1"


@pytest.mark.asyncio
async def test_wechat_safe_mode_verification_and_callback() -> None:
    encoding_key = base64.b64encode(b"k" * 32).decode().rstrip("=")
    adapter = WeChatOfficialAccountAdapter(
        wechat_settings(WECHAT_ENCODING_AES_KEY=encoding_key)
    )
    timestamp, nonce = "1700000000", "nonce"
    encrypted_echo = wechat_encrypt("echo-secure", "appid", encoding_key)
    verify_query = {
        "msg_signature": encrypted_signature("token", timestamp, nonce, encrypted_echo),
        "timestamp": timestamp,
        "nonce": nonce,
        "echostr": encrypted_echo,
    }
    verified = await adapter.receive(ChannelHttpRequest("GET", {}, verify_query, b""))
    assert verified.response == "echo-secure"

    message_xml = """<xml>
      <ToUserName>gh_1</ToUserName>
      <FromUserName>openid-1</FromUserName>
      <CreateTime>1700000000</CreateTime>
      <MsgType>text</MsgType>
      <Content>安全模式你好</Content>
      <MsgId>456</MsgId>
    </xml>"""
    encrypted_message = wechat_encrypt(message_xml, "appid", encoding_key)
    callback_xml = f"<xml><Encrypt><![CDATA[{encrypted_message}]]></Encrypt></xml>".encode()
    callback_query = {
        "msg_signature": encrypted_signature("token", timestamp, nonce, encrypted_message),
        "timestamp": timestamp,
        "nonce": nonce,
    }
    callback = await adapter.receive(ChannelHttpRequest("POST", {}, callback_query, callback_xml))
    assert callback.message is not None
    assert callback.message.text == "安全模式你好"


def _json(request: httpx.Request) -> dict:
    import json

    return json.loads(request.content)

