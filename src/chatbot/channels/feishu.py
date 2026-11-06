from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import time
from typing import Any

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ..config import Settings
from ..models import ChatResponse, OutboundItem
from .base import (
    ChannelAuthError,
    ChannelDeliveryError,
    ChannelHttpRequest,
    ChannelPayloadError,
    ChannelReceiveResult,
    InboundMessage,
)

_MENTION_RE = re.compile(r"<at[^>]*>.*?</at>|@_user_\d+", re.IGNORECASE)


class FeishuAdapter:
    name = "feishu"  # type: ignore[assignment]

    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._client = client or httpx.AsyncClient(timeout=settings.feishu_timeout)
        self._owns_client = client is None
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()

    async def receive(self, request: ChannelHttpRequest) -> ChannelReceiveResult:
        payload = self._decode_payload(request)
        if payload.get("type") == "url_verification":
            self._check_verification_token(payload)
            return ChannelReceiveResult(
                response={"challenge": payload.get("challenge", "")},
                process_async=False,
            )

        header = payload.get("header") or {}
        event_type = header.get("event_type") or payload.get("type")
        if event_type == "im.message.receive_v1":
            message = self._parse_message(payload, header)
        elif event_type == "card.action.trigger":
            message = self._parse_card_action(payload, header)
        else:
            message = None

        return ChannelReceiveResult(
            message=message,
            response={"code": 0},
            process_async=True,
        )

    async def send(self, inbound: InboundMessage, response: ChatResponse) -> None:
        chat_id = str(inbound.reply.get("chat_id") or "")
        if not chat_id:
            raise ChannelDeliveryError("feishu reply context has no chat_id")

        for item in response.messages:
            if item.kind == "text":
                await self._send_text(chat_id, item.text or "")
            elif item.kind == "select":
                await self._send_card(chat_id, self._build_select_card(item, response.session_id))
            elif item.kind == "card":
                await self._send_card(chat_id, self._build_payload_card(item, response.session_id))
            else:
                await self._send_text(chat_id, item.text or "正在为你转接人工客服，请稍等。")

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _decode_payload(self, request: ChannelHttpRequest) -> dict[str, Any]:
        self._verify_signature(request)
        try:
            payload = json.loads(request.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelPayloadError("invalid Feishu JSON payload") from exc

        encrypted = payload.get("encrypt")
        if encrypted:
            if not self._settings.feishu_encrypt_key:
                raise ChannelAuthError("encrypted Feishu event requires FEISHU_ENCRYPT_KEY")
            payload = self._decrypt(str(encrypted))
        self._check_verification_token(payload)
        return payload

    def _verify_signature(self, request: ChannelHttpRequest) -> None:
        encrypt_key = self._settings.feishu_encrypt_key
        if not encrypt_key:
            return
        timestamp = request.headers.get("x-lark-request-timestamp", "")
        nonce = request.headers.get("x-lark-request-nonce", "")
        signature = request.headers.get("x-lark-signature", "")
        if not timestamp or not nonce or not signature:
            raise ChannelAuthError("missing Feishu signature headers")
        body = request.body.decode("utf-8")
        expected = hashlib.sha256(
            f"{timestamp}{nonce}{encrypt_key}{body}".encode()
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise ChannelAuthError("invalid Feishu signature")

    def _check_verification_token(self, payload: dict[str, Any]) -> None:
        expected = self._settings.feishu_verification_token
        if not expected:
            return
        header = payload.get("header") or {}
        token = payload.get("token") or header.get("token")
        if token and not hmac.compare_digest(str(token), expected):
            raise ChannelAuthError("invalid Feishu verification token")

    def _decrypt(self, encrypted: str) -> dict[str, Any]:
        key = hashlib.sha256(self._settings.feishu_encrypt_key.encode()).digest()
        try:
            raw = base64.b64decode(encrypted)
            iv, ciphertext = raw[:16], raw[16:]
            decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
            plain = decryptor.update(ciphertext) + decryptor.finalize()
            padding = plain[-1]
            if not 1 <= padding <= 16:
                raise ValueError("invalid PKCS7 padding")
            plain = plain[:-padding]
            return json.loads(plain.decode("utf-8"))
        except Exception as exc:
            raise ChannelAuthError("failed to decrypt Feishu event") from exc

    def _parse_message(
        self, payload: dict[str, Any], header: dict[str, Any]
    ) -> InboundMessage | None:
        event = payload.get("event") or {}
        message = event.get("message") or {}
        sender = event.get("sender") or {}
        sender_id = sender.get("sender_id") or {}
        user_id = str(sender_id.get("open_id") or sender_id.get("user_id") or "")
        if not user_id:
            return None

        text = _extract_feishu_text(message)
        if not text:
            return None

        chat_id = str(message.get("chat_id") or "")
        chat_type = str(message.get("chat_type") or "p2p")
        message_id = str(message.get("message_id") or "")
        session_key = f"{chat_id}:{user_id}" if chat_type == "group" else chat_id or user_id
        event_id = str(header.get("event_id") or message_id or "")
        return InboundMessage(
            channel="feishu",
            tenant_id=self._settings.feishu_tenant_id,
            user_id=user_id,
            session_key=session_key,
            text=text,
            event_id=event_id or None,
            reply={"chat_id": chat_id, "message_id": message_id},
            raw=payload,
        )

    def _parse_card_action(
        self, payload: dict[str, Any], header: dict[str, Any]
    ) -> InboundMessage | None:
        event = payload.get("event") or payload
        action = event.get("action") or {}
        value = action.get("value") or event.get("action_value")
        parsed_value = _parse_action_value(value)
        selection = _extract_card_selection(action, parsed_value)

        operator = event.get("operator") or {}
        operator_id = operator.get("operator_id") or {}
        user_id = str(
            operator_id.get("open_id")
            or event.get("operator_id")
            or event.get("open_id")
            or ""
        )
        if not user_id:
            return None

        context = event.get("context") or {}
        chat_id = str(context.get("open_chat_id") or event.get("chat_id") or "")
        message_id = str(context.get("open_message_id") or event.get("message_id") or "")
        session_id = (
            str(parsed_value.get("session_id"))
            if isinstance(parsed_value, dict) and parsed_value.get("session_id")
            else None
        )
        event_id = str(header.get("event_id") or "")
        text = f"我选择了：{selection}" if selection else "我点击了卡片上的按钮"

        return InboundMessage(
            channel="feishu",
            tenant_id=self._settings.feishu_tenant_id,
            user_id=user_id,
            session_key=chat_id or user_id,
            text=text,
            event_id=event_id or None,
            session_id=session_id,
            reply={"chat_id": chat_id, "message_id": message_id},
            raw=payload,
        )

    async def _send_text(self, chat_id: str, text: str) -> None:
        if not text.strip():
            return
        await self._send_message(chat_id, "text", {"text": text})

    async def _send_card(self, chat_id: str, card: dict[str, Any]) -> None:
        await self._send_message(chat_id, "interactive", card)

    async def _send_message(self, chat_id: str, msg_type: str, content: dict[str, Any]) -> None:
        token = await self._tenant_access_token()
        response = await self._client.post(
            f"{self._settings.feishu_api_base_url}/open-apis/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            headers={"Authorization": f"Bearer {token}"},
            json={
                "receive_id": chat_id,
                "msg_type": msg_type,
                "content": json.dumps(content, ensure_ascii=False),
            },
        )
        self._raise_feishu_error(response, "send message")

    async def _tenant_access_token(self) -> str:
        async with self._token_lock:
            now = time.time()
            if self._token and now < self._token_expires_at - 60:
                return self._token
            response = await self._client.post(
                f"{self._settings.feishu_api_base_url}/open-apis/auth/v3/tenant_access_token/internal",
                json={
                    "app_id": self._settings.feishu_app_id,
                    "app_secret": self._settings.feishu_app_secret,
                },
            )
            self._raise_feishu_error(response, "get tenant access token")
            data = response.json()
            self._token = str(data["tenant_access_token"])
            self._token_expires_at = now + int(data.get("expire", 7200))
            return self._token

    @staticmethod
    def _raise_feishu_error(response: httpx.Response, action: str) -> None:
        try:
            data = response.json()
        except ValueError:
            data = {}
        code = data.get("code")
        if response.is_error or (code not in (None, 0)):
            raise ChannelDeliveryError(
                f"Feishu {action} failed: status={response.status_code} code={code} "
                f"message={data.get('msg') or data.get('message') or response.text[:200]}"
            )

    def _build_select_card(self, item: OutboundItem, session_id: str) -> dict[str, Any]:
        payload = item.payload
        question = str(payload.get("question") or "请选择")
        options = payload.get("options") if isinstance(payload.get("options"), list) else []
        actions = []
        for option in options[:6]:
            if isinstance(option, dict):
                label = str(option.get("label") or option.get("value") or "")
                value = str(option.get("value") or label)
            else:
                label = value = str(option)
            if not label:
                continue
            actions.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": label},
                    "type": "default",
                    "value": {
                        "action": "select",
                        "value": value,
                        "session_id": session_id,
                    },
                }
            )
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "blue",
                "title": {"tag": "plain_text", "content": question},
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": question}},
                {"tag": "action", "actions": actions},
            ],
        }

    def _build_payload_card(self, item: OutboundItem, session_id: str) -> dict[str, Any]:
        payload = item.payload
        title = str(payload.get("title") or "消息")
        body = str(payload.get("body") or item.text or "")
        buttons = payload.get("buttons") if isinstance(payload.get("buttons"), list) else []
        elements: list[dict[str, Any]] = [
            {"tag": "div", "text": {"tag": "lark_md", "content": body}}
        ]
        actions = []
        for button in buttons[:4]:
            label = str(button)
            actions.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": label},
                    "type": "default",
                    "value": {
                        "action": "card_button",
                        "value": label,
                        "session_id": session_id,
                    },
                }
            )
        if actions:
            elements.append({"tag": "action", "actions": actions})
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "blue",
                "title": {"tag": "plain_text", "content": title},
            },
            "elements": elements,
        }


def _extract_feishu_text(message: dict[str, Any]) -> str:
    message_type = str(message.get("message_type") or "")
    try:
        content = json.loads(message.get("content") or "{}")
    except json.JSONDecodeError:
        return ""

    if message_type == "text":
        text = str(content.get("text") or "")
    elif message_type == "post":
        text = _extract_post_text(content)
    else:
        return ""
    return _MENTION_RE.sub("", text).strip()


def _extract_post_text(content: dict[str, Any]) -> str:
    locale = next((value for value in content.values() if isinstance(value, dict)), {})
    lines: list[str] = []
    for line in locale.get("content", []):
        parts: list[str] = []
        for block in line:
            if isinstance(block, dict) and block.get("tag") == "text":
                parts.append(str(block.get("text") or ""))
        if parts:
            lines.append("".join(parts))
    return "\n".join(lines)


def _parse_action_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _extract_card_selection(action: dict[str, Any], value: Any) -> str:
    if isinstance(value, dict):
        selected = value.get("value") or value.get("label")
        if selected:
            return str(selected)
    elif value:
        return str(value)
    for key in ("option", "options", "input_value"):
        selected = action.get(key)
        if selected:
            return str(selected)
    return ""


__all__ = ["FeishuAdapter"]

