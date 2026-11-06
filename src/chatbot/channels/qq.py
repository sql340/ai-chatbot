from __future__ import annotations

import json
import re
from typing import Any

import httpx

from ..config import Settings
from ..models import ChatResponse
from .base import (
    ChannelAuthError,
    ChannelDeliveryError,
    ChannelHttpRequest,
    ChannelPayloadError,
    ChannelReceiveResult,
    InboundMessage,
    render_as_text,
)

_CQ_CODE_RE = re.compile(r"\[CQ:[^\]]+\]")


class QQOneBotAdapter:
    """QQ adapter for the OneBot 11 HTTP event/reply protocol."""

    name = "qq"  # type: ignore[assignment]

    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._client = client or httpx.AsyncClient(timeout=settings.qq_timeout)
        self._owns_client = client is None

    async def receive(self, request: ChannelHttpRequest) -> ChannelReceiveResult:
        self._verify_token(request)
        try:
            payload = json.loads(request.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelPayloadError("invalid QQ OneBot JSON payload") from exc

        message = self._parse_message(payload)
        return ChannelReceiveResult(message=message, response={"status": "ok"})

    async def send(self, inbound: InboundMessage, response: ChatResponse) -> None:
        for item in response.messages:
            text = render_as_text(item.text, item.payload)
            if not text.strip():
                continue
            if inbound.reply.get("group_id") is not None:
                await self._call_api(
                    "send_group_msg",
                    {"group_id": inbound.reply["group_id"], "message": text, "auto_escape": True},
                )
            else:
                await self._call_api(
                    "send_private_msg",
                    {"user_id": inbound.user_id, "message": text, "auto_escape": True},
                )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _verify_token(self, request: ChannelHttpRequest) -> None:
        expected = self._settings.qq_onebot_access_token
        if not expected:
            return
        authorization = request.headers.get("authorization", "")
        token = authorization.removeprefix("Bearer ").strip()
        if token != expected:
            raise ChannelAuthError("invalid QQ OneBot access token")

    def _parse_message(self, payload: dict[str, Any]) -> InboundMessage | None:
        if payload.get("post_type") != "message":
            return None
        user_id = str(payload.get("user_id") or "")
        self_id = str(payload.get("self_id") or "")
        if not user_id or user_id == self_id:
            return None

        text = _extract_onebot_text(payload).strip()
        if not text:
            return None

        message_type = str(payload.get("message_type") or "private")
        group_id = payload.get("group_id")
        if message_type == "group" and group_id is not None:
            session_key = f"group:{group_id}:{user_id}"
            reply: dict[str, Any] = {"group_id": group_id}
        else:
            session_key = f"private:{user_id}"
            reply = {"user_id": user_id}

        return InboundMessage(
            channel="qq",
            tenant_id=self._settings.qq_tenant_id,
            user_id=user_id,
            session_key=session_key,
            text=text,
            event_id=str(payload.get("message_id") or payload.get("time") or "") or None,
            reply=reply,
            raw=payload,
        )

    async def _call_api(self, action: str, payload: dict[str, Any]) -> None:
        headers: dict[str, str] = {}
        if self._settings.qq_onebot_access_token:
            headers["Authorization"] = f"Bearer {self._settings.qq_onebot_access_token}"
        response = await self._client.post(
            f"{self._settings.qq_onebot_api_base_url}/{action}",
            headers=headers,
            json=payload,
        )
        if response.is_error:
            raise ChannelDeliveryError(
                f"QQ OneBot {action} failed: status={response.status_code} "
                f"body={response.text[:300]}"
            )
        try:
            data = response.json()
        except ValueError:
            return
        if data.get("status") == "failed" or data.get("retcode") not in (None, 0):
            raise ChannelDeliveryError(
                f"QQ OneBot {action} failed: retcode={data.get('retcode')} "
                f"message={data.get('message') or data.get('wording')}"
            )


def _extract_onebot_text(payload: dict[str, Any]) -> str:
    message = payload.get("message")
    if isinstance(message, list):
        parts = [
            str(segment.get("data", {}).get("text") or "")
            for segment in message
            if isinstance(segment, dict) and segment.get("type") == "text"
        ]
        if parts:
            return "".join(parts)
    raw_message = payload.get("raw_message")
    if isinstance(raw_message, str):
        return _CQ_CODE_RE.sub("", raw_message)
    return ""


__all__ = ["QQOneBotAdapter"]

