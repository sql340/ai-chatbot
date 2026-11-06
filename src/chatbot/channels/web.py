from __future__ import annotations

import json

from pydantic import ValidationError

from ..models import ChatRequest, ChatResponse
from .base import (
    ChannelHttpRequest,
    ChannelPayloadError,
    ChannelReceiveResult,
    InboundMessage,
)


class WebAdapter:
    """Browser/web-component adapter.

    Authentication is intentionally delegated to the web gateway. It must
    overwrite tenant_id/user_id before forwarding requests to this service.
    """

    name = "web"  # type: ignore[assignment]

    async def receive(self, request: ChannelHttpRequest) -> ChannelReceiveResult:
        try:
            payload = ChatRequest.model_validate_json(request.body)
        except (ValidationError, json.JSONDecodeError) as exc:
            raise ChannelPayloadError("invalid web chat payload") from exc
        return ChannelReceiveResult(
            message=self.from_chat_request(payload),
            process_async=False,
        )

    def from_chat_request(self, payload: ChatRequest) -> InboundMessage:
        return InboundMessage(
            channel="web",
            tenant_id=payload.tenant_id,
            user_id=payload.user_id,
            session_key=payload.user_id,
            text=payload.message,
            session_id=payload.session_id,
        )

    async def send(self, inbound: InboundMessage, response: ChatResponse) -> None:
        return None

    async def close(self) -> None:
        return None


__all__ = ["WebAdapter"]

