from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from ..models import ChannelName, ChatResponse


class ChannelAuthError(Exception):
    """The incoming request failed channel signature/token verification."""


class ChannelPayloadError(Exception):
    """The incoming request is authenticated but malformed."""


class ChannelDeliveryError(Exception):
    """An outbound channel API call failed."""


@dataclass(frozen=True, slots=True)
class ChannelHttpRequest:
    method: str
    headers: dict[str, str]
    query: dict[str, str]
    body: bytes


@dataclass(frozen=True, slots=True)
class InboundMessage:
    channel: ChannelName
    tenant_id: str
    user_id: str
    session_key: str
    text: str
    event_id: str | None = None
    session_id: str | None = None
    reply: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ChannelReceiveResult:
    message: InboundMessage | None = None
    response: Any = None
    status_code: int = 200
    media_type: str = "application/json"
    process_async: bool = True


class ChannelAdapter(Protocol):
    name: ChannelName

    async def receive(self, request: ChannelHttpRequest) -> ChannelReceiveResult: ...

    async def send(self, inbound: InboundMessage, response: ChatResponse) -> None: ...

    async def close(self) -> None: ...


def render_as_text(text: str | None, payload: dict[str, Any]) -> str:
    """Render a channel-agnostic action for channels without rich cards."""

    if text:
        return text
    title = str(payload.get("title") or payload.get("question") or "").strip()
    body = str(payload.get("body") or "").strip()
    options = payload.get("options")
    buttons = payload.get("buttons")

    lines: list[str] = []
    if title:
        lines.append(title)
    if body:
        lines.append(body)
    values: list[str] = []
    if isinstance(options, list):
        for option in options:
            if isinstance(option, dict):
                value = option.get("label") or option.get("value")
            else:
                value = option
            if value:
                values.append(str(value))
    if isinstance(buttons, list):
        values.extend(str(item) for item in buttons if item)
    if values:
        lines.extend(f"{index}. {value}" for index, value in enumerate(values, start=1))
        lines.append("直接回复序号或内容即可。")
    return "\n".join(lines).strip() or "请稍等，我继续帮你处理。"

