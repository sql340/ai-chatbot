from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

ChannelName = Literal["web", "feishu", "wechat", "qq", "dingtalk"]
SessionStatus = Literal["active", "handed_off", "closed"]
MessageRole = Literal["user", "assistant"]
OutboundKind = Literal["text", "card", "select", "handoff"]


class StoredMessage(BaseModel):
    role: MessageRole
    content: str


class SessionRecord(BaseModel):
    id: str
    tenant_id: str
    user_id: str
    channel: ChannelName
    status: SessionStatus = "active"
    turn_count: int = 0
    messages: list[StoredMessage] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ChatRequest(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    user_id: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=8000)
    session_id: str | None = Field(
        default=None,
        max_length=64,
        pattern=r"^[A-Za-z0-9_-]+$",
    )
    channel: ChannelName = "web"

    @field_validator("message")
    @classmethod
    def _trim_message(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("message must not be blank")
        return value


class OutboundItem(BaseModel):
    kind: OutboundKind
    text: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ChatResponse(BaseModel):
    session_id: str
    turn_no: int
    status: SessionStatus
    messages: list[OutboundItem] = Field(default_factory=list)


class SessionView(BaseModel):
    session_id: str
    tenant_id: str
    user_id: str
    channel: ChannelName
    status: SessionStatus
    turn_count: int
    messages: list[StoredMessage]
    created_at: datetime
    updated_at: datetime


__all__ = [
    "ChannelName",
    "ChatRequest",
    "ChatResponse",
    "OutboundItem",
    "SessionRecord",
    "SessionStatus",
    "SessionView",
    "StoredMessage",
]

