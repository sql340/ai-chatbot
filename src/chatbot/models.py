from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

ChannelName = Literal["web", "feishu", "wechat", "qq", "dingtalk"]
SessionStatus = Literal["active", "handoff_pending", "human_active", "solved", "closed"]
MessageRole = Literal["user", "assistant"]
OutboundKind = Literal["text", "card", "select", "handoff"]


class StoredMessage(BaseModel):
    role: MessageRole
    content: str


class Satisfaction(BaseModel):
    score: int = Field(ge=1, le=5)
    comment: str | None = Field(default=None, max_length=2000)
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SessionRecord(BaseModel):
    id: str
    tenant_id: str
    user_id: str
    channel: ChannelName
    status: SessionStatus = "active"
    status_reason: str | None = None
    turn_count: int = 0
    active_epoch: int = 0
    revision: int = 0
    messages: list[StoredMessage] = Field(default_factory=list)
    parent_session_id: str | None = None
    reopened_count: int = 0
    last_activity_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    solved_at: datetime | None = None
    closed_at: datetime | None = None
    close_reason: str | None = None
    resolution_summary: str | None = Field(default=None, max_length=4000)
    handoff_requested_at: datetime | None = None
    handoff_reason: str | None = None
    satisfaction_prompted_at: datetime | None = None
    satisfaction: Satisfaction | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("status", mode="before")
    @classmethod
    def _migrate_legacy_status(cls, value: object) -> object:
        if value == "handed_off":
            return "handoff_pending"
        return value


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
    epoch: int = 0
    status: SessionStatus
    messages: list[OutboundItem] = Field(default_factory=list)


class SessionCloseRequest(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    user_id: str = Field(min_length=1, max_length=128)
    reason: str = Field(default="user_ended", min_length=1, max_length=200)


class SatisfactionRequest(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    user_id: str = Field(min_length=1, max_length=128)
    score: int = Field(ge=1, le=5)
    comment: str | None = Field(default=None, max_length=2000)


class SessionView(BaseModel):
    session_id: str
    tenant_id: str
    user_id: str
    channel: ChannelName
    status: SessionStatus
    status_reason: str | None
    turn_count: int
    active_epoch: int
    messages: list[StoredMessage]
    parent_session_id: str | None
    reopened_count: int
    last_activity_at: datetime
    solved_at: datetime | None
    closed_at: datetime | None
    close_reason: str | None
    resolution_summary: str | None
    handoff_requested_at: datetime | None
    handoff_reason: str | None
    satisfaction_prompted_at: datetime | None
    satisfaction: Satisfaction | None
    created_at: datetime
    updated_at: datetime


__all__ = [
    "ChannelName",
    "ChatRequest",
    "ChatResponse",
    "OutboundItem",
    "Satisfaction",
    "SatisfactionRequest",
    "SessionCloseRequest",
    "SessionRecord",
    "SessionStatus",
    "SessionView",
    "StoredMessage",
]

