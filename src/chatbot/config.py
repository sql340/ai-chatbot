from __future__ import annotations

import os
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. All values can be overridden through .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "AI Customer Service"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    cors_origins: str = ""

    llm_base_url: str = Field(
        default_factory=lambda: os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    )
    llm_api_key: str = Field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    llm_model: str = "gpt-4.1-mini"
    llm_temperature: float = 0.3
    llm_max_tokens: int = 800
    llm_timeout: float = 60.0
    llm_max_retries: int = 2

    quality_llm_base_url: str = ""
    quality_llm_api_key: str = ""
    quality_llm_model: str = ""
    quality_llm_temperature: float = 0.0

    embedding_base_url: str = ""
    embedding_api_key: str = ""
    embedding_model: str = ""

    session_backend: Literal["redis", "memory"] = "redis"
    redis_url: str = "redis://localhost:6379/0"
    redis_key_prefix: str = "ai-chatbot"
    session_ttl_seconds: int = 1800
    session_lock_ttl_ms: int = 120_000
    session_lock_wait_seconds: float = 2.0

    max_turns: int = 12
    max_history_messages: int = 30
    reply_max_chars: int = 500

    rag_enabled: bool = False
    rag_base_url: str = "http://localhost:8080"
    rag_timeout: float = 30.0
    rag_top_k: int = 4
    rag_tenant_field: str = "tenant_id"
    rag_score_threshold: float | None = None

    decision_engine: Literal["disabled", "jev"] = "disabled"
    jev_base_url: str = ""
    jev_api_key: str = ""
    jev_model: str = ""

    channel_event_dedupe_ttl_seconds: int = 3600
    web_enabled: bool = True

    feishu_enabled: bool = False
    feishu_tenant_id: str = "default"
    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_encrypt_key: str = ""
    feishu_verification_token: str = ""
    feishu_api_base_url: str = "https://open.feishu.cn"
    feishu_timeout: float = 10.0

    qq_enabled: bool = False
    qq_tenant_id: str = "default"
    qq_onebot_api_base_url: str = "http://localhost:3000"
    qq_onebot_access_token: str = ""
    qq_timeout: float = 10.0

    wechat_enabled: bool = False
    wechat_tenant_id: str = "default"
    wechat_token: str = ""
    wechat_encoding_aes_key: str = ""
    wechat_app_id: str = ""
    wechat_app_secret: str = ""
    wechat_api_base_url: str = "https://api.weixin.qq.com"
    wechat_timeout: float = 10.0

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @field_validator(
        "llm_base_url",
        "rag_base_url",
        "feishu_api_base_url",
        "qq_onebot_api_base_url",
        "wechat_api_base_url",
    )
    @classmethod
    def _strip_trailing_slash(cls, value: str) -> str:
        return value.rstrip("/")

    @field_validator(
        "session_ttl_seconds",
        "session_lock_ttl_ms",
        "max_turns",
        "max_history_messages",
        "reply_max_chars",
        "llm_max_tokens",
        "rag_top_k",
        "channel_event_dedupe_ttl_seconds",
    )
    @classmethod
    def _must_be_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be greater than 0")
        return value

    @field_validator("session_lock_wait_seconds")
    @classmethod
    def _non_negative_wait(cls, value: float) -> float:
        if value < 0:
            raise ValueError("must be greater than or equal to 0")
        return value

    @field_validator("rag_tenant_field")
    @classmethod
    def _tenant_field_required_when_rag_enabled(cls, value: str) -> str:
        return value.strip()

    @field_validator("rag_score_threshold", mode="before")
    @classmethod
    def _empty_threshold_is_none(cls, value: object) -> object:
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return value


__all__ = ["Settings"]

