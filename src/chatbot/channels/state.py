from __future__ import annotations

import asyncio
import time
from typing import Protocol

import redis.asyncio as redis

from ..config import Settings


class ChannelBindingStore(Protocol):
    async def get(self, tenant_id: str, channel: str, session_key: str) -> str | None: ...

    async def set(
        self, tenant_id: str, channel: str, session_key: str, session_id: str
    ) -> None: ...

    async def delete(self, tenant_id: str, channel: str, session_key: str) -> None: ...

    async def close(self) -> None: ...


class EventDeduplicator(Protocol):
    async def first_seen(self, key: str, ttl_seconds: int) -> bool: ...

    async def close(self) -> None: ...


class MemoryChannelBindingStore:
    def __init__(self) -> None:
        self._values: dict[tuple[str, str, str], str] = {}
        self._lock = asyncio.Lock()

    async def get(self, tenant_id: str, channel: str, session_key: str) -> str | None:
        async with self._lock:
            return self._values.get((tenant_id, channel, session_key))

    async def set(
        self, tenant_id: str, channel: str, session_key: str, session_id: str
    ) -> None:
        async with self._lock:
            self._values[(tenant_id, channel, session_key)] = session_id

    async def delete(self, tenant_id: str, channel: str, session_key: str) -> None:
        async with self._lock:
            self._values.pop((tenant_id, channel, session_key), None)

    async def close(self) -> None:
        return None


class RedisChannelBindingStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = redis.Redis.from_url(settings.redis_url, decode_responses=True)

    def _key(self, tenant_id: str, channel: str, session_key: str) -> str:
        return f"{self._settings.redis_key_prefix}:{tenant_id}:binding:{channel}:{session_key}"

    async def get(self, tenant_id: str, channel: str, session_key: str) -> str | None:
        return await self._client.get(self._key(tenant_id, channel, session_key))

    async def set(
        self, tenant_id: str, channel: str, session_key: str, session_id: str
    ) -> None:
        await self._client.set(
            self._key(tenant_id, channel, session_key),
            session_id,
            ex=self._settings.session_ttl_seconds,
        )

    async def delete(self, tenant_id: str, channel: str, session_key: str) -> None:
        await self._client.delete(self._key(tenant_id, channel, session_key))

    async def close(self) -> None:
        await self._client.aclose()


class MemoryEventDeduplicator:
    def __init__(self) -> None:
        self._seen: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def first_seen(self, key: str, ttl_seconds: int) -> bool:
        now = time.monotonic()
        async with self._lock:
            expired = [item for item, deadline in self._seen.items() if deadline <= now]
            for item in expired:
                self._seen.pop(item, None)
            if key in self._seen:
                return False
            self._seen[key] = now + ttl_seconds
            return True

    async def close(self) -> None:
        return None


class RedisEventDeduplicator:
    def __init__(self, settings: Settings) -> None:
        self._client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        self._prefix = f"{settings.redis_key_prefix}:event"

    async def first_seen(self, key: str, ttl_seconds: int) -> bool:
        created = await self._client.set(f"{self._prefix}:{key}", "1", ex=ttl_seconds, nx=True)
        return bool(created)

    async def close(self) -> None:
        await self._client.aclose()


def build_channel_binding_store(settings: Settings) -> ChannelBindingStore:
    if settings.session_backend == "memory":
        return MemoryChannelBindingStore()
    return RedisChannelBindingStore(settings)


def build_event_deduplicator(settings: Settings) -> EventDeduplicator:
    if settings.session_backend == "memory":
        return MemoryEventDeduplicator()
    return RedisEventDeduplicator(settings)


__all__ = [
    "ChannelBindingStore",
    "EventDeduplicator",
    "MemoryChannelBindingStore",
    "MemoryEventDeduplicator",
    "RedisChannelBindingStore",
    "RedisEventDeduplicator",
    "build_channel_binding_store",
    "build_event_deduplicator",
]

