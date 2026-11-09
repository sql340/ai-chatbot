from __future__ import annotations

import asyncio
from typing import Protocol

import redis.asyncio as redis
from redis.exceptions import WatchError

from .config import Settings
from .models import SessionRecord


class SessionNotFoundError(Exception):
    pass


class SessionStore(Protocol):
    async def create(self, session: SessionRecord) -> None: ...

    async def get(self, tenant_id: str, session_id: str) -> SessionRecord: ...

    async def save_if_revision(self, session: SessionRecord, expected_revision: int) -> bool: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...


class MemorySessionStore:
    def __init__(self) -> None:
        self._sessions: dict[tuple[str, str], SessionRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, session: SessionRecord) -> None:
        async with self._lock:
            key = (session.tenant_id, session.id)
            if key in self._sessions:
                raise ValueError("session already exists")
            self._sessions[key] = session.model_copy(deep=True)

    async def get(self, tenant_id: str, session_id: str) -> SessionRecord:
        async with self._lock:
            session = self._sessions.get((tenant_id, session_id))
            if session is None:
                raise SessionNotFoundError(session_id)
            return session.model_copy(deep=True)

    async def save_if_revision(self, session: SessionRecord, expected_revision: int) -> bool:
        async with self._lock:
            key = (session.tenant_id, session.id)
            current = self._sessions.get(key)
            if current is None or current.revision != expected_revision:
                return False
            self._sessions[key] = session.model_copy(deep=True)
            return True

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None


class RedisSessionStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = redis.Redis.from_url(settings.redis_url, decode_responses=True)

    def _key(self, tenant_id: str, session_id: str) -> str:
        return f"{self._settings.redis_key_prefix}:{tenant_id}:session:{session_id}"

    async def create(self, session: SessionRecord) -> None:
        created = await self._client.set(
            self._key(session.tenant_id, session.id),
            session.model_dump_json(),
            ex=self._settings.session_ttl_seconds,
            nx=True,
        )
        if not created:
            raise ValueError("session already exists")

    async def get(self, tenant_id: str, session_id: str) -> SessionRecord:
        raw = await self._client.get(self._key(tenant_id, session_id))
        if raw is None:
            raise SessionNotFoundError(session_id)
        return SessionRecord.model_validate_json(raw)

    async def save_if_revision(self, session: SessionRecord, expected_revision: int) -> bool:
        key = self._key(session.tenant_id, session.id)
        async with self._client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(key)
                raw = await pipe.get(key)
                if raw is None:
                    await pipe.unwatch()
                    return False
                current = SessionRecord.model_validate_json(raw)
                if current.revision != expected_revision:
                    await pipe.unwatch()
                    return False
                pipe.multi()
                pipe.set(
                    key,
                    session.model_dump_json(),
                    ex=self._settings.session_ttl_seconds,
                )
                await pipe.execute()
                return True
            except WatchError:
                return False

    async def ping(self) -> bool:
        return bool(await self._client.ping())

    async def close(self) -> None:
        await self._client.aclose()


def build_session_store(settings: Settings) -> SessionStore:
    if settings.session_backend == "memory":
        return MemorySessionStore()
    return RedisSessionStore(settings)


__all__ = [
    "MemorySessionStore",
    "RedisSessionStore",
    "SessionNotFoundError",
    "SessionStore",
    "build_session_store",
]

