from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol

import redis.asyncio as redis

from .config import Settings


class SessionBusyError(Exception):
    pass


class SessionLockManager(Protocol):
    def acquire(self, tenant_id: str, session_id: str) -> AsyncIterator[None]: ...

    async def close(self) -> None: ...


class MemoryLockManager:
    def __init__(self, wait_seconds: float) -> None:
        self._wait_seconds = wait_seconds
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._guard = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, tenant_id: str, session_id: str) -> AsyncIterator[None]:
        key = (tenant_id, session_id)
        async with self._guard:
            lock = self._locks.setdefault(key, asyncio.Lock())
        try:
            await asyncio.wait_for(lock.acquire(), timeout=self._wait_seconds)
        except TimeoutError as exc:
            raise SessionBusyError(session_id) from exc
        try:
            yield
        finally:
            lock.release()

    async def close(self) -> None:
        return None


class RedisLockManager:
    _RELEASE_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
      return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = redis.Redis.from_url(settings.redis_url, decode_responses=True)

    def _key(self, tenant_id: str, session_id: str) -> str:
        return f"{self._settings.redis_key_prefix}:{tenant_id}:lock:{session_id}"

    @asynccontextmanager
    async def acquire(self, tenant_id: str, session_id: str) -> AsyncIterator[None]:
        key = self._key(tenant_id, session_id)
        token = uuid.uuid4().hex
        deadline = asyncio.get_running_loop().time() + self._settings.session_lock_wait_seconds

        while True:
            acquired = await self._client.set(
                key,
                token,
                nx=True,
                px=self._settings.session_lock_ttl_ms,
            )
            if acquired:
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise SessionBusyError(session_id)
            await asyncio.sleep(0.05)

        try:
            yield
        finally:
            await self._client.eval(self._RELEASE_SCRIPT, 1, key, token)

    async def close(self) -> None:
        await self._client.aclose()


def build_lock_manager(settings: Settings) -> SessionLockManager:
    if settings.session_backend == "memory":
        return MemoryLockManager(settings.session_lock_wait_seconds)
    return RedisLockManager(settings)


__all__ = [
    "MemoryLockManager",
    "RedisLockManager",
    "SessionBusyError",
    "SessionLockManager",
    "build_lock_manager",
]

