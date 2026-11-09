from __future__ import annotations

import hashlib
import logging

from ..config import Settings
from ..models import ChatRequest, ChatResponse, OutboundItem
from ..service import (
    AgentInvocationError,
    ChatService,
    GenerationSupersededError,
    SessionClosedError,
    SessionForbiddenError,
)
from ..store import SessionNotFoundError
from .base import ChannelAdapter, InboundMessage
from .state import ChannelBindingStore, EventDeduplicator

logger = logging.getLogger(__name__)


class ChannelDispatcher:
    def __init__(
        self,
        *,
        service: ChatService,
        bindings: ChannelBindingStore,
        deduplicator: EventDeduplicator,
        settings: Settings,
    ) -> None:
        self.service = service
        self.bindings = bindings
        self.deduplicator = deduplicator
        self.settings = settings

    async def handle(self, inbound: InboundMessage) -> ChatResponse | None:
        if inbound.event_id:
            event_key = f"{inbound.channel}:{inbound.tenant_id}:{inbound.event_id}"
            if not await self.deduplicator.first_seen(
                event_key, self.settings.channel_event_dedupe_ttl_seconds
            ):
                logger.info("duplicate channel event ignored: %s", event_key)
                return None

        session_id = await self._resolve_session(inbound)
        request = ChatRequest(
            tenant_id=inbound.tenant_id,
            user_id=inbound.user_id,
            message=inbound.text,
            session_id=session_id,
            channel=inbound.channel,
        )

        try:
            response = await self.service.chat(request)
        except SessionForbiddenError:
            raise
        except (SessionClosedError, SessionNotFoundError):
            if session_id is None:
                raise
            logger.info("channel session %s is no longer reusable; creating a new one", session_id)
            session_id = await self._resolve_session(inbound, reset_closed=True)
            response = await self.service.chat(
                request.model_copy(update={"session_id": session_id})
            )

        await self.bindings.set(
            inbound.tenant_id,
            inbound.channel,
            inbound.session_key,
            response.session_id,
        )
        return response

    async def _resolve_session(
        self, inbound: InboundMessage, *, reset_closed: bool = False
    ) -> str:
        if inbound.session_id and not reset_closed:
            return inbound.session_id

        lock_id = self._conversation_lock_id(inbound)
        async with self.service.locks.acquire(inbound.tenant_id, lock_id):
            session_id = await self.bindings.get(
                inbound.tenant_id, inbound.channel, inbound.session_key
            )
            if session_id:
                try:
                    session = await self.service.get_session(
                        inbound.tenant_id, inbound.user_id, session_id
                    )
                except SessionNotFoundError:
                    await self.bindings.delete(
                        inbound.tenant_id, inbound.channel, inbound.session_key
                    )
                else:
                    if session.status == "active" or not reset_closed:
                        return session.id
                    await self.bindings.delete(
                        inbound.tenant_id, inbound.channel, inbound.session_key
                    )

            session = await self.service.create_session(
                tenant_id=inbound.tenant_id,
                user_id=inbound.user_id,
                channel=inbound.channel,
            )
            await self.bindings.set(
                inbound.tenant_id,
                inbound.channel,
                inbound.session_key,
                session.id,
            )
            return session.id

    @staticmethod
    def _conversation_lock_id(inbound: InboundMessage) -> str:
        digest = hashlib.sha256(
            f"{inbound.channel}:{inbound.session_key}".encode()
        ).hexdigest()[:32]
        return f"conversation-{digest}"

    async def process_and_send(self, adapter: ChannelAdapter, inbound: InboundMessage) -> None:
        try:
            response = await self.handle(inbound)
            if response is None:
                return
            if not await self.service.is_current(
                inbound.tenant_id, response.session_id, response.epoch
            ):
                logger.info(
                    "dropping stale response for %s session %s epoch %s",
                    inbound.channel,
                    response.session_id,
                    response.epoch,
                )
                return
            await adapter.send(inbound, response)
        except GenerationSupersededError:
            logger.info("generation superseded for channel %s", inbound.channel)
            return
        except AgentInvocationError:
            logger.exception("agent failed for channel %s", inbound.channel)
            await self._send_fallback(adapter, inbound, "客服暂时无法回复，请稍后再试。")
        except Exception:
            logger.exception("channel processing failed for %s", inbound.channel)
            await self._send_fallback(adapter, inbound, "消息处理失败，请稍后再试或联系人工客服。")

    async def _send_fallback(
        self, adapter: ChannelAdapter, inbound: InboundMessage, text: str
    ) -> None:
        try:
            await adapter.send(
                inbound,
                ChatResponse(
                    session_id=inbound.session_id or "",
                    turn_no=0,
                    status="active",
                    messages=[OutboundItem(kind="text", text=text)],
                ),
            )
        except Exception:
            logger.exception("failed to send channel fallback message")


__all__ = ["ChannelDispatcher"]

