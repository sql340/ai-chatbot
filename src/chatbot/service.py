from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage
from langgraph.graph.state import CompiledStateGraph

from .config import Settings
from .knowledge import KnowledgeProvider
from .locks import SessionBusyError, SessionLockManager
from .models import (
    ChatRequest,
    ChatResponse,
    OutboundItem,
    SessionRecord,
    SessionStatus,
    StoredMessage,
)
from .store import SessionNotFoundError, SessionStore
from .text import content_to_text, split_reply
from .tools import AgentContext

logger = logging.getLogger(__name__)


class SessionForbiddenError(Exception):
    pass


class SessionClosedError(Exception):
    pass


class AgentInvocationError(Exception):
    pass


class GenerationSupersededError(Exception):
    def __init__(self, session_id: str, epoch: int) -> None:
        super().__init__(f"generation {epoch} for session {session_id} was superseded")
        self.session_id = session_id
        self.epoch = epoch


@dataclass
class GenerationHandle:
    tenant_id: str
    session_id: str
    epoch: int
    turn_no: int
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[Any] | None = None


class ChatService:
    def __init__(
        self,
        *,
        settings: Settings,
        agent: CompiledStateGraph,
        store: SessionStore,
        locks: SessionLockManager,
        knowledge: KnowledgeProvider | None = None,
    ) -> None:
        self.settings = settings
        self.agent = agent
        self.store = store
        self.locks = locks
        self.knowledge = knowledge
        self._generations: dict[str, GenerationHandle] = {}

    async def create_session(
        self,
        *,
        tenant_id: str,
        user_id: str,
        channel: str,
        session_id: str | None = None,
    ) -> SessionRecord:
        session = SessionRecord(
            id=session_id or str(uuid.uuid4()),
            tenant_id=tenant_id,
            user_id=user_id,
            channel=channel,  # type: ignore[arg-type]
        )
        await self.store.create(session)
        return session

    async def chat(self, request: ChatRequest) -> ChatResponse:
        session_id = request.session_id or str(uuid.uuid4())
        await self._precheck_existing(request, session_id)
        if request.session_id is None:
            await self.create_session(
                tenant_id=request.tenant_id,
                user_id=request.user_id,
                channel=request.channel,
                session_id=session_id,
            )

        session, handle, turn_no = await self._begin_turn(request, session_id)
        if handle is None:
            return await self._force_handoff(session, turn_no)

        try:
            await self._ensure_current(handle)
            messages = self._build_messages(session)
            context = self._agent_context(session)
            task = asyncio.create_task(
                self.agent.ainvoke(
                    {"messages": messages},
                    context=context,
                    config={"recursion_limit": 40},
                )
            )
            handle.task = task
            try:
                result = await task
            except asyncio.CancelledError:
                if handle.cancel_event.is_set():
                    raise GenerationSupersededError(handle.session_id, handle.epoch) from None
                raise
            except Exception as exc:
                logger.exception("agent invocation failed for session %s", session_id)
                raise AgentInvocationError(str(exc)) from exc

            await self._ensure_current(handle)
            text = self._last_ai_text(result)
            items = self._finalize_items(text, context, turn_no)
            status: SessionStatus = (
                "handed_off"
                if context.handoff_requested or turn_no >= self.settings.max_turns
                else "active"
            )
            await self._commit_turn(handle, text, status)
            return ChatResponse(
                session_id=session.id,
                turn_no=turn_no,
                epoch=handle.epoch,
                status=status,
                messages=items,
            )
        except asyncio.CancelledError:
            if handle.cancel_event.is_set():
                raise GenerationSupersededError(handle.session_id, handle.epoch) from None
            raise
        finally:
            self._finish_generation(handle)

    async def stream(self, request: ChatRequest) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        session_id = request.session_id or str(uuid.uuid4())
        handle: GenerationHandle | None = None
        try:
            await self._precheck_existing(request, session_id)
            if request.session_id is None:
                await self.create_session(
                    tenant_id=request.tenant_id,
                    user_id=request.user_id,
                    channel=request.channel,
                    session_id=session_id,
                )

            session, handle, turn_no = await self._begin_turn(request, session_id)
            if handle is None:
                response = await self._force_handoff(session, turn_no)
                yield "session", {
                    "session_id": response.session_id,
                    "turn_no": response.turn_no,
                    "epoch": response.epoch,
                }
                for item in response.messages:
                    yield "action", item.model_dump(mode="json")
                yield "done", {
                    "session_id": response.session_id,
                    "turn_no": response.turn_no,
                    "epoch": response.epoch,
                    "status": response.status,
                }
                return

            yield "session", {
                "session_id": session.id,
                "turn_no": turn_no,
                "epoch": handle.epoch,
            }

            messages = self._build_messages(session)
            context = self._agent_context(session)
            streamed_text = ""
            final_state: dict[str, Any] | None = None
            try:
                async for mode, payload in self.agent.astream(
                    {"messages": messages},
                    context=context,
                    config={"recursion_limit": 40},
                    stream_mode=["messages", "values"],
                ):
                    await self._ensure_current(handle)
                    if mode == "messages":
                        chunk, _metadata = payload
                        if isinstance(chunk, AIMessage):
                            text_delta = content_to_text(chunk.content)
                            if text_delta:
                                streamed_text += text_delta
                                yield "delta", {
                                    "content": text_delta,
                                    "epoch": handle.epoch,
                                }
                    elif mode == "values":
                        final_state = payload
                await self._ensure_current(handle)
            except GenerationSupersededError:
                yield "error", {
                    "code": "superseded",
                    "message": "收到新消息，已按最新消息重新处理。",
                }
                yield "done", {
                    "session_id": handle.session_id,
                    "turn_no": handle.turn_no,
                    "epoch": handle.epoch,
                    "status": "active",
                }
                return
            except asyncio.CancelledError:
                if handle.cancel_event.is_set():
                    yield "error", {
                        "code": "superseded",
                        "message": "收到新消息，已按最新消息重新处理。",
                    }
                    yield "done", {
                        "session_id": handle.session_id,
                        "turn_no": handle.turn_no,
                        "epoch": handle.epoch,
                        "status": "active",
                    }
                    return
                logger.info("stream cancelled for session %s", session.id)
                raise
            except Exception:
                logger.exception("agent stream failed for session %s", session.id)
                yield "error", {
                    "code": "agent_error",
                    "message": "客服暂时无法回复，请稍后重试。",
                }
                yield "done", {
                    "session_id": session.id,
                    "turn_no": turn_no,
                    "epoch": handle.epoch,
                    "status": session.status,
                }
                return

            canonical_text = self._last_ai_text(final_state) or streamed_text
            items = self._finalize_items(canonical_text, context, turn_no)
            status: SessionStatus = (
                "handed_off"
                if context.handoff_requested or turn_no >= self.settings.max_turns
                else "active"
            )

            for item in items:
                await self._ensure_current(handle)
                event = "message" if item.kind == "text" else "action"
                yield event, item.model_dump(mode="json")

            await self._commit_turn(handle, canonical_text, status)
            yield "done", {
                "session_id": session.id,
                "turn_no": turn_no,
                "epoch": handle.epoch,
                "status": status,
            }
        except GenerationSupersededError:
            yield "error", {
                "code": "superseded",
                "message": "收到新消息，已按最新消息重新处理。",
            }
            yield "done", {
                "session_id": session_id,
                "turn_no": 0,
                "epoch": 0,
                "status": "active",
            }
        except SessionBusyError:
            yield "error", {"code": "session_busy", "message": "上一条消息还在处理中，请稍后再试。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "epoch": 0, "status": "active"}
        except SessionNotFoundError:
            yield "error", {"code": "session_not_found", "message": "会话不存在或已过期。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "epoch": 0, "status": "closed"}
        except SessionForbiddenError:
            yield "error", {"code": "forbidden", "message": "当前用户无权访问该会话。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "epoch": 0, "status": "closed"}
        except SessionClosedError:
            yield "error", {"code": "session_closed", "message": "会话已经结束，请开启新会话。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "epoch": 0, "status": "closed"}
        finally:
            if handle is not None:
                self._finish_generation(handle)

    async def is_current(self, tenant_id: str, session_id: str, epoch: int) -> bool:
        if not session_id or epoch <= 0:
            return False
        try:
            session = await self.store.get(tenant_id, session_id)
        except SessionNotFoundError:
            return False
        return session.active_epoch == epoch

    async def get_session(self, tenant_id: str, user_id: str, session_id: str) -> SessionRecord:
        session = await self.store.get(tenant_id, session_id)
        if session.user_id != user_id:
            raise SessionForbiddenError(session_id)
        return session

    async def health(self) -> dict[str, Any]:
        try:
            store_ok = await self.store.ping()
        except Exception:
            logger.exception("session store health check failed")
            store_ok = False
        return {
            "status": "ok" if store_ok else "degraded",
            "llm_configured": bool(self.settings.llm_api_key),
            "llm_model": self.settings.llm_model,
            "quality_llm_configured": bool(
                self.settings.quality_llm_model and self.settings.quality_llm_api_key
            ),
            "embedding_configured": bool(self.settings.embedding_model),
            "rag_enabled": self.settings.rag_enabled,
            "session_backend": self.settings.session_backend,
            "agent_backend": "state (no shell)",
            "store_ok": store_ok,
        }

    async def close(self) -> None:
        if self.knowledge is not None:
            await self.knowledge.close()
        await self.locks.close()
        await self.store.close()

    async def _begin_turn(
        self, request: ChatRequest, session_id: str
    ) -> tuple[SessionRecord, GenerationHandle | None, int]:
        while True:
            session = await self.store.get(request.tenant_id, session_id)
            if session.user_id != request.user_id:
                raise SessionForbiddenError(session_id)
            self._ensure_active(session)

            turn_no = session.turn_count + 1
            if turn_no > self.settings.max_turns:
                return session, None, turn_no

            expected_revision = session.revision
            session.messages.append(StoredMessage(role="user", content=request.message))
            session.turn_count = turn_no
            session.active_epoch += 1
            session.revision = expected_revision + 1
            session.updated_at = datetime.now(UTC)

            if not await self.store.save_if_revision(session, expected_revision):
                await asyncio.sleep(0)
                continue

            handle = GenerationHandle(
                tenant_id=session.tenant_id,
                session_id=session.id,
                epoch=session.active_epoch,
                turn_no=turn_no,
            )
            self._supersede_previous(handle)
            return session, handle, turn_no

    def _supersede_previous(self, handle: GenerationHandle) -> None:
        previous = self._generations.get(handle.session_id)
        if previous is not None and previous.epoch < handle.epoch:
            previous.cancel_event.set()
            if previous.task is not None and not previous.task.done():
                previous.task.cancel()
        self._generations[handle.session_id] = handle

    def _finish_generation(self, handle: GenerationHandle) -> None:
        if self._generations.get(handle.session_id) is handle:
            self._generations.pop(handle.session_id, None)

    async def _ensure_current(self, handle: GenerationHandle) -> None:
        if handle.cancel_event.is_set():
            raise GenerationSupersededError(handle.session_id, handle.epoch)
        try:
            session = await self.store.get(handle.tenant_id, handle.session_id)
        except SessionNotFoundError:
            raise GenerationSupersededError(handle.session_id, handle.epoch) from None
        if session.active_epoch != handle.epoch:
            handle.cancel_event.set()
            raise GenerationSupersededError(handle.session_id, handle.epoch)

    async def _commit_turn(
        self,
        handle: GenerationHandle,
        assistant_text: str,
        status: SessionStatus,
    ) -> None:
        while True:
            session = await self.store.get(handle.tenant_id, handle.session_id)
            if session.active_epoch != handle.epoch:
                raise GenerationSupersededError(handle.session_id, handle.epoch)
            if session.status != "active":
                raise SessionClosedError(handle.session_id)

            expected_revision = session.revision
            if assistant_text.strip():
                session.messages.append(
                    StoredMessage(role="assistant", content=assistant_text.strip())
                )
            session.status = status
            session.revision = expected_revision + 1
            session.updated_at = datetime.now(UTC)
            if await self.store.save_if_revision(session, expected_revision):
                return
            await asyncio.sleep(0)

    async def _precheck_existing(self, request: ChatRequest, session_id: str) -> None:
        """Reject cross-user access before starting generation."""

        if request.session_id is None:
            return
        session = await self.store.get(request.tenant_id, session_id)
        if session.user_id != request.user_id:
            raise SessionForbiddenError(session_id)

    def _ensure_active(self, session: SessionRecord) -> None:
        if session.status != "active":
            raise SessionClosedError(session.id)

    async def _force_handoff(self, session: SessionRecord, turn_no: int) -> ChatResponse:
        item = OutboundItem(
            kind="handoff",
            text="这次对话已经达到最大轮次，我帮你转接人工同事继续处理。",
            payload={"reason": "max_turns"},
        )
        while True:
            current = await self.store.get(session.tenant_id, session.id)
            self._ensure_active(current)
            expected_revision = current.revision
            current.status = "handed_off"
            current.revision = expected_revision + 1
            current.updated_at = datetime.now(UTC)
            if await self.store.save_if_revision(current, expected_revision):
                return ChatResponse(
                    session_id=current.id,
                    turn_no=turn_no,
                    epoch=current.active_epoch,
                    status="handed_off",
                    messages=[item],
                )
            await asyncio.sleep(0)

    def _build_messages(self, session: SessionRecord) -> list[AnyMessage]:
        history = session.messages[-self.settings.max_history_messages :]
        messages: list[AnyMessage] = []
        for item in history:
            if item.role == "user":
                messages.append(HumanMessage(content=item.content))
            else:
                messages.append(AIMessage(content=item.content))
        return messages

    def _agent_context(self, session: SessionRecord) -> AgentContext:
        return AgentContext(
            tenant_id=session.tenant_id,
            user_id=session.user_id,
            session_id=session.id,
            channel=session.channel,
        )

    def _last_ai_text(self, state: dict[str, Any] | None) -> str:
        if not state:
            return ""
        for message in reversed(state.get("messages", [])):
            if isinstance(message, AIMessage):
                text = content_to_text(message.content).strip()
                if text:
                    return text
        return ""

    def _finalize_items(self, text: str, context: AgentContext, turn_no: int) -> list[OutboundItem]:
        items = [
            OutboundItem(kind="text", text=part)
            for part in split_reply(text, self.settings.reply_max_chars)
        ]
        items.extend(context.outbox)

        if turn_no >= self.settings.max_turns and not context.handoff_requested:
            items.append(
                OutboundItem(
                    kind="handoff",
                    text="这次对话已经比较长了，我帮你转接人工同事继续处理。",
                    payload={"reason": "max_turns"},
                )
            )

        if not items:
            items.append(
                OutboundItem(
                    kind="handoff",
                    text="我暂时没能确认该怎么处理，帮你转接人工同事。",
                    payload={"reason": "empty_model_response"},
                )
            )
        return items


__all__ = [
    "AgentInvocationError",
    "ChatService",
    "GenerationSupersededError",
    "SessionClosedError",
    "SessionForbiddenError",
]
