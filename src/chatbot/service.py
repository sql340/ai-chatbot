from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage
from langgraph.graph.state import CompiledStateGraph

from .config import Settings
from .knowledge import KnowledgeProvider
from .lifecycle import (
    can_reopen,
    idle_close_target,
    is_human_owned,
    now_utc,
    transition_session,
)
from .locks import SessionBusyError, SessionLockManager
from .models import (
    ChatRequest,
    ChatResponse,
    OutboundItem,
    Satisfaction,
    SatisfactionRequest,
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


class SatisfactionUnavailableError(Exception):
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


@dataclass
class TurnStart:
    session: SessionRecord
    handle: GenerationHandle | None
    turn_no: int
    immediate_response: ChatResponse | None = None


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
        parent_session_id: str | None = None,
    ) -> SessionRecord:
        session = SessionRecord(
            id=session_id or str(uuid.uuid4()),
            tenant_id=tenant_id,
            user_id=user_id,
            channel=channel,  # type: ignore[arg-type]
            parent_session_id=parent_session_id,
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

        start = await self._begin_turn(request, session_id)
        if start.immediate_response is not None:
            return start.immediate_response

        handle = start.handle
        assert handle is not None
        try:
            await self._ensure_current(handle)
            messages = self._build_messages(start.session)
            context = self._agent_context(start.session)
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
            items, status, reason, resolution_summary = self._build_turn_result(
                text, context, start.turn_no
            )
            await self._commit_turn(
                handle,
                text,
                status,
                reason=reason,
                resolution_summary=resolution_summary,
            )
            return ChatResponse(
                session_id=start.session.id,
                turn_no=start.turn_no,
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

            start = await self._begin_turn(request, session_id)
            if start.immediate_response is not None:
                response = start.immediate_response
                yield "session", {
                    "session_id": response.session_id,
                    "turn_no": response.turn_no,
                    "epoch": response.epoch,
                }
                for item in response.messages:
                    event = "message" if item.kind == "text" else "action"
                    yield event, item.model_dump(mode="json")
                yield "done", {
                    "session_id": response.session_id,
                    "turn_no": response.turn_no,
                    "epoch": response.epoch,
                    "status": response.status,
                }
                return

            handle = start.handle
            assert handle is not None
            yield "session", {
                "session_id": start.session.id,
                "turn_no": start.turn_no,
                "epoch": handle.epoch,
            }

            messages = self._build_messages(start.session)
            context = self._agent_context(start.session)
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
                logger.info("stream cancelled for session %s", start.session.id)
                raise
            except Exception:
                logger.exception("agent stream failed for session %s", start.session.id)
                yield "error", {
                    "code": "agent_error",
                    "message": "客服暂时无法回复，请稍后重试。",
                }
                yield "done", {
                    "session_id": start.session.id,
                    "turn_no": start.turn_no,
                    "epoch": handle.epoch,
                    "status": start.session.status,
                }
                return

            canonical_text = self._last_ai_text(final_state) or streamed_text
            items, status, reason, resolution_summary = self._build_turn_result(
                canonical_text, context, start.turn_no
            )

            await self._commit_turn(
                handle,
                canonical_text,
                status,
                reason=reason,
                resolution_summary=resolution_summary,
            )
            for item in items:
                event = "message" if item.kind == "text" else "action"
                yield event, item.model_dump(mode="json")
            yield "done", {
                "session_id": start.session.id,
                "turn_no": start.turn_no,
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

    async def submit_satisfaction(
        self,
        request: SatisfactionRequest,
        session_id: str,
    ) -> SessionRecord:
        session = await self.get_session(request.tenant_id, request.user_id, session_id)
        if session.satisfaction is not None:
            return session
        return await self._record_satisfaction(
            request.tenant_id,
            request.user_id,
            session_id,
            score=request.score,
            comment=request.comment,
        )

    async def close_session(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        reason: str,
    ) -> SessionRecord:
        await self.get_session(tenant_id, user_id, session_id)
        return await self._close_session(tenant_id, session_id, reason=reason)

    async def sweep_lifecycle(self) -> dict[str, int]:
        """Close idle bot sessions and expired solved sessions."""

        closed_idle = 0
        closed_solved = 0
        for candidate in await self.store.list_sessions():
            closed_reason = await self._close_if_expired(
                candidate.tenant_id,
                candidate.id,
            )
            if closed_reason == "idle_timeout":
                closed_idle += 1
            elif closed_reason == "solved_timeout":
                closed_solved += 1
        return {"idle_timeout": closed_idle, "solved_timeout": closed_solved}

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
        for handle in list(self._generations.values()):
            handle.cancel_event.set()
            if handle.task is not None and not handle.task.done():
                handle.task.cancel()
        self._generations.clear()
        if self.knowledge is not None:
            await self.knowledge.close()
        await self.locks.close()
        await self.store.close()

    async def _begin_turn(self, request: ChatRequest, session_id: str) -> TurnStart:
        while True:
            session = await self.store.get(request.tenant_id, session_id)
            if session.user_id != request.user_id:
                raise SessionForbiddenError(session_id)
            now = now_utc()

            if session.status == "closed":
                raise SessionClosedError(session_id)

            if is_human_owned(session.status):
                expected_revision = session.revision
                session.messages.append(StoredMessage(role="user", content=request.message))
                session.active_epoch += 1
                session.last_activity_at = now
                item = OutboundItem(
                    kind="handoff",
                    text=(
                        "你的补充已经记录，正在为你转接人工同事。"
                        if session.status == "handoff_pending"
                        else "人工同事正在处理，你的补充已经记录。"
                    ),
                    payload={"reason": "human_owned"},
                )
                session.messages.append(StoredMessage(role="assistant", content=item.text or ""))
                session.revision = expected_revision + 1
                session.updated_at = now
                if await self.store.save_if_revision(session, expected_revision):
                    self._cancel_generation(session.id)
                    return TurnStart(
                        session=session,
                        handle=None,
                        turn_no=session.turn_count,
                        immediate_response=ChatResponse(
                            session_id=session.id,
                            turn_no=session.turn_count,
                            epoch=session.active_epoch,
                            status=session.status,
                            messages=[item],
                        ),
                    )
                await asyncio.sleep(0)
                continue

            if session.status == "solved":
                if (
                    session.satisfaction is None
                    and session.satisfaction_prompted_at is not None
                    and request.message in {"1", "2", "3", "4", "5"}
                ):
                    score = int(request.message)
                    solved = await self._record_satisfaction(
                        request.tenant_id,
                        request.user_id,
                        session_id,
                        score=score,
                        comment=None,
                        user_text=request.message,
                    )
                    return TurnStart(
                        session=solved,
                        handle=None,
                        turn_no=solved.turn_count,
                        immediate_response=self._satisfaction_response(solved),
                    )
                if not can_reopen(
                    session,
                    reopen_window_seconds=self.settings.reopen_window_seconds,
                    now=now,
                ):
                    await self._close_session(
                        request.tenant_id,
                        session_id,
                        reason="solved_timeout",
                        expected_status="solved",
                    )
                    raise SessionClosedError(session_id)

                transition_session(session, "active", reason="reopened", now=now)

            if session.status != "active":
                await asyncio.sleep(0)
                continue

            if session.turn_count >= self.settings.max_turns:
                expected_revision = session.revision
                session.messages.append(StoredMessage(role="user", content=request.message))
                session.active_epoch += 1
                session.last_activity_at = now
                transition_session(session, "handoff_pending", reason="max_turns", now=now)
                item = OutboundItem(
                    kind="handoff",
                    text="这次对话已经达到最大轮次，我帮你转接人工同事继续处理。",
                    payload={"reason": "max_turns"},
                )
                session.revision = expected_revision + 1
                if await self.store.save_if_revision(session, expected_revision):
                    self._cancel_generation(session.id)
                    return TurnStart(
                        session=session,
                        handle=None,
                        turn_no=session.turn_count,
                        immediate_response=ChatResponse(
                            session_id=session.id,
                            turn_no=session.turn_count,
                            epoch=session.active_epoch,
                            status="handoff_pending",
                            messages=[item],
                        ),
                    )
                await asyncio.sleep(0)
                continue

            expected_revision = session.revision
            turn_no = session.turn_count + 1
            session.messages.append(StoredMessage(role="user", content=request.message))
            session.active_epoch += 1
            session.last_activity_at = now
            session.revision = expected_revision + 1
            session.updated_at = now

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
            return TurnStart(session=session, handle=handle, turn_no=turn_no)

    async def _commit_turn(
        self,
        handle: GenerationHandle,
        assistant_text: str,
        status: SessionStatus,
        *,
        reason: str | None,
        resolution_summary: str | None,
    ) -> None:
        while True:
            session = await self.store.get(handle.tenant_id, handle.session_id)
            if session.active_epoch != handle.epoch:
                raise GenerationSupersededError(handle.session_id, handle.epoch)
            if session.status != "active":
                raise SessionClosedError(handle.session_id)

            expected_revision = session.revision
            now = now_utc()
            if assistant_text.strip():
                session.messages.append(
                    StoredMessage(role="assistant", content=assistant_text.strip())
                )
            if status == "active":
                session.status_reason = reason
                session.updated_at = now
            else:
                transition_session(session, status, reason=reason, now=now)
            if status == "solved":
                session.resolution_summary = resolution_summary
                session.satisfaction_prompted_at = now
            session.turn_count = handle.turn_no
            session.last_activity_at = now
            session.revision = expected_revision + 1
            if await self.store.save_if_revision(session, expected_revision):
                return
            await asyncio.sleep(0)

    async def _record_satisfaction(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        *,
        score: int,
        comment: str | None,
        user_text: str | None = None,
    ) -> SessionRecord:
        while True:
            session = await self.store.get(tenant_id, session_id)
            if session.user_id != user_id:
                raise SessionForbiddenError(session_id)
            if session.satisfaction is not None:
                return session
            if session.status != "solved" or session.satisfaction_prompted_at is None:
                raise SatisfactionUnavailableError(session_id)

            expected_revision = session.revision
            now = now_utc()
            target: SessionStatus = "closed" if score >= 3 else "handoff_pending"
            reason = "satisfaction_submitted" if target == "closed" else "low_satisfaction"
            if user_text:
                session.messages.append(StoredMessage(role="user", content=user_text))
            session.satisfaction = Satisfaction(score=score, comment=comment, submitted_at=now)
            transition_session(session, target, reason=reason, now=now)
            session.active_epoch += 1
            session.last_activity_at = now
            if user_text:
                acknowledgement = self._satisfaction_acknowledgement(target)
                session.messages.append(
                    StoredMessage(role="assistant", content=acknowledgement)
                )
                session.turn_count += 1
            session.revision = expected_revision + 1

            if await self.store.save_if_revision(session, expected_revision):
                self._cancel_generation(session.id)
                return session
            await asyncio.sleep(0)

    def _satisfaction_response(self, session: SessionRecord) -> ChatResponse:
        target = session.status
        messages = [
            OutboundItem(
                kind="text",
                text=self._satisfaction_acknowledgement(target),
            )
        ]
        if target == "handoff_pending":
            messages.append(
                OutboundItem(
                    kind="handoff",
                    text="这次没有解决好，我帮你转接人工同事继续处理。",
                    payload={"reason": "low_satisfaction"},
                )
            )
        return ChatResponse(
            session_id=session.id,
            turn_no=session.turn_count,
            epoch=session.active_epoch,
            status=target,
            messages=messages,
        )

    @staticmethod
    def _satisfaction_acknowledgement(status: SessionStatus) -> str:
        if status == "handoff_pending":
            return "抱歉这次没有解决好，我帮你转接人工同事继续处理。"
        return "谢谢你的评分，很高兴这次能帮到你。"

    async def _close_session(
        self,
        tenant_id: str,
        session_id: str,
        *,
        reason: str,
        expected_status: SessionStatus | None = None,
    ) -> SessionRecord:
        while True:
            session = await self.store.get(tenant_id, session_id)
            if session.status == "closed":
                return session
            if expected_status is not None and session.status != expected_status:
                raise SessionClosedError(session_id)

            expected_revision = session.revision
            now = now_utc()
            transition_session(session, "closed", reason=reason, now=now)
            session.active_epoch += 1
            session.last_activity_at = now
            session.revision = expected_revision + 1
            if await self.store.save_if_revision(session, expected_revision):
                self._cancel_generation(session.id)
                return session
            await asyncio.sleep(0)

    async def _close_if_expired(self, tenant_id: str, session_id: str) -> str | None:
        while True:
            try:
                session = await self.store.get(tenant_id, session_id)
            except SessionNotFoundError:
                return None
            target = idle_close_target(
                session,
                idle_close_seconds=self.settings.idle_close_seconds,
                solved_close_seconds=self.settings.solved_close_seconds,
            )
            if target is None:
                return None
            _status, reason = target
            expected_revision = session.revision
            now = now_utc()
            transition_session(session, "closed", reason=reason, now=now)
            session.active_epoch += 1
            session.last_activity_at = now
            session.revision = expected_revision + 1
            if await self.store.save_if_revision(session, expected_revision):
                self._cancel_generation(session.id)
                return reason
            await asyncio.sleep(0)

    def _build_turn_result(
        self,
        text: str,
        context: AgentContext,
        turn_no: int,
    ) -> tuple[list[OutboundItem], SessionStatus, str | None, str | None]:
        items = [
            OutboundItem(kind="text", text=part)
            for part in split_reply(text, self.settings.reply_max_chars)
        ]
        items.extend(context.outbox)

        if context.handoff_requested:
            status: SessionStatus = "handoff_pending"
            reason = context.handoff_reason or "agent_handoff"
            resolution_summary = None
        elif context.resolution_requested:
            status = "solved"
            reason = "resolved"
            resolution_summary = context.resolution_summary or "用户问题已确认解决。"
        elif turn_no >= self.settings.max_turns:
            status = "handoff_pending"
            reason = "max_turns"
            resolution_summary = None
            if not any(item.kind == "handoff" for item in items):
                items.append(
                    OutboundItem(
                        kind="handoff",
                        text="这次对话已经比较长了，我帮你转接人工同事继续处理。",
                        payload={"reason": "max_turns"},
                    )
                )
        else:
            status = "active"
            reason = None
            resolution_summary = None

        if status == "solved":
            items.append(self._satisfaction_prompt())

        if not items:
            status = "handoff_pending"
            reason = "empty_model_response"
            resolution_summary = None
            items.append(
                OutboundItem(
                    kind="handoff",
                    text="我暂时没能确认该怎么处理，帮你转接人工同事。",
                    payload={"reason": "empty_model_response"},
                )
            )
        return items, status, reason, resolution_summary

    @staticmethod
    def _satisfaction_prompt() -> OutboundItem:
        return OutboundItem(
            kind="select",
            payload={
                "kind": "satisfaction",
                "question": "这次服务有帮到你吗？请为本次服务打分。",
                "options": [
                    {"label": "1 · 很不满意", "value": "1"},
                    {"label": "2 · 不满意", "value": "2"},
                    {"label": "3 · 一般", "value": "3"},
                    {"label": "4 · 满意", "value": "4"},
                    {"label": "5 · 非常满意", "value": "5"},
                ],
                "allow_multiple": False,
            },
        )

    def _supersede_previous(self, handle: GenerationHandle) -> None:
        previous = self._generations.get(handle.session_id)
        if previous is not None and previous.epoch < handle.epoch:
            previous.cancel_event.set()
            if previous.task is not None and not previous.task.done():
                previous.task.cancel()
        self._generations[handle.session_id] = handle

    def _cancel_generation(self, session_id: str) -> None:
        handle = self._generations.get(session_id)
        if handle is None:
            return
        handle.cancel_event.set()
        if handle.task is not None and not handle.task.done():
            handle.task.cancel()

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

    async def _precheck_existing(self, request: ChatRequest, session_id: str) -> None:
        """Reject cross-user access before starting generation."""

        if request.session_id is None:
            return
        session = await self.store.get(request.tenant_id, session_id)
        if session.user_id != request.user_id:
            raise SessionForbiddenError(session_id)

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


__all__ = [
    "AgentInvocationError",
    "ChatService",
    "GenerationSupersededError",
    "SatisfactionUnavailableError",
    "SessionClosedError",
    "SessionForbiddenError",
]
