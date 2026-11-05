from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
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

    async def chat(self, request: ChatRequest) -> ChatResponse:
        session_id = request.session_id or str(uuid.uuid4())
        await self._precheck_existing(request, session_id)
        async with self.locks.acquire(request.tenant_id, session_id):
            session = await self._load_or_create(request, session_id)
            turn_no = session.turn_count + 1
            self._ensure_active(session)

            if turn_no > self.settings.max_turns:
                return await self._force_handoff(session, turn_no)

            messages = self._build_messages(session, request.message)
            context = self._agent_context(session)
            try:
                result = await self.agent.ainvoke(
                    {"messages": messages},
                    context=context,
                    config={"recursion_limit": 40},
                )
            except Exception as exc:
                logger.exception("agent invocation failed for session %s", session_id)
                raise AgentInvocationError(str(exc)) from exc

            text = self._last_ai_text(result)
            items = self._finalize_items(text, context, turn_no)
            status: SessionStatus = (
                "handed_off"
                if context.handoff_requested or turn_no >= self.settings.max_turns
                else "active"
            )
            self._commit_turn(session, turn_no, request.message, text, status)
            await self.store.save(session)
            return ChatResponse(
                session_id=session.id,
                turn_no=turn_no,
                status=status,
                messages=items,
            )

    async def stream(self, request: ChatRequest) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        session_id = request.session_id or str(uuid.uuid4())
        try:
            await self._precheck_existing(request, session_id)
            async with self.locks.acquire(request.tenant_id, session_id):
                session = await self._load_or_create(request, session_id)
                turn_no = session.turn_count + 1
                self._ensure_active(session)

                if turn_no > self.settings.max_turns:
                    response = await self._force_handoff(session, turn_no)
                    yield "session", {"session_id": response.session_id, "turn_no": response.turn_no}
                    for item in response.messages:
                        yield "action", item.model_dump(mode="json")
                    yield "done", {
                        "session_id": response.session_id,
                        "turn_no": response.turn_no,
                        "status": response.status,
                    }
                    return

                yield "session", {"session_id": session.id, "turn_no": turn_no}

                messages = self._build_messages(session, request.message)
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
                        if mode == "messages":
                            chunk, _metadata = payload
                            if isinstance(chunk, AIMessage):
                                text_delta = content_to_text(chunk.content)
                                if text_delta:
                                    streamed_text += text_delta
                                    yield "delta", {"content": text_delta}
                        elif mode == "values":
                            final_state = payload
                except asyncio.CancelledError:
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
                        "status": session.status,
                    }
                    return

                canonical_text = self._last_ai_text(final_state) or streamed_text
                items = self._finalize_items(canonical_text, context, turn_no)
                status = "handed_off" if context.handoff_requested or turn_no >= self.settings.max_turns else "active"

                for item in items:
                    event = "message" if item.kind == "text" else "action"
                    yield event, item.model_dump(mode="json")

                self._commit_turn(session, turn_no, request.message, canonical_text, status)
                await self.store.save(session)
                yield "done", {
                    "session_id": session.id,
                    "turn_no": turn_no,
                    "status": status,
                }
        except SessionBusyError:
            yield "error", {"code": "session_busy", "message": "上一条消息还在处理中，请稍后再试。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "status": "active"}
        except SessionNotFoundError:
            yield "error", {"code": "session_not_found", "message": "会话不存在或已过期。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "status": "closed"}
        except SessionForbiddenError:
            yield "error", {"code": "forbidden", "message": "当前用户无权访问该会话。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "status": "closed"}
        except SessionClosedError:
            yield "error", {"code": "session_closed", "message": "会话已经结束，请开启新会话。"}
            yield "done", {"session_id": session_id, "turn_no": 0, "status": "closed"}

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

    async def _load_or_create(self, request: ChatRequest, session_id: str) -> SessionRecord:
        if request.session_id is None:
            session = SessionRecord(
                id=session_id,
                tenant_id=request.tenant_id,
                user_id=request.user_id,
                channel=request.channel,
            )
            await self.store.create(session)
            return session

        session = await self.store.get(request.tenant_id, session_id)
        if session.user_id != request.user_id:
            raise SessionForbiddenError(session_id)
        return session

    async def _precheck_existing(self, request: ChatRequest, session_id: str) -> None:
        """Reject cross-user access before waiting on the per-session lock.

        This keeps a busy session from masking a 403/404 and avoids leaking
        whether another user's conversation is currently being generated.
        """

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
        session.status = "handed_off"
        session.updated_at = datetime.now(UTC)
        await self.store.save(session)
        return ChatResponse(
            session_id=session.id,
            turn_no=turn_no,
            status="handed_off",
            messages=[item],
        )

    def _build_messages(self, session: SessionRecord, current: str) -> list[AnyMessage]:
        history = session.messages[-self.settings.max_history_messages :]
        messages: list[AnyMessage] = []
        for item in history:
            if item.role == "user":
                messages.append(HumanMessage(content=item.content))
            else:
                messages.append(AIMessage(content=item.content))
        messages.append(HumanMessage(content=current))
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

    def _commit_turn(
        self,
        session: SessionRecord,
        turn_no: int,
        user_message: str,
        assistant_text: str,
        status: SessionStatus,
    ) -> None:
        session.messages.append(StoredMessage(role="user", content=user_message))
        if assistant_text.strip():
            session.messages.append(StoredMessage(role="assistant", content=assistant_text.strip()))
        session.turn_count = turn_no
        session.status = status
        session.updated_at = datetime.now(UTC)


__all__ = [
    "AgentInvocationError",
    "ChatService",
    "SessionClosedError",
    "SessionForbiddenError",
]

