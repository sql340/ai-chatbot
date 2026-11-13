from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles

from .agent import build_agent
from .channels import (
    ChannelAuthError,
    ChannelHttpRequest,
    ChannelPayloadError,
    build_channel_registry,
)
from .channels.dispatcher import ChannelDispatcher
from .channels.state import build_channel_binding_store, build_event_deduplicator
from .config import Settings
from .knowledge import build_knowledge_provider
from .locks import SessionBusyError, build_lock_manager
from .models import (
    ChatRequest,
    ChatResponse,
    SatisfactionRequest,
    SessionCloseRequest,
    SessionRecord,
    SessionView,
)
from .service import (
    AgentInvocationError,
    ChatService,
    GenerationSupersededError,
    SatisfactionUnavailableError,
    SessionClosedError,
    SessionForbiddenError,
)
from .store import SessionNotFoundError, build_session_store

logger = logging.getLogger(__name__)


def create_app(
    *,
    settings: Settings | None = None,
    service: ChatService | None = None,
) -> FastAPI:
    resolved_settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime = service
        if runtime is None:
            knowledge = build_knowledge_provider(resolved_settings)
            store = build_session_store(resolved_settings)
            locks = build_lock_manager(resolved_settings)
            try:
                store_ok = await store.ping()
            except Exception as exc:
                await store.close()
                await locks.close()
                raise RuntimeError(
                    "session store is unavailable; check SESSION_BACKEND/REDIS_URL"
                ) from exc
            if not store_ok:
                await store.close()
                await locks.close()
                raise RuntimeError("session store is unavailable; check SESSION_BACKEND/REDIS_URL")
            runtime = ChatService(
                settings=resolved_settings,
                agent=build_agent(resolved_settings, knowledge),
                store=store,
                locks=locks,
                knowledge=knowledge,
            )

        app.state.settings = resolved_settings
        app.state.service = runtime
        bindings = build_channel_binding_store(resolved_settings)
        deduplicator = build_event_deduplicator(resolved_settings)
        channels = build_channel_registry(resolved_settings)
        app.state.channel_bindings = bindings
        app.state.channel_deduplicator = deduplicator
        app.state.channels = channels
        app.state.channel_dispatcher = ChannelDispatcher(
            service=runtime,
            bindings=bindings,
            deduplicator=deduplicator,
            settings=resolved_settings,
        )
        sweep_task: asyncio.Task[None] | None = None
        if resolved_settings.lifecycle_sweep_interval_seconds > 0:
            sweep_task = asyncio.create_task(
                _lifecycle_sweeper(
                    runtime,
                    resolved_settings.lifecycle_sweep_interval_seconds,
                )
            )
        try:
            yield
        finally:
            if sweep_task is not None:
                sweep_task.cancel()
                with suppress(asyncio.CancelledError):
                    await sweep_task
            await channels.close()
            await bindings.close()
            await deduplicator.close()
            await runtime.close()

    app = FastAPI(title=resolved_settings.app_name, version="0.1.0", lifespan=lifespan)
    if resolved_settings.cors_origin_list:
        allow_all = resolved_settings.cors_origin_list == ["*"]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=resolved_settings.cors_origin_list,
            allow_credentials=not allow_all,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    static_dir = Path(__file__).with_name("static")
    if resolved_settings.web_enabled and static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

        @app.get("/", include_in_schema=False)
        async def web_ui() -> FileResponse:
            return FileResponse(
                static_dir / "index.html",
                headers={"Cache-Control": "no-cache"},
            )

    @app.get("/health")
    async def health(request: Request) -> dict:
        return await _service(request).health()

    @app.get(
        "/v1/channels/{channel}/events",
        name="receive_channel_event_get",
        operation_id="receive_channel_event_get",
    )
    @app.post(
        "/v1/channels/{channel}/events",
        name="receive_channel_event_post",
        operation_id="receive_channel_event_post",
    )
    async def channel_event(
        channel: str,
        request: Request,
        background_tasks: BackgroundTasks,
    ) -> Response:
        adapter = _channels(request).get(channel)
        if adapter is None:
            raise HTTPException(status_code=404, detail="channel is not enabled")

        channel_request = ChannelHttpRequest(
            method=request.method,
            headers={key.lower(): value for key, value in request.headers.items()},
            query=dict(request.query_params),
            body=await request.body(),
        )
        try:
            result = await adapter.receive(channel_request)
        except ChannelAuthError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ChannelPayloadError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if result.message is None:
            return _channel_response(result.response, result.status_code, result.media_type)

        if result.process_async:
            background_tasks.add_task(
                _channel_dispatcher(request).process_and_send,
                adapter,
                result.message,
            )
            return _channel_response(result.response, result.status_code, result.media_type)

        try:
            response = await _channel_dispatcher(request).handle(result.message)
        except SessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail="session not found or expired") from exc
        except SessionForbiddenError as exc:
            raise HTTPException(status_code=403, detail="session does not belong to this user") from exc
        except SessionClosedError as exc:
            raise HTTPException(status_code=409, detail="session is closed") from exc
        except GenerationSupersededError as exc:
            raise HTTPException(status_code=409, detail="generation superseded") from exc
        except SessionBusyError as exc:
            raise HTTPException(status_code=409, detail="session is busy") from exc
        except AgentInvocationError as exc:
            raise HTTPException(status_code=502, detail="agent invocation failed") from exc
        if response is None:
            return Response(status_code=204)
        return JSONResponse(content=response.model_dump(mode="json"))

    @app.post("/v1/chat", response_model=ChatResponse)
    async def chat(payload: ChatRequest, request: Request) -> ChatResponse:
        try:
            return await _service(request).chat(payload)
        except SessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail="session not found or expired") from exc
        except SessionForbiddenError as exc:
            raise HTTPException(status_code=403, detail="session does not belong to this user") from exc
        except SessionClosedError as exc:
            raise HTTPException(status_code=409, detail="session is closed") from exc
        except GenerationSupersededError as exc:
            raise HTTPException(status_code=409, detail="generation superseded") from exc
        except SessionBusyError as exc:
            raise HTTPException(status_code=409, detail="session is busy") from exc
        except AgentInvocationError as exc:
            raise HTTPException(status_code=502, detail="agent invocation failed") from exc

    @app.post("/v1/chat/stream")
    async def chat_stream(payload: ChatRequest, request: Request) -> StreamingResponse:
        async def events() -> AsyncIterator[str]:
            async for event, data in _service(request).stream(payload):
                yield _sse(event, data)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/v1/sessions/{session_id}", response_model=SessionView)
    async def get_session(
        session_id: str,
        request: Request,
        tenant_id: str = Query(min_length=1, max_length=64),
        user_id: str = Query(min_length=1, max_length=128),
    ) -> SessionView:
        try:
            session = await _service(request).get_session(tenant_id, user_id, session_id)
        except SessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail="session not found or expired") from exc
        except SessionForbiddenError as exc:
            raise HTTPException(status_code=403, detail="session does not belong to this user") from exc
        return _session_view(session)

    @app.post("/v1/sessions/{session_id}/close", response_model=SessionView)
    async def close_session(
        session_id: str,
        payload: SessionCloseRequest,
        request: Request,
    ) -> SessionView:
        try:
            session = await _service(request).close_session(
                payload.tenant_id,
                payload.user_id,
                session_id,
                payload.reason,
            )
        except SessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail="session not found or expired") from exc
        except SessionForbiddenError as exc:
            raise HTTPException(status_code=403, detail="session does not belong to this user") from exc
        except SessionClosedError as exc:
            raise HTTPException(status_code=409, detail="session cannot be closed") from exc
        return _session_view(session)

    @app.post("/v1/sessions/{session_id}/satisfaction", response_model=SessionView)
    async def submit_satisfaction(
        session_id: str,
        payload: SatisfactionRequest,
        request: Request,
    ) -> SessionView:
        try:
            session = await _service(request).submit_satisfaction(payload, session_id)
        except SessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail="session not found or expired") from exc
        except SessionForbiddenError as exc:
            raise HTTPException(status_code=403, detail="session does not belong to this user") from exc
        except SatisfactionUnavailableError as exc:
            raise HTTPException(
                status_code=409,
                detail="satisfaction is not available for this session",
            ) from exc
        return _session_view(session)

    return app


def _service(request: Request) -> ChatService:
    return request.app.state.service


def _channels(request: Request):
    return request.app.state.channels


def _channel_dispatcher(request: Request) -> ChannelDispatcher:
    return request.app.state.channel_dispatcher


def _sse(event: str, data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n"


async def _lifecycle_sweeper(service: ChatService, interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            result = await service.sweep_lifecycle()
            if result["idle_timeout"] or result["solved_timeout"]:
                logger.info("lifecycle sweep closed sessions: %s", result)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("lifecycle sweep failed")


def _session_view(session: SessionRecord) -> SessionView:
    return SessionView(
        session_id=session.id,
        tenant_id=session.tenant_id,
        user_id=session.user_id,
        channel=session.channel,
        status=session.status,
        status_reason=session.status_reason,
        turn_count=session.turn_count,
        active_epoch=session.active_epoch,
        messages=session.messages,
        parent_session_id=session.parent_session_id,
        reopened_count=session.reopened_count,
        last_activity_at=session.last_activity_at,
        solved_at=session.solved_at,
        closed_at=session.closed_at,
        close_reason=session.close_reason,
        resolution_summary=session.resolution_summary,
        handoff_requested_at=session.handoff_requested_at,
        handoff_reason=session.handoff_reason,
        satisfaction_prompted_at=session.satisfaction_prompted_at,
        satisfaction=session.satisfaction,
        created_at=session.created_at,
        updated_at=session.updated_at,
    )


def _channel_response(payload: object, status_code: int, media_type: str) -> Response:
    if isinstance(payload, dict):
        return JSONResponse(payload, status_code=status_code)
    if payload is None:
        return Response(status_code=status_code, media_type=media_type)
    return PlainTextResponse(str(payload), status_code=status_code, media_type=media_type)


__all__ = ["create_app"]

