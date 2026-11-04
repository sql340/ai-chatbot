from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from .config import Settings


class KnowledgeProviderError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class KnowledgeHit:
    content: str
    source: str
    score: float | None = None
    page_number: int | None = None


class KnowledgeProvider(Protocol):
    async def search(self, tenant_id: str, query: str, top_k: int) -> list[KnowledgeHit]: ...

    async def close(self) -> None: ...


class EnterpriseRagProvider:
    """Read-only adapter for enterprise-rag's SSE query endpoint.

    It stops after the `sources` event so the downstream RAG service does not
    spend time or tokens generating a second answer.
    """

    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        if not settings.rag_tenant_field.strip():
            raise ValueError("RAG_TENANT_FIELD is required when RAG is enabled")
        self._settings = settings
        self._client = client or httpx.AsyncClient(timeout=settings.rag_timeout)

    async def search(self, tenant_id: str, query: str, top_k: int) -> list[KnowledgeHit]:
        payload: dict[str, Any] = {
            "question": query,
            "top_k": top_k,
            "include_sources": True,
            "filters": {self._settings.rag_tenant_field: tenant_id},
        }
        if self._settings.rag_score_threshold is not None:
            payload["score_threshold"] = self._settings.rag_score_threshold

        try:
            async with self._client.stream(
                "POST",
                f"{self._settings.rag_base_url}/v1/query",
                json=payload,
                headers={"Accept": "text/event-stream"},
            ) as response:
                response.raise_for_status()
                event_name: str | None = None
                data_lines: list[str] = []
                async for line in response.aiter_lines():
                    if line.startswith("event: "):
                        event_name = line.removeprefix("event: ").strip()
                    elif line.startswith("data: "):
                        data_lines.append(line.removeprefix("data: "))
                    elif line == "":
                        if event_name == "sources":
                            return _parse_sources(data_lines)
                        if event_name == "error":
                            detail = " ".join(data_lines)
                            raise KnowledgeProviderError(detail[:500])
                        event_name = None
                        data_lines = []
        except httpx.HTTPError as exc:
            raise KnowledgeProviderError(str(exc)) from exc
        return []

    async def close(self) -> None:
        await self._client.aclose()


def _parse_sources(data_lines: list[str]) -> list[KnowledgeHit]:
    if not data_lines:
        return []
    try:
        data = json.loads("\n".join(data_lines))
    except json.JSONDecodeError as exc:
        raise KnowledgeProviderError("invalid sources event") from exc

    hits: list[KnowledgeHit] = []
    for item in data.get("items", []):
        content = item.get("content")
        if not isinstance(content, str) or not content.strip():
            continue
        hits.append(
            KnowledgeHit(
                content=content.strip(),
                source=str(item.get("source") or "knowledge"),
                score=item.get("score") if isinstance(item.get("score"), (int, float)) else None,
                page_number=(
                    item.get("page_number") if isinstance(item.get("page_number"), int) else None
                ),
            )
        )
    return hits


def build_knowledge_provider(settings: Settings) -> KnowledgeProvider | None:
    if not settings.rag_enabled:
        return None
    return EnterpriseRagProvider(settings)


__all__ = [
    "EnterpriseRagProvider",
    "KnowledgeHit",
    "KnowledgeProvider",
    "KnowledgeProviderError",
    "build_knowledge_provider",
]

