from __future__ import annotations

import re
from typing import Any

_PARAGRAPH_RE = re.compile(r"\n\s*\n+")
_SENTENCE_RE = re.compile(r"(?<=[。！？!?；;])\s*")


def content_to_text(content: Any) -> str:
    """Normalize LangChain message content to plain text."""

    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""

    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and block.get("type") in {"text", "output_text"}:
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "".join(parts)


def split_reply(text: str, max_chars: int) -> list[str]:
    """Split a model reply on blank lines, then bound each message length."""

    text = text.strip()
    if not text:
        return []

    chunks: list[str] = []
    for paragraph in _PARAGRAPH_RE.split(text):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        chunks.extend(_split_long(paragraph, max_chars))
    return chunks


def _split_long(text: str, max_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]

    sentences = [part.strip() for part in _SENTENCE_RE.split(text) if part.strip()]
    if len(sentences) == 1:
        return [text[i : i + max_chars].strip() for i in range(0, len(text), max_chars)]

    chunks: list[str] = []
    current = ""
    for sentence in sentences:
        if len(sentence) > max_chars:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(
                text_part.strip()
                for text_part in (
                    sentence[i : i + max_chars] for i in range(0, len(sentence), max_chars)
                )
                if text_part.strip()
            )
            continue
        if not current:
            current = sentence
        elif len(current) + len(sentence) <= max_chars:
            current += sentence
        else:
            chunks.append(current)
            current = sentence
    if current:
        chunks.append(current)
    return chunks


__all__ = ["content_to_text", "split_reply"]

