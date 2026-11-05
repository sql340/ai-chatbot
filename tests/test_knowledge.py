import json

import pytest

from chatbot.knowledge import KnowledgeProviderError, _parse_sources


def test_parse_sources() -> None:
    data = {
        "items": [
            {"content": "退款需要 7 天", "source": "policy.md", "score": 0.9, "page_number": 2},
            {"content": "", "source": "ignored.md"},
        ]
    }
    hits = _parse_sources([json.dumps(data, ensure_ascii=False)])
    assert len(hits) == 1
    assert hits[0].content == "退款需要 7 天"
    assert hits[0].page_number == 2


def test_parse_sources_rejects_invalid_json() -> None:
    with pytest.raises(KnowledgeProviderError):
        _parse_sources(["not-json"])

