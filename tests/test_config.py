from chatbot.config import Settings


def test_empty_optional_rag_threshold_is_none() -> None:
    settings = Settings(RAG_SCORE_THRESHOLD="")
    assert settings.rag_score_threshold is None


def test_trailing_slashes_are_removed() -> None:
    settings = Settings(LLM_BASE_URL="http://localhost:8000/v1/", RAG_BASE_URL="http://rag/")
    assert settings.llm_base_url == "http://localhost:8000/v1"
    assert settings.rag_base_url == "http://rag"

