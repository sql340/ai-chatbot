from chatbot.text import content_to_text, split_reply


def test_content_to_text_supports_blocks() -> None:
    assert content_to_text("hello") == "hello"
    assert content_to_text([{"type": "text", "text": "你"}, {"type": "text", "text": "好"}]) == "你好"
    assert content_to_text([{"type": "tool_use", "text": "ignored"}]) == ""


def test_split_reply_uses_blank_lines_and_limit() -> None:
    assert split_reply("第一句\n\n第二句", 100) == ["第一句", "第二句"]
    assert all(len(item) <= 10 for item in split_reply("一二三四五六七八九十一二三四五六", 10))

