from poe_api_wrapper.reverse.utils import resolve_bot_message_delta


def test_first_bot_message_delta_has_no_added_newline():
    message = {
        "author": "Gemini-3.1-Flash-Lite",
        "bot": {"handle": "Gemini-3.1-Flash-Lite"},
        "text": "非常",
    }

    assert resolve_bot_message_delta(message, "") == "非常"


def test_followup_bot_message_delta_removes_previous_text():
    message = {
        "author": "Gemini-3.1-Flash-Lite",
        "bot": {"handle": "Gemini-3.1-Flash-Lite"},
        "text": "非常抱歉",
    }

    assert resolve_bot_message_delta(message, "非常") == "抱歉"


def test_repeated_bot_message_delta_is_empty():
    message = {
        "author": "Gemini-3.1-Flash-Lite",
        "bot": {"handle": "Gemini-3.1-Flash-Lite"},
        "text": "非常",
    }

    assert resolve_bot_message_delta(message, "非常") == ""


def test_human_message_delta_is_empty():
    message = {
        "author": "human",
        "authorUser": {"id": 1},
        "text": "非常",
    }

    assert resolve_bot_message_delta(message, "") == ""
