from __future__ import annotations

import math
import re
from typing import Any

import tiktoken


DEFAULT_POINT_BALANCE_LIMIT = 300

_POINT_RATE_RE = re.compile(
    r"(?P<prefix>up to\s+)?(?P<points>\d+(?:\.\d+)?)\s*(?:points?|积分)\s*/\s*(?:1k\s*tokens|千词元)",
    re.I,
)
_POINT_MESSAGE_RE = re.compile(
    r"(?P<prefix>up to\s+)?(?P<points>\d+(?:\.\d+)?)\s*(?:points?|积分)\s*/\s*(?:message|条消息)",
    re.I,
)


def _encoder():
    return tiktoken.get_encoding("cl100k_base")


def _normalize_rate_type(value: str) -> str:
    raw = str(value or "").strip().lower()
    if "输入" in raw and "图" in raw:
        return "input_image"
    if "输入" in raw:
        return "input_text"
    if "输出" in raw and "图" in raw:
        return "output_image"
    if "输出" in raw and "文本" in raw:
        return "output_text"
    if "缓存" in raw:
        return "cache_discount"

    text = re.sub(r"[^a-z0-9]+", "_", raw)
    text = re.sub(r"_+", "_", text).strip("_")
    aliases = {
        "input_text": "input_text",
        "input": "input_text",
        "input_image": "input_image",
        "output_text": "output_text",
        "text_output": "output_text",
        "output_image": "output_image",
        "image_output": "output_image",
        "image_generation": "image_generation",
        "cache_discount": "cache_discount",
    }
    return aliases.get(text, text)


def _parse_point_rate(cell: str) -> dict[str, Any]:
    text = str(cell or "")
    token_match = _POINT_RATE_RE.search(text)
    if token_match:
        return {
            "points_per_1k_tokens": float(token_match.group("points")),
            "is_upper_bound": bool(token_match.group("prefix")),
        }

    message_match = _POINT_MESSAGE_RE.search(text)
    if message_match:
        return {
            "points_per_message": float(message_match.group("points")),
            "is_upper_bound": bool(message_match.group("prefix")),
        }

    return {}


def parse_rate_menu_markdown(markdown: str) -> dict[str, Any]:
    rates: dict[str, dict[str, Any]] = {}
    table_count = 0

    for line in str(markdown or "").splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or not stripped.endswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) < 3:
            continue
        first_cell = cells[0].strip()
        if not first_cell or set(first_cell) <= {"-"} or first_cell.lower() == "type":
            if set(first_cell) <= {"-"}:
                table_count += 1
            continue

        rate_type = _normalize_rate_type(first_cell)
        parsed = _parse_point_rate(cells[2])
        if not parsed:
            continue

        existing = rates.get(rate_type)
        if existing:
            for key in ("points_per_1k_tokens", "points_per_message"):
                if key in parsed and key in existing:
                    parsed[key] = max(float(existing[key]), float(parsed[key]))
        rates[rate_type] = parsed

    return {
        "rates": rates,
        "has_multiple_tables": table_count > 1,
    }


def _content_text_tokens(content: Any) -> int:
    encoder = _encoder()
    if isinstance(content, str):
        return len(encoder.encode(content))
    if not isinstance(content, list):
        return len(encoder.encode(str(content)))

    total = 0
    for item in content:
        if isinstance(item, str):
            total += len(encoder.encode(item))
            continue
        if not isinstance(item, dict):
            total += len(encoder.encode(str(item)))
            continue
        item_type = item.get("type")
        if item_type == "text":
            total += len(encoder.encode(str(item.get("text") or "")))
        elif "text" in item:
            total += len(encoder.encode(str(item.get("text") or "")))
    return total


def count_chat_completion_message_tokens(messages: Any) -> int:
    if not isinstance(messages, list):
        return len(_encoder().encode(str(messages)))

    total = 0
    encoder = _encoder()
    for message in messages:
        total += 3
        if not isinstance(message, dict):
            total += len(encoder.encode(str(message)))
            continue
        total += len(encoder.encode(str(message.get("role") or "")))
        total += _content_text_tokens(message.get("content"))
        if message.get("name"):
            total += 1 + len(encoder.encode(str(message.get("name"))))
        if message.get("tool_call_id"):
            total += len(encoder.encode(str(message.get("tool_call_id"))))
        if message.get("tool_calls"):
            total += len(encoder.encode(str(message.get("tool_calls"))))
    return total + 3


def estimate_input_points_from_tokens(pricing: dict[str, Any], token_count: int) -> int:
    rates = pricing.get("rates") if isinstance(pricing, dict) else None
    if not isinstance(rates, dict):
        return 0

    input_rate = rates.get("input_text") or rates.get("input")
    if not isinstance(input_rate, dict):
        return 0
    points_per_1k = input_rate.get("points_per_1k_tokens")
    if points_per_1k is None:
        return 0
    return int(math.ceil((max(0, token_count) / 1000.0) * float(points_per_1k)))


def estimate_image_generation_input_points(pricing: dict[str, Any], prompt: str, image_count: int) -> int:
    prompt_points = estimate_input_points_from_tokens(
        pricing,
        len(_encoder().encode(str(prompt or ""))),
    )
    rates = pricing.get("rates") if isinstance(pricing, dict) else None
    if not isinstance(rates, dict):
        return prompt_points

    image_rate = rates.get("image_generation") or rates.get("output_image")
    if not isinstance(image_rate, dict):
        return prompt_points
    points_per_message = image_rate.get("points_per_message")
    if points_per_message is None:
        return prompt_points
    return prompt_points + int(math.ceil(max(1, image_count) * float(points_per_message)))
