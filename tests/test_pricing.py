from poe_api_wrapper.service.pricing import (
    count_chat_completion_message_tokens,
    estimate_input_points_from_tokens,
    parse_rate_menu_markdown,
)


def test_parse_rate_menu_markdown_text_rates():
    markdown = """
| Type | Rate (USD) | Rate (Points) |
|------|------|------|
| Input | $4.29/1M tokens | 142 points/1k tokens |
| Output (text) | $21.46/1M tokens | 709 points/1k tokens |
"""

    pricing = parse_rate_menu_markdown(markdown)

    assert pricing["rates"]["input_text"]["points_per_1k_tokens"] == 142
    assert pricing["rates"]["output_text"]["points_per_1k_tokens"] == 709


def test_parse_rate_menu_markdown_chinese_rates():
    markdown = """
| 类型 | 费率（USD） | 评分（分数） |
|------|------|------|
| 输入 | **$4.29**/百万词元 | 142积分/千词元 |
| 输出（文本） | **$21.46**/百万词元 | 709积分/千词元 |
"""

    pricing = parse_rate_menu_markdown(markdown)

    assert pricing["rates"]["input_text"]["points_per_1k_tokens"] == 142
    assert pricing["rates"]["output_text"]["points_per_1k_tokens"] == 709


def test_estimate_input_points_from_tokens_rounds_up():
    pricing = {"rates": {"input_text": {"points_per_1k_tokens": 142}}}

    assert estimate_input_points_from_tokens(pricing, 1001) == 143


def test_count_chat_completion_message_tokens_accepts_openai_content_parts():
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "image_url", "image_url": {"url": "https://example.test/a.png"}},
            ],
        }
    ]

    assert count_chat_completion_message_tokens(messages) > 0


def test_parse_rate_menu_markdown_multiple_tables():
    markdown = """
| Type | Rate (USD) | Rate (Points) |
|------|------|------|
| Input | $2.25/1M tokens | 75 points/1k tokens |
| Output (text) | $9.0/1M tokens | 300 points/1k tokens |

 _Image Generation_
| Type | Rate (USD) | Rate (Points) |
|------|------|------|
| Input (text) | $4.53/1M tokens | 151 points/1k tokens |
| High fidelity editing | $0.06 | 2000 points |
"""
    pricing = parse_rate_menu_markdown(markdown)
    assert pricing["rates"]["input_text"]["points_per_1k_tokens"] == 75
    assert pricing["rates"]["output_text"]["points_per_1k_tokens"] == 300
    assert pricing["rates"]["high_fidelity_editing"]["points_per_message"] == 2000
