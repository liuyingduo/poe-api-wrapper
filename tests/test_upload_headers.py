import pytest

from poe_api_wrapper.reverse.api import PoeApi
from poe_api_wrapper.reverse.async_api import AsyncPoeApi
from poe_api_wrapper.service import helpers


class FakeClient:
    def __init__(self, headers):
        self.headers = headers

    def close(self):
        return None


def test_sync_finish_upload_headers_match_browser_upload_fingerprint():
    api = PoeApi.__new__(PoeApi)
    api.client = FakeClient(
        {
            "Poe-Formkey": "formkey",
            "Referer": "https://poe.com/chat/7lhxzj710vnp4eufb4e",
        }
    )

    headers = api._build_finish_upload_headers()

    assert headers["Poe-Formkey"] == "formkey"
    assert headers["Referer"] == "https://poe.com/chat/7lhxzj710vnp4eufb4e"
    assert "Chrome/148.0.0.0" in headers["User-Agent"]
    assert '"Chromium";v="148"' in headers["Sec-Ch-Ua"]
    assert "Content-Type" not in headers
    assert "Origin" not in headers


def test_async_finish_upload_headers_match_browser_upload_fingerprint():
    api = AsyncPoeApi.__new__(AsyncPoeApi)
    api.client = FakeClient(
        {
            "Poe-Formkey": "formkey",
            "Referer": "https://poe.com/chat/7lhxzj710vnp4eufb4e",
        }
    )

    headers = api._build_finish_upload_headers()

    assert headers["Poe-Formkey"] == "formkey"
    assert headers["Referer"] == "https://poe.com/chat/7lhxzj710vnp4eufb4e"
    assert "Chrome/148.0.0.0" in headers["User-Agent"]
    assert '"Chromium";v="148"' in headers["Sec-Ch-Ua"]
    assert "Content-Type" not in headers
    assert "Origin" not in headers


@pytest.mark.asyncio
async def test_split_content_collects_file_url_attachments():
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "请分析这个文件"},
                {"type": "file", "file": {"url": "https://example.test/source.py"}},
            ],
        }
    ]

    text_messages, attachment_urls = await helpers.__split_content(messages)

    assert text_messages == [{"role": "user", "content": "请分析这个文件"}]
    assert attachment_urls == ["https://example.test/source.py"]
