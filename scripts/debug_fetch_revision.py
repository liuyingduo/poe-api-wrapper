"""Debug the two-step poe-revision fetch flow.

Usage:
  python scripts/debug_fetch_revision.py
  python scripts/debug_fetch_revision.py --cf-clearance YOUR_CF_CLEARANCE
  python scripts/debug_fetch_revision.py --dump-login-html login.html
  python scripts/debug_fetch_revision.py --proxy http://127.0.0.1:7897

依赖：
  pip install curl_cffi

注意：
  - 必须安装 curl_cffi（模拟 Chrome TLS 指纹，绕过 Cloudflare Managed Challenge）
  - 若在中国大陆网络下还需配置代理，默认使用 http://127.0.0.1:7897
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    import httpx
    HAS_CURL_CFFI = False
    print(
        "[警告] 未找到 curl_cffi，回退到 httpx。"
        "Cloudflare Managed Challenge 将无法绕过。\n"
        "建议运行: pip install curl_cffi",
        file=sys.stderr,
    )


# 旧路径：translations/<40位hash>/（已废弃）
POE_REVISION_RE_OLD = re.compile(r"poecdn\.net/assets/translations/([a-f0-9]{40})/")
# 新路径：从 link 响应头或 HTML 中提取 Next.js buildId（_next/static/<buildId>/）
POE_REVISION_RE_NEW = re.compile(r"poecdn\.net/assets/_next/static/([^/]+)/(?:chunks|css)/")
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "zh-CN,zh;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    ),
}


def _title_of(html: str) -> str:
    m = TITLE_RE.search(html)
    if not m:
        return ""
    return " ".join(m.group(1).split())


def _preview(text: str, limit: int = 180) -> str:
    return text[:limit].replace("\n", " ").replace("\r", " ")


def _find_cookie_value(resp: httpx.Response, cookie_name: str) -> str:
    value = resp.cookies.get(cookie_name)
    if value:
        return value
    for set_cookie in resp.headers.get_list("set-cookie"):
        if set_cookie.startswith(f"{cookie_name}="):
            return set_cookie.split(";", 1)[0].split("=", 1)[1]
    return ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug poe-revision fetch flow.")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds.")
    parser.add_argument(
        "--cf-clearance",
        default="",
        help="Optional cf_clearance cookie. If empty, tries env POE_FETCH_CF_CLEARANCE/POE_CF_CLEARANCE.",
    )
    parser.add_argument(
        "--dump-login-html",
        default="",
        help="Optional path to dump login response HTML for inspection.",
    )
    parser.add_argument(
        "--proxy",
        default="",
        help=(
            "HTTP/HTTPS proxy URL, e.g. http://127.0.0.1:7897. "
            "If empty, tries env HTTPS_PROXY / HTTP_PROXY / ALL_PROXY. "
            "在中国大陆网络环境下此参数必须提供，否则 Cloudflare 会返回 403。"
        ),
    )
    return parser.parse_args()


def _resolve_proxy(args: argparse.Namespace) -> str:
    """优先级：命令行 > HTTPS_PROXY > HTTP_PROXY > ALL_PROXY > 硬编码默认值"""
    return (
        args.proxy.strip()
        or os.getenv("HTTPS_PROXY", "").strip()
        or os.getenv("https_proxy", "").strip()
        or os.getenv("HTTP_PROXY", "").strip()
        or os.getenv("http_proxy", "").strip()
        or os.getenv("ALL_PROXY", "").strip()
        or os.getenv("all_proxy", "").strip()
        or "http://127.0.0.1:7897"
    )


def _extract_revision(html: str, link_header: str) -> str:
    """从 HTML 或 link 响应头中提取 poe-revision / Next.js buildId。"""
    # 优先用新格式正则搜索 link 响应头
    for pattern in (POE_REVISION_RE_NEW, POE_REVISION_RE_OLD):
        m = pattern.search(link_header) or pattern.search(html)
        if m:
            return m.group(1)
    return ""


def main() -> int:
    args = parse_args()
    cf_clearance = (
        args.cf_clearance.strip()
        or os.getenv("POE_FETCH_CF_CLEARANCE", "").strip()
        or os.getenv("POE_CF_CLEARANCE", "").strip()
    )

    proxy_url = _resolve_proxy(args)
    print(f"[proxy] {proxy_url or '(none)'}")
    print(f"[engine] {'curl_cffi (Chrome impersonation)' if HAS_CURL_CFFI else 'httpx (fallback, may be blocked by Cloudflare)'}")

    if HAS_CURL_CFFI:
        # curl_cffi：模拟 Chrome145 TLS 指纹，直接绕过 Cloudflare
        session_kwargs = dict(impersonate="chrome131", timeout=args.timeout)
        if proxy_url:
            session_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}

        with cffi_requests.Session(**session_kwargs) as session:
            print("[1/2] GET https://poe.com/")
            home = session.get("https://poe.com/", headers=HEADERS, allow_redirects=False)
            p_b = home.cookies.get("p-b", "")
            cf_bm = home.cookies.get("__cf_bm", "")

            print(f"  status={home.status_code}")
            print(f"  location={home.headers.get('location', '')}")
            print(f"  p-b_present={bool(p_b)} __cf_bm_present={bool(cf_bm)}")

            # 优先从 307 的 link 响应头提取 revision
            home_link = home.headers.get("link", "")
            revision = _extract_revision("", home_link)
            if revision:
                print(f"\nSUCCESS (from 307 link header): poe-revision={revision}")
                return 0

            cookies: dict[str, str] = {}
            if p_b:
                cookies["p-b"] = p_b
            if cf_bm:
                cookies["__cf_bm"] = cf_bm
            if cf_clearance:
                cookies["cf_clearance"] = cf_clearance

            print("\n[2/2] GET https://poe.com/login?redirect_url=%2F")
            print(f"  sending_cookies={list(cookies.keys())}")
            login = session.get(
                "https://poe.com/login?redirect_url=%2F",
                headers=HEADERS,
                cookies=cookies if cookies else None,
                allow_redirects=False,
            )
            print(f"  status={login.status_code}")
            print(f"  content_type={login.headers.get('content-type', '')}")
            print(f"  title={_title_of(login.text)!r}")
            print(f"  body_preview={_preview(login.text)!r}")

            if args.dump_login_html:
                dump_path = Path(args.dump_login_html)
                dump_path.write_text(login.text, encoding="utf-8")
                print(f"  dumped_login_html={dump_path.resolve()}")

            link_header = login.headers.get("link", "")
            revision = _extract_revision(login.text, link_header)
            if revision:
                print(f"\nSUCCESS (from login): poe-revision={revision}")
                return 0

            if "Just a moment" in login.text:
                print("\nFAILED: 仍然遇到 Cloudflare challenge，请尝试更换 impersonate 版本或提供 cf_clearance cookie")
            else:
                print("\nFAILED: No poe-revision pattern found.")
                print("  提示：请用 --dump-login-html login.html 保存页面后人工检查")
            return 2

    else:
        # 回退到 httpx（大概率被 Cloudflare 拦截）
        client_kwargs = dict(timeout=args.timeout, follow_redirects=False)
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        with httpx.Client(**client_kwargs) as client:
            print("[1/2] GET https://poe.com/")
            home = client.get("https://poe.com/", headers=HEADERS)
            p_b = client.cookies.get("p-b") or _find_cookie_value(home, "p-b")
            cf_bm = client.cookies.get("__cf_bm") or _find_cookie_value(home, "__cf_bm")

            print(f"  status={home.status_code}")
            print(f"  location={home.headers.get('location', '')}")
            print(f"  p-b_present={bool(p_b)} __cf_bm_present={bool(cf_bm)}")

            home_link = home.headers.get("link", "")
            revision = _extract_revision("", home_link)
            if revision:
                print(f"\nSUCCESS (from 307 link header): poe-revision={revision}")
                return 0

            cookies: dict[str, str] = {}
            if p_b:
                cookies["p-b"] = p_b
            if cf_bm:
                cookies["__cf_bm"] = cf_bm
            if cf_clearance:
                cookies["cf_clearance"] = cf_clearance

            print("\n[2/2] GET https://poe.com/login?redirect_url=%2F")
            login = client.get(
                "https://poe.com/login?redirect_url=%2F",
                headers=HEADERS,
                cookies=cookies if cookies else None,
            )
            print(f"  status={login.status_code}")
            print(f"  title={_title_of(login.text)!r}")
            print(f"  body_preview={_preview(login.text)!r}")

            if args.dump_login_html:
                Path(args.dump_login_html).write_text(login.text, encoding="utf-8")
                print(f"  dumped_login_html={Path(args.dump_login_html).resolve()}")

            link_header = login.headers.get("link", "")
            revision = _extract_revision(login.text, link_header)
            if revision:
                print(f"\nSUCCESS (from login): poe-revision={revision}")
                return 0

            if "Just a moment" in login.text:
                print("\nFAILED: Cloudflare challenge — 请安装 curl_cffi: pip install curl_cffi")
            else:
                print("\nFAILED: No poe-revision pattern found.")
            return 2

    return 2


if __name__ == "__main__":
    sys.exit(main())