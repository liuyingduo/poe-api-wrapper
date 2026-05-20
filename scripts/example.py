import httpx
import openai
from pathlib import Path

API_KEY = "svc_app_zaiwen"
BASE_URL = "http://192.3.31.234:8003/v1/"
RUN_EXAMPLE = "file_analysis"
IMAGE_URL = "https://ossnew.zaiwen.top/images/9e71ceb8f21a0279dda1f26c7ac1b957d6493e00552ee6e3048d1b1bcbce44cb_part_1.jpeg"
FILE_ANALYSIS_URL = "https://oss.zaiwen.top/hbeed69d299094f89.txt"

http_client = httpx.Client(
    trust_env=False,
    timeout=httpx.Timeout(60.0, connect=5.0),
)
client = openai.OpenAI(
    api_key=API_KEY,
    base_url=BASE_URL,
    http_client=http_client,
)


def print_first_url(label: str, response) -> None:
    if not getattr(response, "data", None):
        raise RuntimeError(f"{label} did not return any result")
    print(f"{label}: {response.data[0].url}")


def print_exception(label: str, exc: Exception) -> None:
    print(f"{label} failed: {type(exc).__name__}: {exc}")
    body = getattr(exc, "body", None)
    if body is not None:
        print(f"{label} error body: {body}")
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            print(f"{label} raw response: {response.text}")
        except Exception:
            pass


def run_chat_stream() -> None:
    stream = client.chat.completions.create(
        model="gpt-5.4-mini",
        messages=[
            {"role": "user", "content": "请用中文总结一下：自动驾驶里，数据为什么比模型更重要？"},
        ],
        stream=True,
        extra_body={"parameters": {"web_search": True}},
    )
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            print(chunk.choices[0].delta.content, end="")
    print()


def run_vision_input() -> None:
    stream = client.chat.completions.create(
        model="GPT-4o",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What's in this image?exaple for 500 word"},
                    {"type": "image_url", "image_url": {"url": IMAGE_URL}},
                ],
            }
        ],
        stream=True,
    )
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            print(chunk.choices[0].delta.content, end="")
    print()


def run_file_analysis() -> None:
    stream = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "请分析这个 Python 文件的实现。\n\n"
                            "请用中文输出，包含这几部分：\n"
                            "1. 这个文件的核心职责\n"
                            "2. 主要函数和执行流程\n"
                            "3. 代码里的潜在问题或可改进点\n"
                            "4. 如果我要继续维护它，最该先关注什么"
                        ),
                    },
                    {
                        "type": "file",
                        "file": {"url": FILE_ANALYSIS_URL},
                    },
                ],
            }
        ],
        stream=True,
    )
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            print(chunk.choices[0].delta.content, end="")
    print()


def run_nano_banana_image() -> None:
    # 推荐写法：走服务层统一参数。
    # 这会被服务转换成 Poe 需要的 parameters:
    # {
    #   "aspect_ratio": "21:9",
    #   "image_size": "1K",
    #   "image_only": true
    # }
    response = client.images.generate(
        model="Nano-Banana-2",
        prompt="一只猫",
        n=1,
        size="21:9",
        extra_body={"image_size": "2K", "image_only": True},
    )
    print_first_url("Nano-Banana-2", response)

    # 如果你想完全按浏览器抓包的 Poe 原始参数传，也可以这样写：
    # response = client.images.generate(
    #     model="Nano-Banana-2",
    #     prompt="一只猫",
    #     n=1,
    #     extra_body={"parameters": {"aspect_ratio": "21:9", "image_size": "1K"}},
    # )


def run_grok_image() -> None:
    # 推荐写法：服务会把 size="3:2" 转成 Poe 的 parameters.aspect = "3:2"
    response = client.images.generate(
        model="Grok-Imagine-Image",
        prompt="一枝花",
        n=1,
        size="3:2",
    )
    print_first_url("Grok-Imagine-Image", response)

    # 原始 Poe 参数写法：
    # response = client.images.generate(
    #     model="Grok-Imagine-Image",
    #     prompt="一枝花",
    #     n=1,
    #     extra_body={"parameters": {"aspect": "3:2"}},
    # )


def run_image_edit() -> None:
    image_bytes = http_client.get(IMAGE_URL).content
    response = client.images.edit(
        model="flux-2-klein-9b-base",
        image=("reference.jpg", image_bytes, "image/jpeg"),
        prompt="add a little flower",
        n=1,
        size="1024x1024",
    )
    print_first_url("Image edit", response)


def run_nano_banana_reference_probe() -> None:
    image_bytes = http_client.get(IMAGE_URL).content

    cases = [
        (
            "nano-banana-2 edit via SDK multipart",
            lambda: client.images.edit(
                model="Nano-Banana-2",
                image=("reference.jpg", image_bytes, "image/jpeg"),
                prompt="add a little flower, and generate a wide-screen version with the same subject based on the reference image",
                n=1,
                size="21:9",
                extra_body={"image_size": "1K"},
            ),
        ),
        (
            "nano-banana-2 edit via raw JSON URL",
            lambda: http_client.post(
                f"{BASE_URL.rstrip('/')}/images/edits",
                headers={"Authorization": f"Bearer {API_KEY}"},
                json={
                    "model": "Nano-Banana-2",
                    "image": IMAGE_URL,
                    "prompt": "add a little flower, and generate a wide-screen version with the same subject based on the reference image",
                    "n": 1,
                    "size": "21:9",
                    "image_size": "1K",
                },
            ),
        ),
        (
            "nano-banana-2 edit via raw JSON URL with raw parameters",
            lambda: http_client.post(
                f"{BASE_URL.rstrip('/')}/images/edits",
                headers={"Authorization": f"Bearer {API_KEY}"},
                json={
                    "model": "Nano-Banana-2",
                    "image": IMAGE_URL,
                    "prompt": "add a little flower, and generate a wide-screen version with the same subject based on the reference image",
                    "n": 1,
                    "parameters": {
                        "aspect_ratio": "21:9",
                        "image_size": "1K",
                        "image_only": True,
                    },
                },
            ),
        ),
    ]

    for label, runner in cases:
        print(f"\n=== {label} ===")
        try:
            result = runner()
            if isinstance(result, httpx.Response):
                print(f"status={result.status_code}")
                try:
                    payload = result.json()
                except Exception:
                    payload = result.text
                print(payload)
                result.raise_for_status()
            else:
                print_first_url(label, result)
        except Exception as exc:
            print_exception(label, exc)


if __name__ == "__main__":
    if RUN_EXAMPLE == "chat_stream":
        run_chat_stream()
    elif RUN_EXAMPLE == "vision_input":
        run_vision_input()
    elif RUN_EXAMPLE == "file_analysis":
        run_file_analysis()
    elif RUN_EXAMPLE == "nano_banana_image":
        run_nano_banana_image()
    elif RUN_EXAMPLE == "grok_image":
        run_grok_image()
    elif RUN_EXAMPLE == "image_edit":
        run_image_edit()
    elif RUN_EXAMPLE == "nano_banana_reference_probe":
        run_nano_banana_reference_probe()
    else:
        raise ValueError(f"Unsupported RUN_EXAMPLE: {RUN_EXAMPLE}")
