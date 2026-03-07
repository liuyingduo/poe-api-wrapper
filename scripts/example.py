import httpx
import openai


http_client = httpx.Client(
    trust_env=False,
    timeout=httpx.Timeout(60.0, connect=5.0),
)

client = openai.OpenAI(
    api_key="svc_app_zaiwen",
    base_url="http://207.180.218.216:8003/v1/",
    http_client=http_client,
)


def generate_music_link(prompt: str) -> str:
    resp = client.chat.completions.create(
        model="hailuo-music-v1.5",
        messages=[{"role": "user", "content": prompt}],
        stream=False,
    )
    content = (resp.choices[0].message.content or "").strip()
    if not content:
        raise RuntimeError("hailuo-music-v1.5 返回为空，未拿到音乐链接")
    return content


def generate_nano_banana_image(prompt: str, size: str = "16x9", image_size: str = "1K") -> str:
    payload = {
        "model": "nano-banana-2",
        "prompt": prompt,
        "n": 1,
        "size": size,
        "extra_body": {"image_size": image_size},
    }
    resp = client.images.generate(**payload)
    if not resp.data:
        raise RuntimeError("nano-banana-2 返回为空，未拿到图片链接")
    first = resp.data[0]
    if first is None or not first.url:
        raise RuntimeError("nano-banana-2 返回为空，未拿到图片链接")
    return first.url


if __name__ == "__main__":
    music_url = generate_music_link("生成一段轻快的 lo-fi 音乐，时长约 20 秒")
    print("Music URL:", music_url)

    image_url = generate_nano_banana_image(
        "A minimalist cat poster, warm color palette",
        size="16x9",
        image_size="1K",
    )
    print("Nano-Banana URL:", image_url)
