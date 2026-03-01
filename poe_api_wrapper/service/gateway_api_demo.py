import json
import httpx


# Hard-coded service config
BASE_URL = "http://127.0.0.1:8000"
SERVICE_API_KEY = "svc_app_a"

# Hard-coded models
CHAT_MODEL = "gpt-4o-mini"
IMAGE_MODEL = "playground-v2.5"


def auth_headers() -> dict:
    return {
        "Authorization": f"Bearer {SERVICE_API_KEY}",
        "Content-Type": "application/json",
    }


def list_models(client: httpx.Client) -> None:
    url = f"{BASE_URL}/v1/models"
    resp = client.get(url, headers=auth_headers(), timeout=30)
    print(f"[GET] {url} -> {resp.status_code}")
    print(resp.text[:1200])
    print("-" * 80)


def chat_completion(client: httpx.Client) -> None:
    url = f"{BASE_URL}/v1/chat/completions"
    payload = {
        "model": CHAT_MODEL,
        "messages": [
            {"role": "system", "content": "You are a concise assistant."},
            {"role": "user", "content": "用一句话介绍北京。"},
        ],
        "stream": False,
        "metadata": {
            "session_id": "demo-session-001"
        },
    }
    resp = client.post(url, headers=auth_headers(), json=payload, timeout=120)
    print(f"[POST] {url} -> {resp.status_code}")
    print(resp.text[:1200])
    print("-" * 80)


def image_generation(client: httpx.Client) -> None:
    url = f"{BASE_URL}/v1/images/generations"
    payload = {
        "model": IMAGE_MODEL,
        "prompt": "A watercolor painting of a small red boat on a quiet lake at sunrise.",
        "n": 1,
        "size": "1024x1024",
    }
    resp = client.post(url, headers=auth_headers(), json=payload, timeout=180)
    print(f"[POST] {url} -> {resp.status_code}")
    print(resp.text[:1200])

    if resp.status_code == 200:
        data = resp.json()
        if data.get("data") and len(data["data"]) > 0:
            print("Image URL:", data["data"][0].get("url"))
    print("-" * 80)


def main() -> None:
    with httpx.Client() as client:
        print("Gateway demo started")
        print(f"BASE_URL={BASE_URL}")
        print(f"CHAT_MODEL={CHAT_MODEL}")
        print(f"IMAGE_MODEL={IMAGE_MODEL}")
        print("-" * 80)

        list_models(client)
        chat_completion(client)
        image_generation(client)

        print("Gateway demo finished")


if __name__ == "__main__":
    main()

