import io
import httpx
import openai

http_client = httpx.Client(
    trust_env=False,  # Do not route localhost calls through HTTP_PROXY/HTTPS_PROXY.
    timeout=httpx.Timeout(60.0, connect=5.0),
)
client = openai.OpenAI(
    api_key="svc_app_zaiwen",
    base_url="http://207.180.218.216:8003/v1/",
    http_client=http_client,
)

# ── Non-Streaming Example ──────────────────────────────────────────────────────
# response = client.chat.completions.create(
#     model="gpt-3.5-turbo",
#     messages=[
#         {"role": "system", "content": "You are a helpful assistant."},
#         {"role": "user", "content": "Hello!"},
#     ],
# )
# print(response.choices[0].message.content)

# # ── Streaming Example ──────────────────────────────────────────────────────────
# stream = client.chat.completions.create(
#     model="gpt-3.5-turbo",
#     messages=[
#         {"role": "user", "content": "this is a test request, write a short poem"},
#     ],
#     stream=True,
# )
# for chunk in stream:
#     if chunk.choices[0].delta.content is not None:
#         print(chunk.choices[0].delta.content, end="")
# # print()

# # # ── Vision / Image-Input Example ───────────────────────────────────────────────
IMAGE_URL = "https://ossnew.zaiwen.top/images/e95211642901a534d1ae572b5615f138b2b78c07aade05f2265626d0410c8deb.jpeg"

image_input = client.chat.completions.create(
    model="GPT-4o",
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What's in this image?"},
                {"type": "image_url", "image_url": {"url": IMAGE_URL}},
            ],
        }
    ],
)
print(image_input.choices[0].message.content)

# ── Image-Edit Example ─────────────────────────────────────────────────────────
# Download the reference image and pass it as a file-like object.
image_bytes = http_client.get(IMAGE_URL).content
edit_resp = client.images.edit(
    model="flux-2-klein-9b-base",
    image=("reference.jpg", image_bytes, "image/jpeg"),
    prompt="add a little flower",
    n=1,
    size="1024x1024",
)
print(edit_resp.data[0].url)


import concurrent.futures
import time

TOTAL_REQUESTS = 1
PROMPT = "A cute girl with a little flower"
MODEL = "flux-2-klein-9b-base"

def generate_image(index: int):
    try:
        start = time.time()
        result = client.images.generate(
            model=MODEL,
            prompt=PROMPT,
            n=1,
        )
        elapsed = time.time() - start
        url = result.data[0].url if result.data else None
        print(f"[{index:03d}] 成功 ({elapsed:.2f}s): {url}")
        return {"index": index, "success": True, "url": url, "elapsed": elapsed}
    except Exception as e:
        print(f"[{index:03d}] 失败: {e}")
        return {"index": index, "success": False, "error": str(e)}

print(f"开始发起 {TOTAL_REQUESTS} 个并发图片生成请求...")
overall_start = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=TOTAL_REQUESTS) as executor:
    futures = {executor.submit(generate_image, i): i for i in range(1, TOTAL_REQUESTS + 1)}
    results = [future.result() for future in concurrent.futures.as_completed(futures)]

overall_elapsed = time.time() - overall_start
success_count = sum(1 for r in results if r["success"])
fail_count = TOTAL_REQUESTS - success_count

print(f"\n===== 测试完成 =====")
print(f"总请求数: {TOTAL_REQUESTS}")
print(f"成功: {success_count} | 失败: {fail_count}")
print(f"总耗时: {overall_elapsed:.2f}s")

