import io
import httpx
import openai

http_client = httpx.Client(
    trust_env=False,  # Do not route localhost calls through HTTP_PROXY/HTTPS_PROXY.
    timeout=httpx.Timeout(60.0, connect=5.0),
)
client = openai.OpenAI(
    api_key="svc_app_zaiwen",
    base_url="http://127.0.0.1:8003/v1/",
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
# print()

# # ── Vision / Image-Input Example ───────────────────────────────────────────────
IMAGE_URL = "https://ossnew.zaiwen.top/images/e95211642901a534d1ae572b5615f138b2b78c07aade05f2265626d0410c8deb.jpeg"

# image_input = client.chat.completions.create(
#     model="GPT-4o",
#     messages=[
#         {
#             "role": "user",
#             "content": [
#                 {"type": "text", "text": "What's in this image?"},
#                 {"type": "image_url", "image_url": {"url": IMAGE_URL}},
#             ],
#         }
#     ],
# )
# print(image_input.choices[0].message.content)

# ── Image-Edit Example ─────────────────────────────────────────────────────────
# Download the reference image and pass it as a file-like object.
image_bytes = http_client.get(IMAGE_URL).content
edit_resp = client.images.edit(
    model="playground-v2.5",
    image=("reference.jpg", image_bytes, "image/jpeg"),
    prompt="add a little flower",
    n=1,
    size="1024x1024",
)
print(edit_resp.data[0].url)
