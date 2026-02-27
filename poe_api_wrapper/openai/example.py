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

# # Non-Streaming Example
# response = client.chat.completions.create(
#     model="gpt-3.5-turbo", 
#     messages = [
#                 {"role": "system", "content": "You are a helpful assistant."},
#                 {"role": "user", "content": "Hello!"}
#             ]
# )

# print(response.choices[0].message.content)

# # Streaming Example
# stream = client.chat.completions.create(
#     model="gpt-3.5-turbo", 
#     messages = [
#                 {"role": "user", "content": "this is a test request, write a short poem"}
#             ],
#     stream=True
# )

# for chunk in stream:
#     if chunk.choices[0].delta.content is not None:
#         print(chunk.choices[0].delta.content, end="")
        
image_input = client.chat.completions.create(
    model="GPT-4o",
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What's in this image?"},
                {
                    "type": "image_url",
                    "image_url": "https://ossnew.zaiwen.top/images/e95211642901a534d1ae572b5615f138b2b78c07aade05f2265626d0410c8deb.jpeg",
                },
            ],
        }
    ]
)

print(image_input.choices[0].message.content)

images_url = client.images.generate(
  model="playground-v2.5",
  prompt="A cute baby sea otter",
  n=1, # The number of images to generate
)

print(images_url)
