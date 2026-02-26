import os
from pathlib import Path
from poe_api_wrapper import PoeApi


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]

        os.environ.setdefault(key, value)


load_dotenv()

tokens = {
    "p-b": os.environ["POE_P_B"],
    "formkey": os.environ["POE_FORMKEY"],
    "cf_clearance": os.environ["POE_CF_CLEARANCE"],
}

if os.environ.get("POE_CF_BM"):
    tokens["__cf_bm"] = os.environ["POE_CF_BM"]
if os.environ.get("POE_P_LAT"):
    tokens["p-lat"] = os.environ["POE_P_LAT"]
if os.environ.get("POE_REVISION"):
    tokens["poe-revision"] = os.environ["POE_REVISION"]

headers = {
    # Keep this aligned with the browser where cookies were generated.
    "User-Agent": os.environ.get(
        "POE_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
    ),
    "Referer": "https://poe.com/login",
    "Origin": "https://poe.com",
}

client = PoeApi(tokens=tokens, headers=headers)
bot_name = os.environ.get("POE_BOT", "Assistant")
prompt = os.environ.get("POE_PROMPT", "Hello, reply with one short line to confirm test success.")

for chunk in client.send_message(bot=bot_name, message=prompt):
    print(chunk.get("response", ""), end="", flush=True)

print("\n\nDone")
