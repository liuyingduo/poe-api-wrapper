# poe-api-wrapper (Local Service Mode)

This repository is now organized as a **local service project**.

- No packaging / publishing workflow
- API service layer and reverse-engineered client layer are separated
- Service layer directly imports reverse layer

## Project Layout

```text
.
├─ .env.gateway                     # local runtime config (single source)
├─ .env.gateway.example            # config template
├─ main.py                         # start local gateway service
├─ requirements.txt
├─ scripts/
│  ├─ smoke.py                     # reverse layer smoke test
│  ├─ refresh_all_points.py        # refresh account points in Mongo
│  ├─ manual_e2e_test.py           # interactive manual integration test
│  └─ example.py                   # OpenAI-compatible gateway example client
├─ tests/                          # unit tests (gateway utility + selection)
└─ poe_api_wrapper/
   ├─ __init__.py                  # lazy exports, no side effects
   ├─ __main__.py                  # reverse CLI entry
   ├─ cli.py
   ├─ llm.py                       # compatibility re-export to service layer
   ├─ reverse/
   │  ├─ api.py
   │  ├─ async_api.py
   │  ├─ bundles.py
   │  ├─ queries.py
   │  ├─ utils.py
   │  ├─ proxies.py
   │  └─ example.py
   └─ service/
      ├─ gateway_api.py            # FastAPI app + OpenAI-compatible routes
      ├─ gateway.py                # account repository / selector / pool / refresher
      ├─ helpers.py
      ├─ types.py
      ├─ models.json
      ├─ MONGO_GATEWAY.md
      └─ gateway_api_demo.py
```

## Layer Dependency

- Reverse layer: `poe_api_wrapper.reverse.*`
- Service layer: `poe_api_wrapper.service.*`
- Allowed direction: `service -> reverse`
- Avoid importing service code from reverse layer

## Run Locally

1. Create venv and install dependencies.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

2. Prepare gateway env file.

```bash
copy .env.gateway.example .env.gateway
```

3. Start service.

```bash
python main.py
```

Equivalent:

```bash
uvicorn poe_api_wrapper.service.gateway_api:app --host 127.0.0.1 --port 8000 --workers 1
```

## Reverse Layer Usage

```python
from poe_api_wrapper.reverse import PoeApi

client = PoeApi(tokens={"p-b": "...", "p-lat": "..."})
for chunk in client.send_message(bot="Assistant", message="hello"):
    print(chunk.get("response", ""), end="")
```

## Service Layer Usage

```python
from poe_api_wrapper.service import start_server

start_server(address="127.0.0.1", port="8000")
```

## Scripts

- `python scripts/smoke.py`
- `python scripts/refresh_all_points.py --dry-run`
- `python scripts/manual_e2e_test.py`
- `python scripts/example.py`

## Tests

```bash
pytest -q tests
```

`tox.ini` is configured to run `pytest -q tests`.

## Notes

- Packaging files were removed intentionally.
- `poe_api_wrapper.__init__` has no proxy/network side effects at import time.
- For service setup details, read `poe_api_wrapper/service/MONGO_GATEWAY.md`.
