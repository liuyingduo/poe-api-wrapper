# Mongo Gateway Quickstart

## 1) Required Environment Variables

You can use either:

1. Process environment variables, or
2. A config file (`.env.gateway`) loaded automatically at startup.

Recommended: copy template and fill values.

```bash
cp .env.gateway.example .env.gateway
```

The server loads config from:

- `GATEWAY_CONFIG_FILE` (if specified), then
- `./.env.gateway`

```bash
MONGODB_URI=mongodb://127.0.0.1:27017
MONGODB_DB=poe_gateway
FERNET_KEY=<fernet-key>
ADMIN_API_KEY=<admin-key>
SERVICE_API_KEYS_BOOTSTRAP=<service-key-1>,<service-key-2>
POE_REVISION=b759a4647fa291beba4792e9139bc4c014399434
DAILY_RESET_TIMEZONE=America/Los_Angeles
DAILY_RESET_HOUR=0
DAILY_RESET_POINT_BALANCE=3000

# Optional tuning
MAX_INFLIGHT_PER_ACCOUNT=2
GLOBAL_INFLIGHT_LIMIT=200
DEPLETED_THRESHOLD=20
REFRESH_INTERVAL_SECONDS=600
RECENT_ACTIVE_MINUTES=120
COOLDOWN_SECONDS=120
PREWARM_CONCURRENCY=5

# Startup behavior controls
PREWARM_ON_STARTUP=true
PREWARM_ACCOUNT_TIMEOUT_SECONDS=20
AUTO_FETCH_POE_REVISION=true
```

### What are `ADMIN_API_KEY` and `SERVICE_API_KEYS_BOOTSTRAP`?

- `ADMIN_API_KEY`:
  - The management key used only for:
    - `POST /admin/accounts/upsert`
    - `GET /admin/accounts`
  - You define it yourself as a strong random string.
  - Example: `ADMIN_API_KEY=adm_7Qm2...`

- `SERVICE_API_KEYS_BOOTSTRAP`:
  - Initial business API keys (comma-separated) for OpenAI-compatible endpoints, e.g. `/v1/chat/completions`.
  - The gateway hashes and stores them in Mongo on startup.
  - Client requests must use one of these keys in:
    - `Authorization: Bearer <service-key>`
  - Example:
    - `SERVICE_API_KEYS_BOOTSTRAP=svc_app_a,svc_app_b`
    - Then callers can use `Bearer svc_app_a`

Notes:

- `ADMIN_API_KEY` should never be used by external business callers.
- Keep admin and service keys strictly separated.
- `POE_REVISION` is gateway-level default; account-level `poe_revision` (if provided on upsert) takes precedence.

Generate a fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## 2) Start Service (tmux)

```bash
tmux new -s poe-gateway "uvicorn poe_api_wrapper.service.gateway_api:app --host 0.0.0.0 --port 8000 --workers 1"
```

## 3) Add / Update Account

```bash
curl -X POST "http://127.0.0.1:8000/admin/accounts/upsert" ^
  -H "Authorization: Bearer <ADMIN_API_KEY>" ^
  -H "Content-Type: application/json" ^
  -d "{\"email\":\"user@example.com\",\"poe_p_b\":\"...\",\"poe_cf_clearance\":\"...\",\"p_lat\":\"...\",\"poe_cf_bm\":\"...\",\"user_agent\":\"Mozilla/5.0 ...\"}"
```

## 4) Query Accounts

```bash
curl "http://127.0.0.1:8000/admin/accounts?page=1&page_size=20" ^
  -H "Authorization: Bearer <ADMIN_API_KEY>"
```

## 5) Chat Completions

```bash
curl -X POST "http://127.0.0.1:8000/v1/chat/completions" ^
  -H "Authorization: Bearer <SERVICE_API_KEY>" ^
  -H "Content-Type: application/json" ^
  -d "{\"model\":\"gpt-4o-mini\",\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}],\"metadata\":{\"session_id\":\"s1\"}}"
```

## 6) Health Endpoints

- `GET /healthz`
- `GET /readyz`
