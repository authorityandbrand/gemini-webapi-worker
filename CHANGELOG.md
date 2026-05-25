# Changelog — gemini-webapi-worker

## 2026-05-25: Fix model selection + SNlM0e wiring (critical)

### Fixed
- **Model selection had no effect.** `generateViaWebCookie` computed `webModel`
  from `WEB_MODELS` but never sent it. `buildModelHeaders()` (the
  `x-goog-ext-525001229-jspb` model selector) was never called, so every request
  used the account-default model regardless of `model` / `task_type`. The model
  id is now passed through `batchExecute()` and attached as a request header.
- **SNlM0e token was never consumed.** batchexecute sent `at: ""` and
  `getSessionData()` only fetched the SA bearer token — it never read the
  `gemini_snlm0e` key that `scripts/push-session.py` seeds into the shared CACHE
  namespace. From Cloudflare IPs this triggers Google's `/sorry` abuse page. New
  `fetchSnlm0e()` reads `gemini_snlm0e` from `KV_CACHE` (falls back to `KV`) and
  populates the `at` param. Cache is cleared on `/rotate`, `/cookies/update`, and
  401/403 auth failures.

### Added
- `/health` `cookie_rotation.cached_snlm0e` flag for debugging the token path.

### Still open (not addressed here)
- Cookie read path (`fetchNLMCookies`) does not read the KV keys
  `push-session.py` writes (`nlm:cookie_jar_v2` / `gemini_auth`); it relies on
  the `GOOGLE_AUTH` / `NLM` bindings instead.
- `AI_GATEWAY` binding is declared but `generateViaWorkersAI` does not route
  through it; `gem`, `temperature`, and `max_tokens` are accepted but not applied.
- Docs below this entry still reference the removed `GEMINI_API_KEY` path.

---

## 2026-05-02: AI Gateway Binding, GEMINI_API_KEY Added, Secret Cleanup

### Added
- Formal `[[ai_gateways]]` binding in `wrangler.toml` — Workers AI fallback now routes through `automation-hub` AI Gateway
- `GEMINI_API_KEY` secret added — was missing, blocking Gemini function calling via API key path (Path A in `handleOpenAICompletions`)

### Changed
- `generateViaWorkersAI()` in `src/index.js` now passes `{ gateway: { id: "automation-hub" } }` as third argument to `env.AI.run()`
- `wrangler.toml`: `AI_GATEWAY_ID` var retained for backward-compat URL construction; formal `[[ai_gateways]]` binding added alongside it
- Service binding and secrets sections rewritten with role documentation and dead-secret identification

### Architecture Clarification
- Claude model calls: `HUB` service binding → claude-brain → `claude.ai` (SESSION_KEY in claude-brain). NOT Anthropic API.
- Gemini API calls: `GEMINI_API_KEY` → `gateway.ai.cloudflare.com/v1/{account}/automation-hub/google-ai-studio/...`
- Workers AI fallback: `env.AI.run()` with `{ gateway: { id: "automation-hub" } }`

### Security — Secret Cleanup
Removed 14 dead secrets that were never read by any active code path:
`APISID`, `HSID`, `OSID`, `SAPISID`, `SECURE_1PAPISID`, `SECURE_1PSID`, `SECURE_1PSIDTS`, `SID`, `SSID`, `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REFRESH_TOKEN`

Remaining active secrets: `GEMINI_API_KEY`, `SESSION_KEY`, `SESSION_PUSH_KEY`, `CLOUDFLARE_API_TOKEN`, `GITHUB_TOKEN`

---

## 2026-04-13: Gemini Notebook CRUD + Workspace Relay + Cookie Requirements

### IMPORTANT: Cookie Requirements

**All batchexecute operations (Gem CRUD, notebook CRUD) require:**
1. Fresh cookies from Chrome Canary Profile 1 via `browser_cookie3`
2. `snlm0e` CSRF token cached in KV key `gemini_snlm0e` (extracted by Python SDK init)
3. Cookies pushed to KV keys: `notebooklm_auth`, `gemini_auth`

**Without these, batchexecute hits Google's `/sorry` abuse detection from Cloudflare IPs.**

The `/generate` endpoint (StreamGenerate) does NOT need `snlm0e` and works with basic cookies. But batchexecute endpoints for Gem and Notebook operations require the full token.

**Cookie refresh procedure:**
```bash
# 1. Extract from Chrome Canary (requires browser_cookie3)
cd ~/notebooklm-worker && python3.13 scripts/sync-chrome-cookies.py

# 2. Extract snlm0e via Python SDK
cd ~/Gemini-API-Developer && python3.13 -c "
import asyncio, json, sys, time
sys.path.insert(0, 'src')
from gemini_webapi.client import GeminiClient
async def main():
    data = json.load(open('cookies-authorityandbrand.json'))
    cookies = data.get('cookies', data)
    client = GeminiClient(cookies.get('__Secure-1PSID',''), cookies.get('__Secure-1PSIDTS',''))
    client.cookies = cookies
    await client.init(timeout=30, auto_close=False, auto_refresh=False, verbose=False)
    snlm0e = client.access_token
    json.dump({'value': snlm0e, 'ts': time.time()}, open('/tmp/snlm0e_cache.json','w'))
    print(f'snlm0e: {snlm0e[:30]}...')
    await client.close()
asyncio.run(main())
"

# 3. Push snlm0e to KV
cd ~/gemini-webapi-worker
npx wrangler kv key put --namespace-id "e0ba67cf57c24ab49a7a3be5f20ece05" \
  "gemini_snlm0e" --path /tmp/snlm0e_cache.json --remote

# 4. Rotate worker cache
curl -s -X POST https://gemini-webapi-worker.authorityandbrand.workers.dev/rotate
```

### Added — Notebook CRUD via GEMINI binding
- 6 notebook GRPC IDs: `LIST_NOTEBOOKS`, `GET_NOTEBOOK`, `ADD_SOURCE`, `DELETE_SOURCE`, `DELETE_NOTEBOOK`, `READ_SOURCE_CONTENT`
- New `GEMINI` binding type in WORKSPACE_ACTIONS — routes notebook ops through worker's own batchexecute (same auth as /generate)
- `handleGeminiNotebook()` function: list, create, get, add_source, read_source, query operations
- `parseBatchResponse()` now keys by both identifier AND RPC ID for reliable frame extraction
- `getSessionData()` falls back to KV-cached `snlm0e` when page extraction fails (datacenter IP blocked)
- `batchExecute()` makes `at` (snlm0e) parameter optional

### Added — Workspace Relay
- 59 total actions (33 GWS + 26 notebook)
- `POST /workspace/execute` — direct JSON action execution
- `POST /workspace/chat` — natural language → Gemini plans → auto-execute
- `POST /workspace/run` — named workflow recipes (10 workflows)
- `GET /workspace/actions` — action catalog
- `GET /workspace/workflows` — recipe catalog (40 recipes, 10 personas)
- 30+ action aliases for natural language resolution
- Account-aware system prompt (authorityandbrand@gmail.com)

### Added — Notebook Studio actions
- generate_audio, generate_report, generate_briefing, generate_faq, generate_timeline, generate_study_guide
- create_note, list_notes, generate_mind_map
- share_notebook, notebook_share_status, notebook_conversations

### Fixed
- Removed 4 dead aliases (create_folder, rename_file, move_file, delete_file)
- Removed 5 dead NLM_INTERNAL handlers

### Notebook routing
- `create_notebook`, `list_notebooks`, `get_notebook`, `query_notebook`, `add_source`, `add_text_source`, `get_source_content`, `list_sources` → GEMINI binding (worker's own batchexecute)
- `search_notebooks` → NLM binding (KV-cached catalog, always works)
- `start_research`, studio actions → NLM binding
