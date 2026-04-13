# Changelog — gemini-webapi-worker

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
