/**
 * Gemini WebAPI Cloudflare Worker  v5.3.1
 *
 * NEW in v5.2:
 *   - Drive file context: pass `drive_file_ids` in /generate to inject Google Drive
 *     file content as grounding (fetched via GWS service binding, non-blocking)
 *   - Resilience: all context sources (NLM, Drive) are non-blocking — worker answers
 *     even if every binding is down; falls back through API → web-cookie → Workers AI
 *   - HUB fallback: if AI Gateway is unreachable, retries direct Gemini API endpoint
 *   - Extension support documented in /health; inner[6]=[1] already enables @YouTube,
 *     @Gmail, @Maps, @Flights, @Hotels, @Finance in web-cookie mode prompts
 *
 * NEW in v5.1:
 *   - MCP server: POST /mcp (Streamable HTTP transport, MCP spec 2025-03-26)
 *     Connect: claude.ai → Settings → Integrations → Add custom integration
 *     URL: https://gemini-webapi-worker.authorityandbrand.workers.dev/mcp
 *     Tools: gemini_generate, gemini_list_gems, gemini_create_gem, gemini_delete_gem,
 *            nlm_catalog_search, nlm_notebook_query, nlm_source_content
 *
 * NEW in v5:
 *   - Gem CRUD: GET /gems, POST /gems, PUT /gems/:id, DELETE /gems/:id
 *   - gem param in /generate (web-cookie: inner[19]; api: system prefix)
 *   - Model selection with correct x-goog-ext headers for web-cookie path
 *   - Rich response: thoughts, images, candidates, session (cid/rid/rcid)
 *   - POST /generate/stream — Server-Sent Events streaming
 *   - Promise.allSettled + AbortSignal timeouts for resilient NLM calls
 *
 * FULL FEATURE MATRIX (what this worker can do standalone):
 *   AUTH        GEMINI_API_KEY (official API) | SECURE_1PSID (web, full features) | Workers AI (fallback)
 *   GENERATE    POST /generate — prompt + model + system + gem + chat_meta
 *   STREAM      POST /generate/stream — SSE: thoughts / candidate / done
 *   GEMS        GET|POST /gems, PUT|DELETE /gems/:id  (requires SECURE_1PSID)
 *   EXTENSIONS  @YouTube @Gmail @Maps @Flights @Hotels @Finance in prompt  (SECURE_1PSID)
 *   NLM         Auto-grounding from 122 notebooks / 2690 legal sources  (NLM binding)
 *   DRIVE       drive_file_ids in /generate injects file text as context  (GWS binding)
 *   MCP         POST /mcp — 7 tools for claude.ai integration
 *   MODELS      9 web-cookie models: gemini-3-flash/pro/thinking ± plus/advanced
 *   MULTI-TURN  chat_meta=[cid,rid,...] for persistent conversation context
 *
 * Auth: Web-cookie ONLY (Gemini Advanced subscription)
 *   1. R2_AUTH bucket "auth-state.json" → shared with NLM worker (single source of truth)
 *   2. SECURE_1PSID / SESSION_KEY worker secrets → fallback if R2 unavailable
 *   3. env.AI → Workers AI Gemma-3 (emergency fallback, no external keys)
 *
 * Service bindings (all optional — worker degrades gracefully if any are absent):
 *   HUB  → ai-automation-hub   (AI Gateway proxy for official API)
 *   NLM  → notebooklm-worker   (122 legal notebooks, catalog KV cache)
 *   GWS  → gws-worker          (Drive file content, Gmail, Calendar, 80 tools)
 *   AI   → Workers AI          (Gemma-3 fallback generation)
 */

// ---------- Constants ----------

const ACCOUNT_ID    = "e105d76aa6c851abdbd13d34d901cc7c";
const GW_ID         = "automation-hub";
const GEMINI_GW     = `https://gateway.ai.cloudflare.com/v1/${ACCOUNT_ID}/${GW_ID}/google-ai-studio/v1beta/models`;
const WORKERS_MODEL = "@cf/google/gemma-3-12b-it";

const ENDPOINT_INIT       = "https://gemini.google.com/app";
const ENDPOINT_GENERATE   = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate";
const ENDPOINT_BATCH_EXEC = "https://gemini.google.com/_/BardChatUi/data/batchexecute";

// Inner request list indices (mirrors constants.py)
const GEM_FLAG_INDEX            = 19;
const STREAMING_FLAG_INDEX      = 7;
const TEMPORARY_CHAT_FLAG_INDEX = 45;
const DEFAULT_METADATA = ["", "", "", null, null, null, null, null, null, ""];

// GRPC IDs for gem and notebook operations
const GRPC = {
  LIST_GEMS:   "CNgdBe",
  CREATE_GEM:  "oMH3Zd",
  UPDATE_GEM:  "kHv0Vd",
  DELETE_GEM:  "UXcSJb",
  // Notebook RPCs (from constants.py GRPC enum)
  LIST_NOTEBOOKS:      "NXpLKc",
  GET_NOTEBOOK:        "HcT8bb",
  ADD_SOURCE:          "ko3zcd",
  DELETE_SOURCE:       "AptDmf",
  DELETE_NOTEBOOK:     "Nwkn9",
  READ_SOURCE_CONTENT: "tr032e",
  // CREATE_NOTEBOOK reuses CREATE_GEM ("oMH3Zd") with [2] type flag
  // RENAME/INSTRUCT reuses UPDATE_GEM ("kHv0Vd") with notebook payload
};

// Model headers for web-cookie path (from constants.py build_model_header)
const WEB_MODELS = {
  "gemini-3-flash":                  { id: "fbb127bbb056c959", cap: 1 },
  "gemini-3-pro":                    { id: "9d8ca3786ebdfbea", cap: 1 },
  "gemini-3-flash-thinking":         { id: "5bf011840784117a", cap: 1 },
  "gemini-3-flash-plus":             { id: "56fdd199312815e2", cap: 4 },
  "gemini-3-pro-plus":               { id: "e6fa609c3fa255c0", cap: 4 },
  "gemini-3-flash-thinking-plus":    { id: "e051ce1aa80aa576", cap: 4 },
  "gemini-3-flash-advanced":         { id: "56fdd199312815e2", cap: 2 },
  "gemini-3-pro-advanced":           { id: "e6fa609c3fa255c0", cap: 2 },
  "gemini-3-flash-thinking-advanced":{ id: "e051ce1aa80aa576", cap: 2 },
};

// Map user-facing model names to official Gemini API model IDs
const API_MODEL_MAP = {
  "gemini-3-flash":                   "gemini-2.5-flash",
  "gemini-3-pro":                     "gemini-2.5-pro",
  "gemini-3-flash-thinking":          "gemini-2.5-flash",
  "gemini-3-flash-plus":              "gemini-2.5-flash",
  "gemini-3-pro-plus":                "gemini-2.5-pro",
  "gemini-3-flash-thinking-plus":     "gemini-2.5-flash",
  "gemini-3-flash-advanced":          "gemini-2.5-flash",
  "gemini-3-pro-advanced":            "gemini-2.5-pro",
  "gemini-3-flash-thinking-advanced": "gemini-2.5-flash",
};

// NLM context tuning
const NLM_CATALOG_LIMIT  = 5;
const NLM_MAX_SUMMARIES  = 3;
const NLM_MAX_SOURCES    = 5;      // was 2 — too thin for grounding
const NLM_SOURCE_CHARS   = 6000;   // was 3000 — need enough context for Gemini to prefer sources over web knowledge
const NLM_SUMMARY_CHARS  = 1200;
const NLM_TIMEOUT_MS     = 8000;

// Drive context tuning
const DRIVE_FILE_CHARS   = 8000;  // chars per Drive file injected as context
const DRIVE_TIMEOUT_MS   = 10000;

// Fallback direct Gemini endpoint (used if AI Gateway is unreachable)
const GEMINI_DIRECT = "https://generativelanguage.googleapis.com/v1beta/models";

// Cookie rotation endpoint (refreshes __Secure-1PSIDTS)
const ENDPOINT_ROTATE = "https://accounts.google.com/RotateCookies";
const ROTATE_MIN_INTERVAL_MS = 60_000;  // 60s between rotations to avoid 429

// ---------- Retry helper for service bindings ----------

/**
 * Fetch via a service binding with exponential backoff retry.
 * Retries on network errors and 5xx responses. 4xx responses are returned immediately.
 * @param {object} fetcher - Service binding (e.g. env.GOOGLE_AUTH)
 * @param {Request} request - Request to send (will be cloned on each attempt)
 * @param {number} maxRetries - Maximum number of attempts (default 3)
 * @returns {Response}
 */
async function fetchWithRetry(fetcher, request, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const resp = await fetcher.fetch(request.clone());
      if (resp.ok) return resp;
      if (resp.status >= 500 && attempt < maxRetries - 1) {
        await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 200));
        continue;
      }
      return resp;
    } catch (e) {
      if (attempt === maxRetries - 1) throw e;
      await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 200));
    }
  }
}

// ---------- Shared auth: fetch cookies from NLM worker ----------

let _nlmCookies = null;       // Cached cookies fetched from NLM worker
let _nlmCookiesFetchedAt = 0;
const NLM_COOKIE_CACHE_MS = 300_000; // Re-fetch from NLM every 5 min

/**
 * Fetch fresh auth cookies from the SINGLE source of truth: R2_AUTH bucket.
 * This is the same R2 bucket the NLM worker reads from, written by `notebooklm login`.
 *
 * Priority:
 *   1. R2_AUTH bucket "auth-state.json" — THE canonical auth store
 *   2. KV_CACHE key "nlm_auth_cookies" — fast cache of the above
 *   3. NLM worker refresh_auth MCP tool — triggers a refresh if stale
 *
 * All services (Gemini, NLM, GWS) use cookies from this single store.
 */
async function fetchNLMCookies(env) {
  const now = Date.now();
  if (_nlmCookies && (now - _nlmCookiesFetchedAt) < NLM_COOKIE_CACHE_MS) return _nlmCookies;

  // Source 1: Check cookie keys — notebooklm_auth (primary) and profile key (fallback)
  if (env.KV_CACHE) {
    try {
      const [sharedResult, profileResult] = await Promise.allSettled([
        env.KV_CACHE.get("notebooklm_auth", { type: "text" }),
        env.KV_CACHE.get("nlm:cookies:authorityandbrand", { type: "text" }),
      ]);
      const sharedRaw = sharedResult.status === "fulfilled" ? sharedResult.value : null;
      const profileCookies = profileResult.status === "fulfilled" ? profileResult.value : null;

      let sharedStr = null;
      if (sharedRaw) {
        try {
          const shared = JSON.parse(sharedRaw);
          if (shared.cookie_header) { sharedStr = shared.cookie_header; }
        } catch {}
      }

      // Use notebooklm_auth first, fall back to profile cookies
      const cookieStr = sharedStr || profileCookies;
      const source = sharedStr ? "notebooklm_auth"
        : profileCookies ? "nlm:cookies:authorityandbrand" : "none";
      if (cookieStr) {
        _nlmCookies = cookieStr;
        _nlmCookiesFetchedAt = now;
        console.log(`[fetchNLMCookies] Loaded cookies from KV ${source}`);
        return _nlmCookies;
      }
    } catch (err) {
      console.error("KV_CACHE cookie fetch error:", err.message);
    }
  }

  // Source 2: R2_AUTH — backup from `notebooklm login` (may be stale)
  if (env.R2_AUTH) {
    try {
      const obj = await env.R2_AUTH.get("auth-state.json");
      if (obj) {
        const data = await obj.json();
        if (data?.cookies && typeof data.cookies === "object") {
          const cookieStr = Object.entries(data.cookies)
            .map(([k, v]) => `${k}=${v}`)
            .join("; ");
          if (cookieStr) {
            _nlmCookies = cookieStr;
            _nlmCookiesFetchedAt = now;
            return _nlmCookies;
          }
        }
      }
    } catch (err) {
      console.error("R2_AUTH cookie lookup error:", err.message);
    }
  }

  // Source 3: NLM worker refresh_auth — triggers a refresh, then re-read KV
  if (env.NLM) {
    try {
      await env.NLM.fetch(
        new Request("https://notebooklm-worker.internal/mcp", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            jsonrpc: "2.0", id: 1,
            method: "tools/call",
            params: { name: "refresh_auth", arguments: {} },
          }),
          signal: AbortSignal.timeout(10000),
        })
      );
      // After NLM refresh, re-read notebooklm_auth from KV
      if (env.KV_CACHE) {
        const sharedRaw = await env.KV_CACHE.get("notebooklm_auth", { type: "text" });
        if (sharedRaw) {
          try {
            const shared = JSON.parse(sharedRaw);
            if (shared.cookie_header) {
              _nlmCookies = shared.cookie_header;
              _nlmCookiesFetchedAt = now;
              return _nlmCookies;
            }
          } catch {}
        }
      }
    } catch (err) {
      console.error("fetchNLMCookies NLM refresh error:", err.message);
    }
  }
  return null;
}

// ---------- Cookie rotation ----------

let _lastRotateAt = 0;
let _cachedPSIDTS = null;

/**
 * Rotate __Secure-1PSIDTS via Google RotateCookies endpoint.
 * Caches result in KV and isolate memory to avoid 429s.
 */
async function rotateCookies(env) {
  const now = Date.now();

  // Rate-limit: skip if rotated recently
  if (_cachedPSIDTS && (now - _lastRotateAt) < ROTATE_MIN_INTERVAL_MS) {
    return _cachedPSIDTS;
  }

  // Check KV cache first (survives isolate restarts)
  if (env.KV) {
    try {
      const cached = await env.KV.get("rotated_psidts", { type: "json" });
      if (cached && (now - cached.ts) < ROTATE_MIN_INTERVAL_MS) {
        _cachedPSIDTS = cached.value;
        _lastRotateAt = cached.ts;
        return cached.value;
      }
    } catch { /* KV miss — continue to rotate */ }
  }

  const psid = env.SECURE_1PSID || env.SESSION_KEY;
  if (!psid) return env.SECURE_1PSIDTS || null;

  try {
    const cookieStr = buildFullCookieString(env);
    const resp = await fetch(ENDPOINT_ROTATE, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Origin": "https://accounts.google.com",
        Cookie: cookieStr,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
      },
      body: '[000,"-0000000000000000000"]',
      redirect: "manual",
    });

    if (resp.status === 401) {
      console.error("RotateCookies: 401 Unauthorized — cookies may be expired");
      return env.SECURE_1PSIDTS || null;
    }

    // Extract __Secure-1PSIDTS from Set-Cookie headers
    const setCookies = resp.headers.getAll?.("set-cookie") ?? [resp.headers.get("set-cookie")].filter(Boolean);
    for (const sc of setCookies) {
      const match = sc.match(/__Secure-1PSIDTS=([^;]+)/);
      if (match) {
        _cachedPSIDTS = match[1];
        _lastRotateAt = now;

        return _cachedPSIDTS;
      }
    }

    // No new PSIDTS in response — use existing
    return env.SECURE_1PSIDTS || null;
  } catch (err) {
    console.error("RotateCookies error:", err.message);
    return env.SECURE_1PSIDTS || null;
  }
}

// ---------- Shared helpers ----------

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  });
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-API-Key",
  };
}

function getAuthMode(env) {
  // API key mode takes priority if set (enables grounding + NLM injection)
  if (env.GEMINI_API_KEY) return "gemini-api";
  // Web-cookie — uses Gemini Advanced subscription
  // Cookies come from R2_AUTH (shared with NLM worker) or worker secrets
  if (env.SECURE_1PSID || env.SESSION_KEY || _nlmCookies || env.R2_AUTH || env.NLM) return "web-cookie";
  if (env.AI) return "workers-ai";
  return null;
}

/**
 * Build a full cookie string with all auth-related Google cookies.
 * Uses rotated PSIDTS if available, falls back to env secret.
 */
function buildFullCookieString(env) {
  // If we have NLM-sourced cookies (shared auth), use them directly —
  // they're a complete cookie string from a live Playwright session
  if (_nlmCookies) {
    // _nlmCookies can be a cookie string or an object {key: value}
    if (typeof _nlmCookies === "string") return _nlmCookies;
    if (typeof _nlmCookies === "object") {
      return Object.entries(_nlmCookies).map(([k, v]) => `${k}=${v}`).join("; ");
    }
  }

  // Fallback: build from individual worker secrets
  const psid   = env.SECURE_1PSID || env.SESSION_KEY;
  const psidts = _cachedPSIDTS || env.SECURE_1PSIDTS;

  const cookies = [];
  if (psid)   cookies.push(`__Secure-1PSID=${psid}`);
  if (psidts) cookies.push(`__Secure-1PSIDTS=${psidts}`);

  // Additional session cookies (set via FULL_COOKIES secret or individual secrets)
  if (env.SID)    cookies.push(`SID=${env.SID}`);
  if (env.HSID)   cookies.push(`HSID=${env.HSID}`);
  if (env.SSID)   cookies.push(`SSID=${env.SSID}`);
  if (env.APISID) cookies.push(`APISID=${env.APISID}`);
  if (env.SAPISID) cookies.push(`SAPISID=${env.SAPISID}`);
  if (env.SECURE_1PAPISID) cookies.push(`__Secure-1PAPISID=${env.SECURE_1PAPISID}`);

  return cookies.join("; ");
}

function buildCookieString(env) {
  return buildFullCookieString(env);
}

function buildModelHeaders(modelName) {
  const m = WEB_MODELS[modelName];
  if (!m) return {};
  return {
    "x-goog-ext-525001261-jspb": `[1,null,null,null,"${m.id}",null,null,0,[4],null,null,${m.cap}]`,
    "x-goog-ext-73010989-jspb": "[0]",
    "x-goog-ext-73010990-jspb": "[0]",
  };
}

// ---------- Session auth ----------

const CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const CHROME_HEADERS = {
  "User-Agent": CHROME_UA,
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
  "Sec-Ch-Ua-Mobile": "?0",
  "Sec-Ch-Ua-Platform": '"Windows"',
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Site": "same-origin",
  "Sec-Fetch-User": "?1",
  "Upgrade-Insecure-Requests": "1",
};

/**
 * Mirrors the Python library's auth flow:
 *   1. Rotate cookies (refresh PSIDTS)
 *   2. Preflight to www.google.com to collect session cookies (like curl_cffi does)
 *   3. Fetch gemini.google.com/app with combined cookies
 *   4. Handle /sorry abuse redirects by extracting GOOGLE_ABUSE_EXEMPTION cookie
 *   5. Extract SNlM0e access token from HTML
 */
async function getSessionData(env) {
  // Step 0: Try to fetch fresh cookies from NLM worker (shared auth)
  await fetchNLMCookies(env);

  // Step 1: Rotate cookies before fetching session
  await rotateCookies(env);

  const authCookies = buildCookieString(env);

  // Step 2: Preflight to www.google.com with NO auth cookies (mirrors Python lib)
  // Python lib creates a fresh session, visits google.com, collects NID/1P_JAR etc.
  let preflightCookies = "";
  try {
    const pfResp = await fetch("https://www.google.com", {
      headers: { ...CHROME_HEADERS },  // No Cookie header — clean visit
      redirect: "follow",
    });
    // Extract Set-Cookie headers from preflight
    const pfSetCookies = pfResp.headers.getAll?.("set-cookie") ?? [pfResp.headers.get("set-cookie")].filter(Boolean);
    const extraCookies = [];
    for (const sc of pfSetCookies) {
      const m = sc.match(/^([^=]+)=([^;]+)/);
      if (m) extraCookies.push(`${m[1]}=${m[2]}`);
    }
    preflightCookies = extraCookies.join("; ");
  } catch (e) {
    // Preflight is optional — continue without it
  }

  // Merge: preflight first (NID, 1P_JAR, etc.), then auth cookies override
  // This matches Python lib: jar.update(extra_cookies) where extra = preflight
  const mergedCookies = preflightCookies
    ? `${preflightCookies}; ${authCookies}`
    : authCookies;

  // Log cookie names for debugging (visible in worker logs)
  const cookieNames = mergedCookies.split(";").map(c => c.trim().split("=")[0]).filter(Boolean);
  const requiredCookies = ["__Secure-1PSID", "__Secure-1PSIDTS", "__Secure-1PSIDCC", "SID", "HSID", "SSID", "APISID", "SAPISID", "__Secure-1PAPISID"];
  const missingCookies = requiredCookies.filter(r => !cookieNames.includes(r));
  console.log(`[getSessionData] cookie_count=${cookieNames.length}, names=${cookieNames.join(",")}`);
  if (missingCookies.length) console.warn(`[getSessionData] MISSING required cookies: ${missingCookies.join(", ")}`);

  // Step 3: Fetch gemini.google.com/app with full browser-like headers
  let html = "";
  let lastUrl = ENDPOINT_INIT;
  const maxRedirects = 5;

  for (let attempt = 0; attempt < maxRedirects; attempt++) {
    const resp = await fetch(lastUrl, {
      headers: {
        ...CHROME_HEADERS,
        Cookie: mergedCookies,
        Referer: "https://www.google.com/",
      },
      redirect: "manual",
    });

    // Step 4: Handle /sorry abuse redirect — extract GOOGLE_ABUSE_EXEMPTION
    const location = resp.headers.get("location");
    if (resp.status >= 300 && resp.status < 400 && location) {
      // Check for GOOGLE_ABUSE_EXEMPTION in Set-Cookie
      const setCookies = resp.headers.getAll?.("set-cookie") ?? [resp.headers.get("set-cookie")].filter(Boolean);
      for (const sc of setCookies) {
        const abuseMatch = sc.match(/GOOGLE_ABUSE_EXEMPTION=([^;]+)/);
        if (abuseMatch) {
          // Re-merge with abuse exemption cookie and retry
          const abuseCookie = `GOOGLE_ABUSE_EXEMPTION=${abuseMatch[1]}`;
          const retryResp = await fetch(ENDPOINT_INIT, {
            headers: {
              ...CHROME_HEADERS,
              Cookie: `${mergedCookies}; ${abuseCookie}`,
              Referer: "https://www.google.com/",
            },
            redirect: "follow",
          });
          if (retryResp.ok) {
            html = await retryResp.text();
            break;
          }
        }
      }
      // Follow redirect
      lastUrl = location.startsWith("http") ? location : `https://gemini.google.com${location}`;
      continue;
    }

    if (!resp.ok) throw new Error(`Gemini init fetch failed: HTTP ${resp.status} at ${lastUrl}`);
    html = await resp.text();
    break;
  }

  if (!html) throw new Error("Gemini init: no response after redirect chain");

  let snlm0e = html.match(/"SNlM0e":\s*"([^"]+)"/)?.[1];
  if (!snlm0e) {
    // Fallback: check KV for cached snlm0e (pushed by local Python SDK)
    if (env.KV_CACHE) {
      try {
        const cached = await env.KV_CACHE.get("gemini_snlm0e", { type: "json" });
        if (cached?.value && (Date.now() / 1000 - (cached.ts || 0)) < 3600) {
          console.log(`[getSessionData] Using cached snlm0e from KV (age: ${Math.floor(Date.now()/1000 - cached.ts)}s)`);
          snlm0e = cached.value;
        }
      } catch {}
    }
    if (!snlm0e) {
      const htmlSnippet = html.slice(0, 500);
      const hasSorry = html.includes("/sorry/index");
      const hasServiceLogin = html.includes("accounts.google.com/ServiceLogin");
      console.error(`[getSessionData] SNlM0e extraction failed. lastUrl=${lastUrl}, htmlLen=${html.length}, hasSorry=${hasSorry}, hasServiceLogin=${hasServiceLogin}`);
      if (hasSorry) throw new Error("Gemini init blocked by /sorry — use KV cache: push snlm0e from local Python SDK");
      if (hasServiceLogin) throw new Error("Gemini init: cookies invalid — redirected to login");
      throw new Error(`Could not extract SNlM0e from ${lastUrl} (${html.length} bytes)`);
    }
  }

  return {
    snlm0e,
    buildLabel: html.match(/"cfb2h":\s*"([^"]+)"/)?.[1] ?? "",
    sessionId:  html.match(/"FdrFJe":\s*"([^"]+)"/)?.[1] ?? "",
    language:   html.match(/"TuX5cc":\s*"([^"]+)"/)?.[1] ?? "en",
  };
}

// ---------- BATCH_EXEC helper ----------

/**
 * Execute one or more RPCs against the Gemini BATCH_EXEC endpoint.
 * Returns a map of { identifier → parsed_payload } for each wrb.fr response.
 *
 * rpcs: [{ rpcid, payload, identifier? }, ...]
 */
async function batchExecute(rpcs, env, session, { sourcePath = "/app" } = {}) {
  const { snlm0e, buildLabel, sessionId, language } = session;
  const reqId  = Math.floor(Math.random() * 90000) + 10000;
  const rpcids = rpcs.map(r => r.rpcid).join(",");

  // f.req: [[rpcid, payload_str, null, identifier], ...]
  const fReq = rpcs.map(r => [r.rpcid, r.payload, null, r.identifier ?? "generic"]);

  const params = new URLSearchParams({
    rpcids,
    hl: language,
    _reqid: String(reqId),
    rt: "c",
    "source-path": sourcePath,
  });
  if (buildLabel) params.set("bl", buildLabel);
  if (sessionId)  params.set("f.sid", sessionId);

  const cookieStr = buildCookieString(env);
  const reqHeaders = {
    "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
    "Origin": "https://gemini.google.com",
    "Referer": "https://gemini.google.com/",
    "X-Same-Domain": "1",
    "x-goog-ext-525001261-jspb": "[1,null,null,null,null,null,null,null,[4]]",
    "x-goog-ext-73010989-jspb": "[0]",
    Cookie: cookieStr,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
  };
  const bodyParams = { "f.req": JSON.stringify([fReq]) };
  if (snlm0e) bodyParams.at = snlm0e;
  const reqBody = new URLSearchParams(bodyParams).toString();

  const url = `${ENDPOINT_BATCH_EXEC}?${params}`;

  // First attempt with redirect: manual to handle /sorry abuse detection
  let resp = await fetch(url, { method: "POST", headers: reqHeaders, body: reqBody, redirect: "manual" });

  // Handle /sorry redirect — extract GOOGLE_ABUSE_EXEMPTION and retry once
  if (resp.status >= 300 && resp.status < 400) {
    const location = resp.headers.get("location") || "";
    if (location.includes("/sorry")) {
      // Follow the sorry redirect to get the exemption cookie
      const sorryResp = await fetch(location, { headers: { Cookie: cookieStr, "User-Agent": reqHeaders["User-Agent"] }, redirect: "manual" });
      const setCookies = sorryResp.headers.getAll?.("set-cookie") ?? [sorryResp.headers.get("set-cookie")].filter(Boolean);
      let abuseCookie = "";
      for (const sc of setCookies) {
        const m = sc.match(/GOOGLE_ABUSE_EXEMPTION=([^;]+)/);
        if (m) { abuseCookie = `GOOGLE_ABUSE_EXEMPTION=${m[1]}`; break; }
      }
      if (abuseCookie) {
        console.log(`[batchExecute] Got abuse exemption, retrying with exemption cookie`);
        resp = await fetch(url, {
          method: "POST",
          headers: { ...reqHeaders, Cookie: `${cookieStr}; ${abuseCookie}` },
          body: reqBody,
          redirect: "follow",
        });
      } else {
        // No exemption cookie — follow redirect chain normally
        resp = await fetch(url, { method: "POST", headers: reqHeaders, body: reqBody, redirect: "follow" });
      }
    } else {
      // Non-sorry redirect — follow normally
      resp = await fetch(url, { method: "POST", headers: reqHeaders, body: reqBody, redirect: "follow" });
    }
  }

  if (!resp.ok) throw new Error(`BATCH_EXEC HTTP ${resp.status} at ${resp.url || url}`);
  const raw = await resp.text();
  if (raw.length < 50) console.log(`[batchExecute] Short response (${raw.length} chars): ${raw.slice(0, 100)}`);
  return parseBatchResponse(raw);
}

/**
 * Parse a BATCH_EXEC (or StreamGenerate) raw response.
 * Returns { [identifier]: parsedPayload }
 */
function parseBatchResponse(raw) {
  const byId = {};
  for (const line of raw.split("\n")) {
    const t = line.trim();
    if (!t || t.startsWith(")]}'") || /^\d+$/.test(t)) continue;
    try {
      const parsed = JSON.parse(t);
      if (!Array.isArray(parsed)) continue;
      for (const item of parsed) {
        if (!Array.isArray(item) || item[0] !== "wrb.fr" || !item[2]) continue;
        const identifier = item[item.length - 1] ?? "generic";
        const rpcId = item[1] ?? "";
        let payload;
        try { payload = JSON.parse(item[2]); } catch { continue; }
        // Key by both the identifier AND the RPC ID so callers can look up by either
        byId[identifier] = payload;
        if (rpcId) byId[rpcId] = payload;
      }
    } catch {}
  }
  return byId;
}

// ---------- Stream response parser ----------

/**
 * Parse a StreamGenerate raw response into rich output.
 * Returns { text, thoughts, images, candidates, cid, rid, rcid }
 */
function parseStreamResponse(raw) {
  let text = null, thoughts = null;
  let cid = "", rid = "", rcid = "";
  const candidates = [];
  const images = [];

  for (const line of raw.split("\n")) {
    const t = line.trim();
    if (!t || t.startsWith(")]}'") || /^\d+$/.test(t)) continue;
    try {
      const parsed = JSON.parse(t);
      if (!Array.isArray(parsed)) continue;
      for (const item of parsed) {
        if (!Array.isArray(item) || item[0] !== "wrb.fr" || !item[2]) continue;
        let pj;
        try { pj = JSON.parse(item[2]); } catch { continue; }

        // Chat session IDs from metadata
        const mData = pj?.[1];
        if (mData?.[0]) cid = mData[0];
        if (mData?.[1]) rid = mData[1];

        // Candidates
        const candList = pj?.[4] ?? [];
        for (const cand of candList) {
          const candRcid = cand?.[0];
          if (!candRcid) continue;
          rcid = candRcid;

          const candText    = cand?.[1]?.[0] ?? "";
          const candThought = cand?.[37]?.[0]?.[0] ?? "";
          const rawImgs     = cand?.[12]?.[1] ?? [];
          const candImages  = rawImgs
            .map(img => ({ url: img?.[0]?.[0]?.[0] ?? null, alt: img?.[0]?.[4] ?? "" }))
            .filter(img => img.url);

          candidates.push({ rcid: candRcid, text: candText, thoughts: candThought || null, images: candImages });
          // Streaming responses send incremental chunks — take the longest (last) text
          if (candText && (text === null || candText.length > text.length)) text = candText;
          if (candThought && (!thoughts || candThought.length > thoughts.length)) thoughts = candThought;
          images.push(...candImages);
        }
      }
    } catch {}
  }

  if (text === null) throw new Error("Could not parse stream response — no text found.");
  return { text, thoughts: thoughts || null, images, candidates, cid, rid, rcid };
}

// ---------- NLM helpers ----------

async function nlmTool(env, toolName, body = {}) {
  if (!env.NLM) return null;
  const signal = AbortSignal.timeout(NLM_TIMEOUT_MS);
  try {
    const resp = await env.NLM.fetch(
      new Request(`https://notebooklm-worker.internal/tools/${toolName}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal,
      })
    );
    if (!resp.ok) return null;
    return await resp.json();
  } catch { return null; }
}

async function buildNotebookContext(prompt, env, { notebookIds = null, maxSources = NLM_MAX_SOURCES } = {}) {
  let notebooks = [];

  if (notebookIds && notebookIds.length > 0) {
    const settled = await Promise.allSettled(
      notebookIds.map(id => nlmTool(env, "catalog_get", { notebook_id: id }))
    );
    notebooks = settled
      .filter(r => r.status === "fulfilled" && r.value)
      .map(r => r.value.notebook || r.value)
      .filter(Boolean);
  } else {
    const sr = await nlmTool(env, "catalog_search", { query: prompt, limit: String(NLM_CATALOG_LIMIT) });
    notebooks = sr?.results ?? [];
  }

  if (!notebooks.length) {
    if (env.NLM) {
      const health = await nlmHealthCheck(env);
      if (health?.status === "expired" || health?.error) {
        console.warn("[NLM] auth may be expired:", JSON.stringify(health));
      }
    }
    return "";
  }

  const top = notebooks.slice(0, NLM_MAX_SUMMARIES);
  const summaryResults = await Promise.allSettled(
    top.map(nb => nlmTool(env, "notebook_summary", { notebook_id: nb.id }))
  );

  const parts = ["## Knowledge Base Context\n"];
  for (let i = 0; i < top.length; i++) {
    const nb = top[i];
    const sr = summaryResults[i];
    const summary = (sr.status === "fulfilled" ? sr.value?.summary ?? sr.value?.text : "") ?? "";
    if (!summary && !nb.description) continue;
    parts.push(
      `### [${nb.domain?.toUpperCase() ?? "NOTEBOOK"}] ${nb.title}`,
      `_${nb.source_count} sources | keywords: ${(nb.keywords ?? []).join(", ")}_`,
      (summary || nb.description).slice(0, NLM_SUMMARY_CHARS),
      ""
    );
  }

  if (maxSources > 0 && top.length > 0) {
    const topNb = top[0];
    const sourceList = await nlmTool(env, "source_list", { notebook_id: topNb.id });
    const sources = (sourceList?.sources ?? []).slice(0, maxSources);
    if (sources.length > 0) {
      const contentResults = await Promise.allSettled(
        sources.map(s => nlmTool(env, "source_content", { source_id: s.id }))
      );
      parts.push(`### Source Documents from "${topNb.title}"`);
      for (let i = 0; i < sources.length; i++) {
        const src = sources[i];
        const cr = contentResults[i];
        const text = (cr.status === "fulfilled" ? cr.value?.content ?? cr.value?.text : "") ?? "";
        if (!text) continue;
        parts.push(`**${src.title ?? src.id}**\n${text.slice(0, NLM_SOURCE_CHARS)}\n`);
      }
    }
  }

  parts.push("---\nUse the above documents as your primary knowledge base. Cite the notebook/source when referencing specific facts.");
  return parts.join("\n");
}

// ---------- NLM workflow helpers ----------

// Longer timeout for write/generation operations
const NLM_WRITE_TIMEOUT_MS = 30000;

async function nlmWriteTool(env, toolName, body = {}) {
  if (!env.NLM) return null;
  const signal = AbortSignal.timeout(NLM_WRITE_TIMEOUT_MS);
  try {
    const resp = await env.NLM.fetch(
      new Request(`https://notebooklm-worker.internal/tools/${toolName}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal,
      })
    );
    if (!resp.ok) {
      const errText = await resp.text().catch(() => "");
      return { error: true, status: resp.status, message: errText || resp.statusText };
    }
    return await resp.json();
  } catch (e) {
    return { error: true, message: e?.message || "NLM write timeout" };
  }
}

/**
 * Ask a question to a notebook's AI and get an answer grounded in its sources.
 * Unlike buildNotebookContext (which fetches raw source text), this uses NLM's
 * own AI to synthesize an answer with citations.
 */
async function nlmAsk(env, notebookId, question) {
  return nlmWriteTool(env, "notebook_query", { notebook_id: notebookId, query: question });
}

/**
 * Add a URL source to a notebook (web page, YouTube video, etc.)
 */
async function nlmAddSource(env, notebookId, url, title) {
  const body = { notebook_id: notebookId, url };
  if (title) body.title = title;
  return nlmWriteTool(env, "source_add_url", body);
}

/**
 * Add text content as a source to a notebook.
 */
async function nlmAddTextSource(env, notebookId, title, content) {
  return nlmWriteTool(env, "source_add_text", { notebook_id: notebookId, title, content });
}

/**
 * Create a new notebook with optional title.
 */
async function nlmCreateNotebook(env, title) {
  return nlmWriteTool(env, "notebook_create", { title: title || "Untitled" });
}

/**
 * Generate a studio artifact (audio overview, report, quiz, etc.)
 */
async function nlmGenerateArtifact(env, notebookId, artifactType, options = {}) {
  return nlmWriteTool(env, "studio_generate", {
    notebook_id: notebookId,
    type: artifactType,
    ...options,
  });
}

/**
 * Start a research session (web or Drive search) for a notebook.
 */
async function nlmStartResearch(env, notebookId, query, source = "web") {
  return nlmWriteTool(env, "research_start", {
    notebook_id: notebookId,
    query,
    source,
  });
}

/**
 * Create a note in a notebook.
 */
async function nlmCreateNote(env, notebookId, title, content) {
  return nlmWriteTool(env, "note_create", {
    notebook_id: notebookId,
    title,
    content,
  });
}

/**
 * Check NLM auth health — returns status of cookie session.
 */
async function nlmHealthCheck(env) {
  return nlmTool(env, "auth_status");
}

// ---------- GDoc/GSheet ↔ NLM sync workflows ----------
//
// These helpers orchestrate GWS + NLM tools together so Gemini can:
//   1. Create a GDoc/GSheet → add it to an NLM notebook as a living source
//   2. Append/update content → sync the NLM source to pick up changes
//   3. Enable session continuity: write findings now, read them in future sessions

/**
 * Create a Google Doc, add it to an NLM notebook, and register as a living doc.
 * Returns { doc, source, livingDoc } with IDs for future updates.
 *
 * @param {object} env - Worker environment bindings
 * @param {string} notebookId - Target NLM notebook ID
 * @param {string} title - Document title
 * @param {string} [content] - Initial content (optional)
 * @returns {object} { success, doc_id, source_id, title, error? }
 */
async function nlmCreateLinkedDoc(env, notebookId, title, content = "") {
  if (!env.GWS || !env.NLM) {
    return { success: false, error: "GWS and NLM bindings required" };
  }

  // Step 1: Create the Google Doc
  const docResult = await gwsTool(env, "docs_create", { title, content: content || "" });
  if (!docResult) return { success: false, error: "Failed to create Google Doc" };

  let docId;
  try {
    const parsed = typeof docResult === "string" ? JSON.parse(docResult) : docResult;
    docId = parsed.documentId || parsed.id || parsed.doc_id;
  } catch { docId = null; }
  if (!docId) return { success: false, error: "Created doc but could not extract ID", raw: docResult };

  // Step 2: Add as source to NLM notebook
  const source = await nlmWriteTool(env, "source_add_drive", {
    notebook_id: notebookId,
    file_id: docId,
    title,
  });

  // Step 3: Register as living doc for auto-sync
  const living = await nlmWriteTool(env, "living_doc_add", {
    drive_file_id: docId,
    notebook_id: notebookId,
    title,
  });

  return {
    success: true,
    doc_id: docId,
    source_id: source?.source_id || source?.id || null,
    living_doc: !living?.error,
    title,
  };
}

/**
 * Create a Google Sheet, add it to an NLM notebook, and register as a living doc.
 *
 * @param {object} env - Worker environment bindings
 * @param {string} notebookId - Target NLM notebook ID
 * @param {string} title - Sheet title
 * @param {Array<Array>} [initialData] - Optional 2D array of initial values
 * @returns {object} { success, sheet_id, source_id, title, error? }
 */
async function nlmCreateLinkedSheet(env, notebookId, title, initialData = null) {
  if (!env.GWS || !env.NLM) {
    return { success: false, error: "GWS and NLM bindings required" };
  }

  // Step 1: Create the Google Sheet
  const sheetResult = await gwsTool(env, "sheets_create", { title });
  if (!sheetResult) return { success: false, error: "Failed to create Google Sheet" };

  let sheetId;
  try {
    const parsed = typeof sheetResult === "string" ? JSON.parse(sheetResult) : sheetResult;
    sheetId = parsed.spreadsheetId || parsed.id || parsed.sheet_id;
  } catch { sheetId = null; }
  if (!sheetId) return { success: false, error: "Created sheet but could not extract ID", raw: sheetResult };

  // Step 1b: Write initial data if provided
  if (initialData?.length) {
    await gwsTool(env, "sheets_write", {
      spreadsheetId: sheetId,
      range: "Sheet1!A1",
      values: initialData,
    });
  }

  // Step 2: Add as source to NLM notebook
  const source = await nlmWriteTool(env, "source_add_drive", {
    notebook_id: notebookId,
    file_id: sheetId,
    title,
  });

  // Step 3: Register as living doc for auto-sync
  const living = await nlmWriteTool(env, "living_doc_add", {
    drive_file_id: sheetId,
    notebook_id: notebookId,
    title,
  });

  return {
    success: true,
    sheet_id: sheetId,
    source_id: source?.source_id || source?.id || null,
    living_doc: !living?.error,
    title,
  };
}

/**
 * Append text to an existing Google Doc and sync the NLM source.
 * Fetches the doc to find the end index, inserts text there, then triggers NLM sync.
 *
 * @param {object} env - Worker environment bindings
 * @param {string} docId - Google Doc file ID
 * @param {string} text - Text to append
 * @param {string} [sourceId] - NLM source ID to sync (optional, skips sync if not provided)
 * @returns {object} { success, appended, synced, error? }
 */
async function nlmAppendToDoc(env, docId, text, sourceId = null) {
  if (!env.GWS) return { success: false, error: "GWS binding required" };

  // Step 1: Get current doc to find end index
  const docContent = await gwsTool(env, "docs_get", { documentId: docId });
  if (!docContent) return { success: false, error: "Failed to read Google Doc" };

  let endIndex = 1; // Default to beginning if we can't parse
  try {
    const parsed = typeof docContent === "string" ? JSON.parse(docContent) : docContent;
    // Google Docs API: body.content is an array of structural elements
    // The last element's endIndex is where we append
    const body = parsed.body || parsed;
    if (body.content?.length) {
      const last = body.content[body.content.length - 1];
      endIndex = (last.endIndex || 2) - 1; // Insert before final newline
    }
  } catch {
    // If we can't parse the structure, insert at index 1 (after doc start)
    endIndex = 1;
  }

  // Step 2: Append text with a separator
  const separator = endIndex > 1 ? "\n\n---\n\n" : "";
  let writeResult = await gwsTool(env, "docs_modify", {
    documentId: docId,
    operations: [{ type: "insertText", location: endIndex, text: separator + text }],
  });

  // Fallback: if GWS worker docs_modify fails (batchUpdate mapping bug), call Docs API directly
  if (!writeResult && writeResult !== "") {
    const directResult = await gwsDocsAppend(env, docId, text);
    if (!directResult.success) {
      return { success: false, error: directResult.error || "Failed to append to Google Doc" };
    }
  }

  // Step 3: Sync NLM source if source ID provided
  let synced = false;
  if (sourceId && env.NLM) {
    const syncResult = await nlmWriteTool(env, "source_sync", { source_id: sourceId });
    synced = !syncResult?.error;
  }

  return { success: true, appended: true, synced, doc_id: docId };
}

/**
 * Append rows to a Google Sheet and sync the NLM source.
 *
 * @param {object} env - Worker environment bindings
 * @param {string} sheetId - Google Sheet file ID
 * @param {Array<Array>} rows - 2D array of rows to append
 * @param {string} [sourceId] - NLM source ID to sync (optional)
 * @param {string} [range] - Target range (default "Sheet1")
 * @returns {object} { success, appended, synced, error? }
 */
async function nlmAppendToSheet(env, sheetId, rows, sourceId = null, range = "Sheet1") {
  if (!env.GWS) return { success: false, error: "GWS binding required" };

  // Step 1: Read current data to find next empty row
  const current = await gwsTool(env, "sheets_read", {
    spreadsheetId: sheetId,
    range: `${range}!A:A`,
  });

  let nextRow = 1;
  try {
    const parsed = typeof current === "string" ? JSON.parse(current) : current;
    const values = parsed.values || parsed;
    if (Array.isArray(values)) nextRow = values.length + 1;
  } catch { /* start at row 1 */ }

  // Step 2: Write new rows
  const writeResult = await gwsTool(env, "sheets_write", {
    spreadsheetId: sheetId,
    range: `${range}!A${nextRow}`,
    values: rows,
  });
  if (!writeResult && writeResult !== "") {
    return { success: false, error: "Failed to append to Google Sheet" };
  }

  // Step 3: Sync NLM source if source ID provided
  let synced = false;
  if (sourceId && env.NLM) {
    const syncResult = await nlmWriteTool(env, "source_sync", { source_id: sourceId });
    synced = !syncResult?.error;
  }

  return { success: true, appended: true, synced, sheet_id: sheetId, rows_added: rows.length };
}

/**
 * Sync all living docs in NLM — triggers re-ingestion of any updated Drive files.
 * Call this at the end of a session after making GDoc/GSheet updates.
 */
async function nlmSyncAllDocs(env) {
  if (!env.NLM) return { success: false, error: "NLM binding required" };

  // Check which docs are stale first
  const stale = await nlmWriteTool(env, "living_doc_check_stale", {});
  const syncResult = await nlmWriteTool(env, "living_doc_sync_all", {});

  return {
    success: !syncResult?.error,
    stale_count: stale?.stale?.length ?? null,
    sync_result: syncResult,
  };
}

// ---------- GWS / Drive helpers ----------

/**
 * Call a GWS MCP tool via the internal service binding.
 * X-GWS-Self:1 identifies this as an internal trusted caller.
 * Returns the text content of the tool result, or null on any failure.
 */
async function gwsTool(env, toolName, args = {}) {
  if (!env.GWS) return null;
  try {
    const resp = await env.GWS.fetch(
      new Request("https://gws-worker.internal/mcp", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-GWS-Self": "1",
        },
        body: JSON.stringify({
          jsonrpc: "2.0", id: 1,
          method: "tools/call",
          params: { name: toolName, arguments: args },
        }),
        signal: AbortSignal.timeout(DRIVE_TIMEOUT_MS),
      })
    );
    if (!resp.ok) return null;
    const data = await resp.json();
    // MCP tool result: { result: { content: [{ type:"text", text:"..." }] } }
    return data?.result?.content?.[0]?.text ?? null;
  } catch { return null; }
}

/**
 * Directly append text to a Google Doc via the Docs API using SA token.
 * Bypasses the GWS worker's docs_modify tool (which has a batchUpdate mapping bug).
 *
 * @param {object} env - Worker environment bindings (needs GAUTH or KV_CACHE/KV)
 * @param {string} docId - Google Doc file ID
 * @param {string} text - Text to append
 * @returns {object} { success, error? }
 */
async function gwsDocsAppend(env, docId, text) {
  // Get SA token (same pattern as generateViaOfficialAPI)
  let saToken = null;
  if (env.GOOGLE_AUTH) {
    try {
      const authResp = await fetchWithRetry(env.GOOGLE_AUTH, new Request("https://google-auth-worker.internal/token", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ force: false }),
      }));
      if (authResp.ok) {
        const authData = await authResp.json();
        saToken = authData.access_token;
      }
    } catch { /* GAUTH binding miss */ }
  }
  if (!saToken && env.KV_CACHE) {
    try { saToken = await env.KV_CACHE.get("google_access_token"); } catch { /* KV miss */ }
  }
  if (!saToken && env.KV) {
    try { saToken = await env.KV.get("google_access_token"); } catch { /* KV miss */ }
  }
  if (!saToken) return { success: false, error: "No SA token available" };

  const headers = { Authorization: `Bearer ${saToken}`, "Content-Type": "application/json" };

  // Step 1: GET doc to find end index
  let endIndex = 1;
  try {
    const docResp = await fetch(`https://docs.googleapis.com/v1/documents/${docId}`, { headers });
    if (!docResp.ok) return { success: false, error: `GET doc failed: ${docResp.status}` };
    const doc = await docResp.json();
    if (doc.body?.content?.length) {
      const last = doc.body.content[doc.body.content.length - 1];
      endIndex = (last.endIndex || 2) - 1;
    }
  } catch (e) {
    return { success: false, error: `GET doc error: ${e.message}` };
  }

  // Step 2: batchUpdate to insert text
  const separator = endIndex > 1 ? "\n\n---\n\n" : "";
  try {
    const batchResp = await fetch(`https://docs.googleapis.com/v1/documents/${docId}:batchUpdate`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        requests: [{
          insertText: {
            location: { index: endIndex },
            text: separator + text,
          },
        }],
      }),
    });
    if (!batchResp.ok) {
      const errText = await batchResp.text().catch(() => "");
      return { success: false, error: `batchUpdate failed: ${batchResp.status} ${errText}` };
    }
    return { success: true };
  } catch (e) {
    return { success: false, error: `batchUpdate error: ${e.message}` };
  }
}

/**
 * Fetch Google Drive file content via GWS binding and build a context block.
 * Accepts file IDs only. Falls back to empty string if GWS is unavailable.
 */
async function buildDriveContext(driveFileIds, env) {
  if (!driveFileIds?.length || !env.GWS) return "";

  const results = await Promise.allSettled(
    driveFileIds.map(id =>
      gwsTool(env, "drive_get", { fileId: id })
        .then(async metaRaw => {
          // Parse metadata to check file type
          let meta, mimeType = "";
          try {
            const parsed = typeof metaRaw === "string" ? JSON.parse(metaRaw) : metaRaw;
            meta = parsed?.metadata || parsed;
            mimeType = meta?.mimeType || "";
          } catch { meta = metaRaw; }

          let content = null;
          // Google Docs/Sheets/Slides → export as text (works)
          if (mimeType.includes("vnd.google-apps.")) {
            content = await gwsTool(env, "drive_export", {
              fileId: id,
              mimeType: "text/plain",
            });
          }
          // PDFs/binary → drive_get returns raw content in the response
          // If raw content is binary/unreadable, mark as needing local extraction
          if (!content) {
            const raw = await gwsTool(env, "drive_get", { fileId: id });
            // Check if content is readable text (not binary PDF)
            if (raw && typeof raw === "string" && !raw.startsWith("%PDF") && raw.length > 20) {
              content = raw;
            } else {
              content = `[PDF file: ${meta?.name || id} — ${meta?.size || "?"} bytes. Extract text locally with pymupdf4llm before sending to Gemini.]`;
            }
          }
          return { id, meta, content };
        })
    )
  );

  const parts = ["## Google Drive Context\n"];
  let added = 0;
  for (const r of results) {
    if (r.status !== "fulfilled" || !r.value?.content) continue;
    const { id, meta, content } = r.value;
    const label = meta ? `${meta}` : id;
    parts.push(`### Drive: ${label}\n${content.slice(0, DRIVE_FILE_CHARS)}\n`);
    added++;
  }

  if (!added) return "";
  parts.push("---\nUse the above Drive documents as additional context.");
  return parts.join("\n");
}

// ---------- Generation modes ----------

async function generateViaOfficialAPI(prompt, env, { model = "gemini-2.5-flash", system = null } = {}) {
  const reqBody = { contents: [{ role: "user", parts: [{ text: prompt }] }] };
  if (system) reqBody.system_instruction = { parts: [{ text: system }] };
  const bodyStr = JSON.stringify(reqBody);

  // Get service account token — try GAUTH service binding first (real-time), then KV cache
  let saToken = null;

  // Method 1: Call google-auth-worker directly via service binding (always fresh)
  if (!saToken && env.GOOGLE_AUTH) {
    try {
      const authResp = await fetchWithRetry(env.GOOGLE_AUTH, new Request("https://google-auth-worker.internal/token", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ force: false }),
      }));
      if (authResp.ok) {
        const authData = await authResp.json();
        saToken = authData.access_token;
      }
    } catch (e) { /* GAUTH binding miss */ }
  }

  // Method 2: Read from shared KV cache (written by google-auth-worker cron)
  if (!saToken && env.KV_CACHE) {
    try { saToken = await env.KV_CACHE.get("google_access_token"); } catch (e) { /* KV miss */ }
  }
  if (!saToken && env.KV) {
    try { saToken = await env.KV.get("google_access_token"); } catch (e) { /* KV miss */ }
  }

  // Resolve user-facing model name to official API model ID
  const apiModel = API_MODEL_MAP[model] || model;

  // Build endpoint list: service account token (paid) → API key via gateway → API key direct
  const endpoints = [];
  if (saToken) {
    endpoints.push({ url: `${GEMINI_DIRECT}/${apiModel}:generateContent`, auth: `Bearer ${saToken}`, label: "sa-token" });
  }
  if (env.GEMINI_API_KEY) {
    endpoints.push({ url: `${GEMINI_GW}/${apiModel}:generateContent`, auth: `Bearer ${env.GEMINI_API_KEY}`, label: "gateway" });
    endpoints.push({ url: `${GEMINI_DIRECT}/${apiModel}:generateContent?key=${env.GEMINI_API_KEY}`, auth: null, label: "direct-key" });
  }

  if (endpoints.length === 0) {
    throw new Error("No Gemini API auth available. Waiting for google-auth-worker to populate KV with service account token.");
  }

  let lastErr;
  for (const ep of endpoints) {
    try {
      const headers = { "Content-Type": "application/json" };
      if (ep.auth) headers.Authorization = ep.auth;
      const resp = await fetch(ep.url, { method: "POST", headers, body: bodyStr });
      if (!resp.ok) { lastErr = new Error(`Gemini API ${resp.status} (${ep.label}): ${(await resp.text()).slice(0, 300)}`); continue; }
      const data = await resp.json();
      const text = data?.candidates?.[0]?.content?.parts?.[0]?.text;
      if (!text) { lastErr = new Error("No text in Gemini API response"); continue; }
      return { text, mode: `gemini-api (${ep.label})`, model };
    } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

async function generateViaWebCookie(prompt, env, {
  model      = null,
  temporary  = false,
  system     = null,
  gemId      = null,
  chatMeta   = null,   // [cid, rid, rcid, ...] for multi-turn
} = {}) {
  let session;
  try {
    session = await getSessionData(env);
  } catch (e) {
    console.log(`[generateViaWebCookie] getSessionData failed: ${e.message}, using minimal session`);
    session = { snlm0e: null, buildLabel: "", sessionId: "", language: "en" };
  }
  const { snlm0e, buildLabel, sessionId, language } = session;

  const reqId  = Math.floor(Math.random() * 90000) + 10000;
  const uuidVal = crypto.randomUUID().toUpperCase();

  const fullPrompt = system ? `${system}\n\n---\n\n${prompt}` : prompt;

  const inner = new Array(69).fill(null);
  inner[0] = [fullPrompt, 0, null, null, null, null, 0];
  inner[1] = [language];
  inner[2] = chatMeta ?? DEFAULT_METADATA;
  inner[6] = [1];
  inner[STREAMING_FLAG_INDEX] = 1;
  inner[10] = 1; inner[11] = 0; inner[17] = [[0]]; inner[18] = 0;
  inner[27] = 1; inner[30] = [4]; inner[41] = [1];
  if (gemId) inner[GEM_FLAG_INDEX] = gemId;
  if (temporary) inner[TEMPORARY_CHAT_FLAG_INDEX] = 1;
  inner[53] = 0; inner[59] = uuidVal; inner[61] = []; inner[68] = 2;

  const params = new URLSearchParams({ hl: language, _reqid: String(reqId), rt: "c" });
  if (buildLabel) params.set("bl", buildLabel);
  if (sessionId)  params.set("f.sid", sessionId);

  const modelHeaders = model ? buildModelHeaders(model) : {};

  const resp = await fetch(`${ENDPOINT_GENERATE}?${params}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
      "Origin": "https://gemini.google.com",
      "Referer": "https://gemini.google.com/",
      "X-Same-Domain": "1",
      "Sec-Fetch-Dest": "empty",
      "Sec-Fetch-Mode": "cors",
      "Sec-Fetch-Site": "same-origin",
      "x-goog-ext-525005358-jspb": `["${uuidVal}",1]`,
      ...modelHeaders,
      Cookie: buildCookieString(env),
      "User-Agent": CHROME_UA,
    },
    body: new URLSearchParams({ at: snlm0e, "f.req": JSON.stringify([null, JSON.stringify(inner)]) }).toString(),
  });

  if (!resp.ok) throw new Error(`StreamGenerate HTTP ${resp.status}`);
  const raw = await resp.text();
  const parsed = parseStreamResponse(raw);
  return { ...parsed, mode: "web-cookie", session };
}

async function generateViaWorkersAI(prompt, env, { system = null } = {}) {
  const messages = [];
  if (system) messages.push({ role: "system", content: system });
  messages.push({ role: "user", content: prompt });
  const result = await env.AI.run(WORKERS_MODEL, { messages });
  if (!result?.response) throw new Error("No response from Workers AI");
  return { text: result.response, mode: "workers-ai", model: WORKERS_MODEL };
}

// ---------- /generate handler ----------

async function handleGenerate(request, env) {
  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Request body must be valid JSON." }, 400); }

  const {
    prompt,
    model        = null,
    temporary    = false,
    system       = null,
    gem          = null,      // gem ID (web-cookie) or system prompt text (api mode)
    chat_meta    = null,      // [cid, rid, rcid, ...] for multi-turn (web-cookie)
    notebooks    = true,
    notebook_ids = null,
    max_sources  = NLM_MAX_SOURCES,
    drive_file_ids = null,    // Google Drive file IDs to inject as context
  } = body;

  if (!prompt?.trim()) return jsonResponse({ error: "'prompt' is required and must be a non-empty string." }, 400);

  const authMode = getAuthMode(env);
  if (!authMode) return jsonResponse({
    error: "No auth configured.",
    hint: "Set GEMINI_API_KEY, SECURE_1PSID, or SESSION_KEY as worker secrets.",
  }, 500);

  // Build context sources in parallel
  // NLM injection ALWAYS runs when notebooks=true — web-cookie batchexecute does NOT natively
  // query NLM notebooks (only the browser Notebook UI does). We must prepend source context.
  // Drive injection ALWAYS runs when file IDs provided (Gemini doesn't auto-attach Drive files from API calls)
  const skipNlmInjection = false;  // Never skip — grounding requires explicit injection
  const [notebookResult, driveResult] = await Promise.allSettled([
    (notebooks && env.NLM && !skipNlmInjection)
      ? buildNotebookContext(prompt, env, { notebookIds: notebook_ids, maxSources: max_sources })
      : Promise.resolve(""),
    (drive_file_ids?.length && env.GWS)
      ? buildDriveContext(drive_file_ids, env)
      : Promise.resolve(""),
  ]);

  const notebookContext = notebookResult.status === "fulfilled" ? notebookResult.value : "";
  const driveContext    = driveResult.status === "fulfilled"    ? driveResult.value    : "";
  const notebooksMeta   = env.NLM ? {
    grounded: !!notebookContext,
    binding: "NLM → notebooklm-worker",
    ...(notebookResult.status === "rejected" ? { error: notebookResult.reason?.message } : {}),
  } : null;
  const driveMeta = drive_file_ids?.length ? {
    files: drive_file_ids.length,
    grounded: !!driveContext,
    binding: "GWS → gws-worker",
    ...(driveResult.status === "rejected" ? { error: driveResult.reason?.message } : {}),
  } : null;

  const fullSystem = [notebookContext, driveContext, system].filter(Boolean).join("\n\n") || null;
  // In API mode, gem acts as additional system instruction
  const apiSystem  = gem && authMode === "gemini-api"
    ? [fullSystem, typeof gem === "string" ? gem : null].filter(Boolean).join("\n\n") || null
    : fullSystem;

  try {
    let result;
    if (authMode === "web-cookie") {
      result = await generateViaWebCookie(prompt, env, {
        model,
        temporary,
        system: fullSystem,
        gemId: gem,
        chatMeta: chat_meta,
      });
    } else if (authMode === "workers-ai") {
      result = await generateViaWorkersAI(prompt, env, { system: fullSystem });
    } else {
      return jsonResponse({ error: "No auth configured. Run 'notebooklm login' to set up cookies.", hint: "Web-cookie mode requires valid Google session cookies in R2_AUTH." }, 500);
    }

    // Omit session object from result (just expose ids)
    const { session: _sess, ...rest } = result;
    return jsonResponse({
      success: true,
      ...rest,
      ...(result.cid ? { chat: { cid: result.cid, rid: result.rid, rcid: result.rcid } } : {}),
      ...(notebooksMeta ? { notebooks: notebooksMeta } : {}),
      ...(driveMeta    ? { drive: driveMeta }             : {}),
      auth_mode: authMode,
    });
  } catch (err) {
    return jsonResponse({ error: err.message, mode: authMode, hint: "If cookies expired, run 'notebooklm login' to refresh auth in R2." }, 500);
  }
}

// ---------- /generate/stream handler (SSE) ----------

async function handleGenerateStream(request, env) {
  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Request body must be valid JSON." }, 400); }

  const { prompt, model = null, system = null, gem = null, chat_meta = null, temporary = false } = body;
  if (!prompt?.trim()) return jsonResponse({ error: "'prompt' required." }, 400);

  const authMode = getAuthMode(env);
  if (authMode !== "web-cookie" && authMode !== "gemini-api")
    return jsonResponse({ error: "Streaming requires GEMINI_API_KEY or SECURE_1PSID." }, 400);

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const enc    = new TextEncoder();

  const send = async (event, data) => {
    await writer.write(enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
  };

  (async () => {
    try {
      if (authMode === "gemini-api") {
        // Official API doesn't support SSE here — run full response, emit as one chunk
        const result = await generateViaOfficialAPI(prompt, env, { model: model ?? "gemini-2.5-flash", system });
        await send("chunk", { text: result.text });
        await send("done", { text: result.text, mode: result.mode });
      } else {
        // Web-cookie: fetch StreamGenerate, emit parsed result
        const result = await generateViaWebCookie(prompt, env, {
          model, temporary, system, gemId: gem, chatMeta: chat_meta,
        });
        // Emit full response as single chunk (true streaming would require incremental parsing)
        if (result.thoughts) await send("thoughts", { text: result.thoughts });
        for (const cand of result.candidates) {
          await send("candidate", { rcid: cand.rcid, text: cand.text, thoughts: cand.thoughts });
        }
        await send("done", {
          text: result.text,
          mode: result.mode,
          chat: result.cid ? { cid: result.cid, rid: result.rid, rcid: result.rcid } : null,
        });
      }
    } catch (err) {
      await send("error", { error: err.message }).catch(() => {});
    } finally {
      await writer.close().catch(() => {});
    }
  })().catch(() => {}); // swallow any escape from finally

  return new Response(readable, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Access-Control-Allow-Origin": "*",
      Connection: "keep-alive",
    },
  });
}

// ---------- Gem CRUD ----------

async function handleGetGems(env) {
  if (getAuthMode(env) !== "web-cookie")
    return jsonResponse({ error: "Gem operations require SECURE_1PSID / SESSION_KEY." }, 400);

  let session;
  try {
    session = await getSessionData(env);
  } catch (e) {
    console.log(`[handleGetGems] getSessionData failed: ${e.message}, using minimal session`);
    session = { snlm0e: null, buildLabel: "", sessionId: "", language: "en" };
  }
  const results = await batchExecute([
    { rpcid: GRPC.LIST_GEMS, payload: `[3,['${session.language}'],0]`, identifier: "system" },
    { rpcid: GRPC.LIST_GEMS, payload: `[2,['${session.language}'],0]`, identifier: "custom" },
  ], env, session);

  const parseGemList = (data) => (data?.[2] ?? []).map(gem => ({
    id:          gem[0],
    name:        gem[1]?.[0] ?? "",
    description: gem[1]?.[1] ?? "",
    prompt:      gem[2]?.[0] ?? null,
  }));

  return jsonResponse({
    predefined: parseGemList(results.system),
    custom:     parseGemList(results.custom),
    total: (results.system?.[2]?.length ?? 0) + (results.custom?.[2]?.length ?? 0),
  });
}

async function handleCreateGem(request, env) {
  if (getAuthMode(env) !== "web-cookie")
    return jsonResponse({ error: "Gem operations require SECURE_1PSID / SESSION_KEY." }, 400);

  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Request body must be valid JSON." }, 400); }

  const { name, prompt, description = "" } = body;
  if (!name?.trim() || !prompt?.trim())
    return jsonResponse({ error: "'name' and 'prompt' are required." }, 400);

  let session;
  try { session = await getSessionData(env); }
  catch (e) { console.log(`[handleCreateGem] getSessionData failed: ${e.message}, using minimal session`); session = { snlm0e: null, buildLabel: "", sessionId: "", language: "en" }; }
  const payload = JSON.stringify([[name, description, prompt, null, null, null, null, null, 0, null, 1, null, null, null, []]]);
  const results = await batchExecute([
    { rpcid: GRPC.CREATE_GEM, payload, identifier: "gem" },
  ], env, session);

  const gemId = results.gem?.[0];
  if (!gemId) return jsonResponse({ error: "Failed to create gem — unexpected response." }, 500);

  return jsonResponse({ success: true, gem: { id: gemId, name, description, prompt, predefined: false } }, 201);
}

async function handleUpdateGem(gemId, request, env) {
  if (getAuthMode(env) !== "web-cookie")
    return jsonResponse({ error: "Gem operations require SECURE_1PSID / SESSION_KEY." }, 400);

  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Request body must be valid JSON." }, 400); }

  const { name, prompt, description = "" } = body;
  if (!name?.trim() || !prompt?.trim())
    return jsonResponse({ error: "'name' and 'prompt' are required." }, 400);

  let session;
  try { session = await getSessionData(env); }
  catch (e) { console.log(`[handleUpdateGem] getSessionData failed: ${e.message}, using minimal session`); session = { snlm0e: null, buildLabel: "", sessionId: "", language: "en" }; }
  const payload = JSON.stringify([gemId, [name, description, prompt, null, null, null, null, null, 0, null, 1, null, null, null, [], 0]]);
  await batchExecute([
    { rpcid: GRPC.UPDATE_GEM, payload, identifier: "gem" },
  ], env, session);

  return jsonResponse({ success: true, gem: { id: gemId, name, description, prompt, predefined: false } });
}

async function handleDeleteGem(gemId, env) {
  if (getAuthMode(env) !== "web-cookie")
    return jsonResponse({ error: "Gem operations require SECURE_1PSID / SESSION_KEY." }, 400);

  let session;
  try { session = await getSessionData(env); }
  catch (e) { console.log(`[handleDeleteGem] getSessionData failed: ${e.message}, using minimal session`); session = { snlm0e: null, buildLabel: "", sessionId: "", language: "en" }; }
  const payload = JSON.stringify([gemId]);
  await batchExecute([
    { rpcid: GRPC.DELETE_GEM, payload, identifier: "gem" },
  ], env, session);

  return jsonResponse({ success: true, deleted: gemId });
}

// ---------- NLM proxy ----------

async function handleNLM(request, env, nlmPath) {
  if (!env.NLM) return jsonResponse({ error: "NLM binding not configured." }, 503);
  const resp = await env.NLM.fetch(
    new Request(`https://notebooklm-worker.internal${nlmPath}`, {
      method: request.method, headers: request.headers, body: request.body,
    })
  );
  return new Response(resp.body, {
    status: resp.status,
    headers: { ...Object.fromEntries(resp.headers), "Access-Control-Allow-Origin": "*" },
  });
}

// ---------- Workspace Execute (Gem → GWS write relay) ----------

// ---------- Workspace Chat (natural language → generate → auto-execute) ----------

const WORKSPACE_SYSTEM_PROMPT = `You are a Google Workspace assistant for authorityandbrand@gmail.com (Jimmy Nguyen). You have full read/write access to this account's Google Workspace: Gmail, Drive, Docs, Sheets, Slides, Calendar, Tasks, Contacts, Chat, and 122 NotebookLM notebooks (legal case research for Nguyen v. Fay Servicing LLC, 4:25-cv-00952-ALM-BD).

When the user asks you to create, update, search, or manage workspace items, you MUST output a JSON action block.

IMPORTANT: Always respond with TWO parts:
1. A brief natural language explanation of what you'll do
2. A JSON code block with the actions

Available actions (use ONLY these exact names):
DOCS: create_doc (title, content), update_doc (documentId, requests)
SHEETS: create_sheet (title), write_sheet (spreadsheetId, range, values), read_sheet (spreadsheetId, range)
GMAIL: send_email (to, subject, body), draft_email (to, subject, body), search_email (query)
DRIVE: create_file (name, content), search_drive (query), copy_file (file_id, name), share_file (file_id, email, role), list_folder (folder_id), get_file (file_id), get_link (file_id), export_file (file_id, mime_type)
CALENDAR: create_event (summary, start, end, description), list_calendars, delete_event (event_id)
TASKS: create_task (title, notes, due), list_tasks (task_list_id), list_task_lists, create_task_list (title), delete_task (task_id)
NOTEBOOKS: create_notebook (title), list_notebooks, get_notebook (notebook_id), query_notebook (notebook_id, question), search_notebooks (query, limit), add_source (notebook_id, url, title), add_text_source (notebook_id, title, content), list_sources (notebook_id), get_source_content (source_id), start_research (notebook_id, query, source)
NOTEBOOK STUDIO: generate_audio (notebook_id), generate_report (notebook_id, topic), generate_briefing (notebook_id, topic), generate_faq (notebook_id, topic), generate_timeline (notebook_id, topic), generate_study_guide (notebook_id, topic)
NOTEBOOK NOTES: create_note (notebook_id, title, content), list_notes (notebook_id)
NOTEBOOK MIND MAP: generate_mind_map (notebook_id)
NOTEBOOK SHARING: share_notebook (notebook_id, email, role), notebook_share_status (notebook_id)
NOTEBOOK CONVERSATIONS: notebook_conversations (notebook_id)
LINKED DOCS: create_linked_doc (notebook_id, title, content), create_linked_sheet (notebook_id, title, initial_data), append_to_doc (doc_id, text, source_id), append_to_sheet (spreadsheet_id, rows, source_id), sync_all_sources
CONTACTS: search_contacts (query), list_contacts
OTHER: web_search (query), list_spaces

NAMED WORKFLOWS (use action "run_workflow" with workflow name):
standup-report, meeting-prep, weekly-digest, case-status, email-to-task, save-email-to-doc, notebook-research, share-doc-and-notify, create-linked-research, post-mortem-setup

Output format — ALWAYS use this exact JSON structure:
\`\`\`json
{"actions": [{"action": "ACTION_NAME", "params": {PARAMS}}]}
\`\`\`

For multi-step workflows, chain multiple actions in the array. For read operations (search, list), include them so results are returned.`;

async function handleWorkspaceChat(request, env) {
  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Invalid JSON body" }, 400); }

  const { prompt, model = null, chat_meta = null, auto_execute = true } = body;
  if (!prompt?.trim()) return jsonResponse({ error: "'prompt' required" }, 400);

  // Step 1: Generate via Gemini with workspace system prompt
  const generateBody = {
    prompt,
    model,
    system: WORKSPACE_SYSTEM_PROMPT,
    chat_meta,
    notebooks: true,  // Keep NLM grounding active
  };

  const genRequest = new Request(request.url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(generateBody),
  });

  const genResponse = await handleGenerate(genRequest, env);
  const genResult = await genResponse.json();

  if (!genResult.success) {
    return jsonResponse({ success: false, phase: "generate", error: genResult.error, hint: genResult.hint });
  }

  // Step 2: Extract JSON action block from the response text
  const responseText = genResult.text || genResult.candidates?.[0] || "";
  const actionJson = extractActionJson(responseText);

  if (!actionJson) {
    // No actions found — just return the text response
    return jsonResponse({
      success: true,
      phase: "chat",
      text: responseText,
      actions_found: false,
      chat: genResult.chat || null,
    });
  }

  if (!auto_execute) {
    // Return the plan without executing
    return jsonResponse({
      success: true,
      phase: "planned",
      text: responseText,
      actions: actionJson,
      chat: genResult.chat || null,
    });
  }

  // Step 3: Auto-execute the actions
  const execRequest = new Request(request.url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(actionJson),
  });

  const execResponse = await handleWorkspaceExecute(execRequest, env);
  const execResult = await execResponse.json();

  return jsonResponse({
    success: true,
    phase: "executed",
    text: responseText,
    actions: actionJson,
    execution: execResult,
    chat: genResult.chat || null,
  });
}

// Extract {"actions": [...]} from Gemini response text (may be in a code block)
function extractActionJson(text) {
  if (!text) return null;

  // Try to find ```json ... ``` block
  const codeBlockMatch = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
  const jsonStr = codeBlockMatch ? codeBlockMatch[1].trim() : null;

  // Also try to find raw {"actions": ...} in the text
  const rawMatch = text.match(/\{"actions"\s*:\s*\[[\s\S]*?\]\s*\}/);

  const candidate = jsonStr || (rawMatch ? rawMatch[0] : null);
  if (!candidate) return null;

  try {
    const parsed = JSON.parse(candidate);
    // Validate it has an actions array
    if (parsed.actions && Array.isArray(parsed.actions) && parsed.actions.length > 0) {
      return parsed;
    }
    // Maybe it's a bare array
    if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].action) {
      return { actions: parsed };
    }
  } catch {}

  return null;
}

// ---------- Workflow Runner (named recipes that chain actions) ----------

const EXECUTABLE_WORKFLOWS = {
  "standup-report": {
    description: "Today's meetings + open tasks as a standup summary",
    steps: [
      { action: "list_calendars", params: {} },
      { action: "list_task_lists", params: {} },
    ],
    post: "standup", // post-processor key
  },
  "email-to-task": {
    description: "Convert a Gmail message into a Google Task",
    requiredParams: ["message_id"],
    steps: (p) => [
      { action: "search_email", params: { query: `rfc822msgid:${p.message_id}` } },
      { action: "create_task", params: { title: p.title || "Task from email", notes: p.notes || `From message: ${p.message_id}` } },
    ],
  },
  "meeting-prep": {
    description: "Prepare for next meeting: agenda, attendees, linked docs",
    steps: [
      { action: "list_calendars", params: {} },
      { action: "search_drive", params: { query: "agenda OR meeting notes" } },
    ],
  },
  "save-email-to-doc": {
    description: "Save Gmail message body into a Google Doc",
    requiredParams: ["message_id", "doc_title"],
    steps: (p) => [
      { action: "search_email", params: { query: `rfc822msgid:${p.message_id}` } },
      { action: "create_doc", params: { title: p.doc_title, content: p.content || "(email content will be inserted)" } },
    ],
  },
  "notebook-research": {
    description: "Search notebooks, query the best match, and create a summary doc",
    requiredParams: ["query"],
    steps: (p) => [
      { action: "search_notebooks", params: { query: p.query, limit: "3" } },
    ],
    post: "notebook-research",
  },
  "case-status": {
    description: "Search legal notebooks for case status, list recent emails, check tasks",
    steps: [
      { action: "search_notebooks", params: { query: "Nguyen v Fay status", limit: "3" } },
      { action: "search_email", params: { query: "Nguyen OR Fay OR 4:25-cv-00952 newer_than:7d" } },
      { action: "list_task_lists", params: {} },
    ],
  },
  "share-doc-and-notify": {
    description: "Share a Doc and email the link to collaborators",
    requiredParams: ["file_id", "email"],
    steps: (p) => [
      { action: "share_file", params: { file_id: p.file_id, email: p.email, role: p.role || "writer" } },
      { action: "get_link", params: { file_id: p.file_id } },
      { action: "draft_email", params: { to: p.email, subject: p.subject || "Document shared with you", body: p.body || `I've shared a document with you. You can access it in your Google Drive.` } },
    ],
  },
  "create-linked-research": {
    description: "Create a notebook, add sources, create a linked Doc for findings",
    requiredParams: ["title"],
    steps: (p) => [
      { action: "create_notebook", params: { title: p.title } },
      // Additional sources can be added after notebook creation
      { action: "create_doc", params: { title: `${p.title} — Findings`, content: `# ${p.title}\n\n## Research Findings\n\n(Add findings here)\n\n## Sources\n\n## Conclusions\n` } },
    ],
  },
  "weekly-digest": {
    description: "Weekly summary: meetings, emails, tasks",
    steps: [
      { action: "list_calendars", params: {} },
      { action: "search_email", params: { query: "newer_than:7d is:important" } },
      { action: "list_task_lists", params: {} },
    ],
  },
  "post-mortem-setup": {
    description: "Create post-mortem doc and schedule review meeting",
    requiredParams: ["title"],
    steps: (p) => [
      { action: "create_doc", params: { title: `Post-Mortem: ${p.title}`, content: `# Post-Mortem: ${p.title}\n\n## Date: ${new Date().toISOString().split('T')[0]}\n\n## Timeline\n\n## Root Cause\n\n## Impact\n\n## Action Items\n\n## Lessons Learned\n` } },
      { action: "create_event", params: { summary: `Post-Mortem Review: ${p.title}`, start: p.start || new Date(Date.now() + 86400000*2).toISOString(), end: p.end || new Date(Date.now() + 86400000*2 + 3600000).toISOString(), description: `Review post-mortem for: ${p.title}` } },
    ],
  },
};

async function handleWorkspaceRun(request, env) {
  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Invalid JSON body" }, 400); }

  const { workflow, params = {} } = body;

  if (!workflow) {
    return jsonResponse({
      error: "Missing 'workflow' name",
      available: Object.fromEntries(Object.entries(EXECUTABLE_WORKFLOWS).map(([k, v]) => [k, v.description])),
    }, 400);
  }

  const wf = EXECUTABLE_WORKFLOWS[workflow];
  if (!wf) {
    return jsonResponse({
      error: `Unknown workflow: ${workflow}`,
      available: Object.fromEntries(Object.entries(EXECUTABLE_WORKFLOWS).map(([k, v]) => [k, v.description])),
    }, 404);
  }

  // Check required params
  if (wf.requiredParams) {
    const missing = wf.requiredParams.filter(p => !params[p]);
    if (missing.length > 0) {
      return jsonResponse({ error: `Missing required params: ${missing.join(", ")}`, required: wf.requiredParams }, 400);
    }
  }

  // Build steps (static array or function)
  const steps = typeof wf.steps === "function" ? wf.steps(params) : wf.steps;

  // Execute all steps
  const execRequest = new Request(request.url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ actions: steps }),
  });

  const execResponse = await handleWorkspaceExecute(execRequest, env);
  const execResult = await execResponse.json();

  return jsonResponse({
    workflow,
    description: wf.description,
    steps_executed: steps.length,
    execution: execResult,
  });
}

// GWS workflow catalog — recipes, personas, and cross-service patterns from gws CLI skills
const GWS_WORKFLOWS = {
  recipes: {
    "email-to-task": { description: "Convert a Gmail message into a Google Task", services: ["gmail", "tasks"], command: "gws workflow +email-to-task" },
    "standup-report": { description: "Today's meetings + open tasks as a standup summary", services: ["calendar", "tasks"], command: "gws workflow +standup-report" },
    "meeting-prep": { description: "Prepare for next meeting: agenda, attendees, linked docs", services: ["calendar", "drive"], command: "gws workflow +meeting-prep" },
    "weekly-digest": { description: "Weekly summary: this week's meetings + unread emails", services: ["calendar", "gmail"], command: "gws workflow +weekly-digest" },
    "file-announce": { description: "Announce a Drive file in a Chat space", services: ["drive", "chat"], command: "gws workflow +file-announce" },
    "save-email-to-doc": { description: "Save Gmail message body into a Google Doc", services: ["gmail", "docs"] },
    "save-email-attachments": { description: "Find Gmail messages with attachments, save to Drive", services: ["gmail", "drive"] },
    "draft-email-from-doc": { description: "Read a Doc and use it as email body", services: ["docs", "gmail"] },
    "create-doc-from-template": { description: "Copy a Doc template, fill content, share", services: ["docs", "drive"] },
    "generate-report-from-sheet": { description: "Read Sheet data and create a formatted Docs report", services: ["sheets", "docs"] },
    "create-events-from-sheet": { description: "Read Sheet rows and create Calendar events", services: ["sheets", "calendar"] },
    "sync-contacts-to-sheet": { description: "Export Google Contacts to a Sheet", services: ["contacts", "sheets"] },
    "create-feedback-form": { description: "Create a Google Form and share via Gmail", services: ["forms", "gmail"] },
    "share-doc-and-notify": { description: "Share a Doc and email collaborators the link", services: ["docs", "gmail", "drive"] },
    "email-drive-link": { description: "Share a Drive file and email the link", services: ["drive", "gmail"] },
    "organize-drive-folder": { description: "Create folder structure and move files", services: ["drive"] },
    "post-mortem-setup": { description: "Create post-mortem Doc, schedule review, notify via Chat", services: ["docs", "calendar", "chat"] },
    "create-expense-tracker": { description: "Set up expense tracking Sheet", services: ["sheets"] },
    "create-task-list": { description: "Set up a Tasks list with initial items", services: ["tasks"] },
    "create-presentation": { description: "Create Slides presentation with initial slides", services: ["slides"] },
    "find-free-time": { description: "Find meeting slot across multiple calendars", services: ["calendar"] },
    "reschedule-meeting": { description: "Move a Calendar event and notify attendees", services: ["calendar"] },
    "share-event-materials": { description: "Share Drive files with Calendar event attendees", services: ["drive", "calendar"] },
    "block-focus-time": { description: "Create recurring focus time blocks on Calendar", services: ["calendar"] },
    "batch-invite-to-event": { description: "Add attendees to an existing Calendar event", services: ["calendar"] },
    "forward-labeled-emails": { description: "Forward Gmail messages with specific label", services: ["gmail"] },
    "label-and-archive-emails": { description: "Apply labels and archive matching emails", services: ["gmail"] },
    "create-gmail-filter": { description: "Create Gmail filter for auto-labeling", services: ["gmail"] },
    "create-vacation-responder": { description: "Enable Gmail out-of-office auto-reply", services: ["gmail"] },
    "find-large-files": { description: "Find large Drive files consuming storage", services: ["drive"] },
    "backup-sheet-as-csv": { description: "Export a Sheet as CSV for backup", services: ["sheets"] },
    "copy-sheet-for-new-month": { description: "Duplicate a Sheet template tab for new month", services: ["sheets"] },
    "compare-sheet-tabs": { description: "Compare two Sheet tabs for differences", services: ["sheets"] },
    "review-overdue-tasks": { description: "Find Tasks past due", services: ["tasks"] },
    "plan-weekly-schedule": { description: "Review Calendar week, identify gaps, add events", services: ["calendar"] },
    "log-deal-update": { description: "Append deal status to a sales tracking Sheet", services: ["sheets"] },
    "share-folder-with-team": { description: "Share a Drive folder with collaborators", services: ["drive"] },
    "create-shared-drive": { description: "Create a Shared Drive and add members", services: ["drive"] },
    "watch-drive-changes": { description: "Subscribe to Drive file/folder change notifications", services: ["drive"] },
    "bulk-download-folder": { description: "List and download all files from a Drive folder", services: ["drive"] },
  },
  personas: {
    "exec-assistant": { description: "Manage executive schedule, inbox, and communications", services: ["gmail", "calendar", "tasks", "drive"] },
    "project-manager": { description: "Coordinate projects — tasks, meetings, docs", services: ["tasks", "calendar", "docs", "drive"] },
    "team-lead": { description: "Run standups, coordinate tasks, communicate", services: ["tasks", "calendar", "chat"] },
    "researcher": { description: "Organize research — references, notes, collaboration", services: ["drive", "docs", "sheets"] },
    "sales-ops": { description: "Track deals, schedule calls, client comms", services: ["sheets", "calendar", "gmail"] },
    "content-creator": { description: "Create, organize, distribute content", services: ["docs", "drive", "gmail", "slides"] },
    "hr-coordinator": { description: "Onboarding, announcements, employee comms", services: ["gmail", "calendar", "forms", "docs"] },
    "event-coordinator": { description: "Plan events — scheduling, invitations, logistics", services: ["calendar", "gmail", "forms"] },
    "customer-support": { description: "Track tickets, respond, escalate issues", services: ["gmail", "sheets", "tasks"] },
    "it-admin": { description: "Monitor security, configure Workspace", services: ["admin", "gmail"] },
  },
  services: ["gmail", "drive", "docs", "sheets", "slides", "calendar", "tasks", "chat", "meet", "forms", "contacts", "classroom", "keep", "apps-script"],
  cli: "gws 0.16.0 — authorityandbrand@gmail.com",
  relay: "POST /workspace/execute — 47 actions via API",
  note: "Recipes are multi-step workflows that chain multiple workspace actions. Use /workspace/execute actions for individual operations, or describe the workflow and let the Gem plan the sequence.",
};

// ---------- Gemini Notebook handler (uses worker's own batchexecute) ----------

async function handleGeminiNotebook(params, spec, env) {
  const action = spec.geminiAction;

  let session;
  try { session = await getSessionData(env); }
  catch (e) {
    console.log(`[handleGeminiNotebook] getSessionData failed: ${e.message}, using minimal session`);
    session = { snlm0e: null, buildLabel: "", sessionId: "", language: "en" };
  }

  switch (action) {
    case "list": {
      // Python SDK: payload=[], source_path="/notebooks/view"
      const results = await batchExecute([
        { rpcid: GRPC.LIST_NOTEBOOKS, payload: JSON.stringify([]), identifier: "nb_list" },
      ], env, session, { sourcePath: "/notebooks/view" });
      return results.nb_list || results[GRPC.LIST_NOTEBOOKS] || { error: "Notebook list failed — snlm0e unavailable (cookie refresh needed)" };
    }

    case "create": {
      // Python SDK: [[title, "", null*14, [2]]], source_path="/notebook"
      const payload = JSON.stringify([[params.title, "", null, null, null, null, null, null, 0, null, 1, null, null, null, null, null, [2]]]);
      const results = await batchExecute([
        { rpcid: GRPC.CREATE_GEM, payload, identifier: "nb_create" },
      ], env, session, { sourcePath: "/notebook" });
      const createData = results.nb_create || results[GRPC.CREATE_GEM];
      const nbId = createData?.[0];
      if (!nbId) return { error: "Failed to create notebook — unexpected response", raw: createData };
      return { success: true, notebook: { id: nbId, title: params.title } };
    }

    case "get": {
      // Python SDK: [notebook_id, ["en"], 0], source_path="/notebook"
      const nbId = params.notebook_id?.startsWith("notebooks/") ? params.notebook_id : `notebooks/${params.notebook_id}`;
      const payload = JSON.stringify([nbId, [session.language || "en"], 0]);
      const results = await batchExecute([
        { rpcid: GRPC.GET_NOTEBOOK, payload, identifier: "nb_get" },
      ], env, session, { sourcePath: "/notebook" });
      return results.nb_get || results[GRPC.GET_NOTEBOOK] || { error: "Notebook not found" };
    }

    case "add_source": {
      // Python SDK: [notebook_id, [null, title, null, null, null, [content, 1]], [1, 3]]
      const nbId = params.notebook_id?.startsWith("notebooks/") ? params.notebook_id : `notebooks/${params.notebook_id}`;
      const nbShort = nbId.split("/")[1];
      const content = params.url || params.content || params.text || "";
      const title = params.title || "Untitled Source";
      const payload = JSON.stringify([nbId, [null, title, null, null, null, [content, 1]], [1, 3]]);
      const results = await batchExecute([
        { rpcid: GRPC.ADD_SOURCE, payload, identifier: "nb_add_src" },
      ], env, session, { sourcePath: `/notebook/notebooks%2F${nbShort}` });
      return results.nb_add_src || results[GRPC.ADD_SOURCE] || { success: true, note: "Source added (response parsing may vary)" };
    }

    case "read_source": {
      // Python SDK: [[[[source_id]]]], source_path="/notebook"
      const sourceId = params.source_id || "";
      const payload = JSON.stringify([[[[sourceId]]]]);
      const results = await batchExecute([
        { rpcid: GRPC.READ_SOURCE_CONTENT, payload, identifier: "nb_read_src" },
      ], env, session, { sourcePath: "/notebook" });
      return results.nb_read_src || results[GRPC.READ_SOURCE_CONTENT] || { error: "Source content not found" };
    }

    case "query": {
      // Query notebook = generate with notebook as gem context
      const nbId = params.notebook_id?.startsWith("notebooks/") ? params.notebook_id : `notebooks/${params.notebook_id}`;
      const question = params.question || params.query || "";
      return await generateViaWebCookie(question, env, { gemId: nbId });
    }

    default:
      return { error: `Unknown Gemini notebook action: ${action}` };
  }
}

// Action catalog: GWS actions route to GWS binding, NLM actions route to NLM binding, GEMINI actions use worker's own batchexecute
const WORKSPACE_ACTIONS = {
  // --- Google Docs ---
  create_doc:    { description: "Create a Google Doc", binding: "GWS", tool: "docs_create" },
  update_doc:    { description: "Update a Google Doc", binding: "GWS", tool: "docs_modify" },
  // --- Google Sheets ---
  create_sheet:  { description: "Create a Google Sheet", binding: "GWS", tool: "sheets_create" },
  write_sheet:   { description: "Write data to a sheet", binding: "GWS", tool: "sheets_write" },
  read_sheet:    { description: "Read data from a sheet", binding: "GWS", tool: "sheets_read" },
  // --- Gmail ---
  send_email:    { description: "Send an email via Gmail", binding: "GWS", tool: "gmail_send" },
  draft_email:   { description: "Create a Gmail draft", binding: "GWS", tool: "gmail_draft" },
  search_email:  { description: "Search Gmail", binding: "GWS", tool: "gmail_search" },
  // --- Google Drive ---
  create_file:   { description: "Create a file on Drive", binding: "GWS", tool: "drive_create" },
  search_drive:  { description: "Search Google Drive", binding: "GWS", tool: "drive_search" },
  // --- Calendar ---
  create_event:  { description: "Create a Calendar event", binding: "GWS", tool: "calendar_create" },
  // --- Tasks ---
  create_task:   { description: "Create a Google Task", binding: "GWS", tool: "tasks_create" },
  // --- Slides ---
  create_slide:  { description: "Create a Slides presentation", binding: "GWS", tool: "slides_create" },

  // --- NotebookLM: core CRUD via Gemini batchexecute (SELF), search/research via NLM ---
  create_notebook:     { description: "Create a new NotebookLM notebook", binding: "GEMINI", geminiAction: "create" },
  list_notebooks:      { description: "List all NotebookLM notebooks", binding: "GEMINI", geminiAction: "list" },
  get_notebook:        { description: "Get notebook details and sources", binding: "GEMINI", geminiAction: "get" },
  query_notebook:      { description: "Ask a question to a notebook (AI synthesis, 5-30s)", binding: "GEMINI", geminiAction: "query" },
  search_notebooks:    { description: "Search across all 122 notebooks by keyword", binding: "NLM", tool: "catalog_search" },
  add_source:          { description: "Add a URL source to a notebook", binding: "GEMINI", geminiAction: "add_source" },
  add_text_source:     { description: "Add text content as a notebook source", binding: "GEMINI", geminiAction: "add_source" },
  get_source_content:  { description: "Read the content of a notebook source", binding: "GEMINI", geminiAction: "read_source" },
  list_sources:        { description: "List all sources in a notebook", binding: "GEMINI", geminiAction: "get" },
  start_research:      { description: "Start web/Drive research and add results to notebook", binding: "NLM", tool: "research", transform: (p) => ({ action: "start", notebook_id: p.notebook_id, query: p.query, source: p.source || "web" }) },

  // --- Notebook Studio & Artifacts ---
  generate_audio:      { description: "Generate audio overview of a notebook", binding: "NLM", tool: "studio", transform: (p) => ({ action: "create", notebook_id: p.notebook_id, format: "audio" }) },
  generate_report:     { description: "Generate a report from notebook sources", binding: "NLM", tool: "studio", transform: (p) => ({ action: "create", notebook_id: p.notebook_id, format: "report", topic: p.topic }) },
  generate_briefing:   { description: "Generate a briefing document from notebook", binding: "NLM", tool: "studio", transform: (p) => ({ action: "create", notebook_id: p.notebook_id, format: "briefing_doc", topic: p.topic }) },
  generate_faq:        { description: "Generate FAQ from notebook sources", binding: "NLM", tool: "studio", transform: (p) => ({ action: "create", notebook_id: p.notebook_id, format: "faq", topic: p.topic }) },
  generate_timeline:   { description: "Generate timeline from notebook", binding: "NLM", tool: "studio", transform: (p) => ({ action: "create", notebook_id: p.notebook_id, format: "timeline", topic: p.topic }) },
  generate_study_guide: { description: "Generate study guide from notebook", binding: "NLM", tool: "studio", transform: (p) => ({ action: "create", notebook_id: p.notebook_id, format: "study_guide", topic: p.topic }) },

  // --- Notebook Notes ---
  create_note:         { description: "Create a note within a notebook", binding: "NLM", tool: "note", transform: (p) => ({ action: "create", notebook_id: p.notebook_id, title: p.title, content: p.content }) },
  list_notes:          { description: "List notes in a notebook", binding: "NLM", tool: "note", transform: (p) => ({ action: "list", notebook_id: p.notebook_id }) },

  // --- Notebook Mind Maps ---
  generate_mind_map:   { description: "Generate a mind map from notebook sources", binding: "NLM", tool: "mind_map", transform: (p) => ({ action: "generate", notebook_id: p.notebook_id }) },

  // --- Notebook Sharing ---
  share_notebook:      { description: "Share a notebook with a collaborator", binding: "NLM", tool: "share", transform: (p) => ({ action: "invite", notebook_id: p.notebook_id, email: p.email, role: p.role || "reader" }) },
  notebook_share_status: { description: "Check sharing status of a notebook", binding: "NLM", tool: "share", transform: (p) => ({ action: "status", notebook_id: p.notebook_id }) },

  // --- Notebook Conversations ---
  notebook_conversations: { description: "Get conversation history for a notebook", binding: "NLM", tool: "conversations", transform: (p) => ({ notebook_id: p.notebook_id }) },

  // --- Linked Docs (Notebook + Workspace combined) ---
  create_linked_doc:   { description: "Create a Google Doc and add it as a notebook source", binding: "NLM_INTERNAL", handler: "nlmCreateLinkedDoc" },
  create_linked_sheet: { description: "Create a Google Sheet and add it as a notebook source", binding: "NLM_INTERNAL", handler: "nlmCreateLinkedSheet" },
  append_to_doc:       { description: "Append text to a linked Doc and sync the notebook source", binding: "NLM_INTERNAL", handler: "nlmAppendDoc" },
  append_to_sheet:     { description: "Append rows to a linked Sheet and sync the notebook source", binding: "NLM_INTERNAL", handler: "nlmAppendSheet" },
  sync_all_sources:    { description: "Sync all living docs — re-ingest updated Drive files into notebook", binding: "NLM_INTERNAL", handler: "nlmSyncAll" },

  // --- Drive File Management (verified tool names from gws_* MCP) ---
  copy_file:     { description: "Copy a file on Drive", binding: "GWS", tool: "drive_copy" },
  share_file:    { description: "Share a file with a user", binding: "GWS", tool: "drive_share" },
  list_folder:   { description: "List files in a Drive folder", binding: "GWS", tool: "drive_list" },
  get_file:      { description: "Get file content/text from Drive", binding: "GWS", tool: "drive_get" },
  get_link:      { description: "Get shareable link for a Drive file", binding: "GWS", tool: "drive_get_link" },
  export_file:   { description: "Export a file (Doc/Sheet/Slide) as PDF/CSV", binding: "GWS", tool: "drive_export" },

  // --- Calendar Management ---
  list_calendars: { description: "List available calendars", binding: "GWS", tool: "calendar_list" },
  delete_event:  { description: "Delete a calendar event", binding: "GWS", tool: "calendar_delete" },

  // --- Task Management ---
  list_task_lists: { description: "List all task lists", binding: "GWS", tool: "tasks_list_lists" },
  list_tasks:    { description: "List tasks in a task list", binding: "GWS", tool: "tasks_list" },
  create_task_list: { description: "Create a new task list", binding: "GWS", tool: "tasks_create_list" },
  delete_task:   { description: "Delete a task", binding: "GWS", tool: "tasks_delete" },

  // --- Gmail Management ---
  list_labels:   { description: "List Gmail labels", binding: "GWS", tool: "gmail_list_labels" },
  list_filters:  { description: "List Gmail filter rules", binding: "GWS", tool: "gmail_list_filters" },

  // --- Contacts ---
  search_contacts: { description: "Search Google Contacts", binding: "GWS", tool: "contacts_search" },
  list_contacts:   { description: "List Google Contacts", binding: "GWS", tool: "contacts_list" },

  // --- Chat ---
  list_spaces:   { description: "List Google Chat spaces", binding: "GWS", tool: "chat_list_spaces" },

  // --- Web Search ---
  web_search:    { description: "Google web search", binding: "GWS", tool: "web_search" },

  // --- Gem Self-Update ---
  update_gem_instructions: { description: "Update a Gem's own system instructions", binding: "SELF", handler: "updateGemInstructions" },
};

// Alias map: natural language variants → canonical action names
const ACTION_ALIASES = {
  search_gmail: "search_email", search_mail: "search_email", find_email: "search_email",
  schedule_meeting: "create_event", schedule_event: "create_event", add_event: "create_event",
  new_doc: "create_doc", make_doc: "create_doc", write_doc: "create_doc",
  new_sheet: "create_sheet", make_sheet: "create_sheet",
  new_notebook: "create_notebook", make_notebook: "create_notebook",
  ask_notebook: "query_notebook", question_notebook: "query_notebook",
  find_notebooks: "search_notebooks", search_notebook: "search_notebooks",
  new_task: "create_task", add_task: "create_task",
  send_mail: "send_email", compose_email: "draft_email",
  list_files: "list_folder", list_drive: "list_folder",
  copy: "copy_file", share: "share_file",
  new_slide: "create_slide", new_presentation: "create_slide",
};

async function handleWorkspaceExecute(request, env) {
  let body;
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ error: "Invalid JSON body" }, 400);
  }

  // Accept both { actions: [...] } and bare array [...]
  const rawActions = body.actions || (Array.isArray(body) ? body : null);
  const actions = rawActions;
  if (!actions || !Array.isArray(actions) || actions.length === 0) {
    return jsonResponse({
      error: "Missing 'actions' array",
      example: { actions: [{ action: "create_doc", params: { title: "Meeting Notes", content: "# Notes..." } }] },
      available: Object.keys(WORKSPACE_ACTIONS),
    }, 400);
  }

  const batch = actions.slice(0, 10);
  const results = [];

  for (const item of batch) {
    const rawAction = item.action;
    const params = item.params || item.parameters || {};
    const action = ACTION_ALIASES[rawAction] || rawAction;
    const spec = WORKSPACE_ACTIONS[action];

    if (!spec) {
      results.push({ action, success: false, error: `Unknown action. Available: ${Object.keys(WORKSPACE_ACTIONS).join(", ")}` });
      continue;
    }

    try {
      let result;

      if (spec.binding === "GWS") {
        if (!env.GWS) { results.push({ action, success: false, error: "GWS binding not configured" }); continue; }
        const text = await proxyMCPCall(env.GWS, spec.tool, params || {}, { "X-GWS-Self": "1" }, 15000);
        try { result = JSON.parse(text); } catch { result = text; }

      } else if (spec.binding === "NLM") {
        if (!env.NLM) { results.push({ action, success: false, error: "NLM binding not configured" }); continue; }
        const toolParams = spec.transform ? spec.transform(params || {}) : (params || {});
        const text = await proxyMCPCall(env.NLM, spec.tool, toolParams, {}, 35000);
        try { result = JSON.parse(text); } catch { result = text; }

      } else if (spec.binding === "NLM_INTERNAL") {
        // Route to internal NLM helper functions
        switch (spec.handler) {
          case "nlmCreateNotebook":
            result = await nlmCreateNotebook(env, params.title); break;
          case "nlmAsk":
            result = await nlmAsk(env, params.notebook_id, params.question || params.query); break;
          case "nlmAddSource":
            result = await nlmAddSource(env, params.notebook_id, params.url, params.title); break;
          case "nlmAddTextSource":
            result = await nlmAddTextSource(env, params.notebook_id, params.title, params.content); break;
          case "nlmStartResearch":
            result = await nlmStartResearch(env, params.notebook_id, params.query, params.source); break;
          case "nlmCreateLinkedDoc":
            result = await nlmCreateLinkedDoc(env, params.notebook_id, params.title, params.content || ""); break;
          case "nlmCreateLinkedSheet":
            result = await nlmCreateLinkedSheet(env, params.notebook_id, params.title, params.initial_data || []); break;
          case "nlmAppendDoc":
            result = await nlmAppendToDoc(env, params.doc_id, params.text, params.source_id); break;
          case "nlmAppendSheet":
            result = await nlmAppendToSheet(env, params.spreadsheet_id, params.rows, params.source_id); break;
          case "nlmSyncAll":
            result = await nlmSyncAllDocs(env); break;
          default:
            result = { error: `Unknown handler: ${spec.handler}` };
        }

      } else if (spec.binding === "GEMINI") {
        // Route through worker's own Gemini batchexecute (same auth as /generate)
        result = await handleGeminiNotebook(params, spec, env);

      } else if (spec.binding === "SELF") {
        // Gem self-modification via web cookie batchexecute
        if (spec.handler === "updateGemInstructions" && params.gem_id && params.prompt) {
          result = await handleUpdateGem(params.gem_id, new Request("https://internal", {
            method: "PUT", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ prompt: params.prompt, name: params.name }),
          }), env).then(r => r.json()).catch(e => ({ error: e.message }));
        } else {
          result = { error: "gem_id and prompt required" };
        }
      }

      results.push({ action, success: true, result });
    } catch (err) {
      results.push({ action, success: false, error: err.message });
    }
  }

  return jsonResponse({ executed: results.length, results });
}

// ---------- GWS proxy ----------

async function handleGWS(request, env, gwsPath) {
  if (!env.GWS) return jsonResponse({ error: "GWS binding not configured." }, 503);
  const resp = await env.GWS.fetch(
    new Request(`https://gws-worker.internal${gwsPath}`, {
      method: request.method, headers: request.headers, body: request.body,
    })
  );
  return new Response(resp.body, {
    status: resp.status,
    headers: { ...Object.fromEntries(resp.headers), "Access-Control-Allow-Origin": "*" },
  });
}

// ---------- MCP Streamable HTTP Transport (spec 2025-03-26) ----------
// Connect at: POST https://gemini-webapi-worker.authorityandbrand.workers.dev/mcp
// In claude.ai → Settings → Integrations → Add custom integration → enter the URL above
//
// DYNAMIC PROXY ARCHITECTURE:
//   tools/list  — merges OWN_MCP_TOOLS + live NLM tools (nlm_ prefix) + live GWS tools (gws_ prefix)
//   tools/call  — routes nlm_* → NLM binding, gws_* → GWS binding, else → own handlers
//   Tool list is cached 2 min in isolate memory so repeated calls don't incur latency

// Our own Gemini-specific tools (always present regardless of bindings)
// Flat tools kept for callOwnMCPTool dispatch (not advertised)
const OWN_MCP_TOOLS_FLAT = [
  "gemini_generate", "gemini_gems",
  "nlm_workflow_ask", "nlm_workflow_add_source", "nlm_workflow_create_notebook",
  "nlm_workflow_create_linked_doc", "nlm_workflow_create_linked_sheet",
  "nlm_workflow_append_doc", "nlm_workflow_append_sheet",
  "nlm_workflow_sync_all", "nlm_workflow_generate_artifact", "nlm_workflow_health",
];

// Grouped tools for tools/list (token-efficient)
const OWN_MCP_TOOLS = [
  {
    name: "gemini",
    description: `Gemini AI with NotebookLM grounding and Drive file context.

Actions: generate, gems

- generate: prompt (required), model, system, gem (Gem ID for persona), notebooks (bool, default true), notebook_ids (array), drive_file_ids (array), chat_meta (array for multi-turn). In web-cookie mode use @YouTube/@Gmail/@Maps in prompt for extensions.
- gems: action (list/create/update/delete), id (for update/delete), name, prompt (system instructions), description`,
    inputSchema: {
      type: "object",
      properties: {
        action:         { type: "string", enum: ["generate", "gems"] },
        prompt:         { type: "string" },
        model:          { type: "string", enum: Object.keys(WEB_MODELS) },
        system:         { type: "string" },
        gem:            { type: "string" },
        notebooks:      { type: "boolean" },
        notebook_ids:   { type: "array", items: { type: "string" } },
        drive_file_ids: { type: "array", items: { type: "string" } },
        chat_meta:      { type: "array" },
        id:             { type: "string" },
        name:           { type: "string" },
        description:    { type: "string" },
      },
      required: ["action"],
    },
  },
  {
    name: "nlm_workflow",
    description: `LIVE NotebookLM operations — query, create notebooks, add sources, generate artifacts, export. For static catalog search of existing notebooks (faster, ~1ms vs 5s) use claude-brain server's notebooklm_registry tool.

NotebookLM workflow orchestration — query notebooks, manage sources, living docs, artifacts.

Actions: ask, add_source, create_notebook, create_linked_doc, create_linked_sheet, append_doc, append_sheet, sync_all, generate_artifact, health

- ask: notebook_id + question (required) — query a notebook's AI
- add_source: notebook_id (required), url or content, title
- create_notebook: title
- create_linked_doc: notebook_id + title (required), content — creates Google Doc + NLM source
- create_linked_sheet: notebook_id + title (required), initial_data (2D array)
- append_doc: doc_id + text (required), source_id — append + sync NLM source
- append_sheet: sheet_id + rows (required), source_id, range
- sync_all: (no params) — sync all stale living docs
- generate_artifact: notebook_id + artifact_type (required: audio/video/report/quiz/briefing/slides/infographic/mindmap/timeline), instructions
- health: (no params) — check NLM service binding status`,
    inputSchema: {
      type: "object",
      properties: {
        action:        { type: "string", enum: ["ask", "add_source", "create_notebook", "create_linked_doc", "create_linked_sheet", "append_doc", "append_sheet", "sync_all", "generate_artifact", "health"] },
        notebook_id:   { type: "string" },
        question:      { type: "string" },
        url:           { type: "string" },
        title:         { type: "string" },
        content:       { type: "string" },
        doc_id:        { type: "string" },
        text:          { type: "string" },
        source_id:     { type: "string" },
        sheet_id:      { type: "string" },
        rows:          { type: "array" },
        range:         { type: "string" },
        initial_data:  { type: "array" },
        artifact_type: { type: "string" },
        instructions:  { type: "string" },
      },
      required: ["action"],
    },
  },
];

// ---------- MCP proxy helpers ----------

let _toolsCache = null;
let _toolsCacheAt = 0;
let _gwsKey = null;  // cached GWS API key fetched via internal /config

/** Fetch GWS Bearer token via internal service binding (/config accepts X-GWS-Self:1). */
async function getGWSKey(binding) {
  if (_gwsKey) return _gwsKey;
  try {
    const resp = await binding.fetch(
      new Request("https://gws-worker.internal/config", {
        headers: { "X-GWS-Self": "1" },
        signal: AbortSignal.timeout(3000),
      })
    );
    if (!resp.ok) return null;
    const data = await resp.json();
    _gwsKey = data.gws_key ?? data.key ?? data.api_key ?? null;
    return _gwsKey;
  } catch { return null; }
}

/** Build auth headers for GWS binding: Bearer token obtained via /config. */
async function gwsHeaders(binding) {
  const key = await getGWSKey(binding);
  return key ? { "Authorization": `Bearer ${key}` } : { "X-GWS-Self": "1" };
}

async function proxyMCPList(binding, extraHeaders = {}) {
  try {
    const resp = await binding.fetch(
      new Request("https://internal/mcp", {
        method: "POST",
        headers: { "Content-Type": "application/json", ...extraHeaders },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }),
        signal: AbortSignal.timeout(5000),
      })
    );
    if (!resp.ok) return [];
    const data = await resp.json();
    return data?.result?.tools ?? [];
  } catch { return []; }
}

async function proxyMCPCall(binding, toolName, args, extraHeaders = {}, timeoutMs = 35000) {
  const resp = await binding.fetch(
    new Request("https://internal/mcp", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...extraHeaders },
      body: JSON.stringify({
        jsonrpc: "2.0", id: 1,
        method: "tools/call",
        params: { name: toolName, arguments: args },
      }),
      signal: AbortSignal.timeout(timeoutMs),
    })
  );
  if (!resp.ok) throw new Error(`Proxy HTTP ${resp.status}`);
  const data = await resp.json();
  if (data.error) throw new Error(data.error.message ?? JSON.stringify(data.error));
  return data?.result?.content?.[0]?.text ?? JSON.stringify(data.result ?? {});
}

/** Merge our tools with live NLM + GWS tool lists (cached 2 min in isolate memory). */
async function getAllMCPTools(env) {
  const now = Date.now();
  if (_toolsCache && (now - _toolsCacheAt) < 120_000) return _toolsCache;

  // NLM fetch and GWS (key-then-list) run fully in parallel.
  // GWS key fetch (max 3s) is chained inside the GWS slot so it never blocks NLM.
  const [nlmResult, gwsResult, hubResult] = await Promise.allSettled([
    env.NLM ? proxyMCPList(env.NLM) : Promise.resolve([]),
    env.GWS ? (async () => {
      const hdrs = await gwsHeaders(env.GWS);
      return proxyMCPList(env.GWS, hdrs);
    })() : Promise.resolve([]),
    env.HUB ? proxyMCPList(env.HUB) : Promise.resolve([]),
  ]);

  const nlmTools = (nlmResult.status === "fulfilled" ? nlmResult.value : [])
    .map(t => ({ ...t, name: `nlm_${t.name}`, description: `[PROXY → notebooklm-worker, +50ms latency] ${t.description ?? t.name} — Prefer calling notebooklm-worker directly when possible.` }));

  const gwsTools = (gwsResult.status === "fulfilled" ? gwsResult.value : [])
    .map(t => ({ ...t, name: `gws_${t.name}`, description: `[PROXY → gws-worker, +50ms latency] ${t.description ?? t.name} — Prefer calling gws-worker directly when possible.` }));

  // Only expose workflow-related HUB tools (workflows, skills, pipeline, agents)
  const HUB_TOOL_ALLOWLIST = new Set(["workflows", "skills", "pipeline", "agents"]);
  const hubTools = (hubResult.status === "fulfilled" ? hubResult.value : [])
    .filter(t => HUB_TOOL_ALLOWLIST.has(t.name))
    .map(t => ({ ...t, name: `hub_${t.name}`, description: `[PROXY → litigation-hub-worker, +50ms latency] ${t.description ?? t.name} — Prefer calling litigation-hub-worker directly when possible.` }));

  _toolsCache = [...OWN_MCP_TOOLS, ...nlmTools, ...gwsTools, ...hubTools];
  _toolsCacheAt = now;
  return _toolsCache;
}

// ---------- Own tool handlers ----------

async function callOwnMCPTool(name, args, env) {
  switch (name) {
    case "gemini_generate": {
      const fakeReq = new Request("https://worker/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ notebooks: true, ...args }),
      });
      const resp = await handleGenerate(fakeReq, env);
      const data = await resp.json();
      if (!data.success) throw new Error(data.error ?? "Generation failed");
      const parts = [data.text];
      if (data.thoughts) parts.push(`\n\n**Reasoning:**\n${data.thoughts}`);
      if (data.chat?.cid) parts.push(`\n\n_Session: cid=${data.chat.cid} rid=${data.chat.rid}_`);
      if (data.notebooks?.grounded) parts.push(`\n_Grounded with NotebookLM_`);
      if (data.drive?.grounded) parts.push(`\n_Grounded with ${data.drive.files} Drive file(s)_`);
      return parts.join("");
    }

    case "gemini_gems": {
      const { action, id, name: gemName, prompt, description = "" } = args;
      switch (action) {
        case "list": {
          const resp = await handleGetGems(env);
          const data = await resp.json();
          if (data.error) throw new Error(data.error);
          return JSON.stringify(data, null, 2);
        }
        case "create": {
          const fakeReq = new Request("https://worker/gems", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: gemName, prompt, description }),
          });
          const resp = await handleCreateGem(fakeReq, env);
          const data = await resp.json();
          if (!data.success) throw new Error(data.error ?? "Create failed");
          return `Created gem '${data.gem.name}' (id: ${data.gem.id})`;
        }
        case "update": {
          const fakeReq = new Request(`https://worker/gems/${id}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: gemName, prompt, description }),
          });
          const resp = await handleUpdateGem(id, fakeReq, env);
          const data = await resp.json();
          if (!data.success) throw new Error(data.error ?? "Update failed");
          return `Updated gem '${data.gem.name}'`;
        }
        case "delete": {
          const resp = await handleDeleteGem(id, env);
          const data = await resp.json();
          if (!data.success) throw new Error(data.error ?? "Delete failed");
          return `Deleted gem: ${id}`;
        }
        default: throw new Error(`Unknown gems action: ${action}`);
      }
    }

    // --- NLM workflow tools ---
    case "nlm_workflow_ask":
      return JSON.stringify(await nlmAsk(env, args.notebook_id, args.question));

    case "nlm_workflow_add_source": {
      const result = args.url
        ? await nlmAddSource(env, args.notebook_id, args.url, args.title)
        : await nlmAddTextSource(env, args.notebook_id, args.title || "Untitled", args.content || "");
      return JSON.stringify(result);
    }

    case "nlm_workflow_create_notebook":
      return JSON.stringify(await nlmCreateNotebook(env, args.title));

    case "nlm_workflow_create_linked_doc":
      return JSON.stringify(await nlmCreateLinkedDoc(env, args.notebook_id, args.title, args.content || ""));

    case "nlm_workflow_create_linked_sheet":
      return JSON.stringify(await nlmCreateLinkedSheet(env, args.notebook_id, args.title, args.initial_data || []));

    case "nlm_workflow_append_doc":
      return JSON.stringify(await nlmAppendToDoc(env, args.doc_id, args.text, args.source_id));

    case "nlm_workflow_append_sheet":
      return JSON.stringify(await nlmAppendToSheet(env, args.sheet_id, args.rows, args.source_id, args.range || "Sheet1"));

    case "nlm_workflow_sync_all":
      return JSON.stringify(await nlmSyncAllDocs(env));

    case "nlm_workflow_generate_artifact":
      return JSON.stringify(await nlmGenerateArtifact(env, args.notebook_id, args.artifact_type, { instructions: args.instructions }));

    case "nlm_workflow_health":
      return JSON.stringify(await nlmHealthCheck(env));

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

async function handleMCP(request, env) {
  const method = request.method.toUpperCase();

  if (method === "GET") {
    return jsonResponse({
      protocolVersion: "2025-03-26",
      capabilities: { tools: {} },
      serverInfo: { name: "gemini-webapi-worker", version: "5.4" },
    });
  }

  if (method !== "POST") return jsonResponse({ error: "Method not allowed" }, 405);

  let rpc;
  try { rpc = await request.json(); }
  catch { return new Response(JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } }), { headers: { "Content-Type": "application/json" } }); }

  const { id, method: rpcMethod, params } = rpc;

  const respond = (result) => new Response(
    JSON.stringify({ jsonrpc: "2.0", id, result }),
    { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
  );
  const mcpErr = (code, msg) => new Response(
    JSON.stringify({ jsonrpc: "2.0", id, error: { code, message: msg } }),
    { status: 200, headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
  );

  switch (rpcMethod) {
    case "initialize":
      return respond({
        protocolVersion: "2025-03-26",
        capabilities: { tools: {} },
        serverInfo: { name: "gemini-webapi-worker", version: "5.4" },
        instructions: [
          "Unified Gemini + NotebookLM + Google Workspace + Workflows MCP hub.",
          "gemini_* tools: AI generation with gem personas, @Extension support, NLM grounding.",
          "nlm_* tools: 119 notebooks / 2500+ legal sources — notebook CRUD, source mgmt, artifacts, research.",
          "gws_* tools: Gmail, Drive, Calendar, Docs, Sheets, Tasks (80 tools via GWS worker).",
          "hub_* tools: Workflows (CRUD + run), Skills, Pipeline, Agents — orchestrate multi-step processes.",
          "Use gemini_generate first — it auto-grounds answers from the legal knowledge base.",
        ].join(" "),
      });

    case "notifications/initialized":
      return new Response(null, { status: 202, headers: { "Access-Control-Allow-Origin": "*" } });

    case "ping":
      return respond({});

    case "tools/list":
      return respond({ tools: await getAllMCPTools(env) });

    case "tools/call": {
      let toolName = params?.name;
      const toolArgs = params?.arguments ?? {};
      if (!toolName) return mcpErr(-32602, "Missing tool name");

      // Route grouped tools: gemini({action: "generate"}) → "gemini_generate"
      // nlm_workflow({action: "ask"}) → "nlm_workflow_ask"
      if (toolArgs.action) {
        const grouped = OWN_MCP_TOOLS.find(t => t.name === toolName);
        if (grouped) {
          toolName = `${toolName}_${toolArgs.action}`;
        }
      }

      try {
        let text;
        if (toolName.startsWith("nlm_workflow_")) {
          text = await callOwnMCPTool(toolName, toolArgs, env);
        } else if (toolName.startsWith("nlm_") && env.NLM) {
          text = await proxyMCPCall(env.NLM, toolName.slice(4), toolArgs, {}, 35000);
        } else if (toolName.startsWith("hub_") && env.HUB) {
          text = await proxyMCPCall(env.HUB, toolName.slice(4), toolArgs, {}, 35000);
        } else if (toolName.startsWith("gws_") && env.GWS) {
          if (toolName === "gws_drive_search" && toolArgs.query && !/\b(contains|in|=|and|or|not|mimeType|fullText|name|modifiedTime)\b/i.test(toolArgs.query)) {
            toolArgs.query = `fullText contains '${toolArgs.query.replace(/'/g, "\\'")}'`;
          }
          text = await proxyMCPCall(env.GWS, toolName.slice(4), toolArgs, { "X-GWS-Self": "1" }, 15000);
        } else {
          text = await callOwnMCPTool(toolName, toolArgs, env);
        }
        return respond({ content: [{ type: "text", text }] });
      } catch (err) {
        return respond({ content: [{ type: "text", text: `Error: ${err.message}` }], isError: true });
      }
    }

    default:
      return mcpErr(-32601, `Method not found: ${rpcMethod}`);
  }
}

// ---------- Main router ----------

export default {
  async fetch(request, env) {
    try {
    const url    = new URL(request.url);
    const method = request.method.toUpperCase();
    let path     = url.pathname;
    // Normalize: strip /v1 prefix added by CF AI Gateway custom provider routing
    if (path.startsWith('/v1/')) path = path.slice(3);
    else if (path === '/v1') path = '/';

    if (method === "OPTIONS") return new Response(null, { headers: corsHeaders() });

    // Health
    if (path === "/health" && method === "GET") {
      return jsonResponse({
        status: "ok",
        service: "gemini-webapi-worker",
        version: "5.4",
        auth_mode: getAuthMode(env) ?? "none",
        cookie_rotation: { enabled: true, cached_psidts: !!_cachedPSIDTS, last_rotate: _lastRotateAt ? new Date(_lastRotateAt).toISOString() : null },
        bindings: { HUB: !!env.HUB, NLM: !!env.NLM, GWS: !!env.GWS, AI: !!env.AI, KV: !!env.KV },
        nlm: env.NLM ? { notebooks: 122, sources: 2690, note: "Auto-grounding on every /generate (notebooks:true)" } : null,
        drive: env.GWS ? { note: "Pass drive_file_ids in /generate body to inject file content as context", binding: "GWS → gws-worker" } : null,
        web_models: Object.keys(WEB_MODELS),
        extensions: {
          note: "Web-cookie auth only. Include @Extension in prompt to activate.",
          available: ["@YouTube", "@Gmail", "@Maps", "@Flights", "@Hotels", "@Finance"],
        },
        resilience: {
          nlm_down: "Context skipped, generation continues",
          gws_down: "Drive context skipped, generation continues",
          gateway_down: "Falls back to direct Gemini API",
          all_auth_down: "Falls back to Workers AI (Gemma-3)",
        },
        mcp: {
          endpoint: `${url.origin}/mcp`,
          transport: "streamable-http",
          spec: "2025-03-26",
          own_tools: OWN_MCP_TOOLS.map(t => t.name),
          proxied: { nlm: "nlm_* (14 NLM tool categories)", gws: "gws_* (80 GWS tools: Gmail/Drive/Calendar/Docs/Sheets)" },
          note: "Tool list is dynamic — GET /mcp or POST tools/list returns live count from all bindings",
          connect: "claude.ai → Settings → Integrations → Add custom integration",
        },
        routes: [
          "GET  /health",
          "GET  /mcp                  MCP server info",
          "POST /mcp                  MCP Streamable HTTP (JSON-RPC: initialize, tools/list, tools/call)",
          "POST /generate  { prompt, model?, system?, gem?, chat_meta?, temporary?, notebooks?, notebook_ids?, max_sources? }",
          "POST /generate/stream  (SSE) — same body as /generate",
          "GET  /gems",
          "POST /gems               { name, prompt, description? }",
          "PUT  /gems/:id           { name, prompt, description? }",
          "DELETE /gems/:id",
          "POST /rotate               Force cookie rotation",
          "POST /cookies/update       Push fresh cookies to KV + memory { cookies: {name: value} }",
          "POST /nlm/tools/:tool",
          "ANY  /gws/*",
        ],
      });
    }

    // Cookie rotation
    if (path === "/rotate" && method === "POST") {
      try {
        const newPSIDTS = await rotateCookies(env);
        return jsonResponse({
          success: true,
          rotated: !!newPSIDTS,
          cached: !!_cachedPSIDTS,
          last_rotate: _lastRotateAt ? new Date(_lastRotateAt).toISOString() : null,
        });
      } catch (err) {
        return jsonResponse({ success: false, error: err.message }, 500);
      }
    }

    // Cookie update — push fresh cookies to KV and in-memory cache (requires auth)
    if (path === "/cookies/update" && method === "POST") {
      try {
        // Auth gate: require SESSION_PUSH_KEY or CLOUDFLARE_API_TOKEN as Bearer token
        const pushKey = env.SESSION_PUSH_KEY || env.CLOUDFLARE_API_TOKEN;
        if (pushKey) {
          const authHeader = request.headers.get("Authorization") || "";
          const token = authHeader.replace(/^Bearer\s+/i, "");
          const aBytes = new TextEncoder().encode(token);
          const bBytes = new TextEncoder().encode(pushKey);
          const tokenValid = aBytes.length === bBytes.length && crypto.subtle.timingSafeEqual(aBytes, bBytes);
          if (!tokenValid) {
            return jsonResponse({ error: "Unauthorized — provide valid Bearer token" }, 401);
          }
        }
        const body = await request.json();
        const cookies = body.cookies; // {name: value, ...} or "name=val; name2=val2"
        if (!cookies || (typeof cookies !== "string" && typeof cookies !== "object")) {
          return jsonResponse({ error: "Missing or invalid 'cookies' in body (expected string or object)" }, 400);
        }

        let cookieStr;
        if (typeof cookies === "string") {
          cookieStr = cookies;
        } else {
          cookieStr = Object.entries(cookies).map(([k, v]) => `${k}=${v}`).join("; ");
        }

        // Update in-memory cache
        _nlmCookies = cookieStr;
        _nlmCookiesFetchedAt = Date.now();

        // Proxy cookie write to google-auth-worker — this worker is read-only for KV auth
        if (env.GOOGLE_AUTH) {
          const proxyResp = await fetchWithRetry(env.GOOGLE_AUTH,
            new Request("https://google-auth-worker.internal/cookies/push", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ target: "gemini", cookies: cookies }),
            })
          );
          return proxyResp;
        }
        return new Response(JSON.stringify({ error: "GOOGLE_AUTH binding unavailable" }), { status: 503, headers: { "Content-Type": "application/json" } });
      } catch (err) {
        return jsonResponse({ error: err.message }, 500);
      }
    }

    // MCP Streamable HTTP
    if (path === "/mcp") return handleMCP(request, env);

    // Generate
    if (path === "/generate" && method === "POST") return handleGenerate(request, env);
    if (path === "/generate/stream" && method === "POST") return handleGenerateStream(request, env);

    // Gems
    if (path === "/gems") {
      if (method === "GET")  return handleGetGems(env);
      if (method === "POST") return handleCreateGem(request, env);
    }
    const gemMatch = path.match(/^\/gems\/([^/]+)$/);
    if (gemMatch) {
      const gemId = decodeURIComponent(gemMatch[1]);
      if (method === "PUT")    return handleUpdateGem(gemId, request, env);
      if (method === "DELETE") return handleDeleteGem(gemId, env);
    }

    // Workspace chat — talk naturally, auto-executes workspace actions
    // Combines Gemini generate + workspace execute in one call
    if (path === "/workspace/chat" && method === "POST") {
      return handleWorkspaceChat(request, env);
    }

    // Workspace run — execute a named workflow recipe
    if (path === "/workspace/run" && method === "POST") {
      return handleWorkspaceRun(request, env);
    }

    // Workspace execute — direct JSON action execution
    if (path === "/workspace/execute" && method === "POST") {
      return handleWorkspaceExecute(request, env);
    }

    // Workspace actions catalog — lists available write actions for Gems
    if (path === "/workspace/actions" && method === "GET") {
      return jsonResponse({
        actions: WORKSPACE_ACTIONS,
        usage: "POST /workspace/execute with { actions: [{ action, params }] }",
        gem_instruction: "When asked to write to workspace, output JSON: { actions: [{ action: 'create_doc', params: { title: '...', content: '...' } }] }",
      });
    }

    // Workspace workflows catalog — recipes, personas, and workflow patterns from gws skills
    if (path === "/workspace/workflows" && method === "GET") {
      return jsonResponse(GWS_WORKFLOWS);
    }

    // Proxies
    if (path.startsWith("/nlm/")) return handleNLM(request, env, path.slice(4));
    if (path.startsWith("/gws/")) return handleGWS(request, env, path.slice(4));

    return jsonResponse({
      error: "Not Found",
      routes: ["GET /health", "GET|POST /mcp", "POST /generate", "POST /generate/stream",
               "GET /gems", "POST /gems", "PUT /gems/:id", "DELETE /gems/:id",
               "POST /workspace/execute", "GET /workspace/actions",
               "POST /nlm/tools/:tool", "ANY /gws/*"],
    }, 404);
    } catch (err) {
      console.error(JSON.stringify({ event: "unhandled_exception", path: new URL(request.url).pathname, error: err.message, stack: err.stack?.split('\n')[0] }));
      return jsonResponse({ error: "Internal server error", message: err.message }, 500);
    }
  },
};
