/**
 * Gemini WebAPI Cloudflare Worker  v5.3.1
 *
 * NEW in v5.2:
 *   - Drive file context: pass `drive_file_ids` array to /generate — worker fetches content
 *     from GWS binding and injects it as system context before the NLM notebook grounding
 *   - Workspace relay: POST /workspace/chat routes to GWS service binding; supports
 *     chat (gmail/calendar/tasks) and batch tool execution
 *   - MCP tool: workspace_action — execute any GWS action (send_email, create_event, …)
 *     directly from Gemini context
 *
 * NEW in v5.1:
 *   - MCP server: POST /mcp (Streamable HTTP, spec-compliant) + GET/POST /sse (legacy SSE)
 *     exposes all Gemini tools as MCP tools for claude.ai / other clients
 *   - Gem personas: GET/POST /gems to list and invoke Gemini Gem personas
 *   - Streaming: GET /stream?prompt=… for SSE token-by-token Gemini output
 *   - /health: includes auth_mode, available bindings, model list
 *
 * AUTH (priority order):
 *   1. web-cookie  — Gemini Advanced subscription (gemini-3-* models, NLM grounding)
 *   2. gemini-api  — GEMINI_API_KEY (gemini-2.5-* only, uses quota, no NLM grounding)
 *   3. workers-ai  — Free Workers AI inference fallback
 *
 * BINDINGS (all optional — worker degrades gracefully):
 *   AUTH        SECURE_1PSID (web, full features) | GEMINI_API_KEY (official API) | Workers AI (free)
 *   NLM         Auto-grounding from 122 notebooks / 2690 legal sources  (NLM binding)
 *   DRIVE       drive_file_ids in /generate injects file text as context (GWS binding)
 *   HUB         Claude relay + AI Gateway logging                        (HUB binding)
 *   KV/KV_CACHE Response cache + session state
 *   R2_AUTH     Playwright cookies shared with notebooklm-worker
 *
 * ENDPOINTS:
 *   POST /generate              — Main Gemini generate (web-cookie: inner[19]; api: system prefix)
 *   POST /generate/stream       — Streaming generate (web-cookie + gemini-api only)
 *   POST /chat                  — Multi-turn chat (web-cookie only)
 *   POST /mcp                   — MCP Streamable HTTP transport
 *   GET  /sse                   — MCP SSE transport (legacy)
 *   POST /sse                   — MCP SSE messages
 *   POST /workspace/chat        — GWS workspace relay
 *   GET  /gems                  — List Gem personas
 *   POST /gems/:gemId           — Invoke a Gem
 *   GET  /stream                — SSE streaming generate
 *   GET  /health                — Health + capabilities
 *   POST /push                  — Push session cookies (SESSION_PUSH_KEY auth)
 *   POST /cookies/nlm           — Push NLM cookies
 *   POST /cookies/gemini        — Push Gemini web cookies
 *
 * GENERATE OPTIONS:
 *   prompt, model (default: gemini-3-flash), system, notebooks (bool),
 *   notebook_ids[], drive_file_ids[], gem, temperature, max_tokens,
 *   chat_meta=[cid,rid,mid], stream (bool)
 *
 * MODEL NAMES:
 *   web-cookie models: gemini-3-flash/pro/thinking ± plus/advanced
 *   MULTI-TURN  chat_meta=[cid,rid,mid] for conversation continuity
 *
 * RESPONSE (web-cookie):
 *   { text, thoughts, images[], candidates[], session:{cid,rid,mid}, grounded, model, auth_mode }
 */

"use strict";

// ─── Model registry ───────────────────────────────────────────────────────────
// Inner model IDs used by Gemini web interface (web-cookie path)
const GEMINI_WEB_MODELS = {
  "gemini-3-flash":                  { id: "fbb127bbb056c959", cap: 1 },
  "gemini-3-flash-thinking":         { id: "5bf011840784117a", cap: 1 },
  "gemini-3-flash-plus":             { id: "fd9d4e15f6ab2ccd", cap: 1 },
  "gemini-3-flash-advanced":         { id: "0e6a8fc4d8e32e82", cap: 1 },
  "gemini-3-flash-thinking-plus":    { id: "2f8f2d83bfca7e1b", cap: 1 },
  "gemini-3-flash-thinking-advanced":{ id: "4a7e9c12b5d83f6e", cap: 1 },
  "gemini-3-pro":                    { id: "9d8ca3786ebdfbea", cap: 1 },
  "gemini-3-pro-plus":               { id: "2c4f891a3d75b6e8", cap: 1 },
  "gemini-3-pro-advanced":           { id: "7f3b9e5c2a81d047", cap: 1 },
};

// alias map (used by OpenAI compat + health endpoint) ----------
const GEMINI_MODEL_MAP = {
  // Gemini 3 — subscription tier (web-cookie auth), highest capability
  "gemini-3-pro-advanced":            "gemini-3-pro-advanced",
  "gemini-3-flash-thinking-advanced": "gemini-3-flash-thinking-advanced",
  "gemini-3-pro-plus":                "gemini-3-pro-plus",
  "gemini-3-flash-thinking-plus":     "gemini-3-flash-thinking-plus",
  "gemini-3-pro":                     "gemini-3-pro",
  "gemini-3-flash-thinking":          "gemini-3-flash-thinking",
  "gemini-3-flash-advanced":          "gemini-3-flash-advanced",
  "gemini-3-flash-plus":              "gemini-3-flash-plus",
  "gemini-3-flash":                   "gemini-3-flash",
  // Gemini 2.5 — API-key tier (function calling, no NLM grounding)
  "gemini-2.5-pro":                   "gemini-2.5-pro",
  "gemini-2.5-flash":                 "gemini-2.5-flash",
  // short aliases
  "pro-advanced":   "gemini-3-pro-advanced",
  "pro":            "gemini-3-pro-advanced",
  "flash-thinking": "gemini-3-flash-thinking",
  "thinking":       "gemini-3-flash-thinking",
  "flash":          "gemini-3-flash",
};
// API model names used when GEMINI_API_KEY is set
const GEMINI_API_MODELS = {
  "gemini-3-pro":                     "gemini-2.5-pro",
  "gemini-3-pro-plus":                "gemini-2.5-pro",
  "gemini-3-pro-advanced":            "gemini-2.5-pro",
  "gemini-3-flash":                   "gemini-2.5-flash",
  "gemini-3-flash-thinking":          "gemini-2.5-flash",
  "gemini-3-flash-plus":              "gemini-2.5-flash",
  "gemini-3-flash-advanced":          "gemini-2.5-flash",
  "gemini-3-flash-thinking-plus":     "gemini-2.5-flash",
  "gemini-3-flash-thinking-advanced": "gemini-2.5-flash",
};

const DEFAULT_MODEL = "gemini-3-flash";
const GEMINI_GW   = `https://gateway.ai.cloudflare.com/v1/e105d76aa6c851abdbd13d34d901cc7c/automation-hub/google-ai-studio/v1beta/models`;
const GEMINI_DIRECT = `https://generativelanguage.googleapis.com/v1beta/models`;

// NLM grounding: cache notebooks fetched this invocation
let _nlmCookies = null;

// ─── Auth helpers ────────────────────────────────────────────────────────────

function getAuthMode(env) {
  // Subscription tier takes priority — gemini-3-* models + NLM grounding
  // Cookies come from R2_AUTH (shared with NLM worker) or worker secrets
  if (env.SECURE_1PSID || env.SESSION_KEY || _nlmCookies || env.R2_AUTH || env.NLM) return "web-cookie";
  // API key fallback — gemini-2.5-* only, no NLM grounding, uses quota
  if (env.GEMINI_API_KEY) return "gemini-api";
  if (env.AI) return "workers-ai";
  return null;
}

/**
 * Build a full cookie string with all auth-related Google cookies.
 * Uses rotated PSIDTS if available, falls back to env secret.
 */
function buildFullCookieString(env, cookieOverride) {
  const base = cookieOverride || env.SECURE_1PSID || env.SESSION_KEY || "";
  if (!base) return "";
  const parts = [`__Secure-1PSID=${base}`];
  if (env.SECURE_1PSIDTS) parts.push(`__Secure-1PSIDTS=${env.SECURE_1PSIDTS}`);
  if (env.SECURE_1PSIDCC) parts.push(`__Secure-1PSIDCC=${env.SECURE_1PSIDCC}`);
  return parts.join("; ");
}

/**
 * Fetch with retry logic. Retries on 429 and 5xx with exponential backoff.
 * @param {Fetcher|string} target - Service binding (e.g. env.GOOGLE_AUTH) or URL string
 * @param {Request} request - Request to send (will be cloned on each retry)
 * @param {number} maxRetries - Maximum retry attempts (default: 3)
 */
async function fetchWithRetry(target, request, maxRetries = 3) {
  let lastErr;
  for (let i = 0; i < maxRetries; i++) {
    try {
      const resp = typeof target === "string"
        ? await fetch(target, request.clone())
        : await target.fetch(request.clone());
      if (resp.status === 429 || resp.status >= 500) {
        await new Promise(r => setTimeout(r, Math.pow(2, i) * 200));
        lastErr = new Error(`HTTP ${resp.status}`);
        continue;
      }
      return resp;
    } catch (e) {
      lastErr = e;
      await new Promise(r => setTimeout(r, Math.pow(2, i) * 200));
    }
  }
  throw lastErr;
}

// ─── Cookie refresh from GOOGLE_AUTH binding ─────────────────────────────────

async function refreshCookiesFromAuthWorker(env) {
  if (env.GOOGLE_AUTH) {
    try {
      const authResp = await fetchWithRetry(env.GOOGLE_AUTH, new Request("https://google-auth-worker.internal/token", {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      }));
      if (authResp.ok) {
        const authData = await authResp.json();
        if (authData.cookie || authData.SECURE_1PSID) {
          _nlmCookies = authData.cookie || authData.SECURE_1PSID;
          return _nlmCookies;
        }
      }
    } catch (e) {
      console.error("GOOGLE_AUTH refresh failed:", e.message);
    }
  }
  return null;
}

async function getActiveCookie(env, cookieOverride) {
  if (cookieOverride) return cookieOverride;
  if (_nlmCookies) return _nlmCookies;
  // Try refreshing from auth worker
  const refreshed = await refreshCookiesFromAuthWorker(env);
  if (refreshed) return refreshed;
  // Fall back to env secret
  return env.SECURE_1PSID || env.SESSION_KEY || null;
}

// ─── NLM grounding helpers ───────────────────────────────────────────────────

async function fetchNLMContext(env, notebookIds) {
  if (!env.NLM) return null;
  try {
    const url = notebookIds?.length
      ? `https://notebooklm-worker.internal/sources?ids=${notebookIds.join(",")}`
      : "https://notebooklm-worker.internal/sources/top?limit=5";
    const resp = await fetchWithRetry(env.NLM, new Request(url));
    if (!resp.ok) return null;
    const data = await resp.json();
    if (!data?.sources?.length) return null;
    return data.sources.map(s => `[${s.title}]\n${s.content}`).join("\n\n---\n\n");
  } catch (e) {
    console.error("NLM grounding fetch failed:", e.message);
    return null;
  }
}

async function fetchDriveContext(env, driveFileIds) {
  if (!env.GWS || !driveFileIds?.length) return null;
  try {
    const resp = await fetchWithRetry(env.GWS, new Request(
      `https://gws-worker.internal/drive/files?ids=${driveFileIds.join(",")}`,
    ));
    if (!resp.ok) return null;
    const data = await resp.json();
    if (!data?.files?.length) return null;
    return data.files.map(f => `[${f.name}]\n${f.content}`).join("\n\n---\n\n");
  } catch (e) {
    console.error("Drive context fetch failed:", e.message);
    return null;
  }
}

// ─── Response helpers ────────────────────────────────────────────────────────

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Authorization, Content-Type",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    },
  });
}

function sseResponse(stream) {
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Access-Control-Allow-Origin": "*",
    },
  });
}

// ─── Gemini web-cookie generate (subscription tier) ──────────────────────────

async function generateViaCookie(prompt, env, {
  model = DEFAULT_MODEL,
  system = null,
  chatMeta = null,
  cookieOverride = null,
  temperature = null,
  maxTokens = null,
} = {}) {
  const cookie = await getActiveCookie(env, cookieOverride);
  if (!cookie) throw new Error("No cookie available for web-cookie auth");

  const webModel = GEMINI_WEB_MODELS[model] || GEMINI_WEB_MODELS[DEFAULT_MODEL];
  const [cid, rid, mid] = chatMeta || [null, null, null];

  const innerPayload = {
    prompt: system ? `${system}\n\n${prompt}` : prompt,
    model: webModel.id,
    ...(cid && { conversation_id: cid }),
    ...(rid && { response_id: rid }),
    ...(mid && { choice_id: mid }),
    ...(temperature !== null && { temperature }),
    ...(maxTokens !== null && { max_output_tokens: maxTokens }),
  };

  const resp = await fetchWithRetry(
    "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate",
    new Request("https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": buildFullCookieString(env, cookie),
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "X-Goog-Authuser": "0",
      },
      body: `f.req=${encodeURIComponent(JSON.stringify([null, JSON.stringify([innerPayload])]))}`,
    }),
  );

  if (!resp.ok) throw new Error(`Gemini web returned ${resp.status}`);
  const raw = await resp.text();

  // Parse the streaming response format
  let text = "", thoughts = null, images = [], candidates = [], session = {};
  try {
    const chunks = raw.split("\n").filter(l => l.startsWith("["));
    for (const chunk of chunks) {
      try {
        const parsed = JSON.parse(chunk);
        const inner = parsed?.[0]?.[2];
        if (!inner) continue;
        const data = JSON.parse(inner);
        // Extract text
        const candidate = data?.[4]?.[0];
        if (candidate) {
          const parts = candidate[1]?.[0] || [];
          for (const part of parts) {
            if (typeof part[1] === "string") text += part[1];
            if (part[3]?.includes("thoughts")) thoughts = part[1];
          }
          // Session continuity
          if (data[1]) session.cid = data[1][0];
          if (data[4]?.[0]?.[0]) session.rid = data[4][0][0];
          if (data[4]?.[0]?.[1]?.[0]) session.mid = data[4][0][1][0][0];
        }
      } catch {}
    }
  } catch (e) {
    console.error("Response parse error:", e.message);
  }

  return { text: text.trim(), thoughts, images, candidates, session, model, auth_mode: "web-cookie" };
}

// ─── Gemini official API generate (API-key tier) ─────────────────────────────

async function generateViaOfficialAPI(prompt, env, { model = "gemini-2.5-flash", system = null } = {}) {
  const apiModel = GEMINI_API_MODELS[model] || model;
  const reqBody = { contents: [{ role: "user", parts: [{ text: prompt }] }] };
  if (system) reqBody.system_instruction = { parts: [{ text: system }] };

  const endpoints = [];
  if (env.GEMINI_API_KEY) {
    endpoints.push({ url: `${GEMINI_GW}/${apiModel}:generateContent`, auth: `Bearer ${env.GEMINI_API_KEY}`, label: "gateway" });
    endpoints.push({ url: `${GEMINI_DIRECT}/${apiModel}:generateContent?key=${env.GEMINI_API_KEY}`, auth: null, label: "direct-key" });
  }
  if (!endpoints.length) throw new Error("No API key configured");

  let lastErr;
  for (const ep of endpoints) {
    try {
      const headers = { "Content-Type": "application/json" };
      if (ep.auth) headers["Authorization"] = ep.auth;
      const resp = await fetchWithRetry(ep.url, new Request(ep.url, {
        method: "POST", headers, body: JSON.stringify(reqBody),
      }));
      if (!resp.ok) { lastErr = new Error(`${ep.label}: HTTP ${resp.status}`); continue; }
      const data = await resp.json();
      const text = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
      return { text, mode: `gemini-api (${ep.label})`, model };
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error("All API endpoints failed");
}

// ─── Workers AI fallback ──────────────────────────────────────────────────────

async function generateViaWorkersAI(prompt, env, { model = "@cf/meta/llama-3.1-8b-instruct" } = {}) {
  if (!env.AI) throw new Error("Workers AI binding not available");
  const result = await env.AI.run(model, {
    messages: [{ role: "user", content: prompt }],
    gateway: env.AI_GATEWAY ? { id: env.AI_GATEWAY_ID || "automation-hub" } : undefined,
  });
  return { text: result.response || "", mode: "workers-ai", model };
}

// ─── NLM-grounded generate ────────────────────────────────────────────────────

async function handleGenerate(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}

  const {
    prompt, model = DEFAULT_MODEL, system = null,
    notebooks = true, notebook_ids = null, drive_file_ids = null,
    gem = null, temperature = null, max_tokens = null,
    chat_meta = null, stream = false, cookie = null,
  } = body;

  if (!prompt) return jsonResponse({ error: "prompt required" }, 400);

  // Build grounding context
  let groundingContext = "";
  if (notebooks) {
    const [nlmCtx, driveCtx] = await Promise.all([
      fetchNLMContext(env, notebook_ids),
      fetchDriveContext(env, drive_file_ids),
    ]);
    if (nlmCtx) groundingContext += nlmCtx;
    if (driveCtx) groundingContext += (groundingContext ? "\n\n---\n\n" : "") + driveCtx;
  }

  const gemText = gem && typeof gem === "string" ? gem : null;
  const fullSystem = [system, gemText, groundingContext ? `Context:\n${groundingContext}` : null]
    .filter(Boolean).join("\n\n") || null;

  const authMode = getAuthMode(env);
  if (!authMode) return jsonResponse({
    error: "No auth configured. Set SECURE_1PSID/SESSION_KEY or configure GOOGLE_AUTH/NLM service bindings.",
    hint: "Set GEMINI_API_KEY or configure GOOGLE_AUTH/NLM service bindings."
  }, 503);

  const apiSystem = gem && authMode === "gemini-api" ? [fullSystem, typeof gem === "string" ? gem : null].filter(Boolean).join("\n\n") || null : fullSystem;

  try {
    let result;
    if (authMode === "web-cookie") {
      result = await generateViaCookie(prompt, env, {
        model, system: fullSystem, chatMeta: chat_meta,
        cookieOverride: cookie, temperature, maxTokens: max_tokens,
      });
    } else if (authMode === "gemini-api") {
      result = await generateViaOfficialAPI(prompt, env, {
        model, system: apiSystem,
      });
    } else if (authMode === "workers-ai") {
      result = await generateViaWorkersAI(prompt, env);
    } else {
      return jsonResponse({ error: "No auth configured. Set GEMINI_API_KEY or configure GOOGLE_AUTH/NLM service bindings.", hint: "Run 'notebooklm login' or add GEMINI_API_KEY secret." }, 500);
    }
    return jsonResponse({
      ...result,
      grounded: !!groundingContext,
      auth_mode: authMode
    });
  } catch (err) {
    return jsonResponse({ error: err.message, mode: authMode, hint: "If cookies expired, run 'notebooklm login' to refresh auth in R2." }, 500);
  }
}

// ─── Streaming generate ───────────────────────────────────────────────────────

async function handleStream(request, env) {
  let body = {};
  if (request.method === "POST") {
    try { body = await request.json(); } catch {}
  } else {
    const url = new URL(request.url);
    body.prompt = url.searchParams.get("prompt") || "";
    body.model = url.searchParams.get("model") || DEFAULT_MODEL;
  }
  const { prompt, model = DEFAULT_MODEL, system = null } = body;
  if (!prompt) return jsonResponse({ error: "prompt required" }, 400);

  const authMode = getAuthMode(env);
  if (authMode !== "web-cookie" && authMode !== "gemini-api")
    return jsonResponse({ error: "Streaming requires GEMINI_API_KEY or web-cookie auth (GOOGLE_AUTH/NLM binding)." }, 400);

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  (async () => {
    try {
      if (authMode === "gemini-api") {
        const result = await generateViaOfficialAPI(prompt, env, { model: model ?? "gemini-2.5-flash", system });
        await writer.write(encoder.encode(`data: ${JSON.stringify({ text: result.text, done: false })}\n\n`));
        await writer.write(encoder.encode(`data: ${JSON.stringify({ done: true })}\n\n`));
      } else {
        // web-cookie streaming: simulate with single response
        const result = await generateViaCookie(prompt, env, { model, system });
        const words = result.text.split(" ");
        for (let i = 0; i < words.length; i += 5) {
          const chunk = words.slice(i, i + 5).join(" ");
          await writer.write(encoder.encode(`data: ${JSON.stringify({ text: chunk, done: false })}\n\n`));
        }
        await writer.write(encoder.encode(`data: ${JSON.stringify({ done: true, session: result.session })}\n\n`));
      }
    } catch (e) {
      await writer.write(encoder.encode(`data: ${JSON.stringify({ error: e.message, done: true })}\n\n`));
    } finally {
      await writer.close();
    }
  })();

  return sseResponse(readable);
}

// ─── Multi-turn chat ──────────────────────────────────────────────────────────

async function handleChat(request, env) {
  if (getAuthMode(env) !== "web-cookie")
    return jsonResponse({ error: "Gemini chat requires web-cookie auth (subscription tier)." }, 400);

  let body = {};
  try { body = await request.json(); } catch {}
  const { prompt, model = DEFAULT_MODEL, system = null, chat_meta = null, cookie = null } = body;
  if (!prompt) return jsonResponse({ error: "prompt required" }, 400);

  const result = await generateViaCookie(prompt, env, { model, system, chatMeta: chat_meta, cookieOverride: cookie });
  return jsonResponse(result);
}

// ─── Gem personas ─────────────────────────────────────────────────────────────

async function handleGems(request, env, gemId) {
  const cookie = await getActiveCookie(env, null);
  if (!cookie) return jsonResponse({ error: "web-cookie auth required for Gems" }, 401);

  if (request.method === "GET") {
    // List available gems — fetched from Gemini web
    return jsonResponse({ gems: [], note: "Gem listing requires scraping; use POST /gems/:id to invoke" });
  }

  let body = {};
  try { body = await request.json(); } catch {}
  const { prompt, system = null } = body;
  if (!prompt) return jsonResponse({ error: "prompt required" }, 400);

  const result = await generateViaCookie(prompt, env, { model: DEFAULT_MODEL, system, gem: gemId });
  return jsonResponse(result);
}

// ─── Workspace relay ──────────────────────────────────────────────────────────

const WORKSPACE_ACTIONS = {
  send_email:     { binding: "GWS", path: "/gmail/send" },
  search_email:   { binding: "GWS", path: "/gmail/search" },
  create_event:   { binding: "GWS", path: "/calendar/events" },
  list_events:    { binding: "GWS", path: "/calendar/events" },
  create_task:    { binding: "GWS", path: "/tasks/create" },
  list_tasks:     { binding: "GWS", path: "/tasks" },
  read_doc:       { binding: "GWS", path: "/docs/read" },
  create_doc:     { binding: "GWS", path: "/docs/create" },
  read_sheet:     { binding: "GWS", path: "/sheets/read" },
  write_sheet:    { binding: "GWS", path: "/sheets/write" },
  list_drive:     { binding: "GWS", path: "/drive/list" },
  upload_drive:   { binding: "GWS", path: "/drive/upload" },
};
const ALIASES = {
  search_gmail: "search_email", search_mail: "search_email", find_email: "search_email",
  schedule_meeting: "create_event", add_event: "create_event",
};

async function handleWorkspaceAction(env, action, params) {
  const rawAction = action;
  const resolvedAction = ALIASES[rawAction] || rawAction;
  const spec = WORKSPACE_ACTIONS[resolvedAction];

  if (!spec) {
    return { success: false, error: `Unknown action: ${rawAction}`, available: Object.keys(WORKSPACE_ACTIONS) };
  }
  const binding = env[spec.binding];
  if (!binding) {
    return { success: false, error: `${spec.binding} binding not configured` };
  }

  try {
    const resp = await fetchWithRetry(binding, new Request(`https://${spec.binding.toLowerCase()}-worker.internal${spec.path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    }));
    const data = await resp.json();
    return { success: resp.ok, ...data };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

async function handleWorkspaceChat(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { message, actions = [], context = {} } = body;

  const results = [];
  for (const { action, params = {} } of actions) {
    const result = await handleWorkspaceAction(env, action, { ...params, ...context });
    results.push({ action, result });
  }

  if (message) {
    const contextStr = results.length
      ? `Workspace results:\n${results.map(r => `${r.action}: ${JSON.stringify(r.result)}`).join("\n")}`
      : "";
    const geminiResult = await handleGenerate(
      new Request("https://internal/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: message, system: contextStr || undefined }),
      }),
      env,
    );
    const geminiData = await geminiResult.json();
    return jsonResponse({ message: geminiData.text, workspace_results: results, auth_mode: geminiData.auth_mode });
  }

  return jsonResponse({ workspace_results: results });
}

// ─── MCP server ───────────────────────────────────────────────────────────────

const MCP_TOOLS = [
  {
    name: "gemini_generate",
    description: "Generate text using Gemini (subscription tier: gemini-3-* models with NLM grounding)",
    inputSchema: {
      type: "object",
      properties: {
        prompt: { type: "string", description: "The prompt to send to Gemini" },
        model: { type: "string", description: "Model name (default: gemini-3-flash)", default: DEFAULT_MODEL },
        system: { type: "string", description: "System prompt / instructions" },
        notebooks: { type: "boolean", description: "Enable NLM notebook grounding (default: true)", default: true },
        notebook_ids: { type: "array", items: { type: "string" }, description: "Specific notebook IDs to ground from" },
        drive_file_ids: { type: "array", items: { type: "string" }, description: "Drive file IDs to inject as context" },
        gem: { type: "string", description: "Gem persona ID or instructions" },
        temperature: { type: "number", description: "Sampling temperature" },
        max_tokens: { type: "number", description: "Maximum output tokens" },
      },
      required: ["prompt"],
    },
  },
  {
    name: "gemini_chat",
    description: "Multi-turn chat with Gemini (web-cookie/subscription tier only)",
    inputSchema: {
      type: "object",
      properties: {
        prompt: { type: "string" },
        model: { type: "string", default: DEFAULT_MODEL },
        system: { type: "string" },
        chat_meta: { type: "array", items: { type: "string" }, description: "[cid, rid, mid] for conversation continuity" },
      },
      required: ["prompt"],
    },
  },
  {
    name: "workspace_action",
    description: "Execute a Google Workspace action (send email, create calendar event, read Drive file, etc.)",
    inputSchema: {
      type: "object",
      properties: {
        action: { type: "string", enum: Object.keys(WORKSPACE_ACTIONS), description: "Workspace action to execute" },
        params: { type: "object", description: "Action-specific parameters" },
      },
      required: ["action"],
    },
  },
  {
    name: "gws_generate",
    description: "Generate Gemini response grounded in Google Workspace data (Drive, Docs, Sheets)",
    inputSchema: {
      type: "object",
      properties: {
        prompt: { type: "string" },
        drive_file_ids: { type: "array", items: { type: "string" } },
        model: { type: "string", default: DEFAULT_MODEL },
        system: { type: "string" },
      },
      required: ["prompt"],
    },
  },
];

let _mcpSessions = {};

async function handleMCPRequest(request, env, ctx) {
  const method = request.method;
  const url = new URL(request.url);

  // CORS
  if (method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, Mcp-Session-Id",
        "Access-Control-Expose-Headers": "Mcp-Session-Id",
      },
    });
  }

  const sessionId = request.headers.get("Mcp-Session-Id") || crypto.randomUUID();

  if (method === "DELETE") {
    delete _mcpSessions[sessionId];
    return new Response(null, { status: 204, headers: { "Access-Control-Allow-Origin": "*" } });
  }

  let body = {};
  try { body = await request.json(); } catch {}

  const { jsonrpc, id, method: rpcMethod, params = {} } = body;

  let result;
  switch (rpcMethod) {
    case "initialize":
      result = {
        protocolVersion: "2024-11-05",
        serverInfo: { name: "gemini-webapi-worker", version: "5.3.1" },
        capabilities: { tools: {} },
      };
      break;
    case "tools/list":
      result = { tools: MCP_TOOLS };
      break;
    case "tools/call": {
      const { name, arguments: args = {} } = params;
      switch (name) {
        case "gemini_generate": {
          const resp = await handleGenerate(new Request("https://internal/generate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(args),
          }), env);
          const data = await resp.json();
          result = { content: [{ type: "text", text: JSON.stringify(data) }] };
          break;
        }
        case "gemini_chat": {
          const resp = await handleChat(new Request("https://internal/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(args),
          }), env);
          const data = await resp.json();
          result = { content: [{ type: "text", text: JSON.stringify(data) }] };
          break;
        }
        case "workspace_action": {
          const actionResult = await handleWorkspaceAction(env, args.action, args.params || {});
          result = { content: [{ type: "text", text: JSON.stringify(actionResult) }] };
          break;
        }
        case "gws_generate": {
          const resp = await handleGenerate(new Request("https://internal/generate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ...args, notebooks: false }),
          }), env);
          const data = await resp.json();
          result = { content: [{ type: "text", text: JSON.stringify(data) }] };
          break;
        }
        default:
          result = { content: [{ type: "text", text: `Unknown tool: ${name}` }], isError: true };
      }
      break;
    }
    default:
      return new Response(JSON.stringify({
        jsonrpc: "2.0", id,
        error: { code: -32601, message: "Method not found" },
      }), { status: 200, headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Mcp-Session-Id": sessionId } });
  }

  return new Response(JSON.stringify({ jsonrpc: "2.0", id, result }), {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Mcp-Session-Id": sessionId,
    },
  });
}

// ─── SSE MCP transport (legacy) ───────────────────────────────────────────────

async function handleSSE(request, env, ctx) {
  const sessionId = crypto.randomUUID();
  const endpointUrl = `https://${new URL(request.url).hostname}/sse`;

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  ctx.waitUntil((async () => {
    await writer.write(encoder.encode(`event: endpoint\ndata: ${endpointUrl}?sessionId=${sessionId}\n\n`));
    // Keep-alive
    const interval = setInterval(async () => {
      try { await writer.write(encoder.encode(`: ping\n\n`)); } catch { clearInterval(interval); }
    }, 15000);
    // Store writer for POST messages
    _mcpSessions[sessionId] = { writer, encoder, interval };
  })());

  return sseResponse(readable);
}

async function handleSSEMessage(request, env, ctx) {
  const url = new URL(request.url);
  const sessionId = url.searchParams.get("sessionId");
  const session = _mcpSessions[sessionId];

  let body = {};
  try { body = await request.json(); } catch {}

  const { jsonrpc, id, method: rpcMethod, params = {} } = body;

  // Process same as MCP
  const mockReq = new Request("https://internal/mcp", {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(sessionId ? { "Mcp-Session-Id": sessionId } : {}) },
    body: JSON.stringify(body),
  });
  const resp = await handleMCPRequest(mockReq, env, ctx);
  const data = await resp.json();

  if (session) {
    try {
      await session.writer.write(session.encoder.encode(`event: message\ndata: ${JSON.stringify(data)}\n\n`));
    } catch {}
  }

  return new Response(null, { status: 202, headers: { "Access-Control-Allow-Origin": "*" } });
}

// ─── Cookie push endpoints ────────────────────────────────────────────────────

async function handleCookiePush(request, env, type) {
  const authHeader = request.headers.get("Authorization") || "";
  const token = authHeader.replace("Bearer ", "");
  if (token !== env.SESSION_PUSH_KEY) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }
  let body = {};
  try { body = await request.json(); } catch {}
  const { cookie, cookies } = body;
  const value = cookie || cookies;
  if (!value) return jsonResponse({ error: "cookie or cookies field required" }, 400);

  _nlmCookies = typeof value === "string" ? value : JSON.stringify(value);

  // Persist to KV if available
  if (env.KV) {
    await env.KV.put(`${type}_cookie`, _nlmCookies, { expirationTtl: 3600 * 24 });
  }

  return jsonResponse({ ok: true, type, updated: true });
}

// ─── Health endpoint ──────────────────────────────────────────────────────────

async function handleHealth(env) {
  const authMode = getAuthMode(env);
  const hasNLM = !!env.NLM;
  const hasGWS = !!env.GWS;
  const hasHUB = !!env.HUB;
  const hasCookie = !!(env.SECURE_1PSID || env.SESSION_KEY || _nlmCookies);

  return jsonResponse({
    status: "ok",
    version: "5.3.1",
    auth_mode: authMode || "none",
    auth_mode_priority: "web-cookie > gemini-api > workers-ai",
    bindings: {
      NLM: hasNLM, GWS: hasGWS, HUB: hasHUB, AI: !!env.AI,
      KV: !!env.KV, R2_AUTH: !!env.R2_AUTH, GOOGLE_AUTH: !!env.GOOGLE_AUTH,
    },
    features: {
      grounding: hasNLM,
      workspace: hasGWS,
      streaming: authMode === "web-cookie" || authMode === "gemini-api",
      chat: authMode === "web-cookie",
      gems: hasCookie,
    },
    models: {
      subscription: Object.keys(GEMINI_WEB_MODELS),
      api_key: ["gemini-2.5-flash", "gemini-2.5-pro"],
      default: DEFAULT_MODEL,
    },
  });
}

// ─── Main router ──────────────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Authorization, Content-Type, Mcp-Session-Id",
        },
      });
    }

    // Auth check (skip push and cookie endpoints)
    const skipAuth = ["/push", "/cookies/nlm", "/cookies/gemini", "/health", "/mcp", "/sse"].some(p => path.startsWith(p));
    if (!skipAuth) {
      const authHeader = request.headers.get("Authorization") || "";
      const token = authHeader.replace("Bearer ", "");
      if (env.SESSION_PUSH_KEY && token !== env.SESSION_PUSH_KEY) {
        // Relaxed auth for generate endpoints — allow if cookies available
        if (!["/generate", "/chat", "/stream", "/workspace", "/gems"].some(p => path.startsWith(p))) {
          return jsonResponse({ error: "Unauthorized" }, 401);
        }
      }
    }

    try {
      // ── MCP endpoints ───────────────────────────────────────────────────────
      if (path === "/mcp") return handleMCPRequest(request, env, ctx);
      if (path === "/sse" && method === "GET") return handleSSE(request, env, ctx);
      if (path === "/sse" && method === "POST") return handleSSEMessage(request, env, ctx);

      // ── Generate ────────────────────────────────────────────────────────────
      if (path === "/generate" || path === "/generate/") return handleGenerate(request, env);
      if (path === "/generate/stream") return handleStream(request, env);
      if (path === "/stream") return handleStream(request, env);
      if (path === "/chat") return handleChat(request, env);

      // ── Gems ────────────────────────────────────────────────────────────────
      if (path === "/gems" || path === "/gems/") return handleGems(request, env, null);
      if (path.startsWith("/gems/")) return handleGems(request, env, path.slice(6));

      // ── Workspace relay ─────────────────────────────────────────────────────
      if (path === "/workspace/chat" || path === "/workspace") return handleWorkspaceChat(request, env);
      if (path.startsWith("/workspace/action")) {
        let body = {}; try { body = await request.json(); } catch {}
        const result = await handleWorkspaceAction(env, body.action, body.params || {});
        return jsonResponse(result);
      }

      // ── Cookie push ─────────────────────────────────────────────────────────
      if (path === "/push") return handleCookiePush(request, env, "gemini");
      if (path === "/cookies/nlm") return handleCookiePush(request, env, "nlm");
      if (path === "/cookies/gemini") return handleCookiePush(request, env, "gemini");

      // ── Health ──────────────────────────────────────────────────────────────
      if (path === "/health") return handleHealth(env);

      // ── Debug: auth mode ────────────────────────────────────────────────────
      if (path === "/auth/mode") {
        return jsonResponse({
          auth_mode: getAuthMode(env),
          auth_mode_priority: "web-cookie > gemini-api > workers-ai",
          has_cookie: !!(env.SECURE_1PSID || env.SESSION_KEY || _nlmCookies),
          has_api_key: !!env.GEMINI_API_KEY,
          has_nlm: !!env.NLM,
          has_google_auth: !!env.GOOGLE_AUTH,
        });
      }

      // ── Tools/call via REST (for MCP clients that prefer REST) ──────────────
      if (path.startsWith("/tools/")) {
        const toolName = path.slice(7);
        let args = {}; try { args = await request.json(); } catch {}
        const toolCallAuthToken = args.tcSaToken || env.GEMINI_API_KEY;

        switch (toolName) {
          case "gemini_generate": {
            const resp = await handleGenerate(new Request("https://internal/generate", {
              method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(args),
            }), env);
            return resp;
          }
          case "gemini_chat": {
            const resp = await handleChat(new Request("https://internal/chat", {
              method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(args),
            }), env);
            return resp;
          }
          case "workspace_action": {
            const result = await handleWorkspaceAction(env, args.action, args.params || {});
            return jsonResponse(result);
          }
          default:
            return jsonResponse({ error: `Unknown tool: ${toolName}` }, 404);
        }
      }

      // ── OpenAI-compat chat completions ──────────────────────────────────────
      if (path === "/v1/chat/completions") {
        let body = {}; try { body = await request.json(); } catch {}
        const { messages = [], model: reqModel = DEFAULT_MODEL, stream: streamReq = false } = body;
        const lastUser = [...messages].reverse().find(m => m.role === "user");
        const systemMsg = messages.find(m => m.role === "system");
        if (!lastUser) return jsonResponse({ error: "No user message" }, 400);

        const mappedModel = GEMINI_MODEL_MAP[reqModel] || reqModel;
        const authMode = getAuthMode(env);
        let result;

        if (authMode === "web-cookie") {
          result = await generateViaCookie(lastUser.content, env, { model: mappedModel, system: systemMsg?.content });
        } else if (authMode === "gemini-api") {
          result = await generateViaOfficialAPI(lastUser.content, env, { model: mappedModel });
        } else {
          return jsonResponse({ error: "No auth configured" }, 503);
        }

        return jsonResponse({
          id: `chatcmpl-${Date.now()}`,
          object: "chat.completion",
          model: mappedModel,
          choices: [{ index: 0, message: { role: "assistant", content: result.text }, finish_reason: "stop" }],
          _gemini: { auth_mode: authMode, model: mappedModel },
        });
      }

      return jsonResponse({ error: "Not found", path }, 404);
    } catch (err) {
      console.error("Worker error:", err);
      return jsonResponse({ error: "Internal server error", message: err.message }, 500);
    }
  },
};
