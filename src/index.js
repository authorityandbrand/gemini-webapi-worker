/**
 * Gemini WebAPI Cloudflare Worker  v5.4.1
 *
 * NEW in v5.2:
 *   - Drive file context: pass `drive_file_ids` in /generate to inject Google Drive
 *     file content as system context before NLM notebook grounding
 *   - Workspace relay: POST /workspace/chat routes to GWS service binding
 *
 * NEW in v5.1:
 *   - MCP server: POST /mcp (Streamable HTTP) + GET/POST /sse (legacy SSE)
 *   - Gem personas: GET/POST /gems
 *   - Streaming: GET /generate/stream (SSE)
 *   - /health: includes auth_mode, bindings, model list
 *
 * v5.4.1 changes:
 *   - Subscription tier (web-cookie / gemini-3-*) is the ONLY auth path
 *   - GEMINI_API_KEY removed entirely — no fallback to official API
 *   - gemini-2.x / gemini-1.x model aliases removed
 *   - tool_use / agentic task types route to gemini-3-flash-thinking-advanced
 *   - OpenAI /chat/completions: tools[] stripped (subscription doesn't support function calling)
 *
 * AUTH (single path):
 *   web-cookie — Gemini Advanced subscription (gemini-3-* models, NLM grounding)
 *   Cookies: SECURE_1PSID (web, Gemini Advanced subscription) | GOOGLE_AUTH/NLM binding | Workers AI (fallback)
 *   Workers AI (env.AI) — free fallback only when no cookies/NLM available
 *
 * BINDINGS (all optional — worker degrades gracefully):
 *   NLM         Auto-grounding from notebooks / legal sources  (NLM binding)
 *   DRIVE       drive_file_ids in /generate injects file text as context (GWS binding)
 *   HUB         Claude relay + AI Gateway logging              (HUB binding)
 *   KV/KV_CACHE Response cache + session state
 *   R2_AUTH     Playwright cookies shared with notebooklm-worker
 *   GOOGLE_AUTH Cookie refresh from google-auth-worker
 *
 * MODELS (subscription tier only):
 *   gemini-3-flash / gemini-3-flash-plus / gemini-3-flash-advanced
 *   gemini-3-flash-thinking / gemini-3-flash-thinking-plus / gemini-3-flash-thinking-advanced
 *   gemini-3-pro / gemini-3-pro-plus / gemini-3-pro-advanced
 */

"use strict";

// ─── Model registry ───────────────────────────────────────────────────────────
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

// API_MODEL_MAP removed — subscription tier only, no API key path

// alias map (used by OpenAI compat + health endpoint)
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
  // Gemini 2.x/1.x removed — subscription tier (gemini-3-*) only
  // Short aliases
  "pro-advanced":   "gemini-3-pro-advanced",
  "pro":            "gemini-3-pro-advanced",
  "flash-thinking": "gemini-3-flash-thinking-advanced",
  "flash":          "gemini-3-flash-advanced",
};

const TASK_MODEL_MAP = {
  // Legal reasoning — highest capability
  legal_analysis:        "gemini-3-pro-advanced",
  strategy:              "gemini-3-pro-advanced",
  rico:                  "gemini-3-pro-advanced",
  constitutional:        "gemini-3-pro-advanced",
  damages:               "gemini-3-pro-advanced",
  // Deep reasoning — chain-of-thought required
  deep_research:         "gemini-3-flash-thinking-advanced",
  contradiction_detect:  "gemini-3-flash-thinking-advanced",
  document_review:       "gemini-3-flash-thinking-plus",
  // High-volume background tasks — speed + cost efficiency
  batch_enrichment:      "gemini-3-flash-advanced",
  summary:               "gemini-3-flash-advanced",
  classify:              "gemini-3-flash-advanced",
  // Tool-calling tasks — route to thinking model (subscription tier)
  tool_use:              "gemini-3-flash-thinking-advanced",
  agentic:               "gemini-3-flash-thinking-advanced",
};

// ---------- OpenAI-compatible endpoint (AI Gateway Custom Provider) ----------
// Two paths:
//   A. tools[] stripped (not supported on subscription), falls through to web-cookie
//   B. no tools        → web-cookie subscription (Gemini 3, NLM grounding)
// Cloudflare AI Gateway strips /v1 prefix → registered at /chat/completions.
async function handleOpenAICompletions(request, env) {
  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: { message: "Request body must be valid JSON.", type: "invalid_request_error" } }, 400); }

  const { model: reqModel, messages = [], temperature, max_tokens, tools, tool_choice, task_type } = body;

  if (!Array.isArray(messages) || messages.length === 0) {
    return jsonResponse({ error: { message: "'messages' must be a non-empty array.", type: "invalid_request_error" } }, 400);
  }

  // Model selection: explicit model > task_type routing > default
  const routedModel = TASK_MODEL_MAP[task_type] || null;
  const geminiModel = GEMINI_MODEL_MAP[reqModel] || reqModel || routedModel || "gemini-3-pro-advanced";

  const created = Math.floor(Date.now() / 1000);
  const completionId = `chatcmpl-${created}-${Math.random().toString(36).slice(2, 10)}`;

  // ── Path A: tools[] stripped — subscription tier does not support function calling ──
  if (Array.isArray(tools) && tools.length > 0) {
    // Drop tool definitions; fall through to web-cookie generate below.
    body.tools = undefined;
    body.tool_choice = undefined;
  }

  // ── Path B: web-cookie subscription (Gemini 3 + NLM grounding) ──
  const systemParts = messages.filter(m => m.role === "system").map(m => m.content);
  const userMessages = messages.filter(m => m.role !== "system");
  const lastUser = userMessages.filter(m => m.role === "user").pop();
  if (!lastUser) return jsonResponse({ error: { message: "No user message found.", type: "invalid_request_error" } }, 400);

  const prompt = typeof lastUser.content === "string" ? lastUser.content
    : Array.isArray(lastUser.content) ? lastUser.content.map(p => p.text ?? "").join("") : String(lastUser.content ?? "");
  const system = systemParts.length > 0 ? systemParts.join("\n\n") : null;

  const generateResp = await handleGenerate(
    new Request("https://internal/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        prompt,
        model: geminiModel,
        system,
        temperature: temperature ?? undefined,
        max_tokens: max_tokens ?? undefined,
        notebooks: true,
      }),
    }),
    env,
  );
  const genData = await generateResp.json();

  if (!generateResp.ok) {
    return jsonResponse({ error: { message: genData.error ?? "Generation failed", type: "server_error" } }, generateResp.status);
  }

  return jsonResponse({
    id: completionId,
    object: "chat.completion",
    created,
    model: geminiModel,
    choices: [{ index: 0, message: { role: "assistant", content: genData.text ?? "" }, finish_reason: "stop" }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    _gemini: { auth_mode: genData.auth_mode, model: geminiModel, grounded: genData.grounded },
  });
}

// ─── NLM cache ────────────────────────────────────────────────────────────────
let _nlmCookies    = null;
let _cachedPSIDTS  = null;
let _lastRotateAt  = null;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...corsHeaders(),
    },
  });
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, Mcp-Session-Id, MCP-Protocol-Version",
    "Access-Control-Expose-Headers":"Mcp-Session-Id, MCP-Protocol-Version",
  };
}

async function fetchWithRetry(target, request, maxRetries = 3) {
  let lastErr;
  for (let i = 0; i < maxRetries; i++) {
    try {
      const resp = typeof target === "string"
        ? await fetch(target, request.clone ? request.clone() : request)
        : await target.fetch(request.clone ? request.clone() : request);
      if (resp.status !== 429 && resp.status < 500) return resp;
      await new Promise(r => setTimeout(r, Math.pow(2, i) * 200));
      lastErr = new Error(`HTTP ${resp.status}`);
    } catch (e) {
      lastErr = e;
      await new Promise(r => setTimeout(r, Math.pow(2, i) * 200));
    }
  }
  throw lastErr;
}

// ─── Auth helpers ─────────────────────────────────────────────────────────────

function getAuthMode(env) {
  // Subscription tier takes priority — gemini-3-* models + NLM grounding
  if (env.SECURE_1PSID || env.SESSION_KEY || _nlmCookies || env.R2_AUTH || env.NLM) return "web-cookie";
  // Workers AI free fallback
  if (env.AI) return "workers-ai";
  return null;
}

async function fetchNLMCookies(env) {
  if (_nlmCookies) return _nlmCookies;
  if (env.GOOGLE_AUTH) {
    try {
      const resp = await fetchWithRetry(env.GOOGLE_AUTH, new Request("https://google-auth-worker.internal/token", { method: "GET" }));
      if (resp.ok) {
        const data = await resp.json();
        _nlmCookies = data.cookie || data.SECURE_1PSID || null;
        return _nlmCookies;
      }
    } catch {}
  }
  if (env.NLM) {
    try {
      const resp = await fetchWithRetry(env.NLM, new Request("https://notebooklm-worker.internal/cookies/gemini", { method: "GET" }));
      if (resp.ok) {
        const data = await resp.json();
        _nlmCookies = data.cookie || data.SECURE_1PSID || null;
        return _nlmCookies;
      }
    } catch {}
  }
  return env.SECURE_1PSID || env.SESSION_KEY || null;
}

async function rotateCookies(env) {
  _cachedPSIDTS = null;
  _nlmCookies = null;
  _lastRotateAt = Date.now();
  return fetchNLMCookies(env);
}

function buildFullCookieString(baseCookie, psidts, psidcc) {
  const parts = [`__Secure-1PSID=${baseCookie}`];
  if (psidts)  parts.push(`__Secure-1PSIDTS=${psidts}`);
  if (psidcc)  parts.push(`__Secure-1PSIDCC=${psidcc}`);
  return parts.join("; ");
}

function buildCookieString(env, cookieOverride) {
  const base = cookieOverride || _nlmCookies || env.SECURE_1PSID || env.SESSION_KEY;
  if (!base) return null;
  return buildFullCookieString(base, _cachedPSIDTS || env.SECURE_1PSIDTS, env.SECURE_1PSIDCC);
}

function buildModelHeaders(modelId) {
  return {
    "x-goog-ext-525001229-jspb": `["${modelId}",null,null,null,""]`,
  };
}

// ─── Session / batchExecute helpers ──────────────────────────────────────────

async function getSessionData(env, cookieStr) {
  // Get SA token (same pattern as before)
  let saToken = null;
  if (env.GOOGLE_AUTH) {
    try {
      const resp = await fetchWithRetry(env.GOOGLE_AUTH, new Request("https://google-auth-worker.internal/sa-token", { method: "GET" }));
      if (resp.ok) { const d = await resp.json(); saToken = d.token || null; }
    } catch {}
  }
  return { saToken };
}

async function batchExecute(cookieStr, payload, env) {
  const { saToken } = await getSessionData(env, cookieStr);
  const headers = {
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    "Cookie": cookieStr,
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "X-Goog-Authuser": "0",
    ...(saToken ? { "Authorization": `Bearer ${saToken}` } : {}),
  };
  const resp = await fetchWithRetry(
    "https://gemini.google.com/_/BardChatUi/data/batchexecute",
    new Request("https://gemini.google.com/_/BardChatUi/data/batchexecute?rpcids=CF5qKf&source-path=/&bl=boq_assistant-bard-web-server&hl=en&soc-app=1&soc-platform=1&soc-device=1&_reqid=1&rt=c", {
      method: "POST",
      headers,
      body: payload,
    }),
  );
  return resp;
}

function parseBatchResponse(raw) {
  try {
    const lines = raw.split("\n");
    for (const line of lines) {
      if (line.startsWith("[[")) {
        const outer = JSON.parse(line);
        for (const item of outer) {
          if (item[0] === "wrb.fr" && item[2]) {
            return JSON.parse(item[2]);
          }
        }
      }
    }
  } catch {}
  return null;
}

function parseStreamResponse(parsed) {
  if (!parsed) return { text: "", thoughts: null, images: [], session: {} };

  let text = "", thoughts = null, images = [], session = {};
  try {
    // Extract text from candidate
    const candidates = parsed[4]?.[0];
    if (candidates) {
      const parts = candidates[1]?.[0] ?? [];
      for (const part of parts) {
        if (typeof part[1] === "string") {
          if (part[3]?.includes?.("thoughts")) thoughts = part[1];
          else text += part[1];
        }
        if (Array.isArray(part[4])) {
          for (const img of part[4]) {
            if (img[0]?.url) images.push({ url: img[0].url, alt: img[0].alt ?? "" });
          }
        }
      }
      session.cid = parsed[1]?.[0] ?? null;
      session.rid = parsed[4]?.[0]?.[0] ?? null;
      session.mid = parsed[4]?.[0]?.[1]?.[0]?.[0] ?? null;
    }
  } catch {}
  return { text: text.trim(), thoughts, images, session };
}

// ─── NLM tool proxy ───────────────────────────────────────────────────────────

async function nlmTool(env, action, params = {}) {
  if (!env.NLM) return { error: "NLM binding not configured" };
  try {
    const resp = await fetchWithRetry(env.NLM, new Request(`https://notebooklm-worker.internal/mcp`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: action, arguments: params } }),
    }));
    if (!resp.ok) return { error: `NLM HTTP ${resp.status}` };
    const data = await resp.json();
    return data.result ?? data.error ?? data;
  } catch (e) {
    return { error: e.message };
  }
}

async function buildNotebookContext(env, { notebookIds, maxSources = 5, temporary = false } = {}) {
  if (!env.NLM) return "";
  try {
    const url = notebookIds?.length
      ? `https://notebooklm-worker.internal/sources?ids=${notebookIds.join(",")}&limit=${maxSources}`
      : `https://notebooklm-worker.internal/sources/top?limit=${maxSources}`;
    const resp = await fetchWithRetry(env.NLM, new Request(url));
    if (!resp.ok) return "";
    const data = await resp.json();
    if (!data?.sources?.length) return "";
    return data.sources.map(s => `### ${s.title}\n${s.content}`).join("\n\n---\n\n");
  } catch {
    return "";
  }
}

// ─── NLM write tools ──────────────────────────────────────────────────────────

async function nlmWriteTool(env, action, params) {
  return nlmTool(env, `nlm_${action}`, params);
}

async function nlmAsk(env, { notebookId, question }) {
  if (!env.NLM) return { error: "NLM binding not configured" };
  try {
    const resp = await fetchWithRetry(env.NLM, new Request(
      `https://notebooklm-worker.internal/notebooks/${notebookId}/ask`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question }) },
    ));
    return resp.ok ? resp.json() : { error: `NLM HTTP ${resp.status}` };
  } catch (e) { return { error: e.message }; }
}

async function nlmAddSource(env, { notebookId, url, content, title }) {
  return nlmWriteTool(env, "add_source", { notebook_id: notebookId, url, content, title });
}

async function nlmAddTextSource(env, { notebookId, content, title }) {
  return nlmWriteTool(env, "add_source", { notebook_id: notebookId, content, title });
}

async function nlmCreateNotebook(env, { title }) {
  return nlmWriteTool(env, "create_notebook", { title });
}

async function nlmGenerateArtifact(env, { notebookId, artifactType, instructions }) {
  return nlmWriteTool(env, "generate_artifact", { notebook_id: notebookId, artifact_type: artifactType, instructions });
}

async function nlmStartResearch(env, { notebookId, topic }) {
  return nlmWriteTool(env, "start_research", { notebook_id: notebookId, topic });
}

async function nlmCreateNote(env, { notebookId, title, content }) {
  return nlmWriteTool(env, "create_note", { notebook_id: notebookId, title, content });
}

async function nlmHealthCheck(env) {
  if (!env.NLM) return { ok: false, error: "NLM binding not configured" };
  try {
    const resp = await fetchWithRetry(env.NLM, new Request("https://notebooklm-worker.internal/health"));
    return resp.ok ? resp.json() : { ok: false, status: resp.status };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function nlmCreateLinkedDoc(env, { notebookId, title, content }) {
  return nlmWriteTool(env, "create_linked_doc", { notebook_id: notebookId, title, content });
}

async function nlmCreateLinkedSheet(env, { notebookId, title, initialData }) {
  return nlmWriteTool(env, "create_linked_sheet", { notebook_id: notebookId, title, initial_data: initialData });
}

async function nlmAppendToDoc(env, { docId, text, sourceId }) {
  return nlmWriteTool(env, "append_doc", { doc_id: docId, text, source_id: sourceId });
}

async function nlmAppendToSheet(env, { sheetId, rows, sourceId, range }) {
  return nlmWriteTool(env, "append_sheet", { sheet_id: sheetId, rows, source_id: sourceId, range });
}

async function nlmSyncAllDocs(env) {
  return nlmWriteTool(env, "sync_all", {});
}

// ─── GWS tool proxy ───────────────────────────────────────────────────────────

async function gwsTool(env, toolName, params = {}) {
  if (!env.GWS) return { error: "GWS binding not configured" };
  try {
    const resp = await fetchWithRetry(env.GWS, new Request("https://gws-worker.internal/mcp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: toolName, arguments: params } }),
    }));
    if (!resp.ok) return { error: `GWS HTTP ${resp.status}` };
    const data = await resp.json();
    return data.result ?? data.error ?? data;
  } catch (e) { return { error: e.message }; }
}

async function gwsDocsAppend(env, { docId, text }) {
  return gwsTool(env, "gws_docs", { action: "append", document_id: docId, text });
}

async function buildDriveContext(env, driveFileIds) {
  if (!env.GWS || !driveFileIds?.length) return "";
  try {
    const resp = await fetchWithRetry(env.GWS, new Request(
      `https://gws-worker.internal/drive/files?ids=${driveFileIds.join(",")}`,
    ));
    if (!resp.ok) return "";
    const data = await resp.json();
    if (!data?.files?.length) return "";
    return data.files.map(f => `### ${f.name}\n${f.content}`).join("\n\n---\n\n");
  } catch { return ""; }
}

// ─── generateViaOfficialAPI stub ──────────────────────────────────────────────
async function generateViaOfficialAPI() {
  throw new Error("API key auth removed — subscription tier only. Use web-cookie (gemini-3-*) path.");
}

// ─── Main generate via Gemini web (subscription) ─────────────────────────────

async function generateViaWebCookie(prompt, env, {
  model       = "gemini-3-flash",
  system      = null,
  gem         = null,
  chatMeta    = null,
  temporary   = false,
  cookieOverride = null,
  temperature = null,
  maxTokens   = null,
} = {}) {
  const baseCookie = cookieOverride || await fetchNLMCookies(env);
  if (!baseCookie) throw new Error("No cookie available. Configure GOOGLE_AUTH or NLM binding, or set SECURE_1PSID secret.");

  const cookieStr = buildCookieString(env, baseCookie);
  const webModel  = WEB_MODELS[model] ?? WEB_MODELS["gemini-3-flash"];

  const fullPrompt = system ? `${system}\n\n${prompt}` : prompt;

  const [cid, rid, mid] = chatMeta ?? [null, null, null];
  const inner = JSON.stringify([
    [fullPrompt, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
    null,
    cid ? [cid, rid, mid] : null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    temporary ? 1 : 0,
  ]);

  const payload = new URLSearchParams({
    "f.req": JSON.stringify([[["CF5qKf", inner, null, "generic"]]]),
    "at": "",
  }).toString();

  const resp = await batchExecute(cookieStr, payload, env);
  if (!resp.ok) {
    if (resp.status === 401 || resp.status === 403) {
      _nlmCookies = null;
      throw new Error(`Cookie auth failed (${resp.status}). Trigger a refresh via POST /rotate.`);
    }
    throw new Error(`Gemini web HTTP ${resp.status}`);
  }

  const raw    = await resp.text();
  const parsed = parseBatchResponse(raw);
  const result = parseStreamResponse(parsed);

  return {
    ...result,
    model,
    auth_mode: "web-cookie",
  };
}

async function generateViaWorkersAI(prompt, env, { model = "@cf/google/gemma-3-12b-it" } = {}) {
  if (!env.AI) throw new Error("Workers AI binding (AI) not configured.");
  const result = await env.AI.run(model, {
    messages: [{ role: "user", content: prompt }],
  });
  return { text: result.response ?? "", model, auth_mode: "workers-ai" };
}

// ─── handleGenerate ───────────────────────────────────────────────────────────

async function handleGenerate(request, env) {
  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Request body must be valid JSON." }, 400); }

  const {
    prompt,
    model         = "gemini-3-flash",
    system        = null,
    gem           = null,
    chat_meta     = null,
    temporary     = false,
    notebooks     = true,
    notebook_ids  = null,
    max_sources   = 5,
    drive_file_ids= null,
    temperature   = null,
    max_tokens    = null,
    cookie        = null,
  } = body;

  if (!prompt) return jsonResponse({ error: "'prompt' is required." }, 400);

  const authMode = getAuthMode(env);
  if (!authMode) return jsonResponse({
    error: "No auth configured.",
    hint: "Set SECURE_1PSID or SESSION_KEY secret, or configure GOOGLE_AUTH/NLM bindings.",
  }, 503);

  // Build grounding context (non-blocking)
  let notebookContext = "";
  let driveContext    = "";
  if (notebooks !== false) {
    [notebookContext, driveContext] = await Promise.all([
      buildNotebookContext(env, { notebookIds: notebook_ids, maxSources: max_sources, temporary }),
      buildDriveContext(env, drive_file_ids),
    ]);
  }

  const fullSystem = [system, gem, notebookContext, driveContext]
    .filter(Boolean).join("\n\n---\n\n") || null;
  const apiSystem = fullSystem;

  try {
    let result;
    if (authMode === "web-cookie") {
      result = await generateViaWebCookie(prompt, env, {
        model, system: fullSystem, gem, chatMeta: chat_meta,
        temporary, cookieOverride: cookie, temperature, maxTokens: max_tokens,
      });
    } else if (authMode === "workers-ai") {
      result = await generateViaWorkersAI(prompt, env);
    } else {
      return jsonResponse({ error: "No auth available." }, 503);
    }

    // Omit session object from result (just expose ids)
    const { session, ...rest } = result;
    return jsonResponse({
      ...rest,
      ...(session ? { cid: session.cid, rid: session.rid, mid: session.mid } : {}),
      grounded: !!(notebookContext || driveContext),
      auth_mode: authMode,
      _meta: { task_type: body.task_type ?? null },
    });
  } catch (err) {
    return jsonResponse({ error: err.message, mode: authMode, hint: "If cookies expired, POST /rotate to refresh." }, 500);
  }
}

// ─── handleGenerateStream ─────────────────────────────────────────────────────

async function handleGenerateStream(request, env) {
  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ error: "Request body must be valid JSON." }, 400); }

  const authMode = getAuthMode(env);
  if (authMode !== "web-cookie")
    return jsonResponse({ error: "Streaming requires web-cookie auth. Ensure SECURE_1PSID/SESSION_KEY or GOOGLE_AUTH/NLM binding is configured." }, 400);

  const { prompt, model = "gemini-3-flash", system = null } = body;
  if (!prompt) return jsonResponse({ error: "'prompt' is required." }, 400);

  const { readable, writable } = new TransformStream();
  const writer  = writable.getWriter();
  const encoder = new TextEncoder();

  async function send(event, data) {
    await writer.write(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
  }

  (async () => {
    try {
      const result = await generateViaWebCookie(prompt, env, { model, system });
      await send("chunk", { text: result.text });
      await send("done",  { session: result.session, model, auth_mode: "web-cookie" });
    } catch (e) {
      await send("error", { error: e.message });
    } finally {
      await writer.close();
    }
  })();

  return new Response(readable, {
    headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", ...corsHeaders() },
  });
}

// ─── Gem management ───────────────────────────────────────────────────────────

async function handleGetGems(env) {
  if (!env.NLM) return jsonResponse({ error: "NLM binding required for Gem management." }, 503);
  try {
    const resp = await fetchWithRetry(env.NLM, new Request("https://notebooklm-worker.internal/gems"));
    return resp.ok ? new Response(resp.body, { status: 200, headers: { "Content-Type": "application/json", ...corsHeaders() } }) : jsonResponse({ error: `NLM ${resp.status}` }, resp.status);
  } catch (e) { return jsonResponse({ error: e.message }, 500); }
}

async function handleCreateGem(request, env) {
  const body = await request.json().catch(() => ({}));
  return nlmWriteTool(env, "create_gem", body).then(r => jsonResponse(r));
}

async function handleUpdateGem(request, env, gemId) {
  const body = await request.json().catch(() => ({}));
  return nlmWriteTool(env, "update_gem", { id: gemId, ...body }).then(r => jsonResponse(r));
}

async function handleDeleteGem(request, env, gemId) {
  return nlmWriteTool(env, "delete_gem", { id: gemId }).then(r => jsonResponse(r));
}

// ─── NLM REST handler ─────────────────────────────────────────────────────────

async function handleNLM(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { action, ...params } = body;
  switch (action) {
    case "ask":               return jsonResponse(await nlmAsk(env, params));
    case "add_source":        return jsonResponse(await nlmAddSource(env, params));
    case "add_text_source":   return jsonResponse(await nlmAddTextSource(env, params));
    case "create_notebook":   return jsonResponse(await nlmCreateNotebook(env, params));
    case "create_linked_doc": return jsonResponse(await nlmCreateLinkedDoc(env, params));
    case "create_linked_sheet":return jsonResponse(await nlmCreateLinkedSheet(env, params));
    case "append_doc":        return jsonResponse(await nlmAppendToDoc(env, params));
    case "append_sheet":      return jsonResponse(await nlmAppendToSheet(env, params));
    case "sync_all":          return jsonResponse(await nlmSyncAllDocs(env));
    case "generate_artifact": return jsonResponse(await nlmGenerateArtifact(env, params));
    case "health":            return jsonResponse(await nlmHealthCheck(env));
    default: return jsonResponse({ error: `Unknown NLM action: ${action}`, available: ["ask","add_source","create_notebook","create_linked_doc","create_linked_sheet","append_doc","append_sheet","sync_all","generate_artifact","health"] }, 400);
  }
}

// ─── Workspace handlers ───────────────────────────────────────────────────────

async function handleWorkspaceChat(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { message, context = {}, action, params = {} } = body;

  if (action) {
    const result = await gwsTool(env, `gws_${action}`, { ...params, ...context });
    return jsonResponse({ workspace_result: result });
  }

  if (!message) return jsonResponse({ error: "message or action required" }, 400);
  const generateResp = await handleGenerate(
    new Request("https://internal/generate", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: message, ...context }),
    }),
    env,
  );
  return generateResp;
}

function extractActionJson(text) {
  const m = text.match(/```(?:json)?\s*(\{[\s\S]*?\})\s*```/);
  if (m) { try { return JSON.parse(m[1]); } catch {} }
  try { return JSON.parse(text); } catch {}
  return null;
}

async function handleWorkspaceRun(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { instructions, context = {} } = body;
  if (!instructions) return jsonResponse({ error: "instructions required" }, 400);

  const planResp = await handleGenerate(new Request("https://internal/generate", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      prompt: instructions,
      system: "You are a workspace automation agent. Respond with a JSON action plan: { action, params }. Available GWS tools: gmail, drive, calendar, docs, sheets, tasks, contacts.",
      model: "gemini-3-flash-thinking-advanced",
    }),
  }), env);
  const planData = await planResp.json();
  const plan = extractActionJson(planData.text ?? "");
  if (!plan) return jsonResponse({ error: "Could not parse action plan", raw: planData.text }, 500);

  const result = await gwsTool(env, `gws_${plan.action}`, plan.params ?? {});
  return jsonResponse({ plan, result });
}

async function handleGeminiNotebook(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { notebook_id, question, model = "gemini-3-pro-advanced" } = body;
  if (!notebook_id || !question) return jsonResponse({ error: "notebook_id and question required" }, 400);

  const [nlmResp, geminiResp] = await Promise.all([
    nlmAsk(env, { notebookId: notebook_id, question }),
    handleGenerate(new Request("https://internal/generate", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: question, model, notebook_ids: [notebook_id] }),
    }), env),
  ]);
  const geminiData = await geminiResp.json();
  return jsonResponse({ nlm: nlmResp, gemini: geminiData });
}

async function handleWorkspaceExecute(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { tool, params = {} } = body;
  if (!tool) return jsonResponse({ error: "tool required" }, 400);
  const result = await gwsTool(env, tool, params);
  return jsonResponse({ tool, result });
}

async function handleGWS(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { tool, action, params = {} } = body;
  const toolName = tool || (action ? `gws_${action}` : null);
  if (!toolName) return jsonResponse({ error: "tool or action required" }, 400);
  return jsonResponse(await gwsTool(env, toolName, params));
}

// ─── GWS auth helpers (legacy) ────────────────────────────────────────────────

function getGWSKey(env) {
  return env.SECURE_1PSID || env.SESSION_KEY || _nlmCookies || null;
}

function gwsHeaders(env) {
  const key = getGWSKey(env);
  return key ? { Cookie: `__Secure-1PSID=${key}` } : {};
}

// ─── MCP server ───────────────────────────────────────────────────────────────

const OWN_MCP_TOOLS = [
  {
    name: "gemini",
    description: `Gemini AI with NotebookLM grounding and Drive file context.

Actions: generate, gems

- generate: prompt (required), model, system, gem (Gem ID for persona), notebooks (bool, default true), notebook_ids (array), drive_file_ids (array), chat_meta (array for multi-turn). Use @YouTube/@Gmail/@Maps in prompt for extensions.
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
    description: `LIVE NotebookLM operations — query, create notebooks, add sources, generate artifacts, export.

Actions: ask, add_source, create_notebook, create_linked_doc, create_linked_sheet, append_doc, append_sheet, sync_all, generate_artifact, health

- ask: notebook_id + question (required)
- add_source: notebook_id (required), url or content, title
- create_notebook: title
- create_linked_doc: notebook_id + title (required), content
- create_linked_sheet: notebook_id + title (required), initial_data (2D array)
- append_doc: doc_id + text (required), source_id
- append_sheet: sheet_id + rows (required), source_id, range
- sync_all: sync all stale living docs
- generate_artifact: notebook_id + artifact_type (audio/video/report/quiz/briefing/slides/infographic/mindmap/timeline), instructions
- health: check NLM service binding status`,
    inputSchema: {
      type: "object",
      properties: {
        action:        { type: "string", enum: ["ask","add_source","create_notebook","create_linked_doc","create_linked_sheet","append_doc","append_sheet","sync_all","generate_artifact","health"] },
        notebook_id:   { type: "string" },
        question:      { type: "string" },
        url:           { type: "string" },
        title:         { type: "string" },
        content:       { type: "string" },
        doc_id:        { type: "string" },
        sheet_id:      { type: "string" },
        rows:          { type: "array" },
        source_id:     { type: "string" },
        range:         { type: "string" },
        artifact_type: { type: "string", enum: ["audio","video","report","quiz","briefing","slides","infographic","mindmap","timeline"] },
        instructions:  { type: "string" },
        text:          { type: "string" },
        initial_data:  { type: "array" },
      },
      required: ["action"],
    },
  },
];

async function proxyMCPList(binding, prefix) {
  if (!binding) return [];
  try {
    const resp = await fetchWithRetry(binding, new Request("https://worker.internal/mcp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }),
    }));
    if (!resp.ok) return [];
    const data = await resp.json();
    const tools = data.result?.tools ?? [];
    return tools.map(t => ({ ...t, name: prefix ? `${prefix}_${t.name}` : t.name }));
  } catch { return []; }
}

async function proxyMCPCall(binding, toolName, args) {
  if (!binding) return { error: "binding not configured" };
  try {
    const resp = await fetchWithRetry(binding, new Request("https://worker.internal/mcp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: toolName, arguments: args } }),
    }));
    if (!resp.ok) return { error: `HTTP ${resp.status}` };
    const data = await resp.json();
    return data.result ?? data.error ?? data;
  } catch (e) { return { error: e.message }; }
}

async function getAllMCPTools(env) {
  const [nlmTools, gwsTools] = await Promise.all([
    proxyMCPList(env.NLM, "nlm"),
    proxyMCPList(env.GWS, "gws"),
  ]);
  return [...OWN_MCP_TOOLS, ...nlmTools, ...gwsTools];
}

async function callOwnMCPTool(name, args, env) {
  if (name === "gemini") {
    const { action = "generate", ...rest } = args;
    if (action === "generate") {
      const resp = await handleGenerate(new Request("https://internal/generate", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(rest),
      }), env);
      const data = await resp.json();
      return { content: [{ type: "text", text: JSON.stringify(data) }] };
    }
    if (action === "gems") {
      const gemsResp = await handleGetGems(env);
      const data = await gemsResp.json();
      return { content: [{ type: "text", text: JSON.stringify(data) }] };
    }
  }
  if (name === "nlm_workflow") {
    const result = await handleNLM(new Request("https://internal/nlm", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(args),
    }), env);
    const data = await result.json();
    return { content: [{ type: "text", text: JSON.stringify(data) }] };
  }
  return { content: [{ type: "text", text: `Unknown own tool: ${name}` }], isError: true };
}

let _mcpSessionId = null;

async function handleMCP(request, env) {
  const method = request.method.toUpperCase();

  if (method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }
  if (method === "DELETE") {
    _mcpSessionId = null;
    return new Response(null, { status: 204, headers: corsHeaders() });
  }
  if (method === "GET") {
    return jsonResponse({
      name: "gemini-webapi-worker",
      version: "5.4.1",
      transport: "streamable-http",
      endpoint: `${new URL(request.url).origin}/mcp`,
      tools: (await getAllMCPTools(env)).map(t => t.name),
    });
  }

  let body;
  try { body = await request.json(); }
  catch { return jsonResponse({ jsonrpc: "2.0", error: { code: -32700, message: "Parse error" } }, 400); }

  const { jsonrpc, id, method: rpcMethod, params = {} } = body;
  const sessionId = request.headers.get("Mcp-Session-Id") ?? (_mcpSessionId ??= crypto.randomUUID());

  let result;
  try {
    switch (rpcMethod) {
      case "initialize":
        result = {
          protocolVersion: "2025-03-26",
          serverInfo: { name: "gemini-webapi-worker", version: "5.4.1" },
          capabilities: { tools: {} },
        };
        break;
      case "tools/list":
        result = { tools: await getAllMCPTools(env) };
        break;
      case "tools/call": {
        const { name, arguments: args = {} } = params;
        const isOwn = OWN_MCP_TOOLS.some(t => t.name === name);
        if (isOwn) {
          result = await callOwnMCPTool(name, args, env);
        } else if (name.startsWith("nlm_")) {
          const proxied = await proxyMCPCall(env.NLM, name.slice(4), args);
          result = { content: [{ type: "text", text: JSON.stringify(proxied) }] };
        } else if (name.startsWith("gws_")) {
          const proxied = await proxyMCPCall(env.GWS, name.slice(4), args);
          result = { content: [{ type: "text", text: JSON.stringify(proxied) }] };
        } else {
          result = { content: [{ type: "text", text: `Unknown tool: ${name}` }], isError: true };
        }
        break;
      }
      default:
        return new Response(JSON.stringify({
          jsonrpc: "2.0", id,
          error: { code: -32601, message: "Method not found" },
        }), { status: 200, headers: { "Content-Type": "application/json", ...corsHeaders(), "Mcp-Session-Id": sessionId } });
    }
  } catch (e) {
    return new Response(JSON.stringify({
      jsonrpc: "2.0", id,
      error: { code: -32603, message: e.message },
    }), { status: 200, headers: { "Content-Type": "application/json", ...corsHeaders(), "Mcp-Session-Id": sessionId } });
  }

  return new Response(JSON.stringify({ jsonrpc: "2.0", id, result }), {
    status: 200,
    headers: { "Content-Type": "application/json", ...corsHeaders(), "Mcp-Session-Id": sessionId },
  });
}

// ─── Main router ──────────────────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    try {
      const url    = new URL(request.url);
      const method = request.method.toUpperCase();
      let path     = url.pathname;
      // Normalize: strip /v1 prefix added by CF AI Gateway custom provider routing
      if (path.startsWith("/v1/")) path = path.slice(3);
      else if (path === "/v1") path = "/";

      if (method === "OPTIONS") return new Response(null, { headers: corsHeaders() });

      // Health
      if (path === "/health" && method === "GET") {
        return jsonResponse({
          status: "ok",
          service: "gemini-webapi-worker",
          version: "5.4.1",
          auth_mode: getAuthMode(env) ?? "none",
          auth_priority: "web-cookie (gemini-3-*) > workers-ai",
          cookie_rotation: {
            enabled: true,
            cached_psidts: !!_cachedPSIDTS,
            last_rotate: _lastRotateAt ? new Date(_lastRotateAt).toISOString() : null,
          },
          bindings: {
            NLM: !!env.NLM, GWS: !!env.GWS, HUB: !!env.HUB,
            AI: !!env.AI, KV: !!env.KV, R2_AUTH: !!env.R2_AUTH, GOOGLE_AUTH: !!env.GOOGLE_AUTH,
          },
          models: {
            subscription: Object.keys(WEB_MODELS),
            default: "gemini-3-flash",
            note: "gemini-3-* subscription only — API key path removed",
          },
          mcp: {
            endpoint: `${url.origin}/mcp`,
            transport: "streamable-http",
            spec: "2025-03-26",
            own_tools: OWN_MCP_TOOLS.map(t => t.name),
            proxied: "nlm_* and gws_* proxied from NLM/GWS bindings",
          },
          routes: [
            "GET  /health",
            "GET  /mcp | POST /mcp                  MCP Streamable HTTP",
            "POST /generate                          Main generate",
            "POST /generate/stream                   SSE streaming",
            "GET  /gems | POST /gems | PUT/DELETE /gems/:id",
            "POST /workspace/chat                    GWS relay",
            "POST /workspace/run                     Agentic GWS execution",
            "POST /workspace/execute                 Direct GWS tool call",
            "POST /workspace/actions                 Batch GWS actions",
            "POST /rotate                            Force cookie refresh",
            "POST /cookies/update                    Push fresh cookies",
            "POST /chat/completions                  OpenAI-compat",
          ],
        });
      }

      // MCP
      if (path === "/mcp") return handleMCP(request, env);

      // Cookie management
      if (path === "/rotate" && method === "POST") {
        const cookie = await rotateCookies(env);
        return jsonResponse({ ok: true, rotated: !!cookie });
      }
      if (path === "/cookies/update" && method === "POST") {
        let body = {}; try { body = await request.json(); } catch {}
        const { cookies } = body;
        if (cookies) {
          _nlmCookies = cookies["__Secure-1PSID"] || cookies.SECURE_1PSID || Object.values(cookies)[0] || null;
          _cachedPSIDTS = cookies["__Secure-1PSIDTS"] || null;
        }
        if (env.KV && _nlmCookies) await env.KV.put("gemini_cookie", _nlmCookies, { expirationTtl: 86400 });
        return jsonResponse({ ok: true });
      }

      // Generate
      if (path === "/generate") return handleGenerate(request, env);
      if (path === "/generate/stream") return handleGenerateStream(request, env);

      // Gems
      if (path === "/gems") {
        if (method === "GET")  return handleGetGems(env);
        if (method === "POST") return handleCreateGem(request, env);
      }
      if (path.startsWith("/gems/")) {
        const gemId = path.slice(6);
        if (method === "PUT")    return handleUpdateGem(request, env, gemId);
        if (method === "DELETE") return handleDeleteGem(request, env, gemId);
      }

      // Workspace
      if (path === "/workspace/chat")    return handleWorkspaceChat(request, env);
      if (path === "/workspace/run")     return handleWorkspaceRun(request, env);
      if (path === "/workspace/execute") return handleWorkspaceExecute(request, env);
      if (path === "/workspace/actions" || path === "/workspace/workflows") return handleGWS(request, env);

      // NLM direct
      if (path === "/nlm") return handleNLM(request, env);

      // OpenAI compat
      if (path === "/chat/completions") return handleOpenAICompletions(request, env);

      return jsonResponse({ error: "Not found", path }, 404);
    } catch (err) {
      console.error("Worker error:", err.stack ?? err.message);
      return jsonResponse({ error: "Internal server error", message: err.message }, 500);
    }
  },
};
