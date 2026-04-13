#!/usr/bin/env python3.13
"""
Push Gemini/NLM web session cookies to ALL stores.

Reads Google cookies from local Chrome, then pushes to:
  1. google-auth-worker R2 (gemini + nlm targets) — feeds the 6h cron auto-refresh
  2. Shared KV nlm:cookie_jar_v2 — what gemini-webapi-worker actually reads
  3. Shared KV notebooklm_auth — what notebooklm-rpc.ts reads

This is the ONE script that seeds the cookie pipeline. Once seeded, the
google-auth-worker 6h cron + Playwright container keeps cookies alive.

Usage:
  python3.13 scripts/push-session.py           # One-shot (seed)
  python3.13 scripts/push-session.py --loop     # Every 30 minutes (keep warm)
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import traceback
import urllib.request

# ── Config ──────────────────────────────────────────────────────────────

AUTH_WORKER_URL = "https://google-auth-worker.authorityandbrand.workers.dev"
D1REST_URL = "https://d1-rest.authorityandbrand.workers.dev"
KV_NAMESPACE_ID = "e0ba67cf57c24ab49a7a3be5f20ece05"  # shared CACHE namespace

# Auth keys from environment (auto-loaded via ~/.zshenv)
AUTH_API_KEY = os.environ.get("AUTH_API_KEY", "")
D1REST_SECRET = os.environ.get("D1REST_SECRET", "")


COOKIE_NAMES = [
    "__Secure-1PSID", "__Secure-1PSIDTS", "__Secure-1PAPISID",
    "__Secure-1PSIDCC", "__Secure-3PSID", "__Secure-3PSIDTS",
    "SID", "HSID", "SSID", "APISID", "SAPISID", "NID",
    "SIDCC", "SNID",
]


def get_cookies_from_chrome():
    """Extract Google cookies from local Chrome.

    load_browser_cookies returns dict[str, list[dict]] where each cookie dict
    has keys: name, value, domain, path, expires.  We flatten the list for the
    target browser into a simple {name: value} mapping.
    """
    from gemini_webapi.utils.load_browser_cookies import load_browser_cookies

    browser_cookies = load_browser_cookies(domain_name="google.com", verbose=False)

    # Try chrome first, then fall back to any available browser
    cookie_list = browser_cookies.get("chrome", [])
    if not cookie_list:
        for _browser, clist in browser_cookies.items():
            if clist:
                cookie_list = clist
                break

    # Flatten list[{name, value, ...}] -> {name: value}
    flat = {c["name"]: c["value"] for c in cookie_list if "name" in c and "value" in c}

    cookies = {}
    for name in COOKIE_NAMES:
        val = flat.get(name, "")
        if val:
            cookies[name] = val

    return cookies


async def get_snlm0e(cookies):
    """Init GeminiClient with extracted cookies and return the SNlM0e access token."""
    from gemini_webapi import GeminiClient

    psid = cookies.get("__Secure-1PSID", "")
    psidts = cookies.get("__Secure-1PSIDTS", "")
    client = GeminiClient(secure_1psid=psid, secure_1psidts=psidts)
    # Inject all cookies so the token fetch succeeds
    client.cookies = cookies
    await client.init(timeout=30, auto_close=False, auto_refresh=False, verbose=False)
    token = client.access_token
    await client.close()
    return token


def http_post(url, data, auth_header=None):
    """Simple HTTP POST with browser-like User-Agent (avoids CF 1010 bot block)."""
    payload = json.dumps(data).encode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    }
    if auth_header:
        headers["Authorization"] = auth_header
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read())
    except Exception as e:
        body = e.read().decode() if hasattr(e, "read") else str(e)
        return {"error": body}


def push_to_auth_worker(cookies, target):
    """Push cookies to google-auth-worker R2 store."""
    result = http_post(
        f"{AUTH_WORKER_URL}/cookies/push",
        {"target": target, "cookies": cookies},
        auth_header=f"Bearer {AUTH_API_KEY}" if AUTH_API_KEY else None,
    )
    ok = result.get("success", False)
    saved = result.get("cookies_saved", 0)
    return ok, f"{'ok' if ok else 'FAIL'} ({saved} cookies)" + (f" error={result.get('error','')}" if not ok else "")


def push_to_kv(key, value):
    """Write a KV value via wrangler CLI (most reliable for JSON)."""
    tmp = f"/tmp/_kv_push_{key.replace(':', '_')}.json"
    with open(tmp, "w") as f:
        json.dump(value, f)
    try:
        result = subprocess.run(
            ["npx", "wrangler", "kv", "key", "put", key, "--path", tmp,
             "--namespace-id", KV_NAMESPACE_ID, "--remote"],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.expanduser("~/d1-rest-worker"),
        )
        if result.returncode == 0:
            return True, "ok"
        return False, result.stderr.strip()[:200]
    except Exception as e:
        return False, str(e)[:200]


def run_once():
    print(f"[{time.strftime('%H:%M:%S')}] Extracting cookies from Chrome...", flush=True)
    cookies = get_cookies_from_chrome()
    if not cookies:
        print("  ERROR: No Google cookies found in Chrome. Log into google.com first.", flush=True)
        return False

    psid = cookies.get("__Secure-1PSID", "")
    print(f"  Found {len(cookies)} cookies (__Secure-1PSID: {psid[:20]}...)", flush=True)
    ts = int(time.time())
    all_ok = True

    # 0. Extract SNlM0e token via GeminiClient
    snlm0e = None
    try:
        snlm0e = asyncio.run(get_snlm0e(cookies))
        if snlm0e:
            print(f"  SNlM0e: {snlm0e[:20]}...", flush=True)
        else:
            print("  WARN: SNlM0e came back None (cookies may be stale)", flush=True)
    except Exception as e:
        print(f"  WARN: Failed to extract SNlM0e: {e}", flush=True)
        traceback.print_exc()

    # 1. Push to google-auth-worker R2 (gemini + nlm)
    for target in ("gemini", "nlm"):
        ok, msg = push_to_auth_worker(cookies, target)
        print(f"  auth-worker/{target}: {msg}", flush=True)
        if not ok:
            all_ok = False

    # 2. Push to shared KV nlm:cookie_jar_v2
    jar_payload = {"cookies": cookies, "updated_at": ts, "source": f"push-session-{time.strftime('%Y%m%d-%H%M%S')}"}
    ok, msg = push_to_kv("nlm:cookie_jar_v2", jar_payload)
    print(f"  KV nlm:cookie_jar_v2: {msg}", flush=True)
    if not ok:
        all_ok = False

    # 3. Push to shared KV notebooklm_auth (legacy format)
    cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())
    nlm_payload = {"cookie_header": cookie_header, "updated_at": ts, "source": "push-session"}
    ok, msg = push_to_kv("notebooklm_auth", nlm_payload)
    print(f"  KV notebooklm_auth: {msg}", flush=True)
    if not ok:
        all_ok = False

    # 4. Push SNlM0e to KV gemini_snlm0e
    if snlm0e:
        snlm0e_payload = {"snlm0e": snlm0e, "updated_at": ts, "source": "push-session"}
        ok, msg = push_to_kv("gemini_snlm0e", snlm0e_payload)
        print(f"  KV gemini_snlm0e: {msg}", flush=True)
        if not ok:
            all_ok = False
    else:
        print("  KV gemini_snlm0e: SKIPPED (no token)", flush=True)

    # 5. Push to shared KV gemini_auth (cookie header for gemini-webapi-worker)
    gemini_payload = {"cookie_header": cookie_header, "updated_at": ts, "source": "push-session"}
    ok, msg = push_to_kv("gemini_auth", gemini_payload)
    print(f"  KV gemini_auth: {msg}", flush=True)
    if not ok:
        all_ok = False

    print(f"  {'All stores updated' if all_ok else 'Some stores failed'}", flush=True)
    return all_ok


def main():
    loop_mode = "--loop" in sys.argv

    if loop_mode:
        interval = 1800  # 30 minutes
        print(f"Running in loop mode (every {interval}s). Ctrl+C to stop.", flush=True)
        while True:
            try:
                run_once()
            except Exception as e:
                print(f"  Error: {e}", flush=True)
            time.sleep(interval)
    else:
        success = run_once()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
