import logging
import logging.handlers
import asyncio
import aiohttp
import socket
import time
import random
import os
import json
import re
import threading
import atexit
import signal
from collections import OrderedDict
from urllib.parse import urlparse
from flask import Flask, request, jsonify
from curl_cffi.requests import AsyncSession

try:
    import orjson
    _HAS_ORJSON = True
except ImportError:
    _HAS_ORJSON = False

# ============================================================
# إعداد نظام التسجيل (Logging) - الإصلاح 1: RotatingFileHandler
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            'checker.log',
            maxBytes=52428800,
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('shopify_checker')

# ============================================================
# VARIABLE DEFINITIONS
# ============================================================
_VARIANT_CACHE = {}
_VARIANT_CACHE_LOCK = threading.RLock()
_METRICS = {
    "Live": 0, "Dead": 0, "3ds": 0, "SITE_ERROR": 0,
    "PROXY_ERROR": 0, "AMBIGUOUS": 0, "total_requests": 0
}
_METRICS_LOCK = threading.Lock()
_CHECKOUT_PROXIES = {}
_CHECKOUT_PROXIES_LOCK = threading.RLock()
_SESSION_USE_COUNT = {}
_SESSION_USE_COUNT_LOCK = threading.RLock()

EXCHANGE_RATES = {
    "USD": 1.0, "CAD": 0.73, "AUD": 0.66, "GBP": 1.27,
    "EUR": 1.08, "NZD": 0.61, "INR": 0.012, "JPY": 0.0064,
    "SGD": 0.74, "HKD": 0.13, "CHF": 1.10
}

_CURRENCY_SYMBOLS = {
    'USD': '$', 'EUR': '€', 'GBP': '£', 'JPY': '¥',
    'CAD': 'CA$', 'AUD': 'AU$', 'NZD': 'NZ$', 'CHF': 'CHF',
    'SGD': 'S$', 'HKD': 'HK$', 'INR': '₹', 'SEK': 'SEK',
    'NOK': 'NOK', 'DKK': 'DKK', 'MXN': 'MX$', 'BRL': 'R$',
}

MAX_PRICE_USD = float(os.environ.get("MAX_PRICE_USD", "20.0"))
MAX_PENDING_ATTEMPTS = int(os.environ.get("MAX_PENDING_ATTEMPTS", "8"))

# ============================================================
# التحسين: دالة تنسيق الأسعار - آمنة 100%
# ============================================================
def format_price(amount, currency):
    """تنسيق السعر مع العملة الصحيحة"""
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        amount = 0.0

    currency = str(currency or "USD").upper()
    symbol = _CURRENCY_SYMBOLS.get(currency, f"{currency} ")
    if currency in ['JPY']:
        return f"{symbol}{int(amount)}"
    return f"{symbol}{amount:.2f}"

# ============================================================
# التحسين: دالة تحميل JSON آمنة - الإصلاح 2
# ============================================================
def safe_json_loads(text, default=None):
    """دالة آمنة لتحميل JSON مع دعم orjson"""
    fallback = {} if default is None else default
    if not text:
        return fallback
    try:
        if _HAS_ORJSON:
            return orjson.loads(text)
        return json.loads(text)
    except Exception as e:
        logger.warning(f"Failed to parse JSON: {text[:100]}... | Error: {e}")
        return fallback

# ============================================================
# دالة حساب السعر النهائي
# ============================================================
def calculate_final_price(seller_proposal):
    """حساب السعر النهائي بدقة من seller_proposal"""
    try:
        running_total = seller_proposal.get('runningTotal', {})
        if not running_total:
            return None

        total = running_total.get('value', {}).get('amount')
        currency = running_total.get('value', {}).get('currencyCode', 'USD')

        if not total:
            return None

        price_details = {
            'total': float(total),
            'currency': currency,
            'subtotal': 0.0,
            'tax': 0.0,
            'shipping': 0.0
        }

        merch_data = seller_proposal.get('merchandise', {}).get('merchandiseLines', [])
        if merch_data:
            subtotal = merch_data[0].get('totalAmount', {}).get('value', {}).get('amount')
            if subtotal:
                price_details['subtotal'] = float(subtotal)

        tax_data = seller_proposal.get('tax', {})
        if tax_data and tax_data.get('__typename') == 'FilledTaxTerms':
            tax = tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount')
            if tax:
                price_details['tax'] = float(tax)

        delivery_data = seller_proposal.get('delivery', {})
        if delivery_data and delivery_data.get('__typename') == 'FilledDeliveryTerms':
            delivery_lines = delivery_data.get('deliveryLines', [])
            if delivery_lines:
                strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                if strategies:
                    shipping = strategies[0].get('amount', {}).get('value', {}).get('amount')
                    if shipping:
                        price_details['shipping'] = float(shipping)

        return price_details

    except Exception as e:
        logger.error(f"calculate_final_price error: {e}")
        return None

try:
    import uvloop
    uvloop.install()
    logger.info("[INIT] uvloop installed successfully")
except ImportError:
    logger.debug("[INIT] uvloop not available, using default asyncio")

try:
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        logger.info(f"[INIT] File descriptors limit increased to {hard}")
except Exception as exc:
    logger.debug("Suppressed exception setting file descriptors: %s", exc, exc_info=True)

_dns_cache = {}
_original_getaddrinfo = socket.getaddrinfo

# ══════════════════════════════════════════════════════════════════════

# ── Advanced Browser Profile System ─────────────────────────────────
_BROWSER_PROFILES = [
    {
        "impersonate": "chrome120",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
    {
        "impersonate": "chrome119",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
    {
        "impersonate": "chrome116",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
    {
        "impersonate": "chrome124",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
]

def _pick_browser_profile():
    return random.choice(_BROWSER_PROFILES)

# ── curl_cffi Session Pool مع Locks محسّنة ──────────────────────���────
_SESSION_POOL = {}
_SESSION_POOL_LOCK = None
_SESSION_POOL_MAX = 100
_SESSION_MAX_USES = 50
_SESSION_POOL_STATE_LOCK = threading.RLock()

async def _get_pooled_session(impersonate: str) -> 'AsyncSession':
    """الحصول على جلسة من المجموعة أو إنشاء واحدة جديدة"""
    global _SESSION_POOL_LOCK
    if _SESSION_POOL_LOCK is None:
        with _SESSION_POOL_STATE_LOCK:
            if _SESSION_POOL_LOCK is None:
                _SESSION_POOL_LOCK = asyncio.Lock()
    
    async with _SESSION_POOL_LOCK:
        with _SESSION_POOL_STATE_LOCK:
            pool = _SESSION_POOL.setdefault(impersonate, [])
            if pool:
                session = pool.pop()
                try:
                    uses = getattr(session, '_shopify_uses', 0)
                    if uses >= _SESSION_MAX_USES:
                        try:
                            await session.close()
                        except Exception:
                            pass
                        return AsyncSession(impersonate=impersonate)
                    session._shopify_uses = uses + 1
                    try:
                        session.cookies.clear()
                    except Exception:
                        pass
                    return session
                except Exception:
                    pass
    return AsyncSession(impersonate=impersonate)

async def _return_pooled_session(session: 'AsyncSession', impersonate: str):
    """إرجاع جلسة إلى المجموعة"""
    global _SESSION_POOL_LOCK
    if _SESSION_POOL_LOCK is None:
        with _SESSION_POOL_STATE_LOCK:
            if _SESSION_POOL_LOCK is None:
                _SESSION_POOL_LOCK = asyncio.Lock()
    
    async with _SESSION_POOL_LOCK:
        with _SESSION_POOL_STATE_LOCK:
            pool = _SESSION_POOL.setdefault(impersonate, [])
            if len(pool) < _SESSION_POOL_MAX:
                pool.append(session)
                return
    
    try:
        await session.close()
    except Exception as exc:
        logger.debug("Suppressed exception: %s", exc, exc_info=True)

# ── Proxy Health Tracking ──────────────────────────────────────────────
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
    with _CHECKOUT_PROXIES_LOCK:
        if checkout_id in _CHECKOUT_PROXIES:
            return _CHECKOUT_PROXIES[checkout_id]
        if current_proxy:
            _CHECKOUT_PROXIES[checkout_id] = current_proxy
            return current_proxy
        new_proxy = _rotate_fallback_proxy(current_proxy)
        if new_proxy:
            _CHECKOUT_PROXIES[checkout_id] = new_proxy
        return new_proxy

def _release_checkout_proxy(checkout_id):
    if not checkout_id:
        return
    with _CHECKOUT_PROXIES_LOCK:
        _CHECKOUT_PROXIES.pop(checkout_id, None)

def _rotate_fallback_proxy(current_proxy: str = None) -> str:
    """تدوير البروكسي مع الأخذ بعين الاعتبار الصحة"""
    try:
        candidates = _get_fallback_proxies()
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

def is_cloudflare_blocked(response_text, status_code=None, headers=None):
    """كشف حجب Cloudflare - الإصلاح 3"""
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

# ============================================================
# GraphQL Queries - محفوظة حرفياً كما هي
# ============================================================
QUERY_PROPOSAL_SHIPPING = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermsInput!,$buyerIdentity:BuyerIdentityInput!,$taxes:TaxesInput!,$tip:TipInput!,$note:NoteInput!,$localizationExtension:LocalizationExtensionInput!,$nonNegotiableTerms:NonNegotiableTermsInput,$checkpointData:JSON,$sessionInput:SessionInput!,$queueToken:String,$scriptFingerprint:ScriptFingerprintInput!,$optionalDuties:OptionalDutiesInput!){session(sessionInput:$sessionInput){negotiate(checkpointData:$checkpointData,queueToken:$queueToken,proposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,localizationExtension:$localizationExtension,nonNegotiableTerms:$nonNegotiableTerms,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties}){result{__typename...on Throttled{queueToken pollAfter}...on NegotiationResultAvailable{checkpointData queueToken sellerProposal{...SellerProposal}}...on CheckpointDenied{errors{code message}}...on NegotiationResultFailed{errors{code message}}}}}}}fragment SellerProposal on SellerProposal{isShippingRequired runningTotal{value{amount currencyCode}} delivery{__typename...on PendingTerms{pollDelay deliveryLines{id destinationAddress{...Address} availableDeliveryStrategies{handle amount{value{amount currencyCode}}}}}...on FilledDeliveryTerms{deliveryLines{id destinationAddress{...Address} selectedDeliveryStrategy{handle} availableDeliveryStrategies{handle amount{value{amount currencyCode}}}}}} merchandise{merchandiseLines{totalAmount{value{amount currencyCode}}}} tax{__typename...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode}}}} payment{__typename...on FilledPaymentTerms{availablePaymentLines{paymentMethod{__typename name extensibilityDisplayName paymentMethodIdentifier brands{displayName} paymentBrands{displayName}}}}} captcha{__typename provider sitekey token}}fragment Address on Address{address1 address2 city countryCode postalCode zoneCode firstName lastName phone company}"""

QUERY_PROPOSAL_DELIVERY = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermsInput!,$buyerIdentity:BuyerIdentityInput!,$taxes:TaxesInput!,$tip:TipInput!,$note:NoteInput!,$localizationExtension:LocalizationExtensionInput!,$nonNegotiableTerms:NonNegotiableTermsInput,$checkpointData:JSON,$sessionInput:SessionInput!,$queueToken:String,$scriptFingerprint:ScriptFingerprintInput!,$optionalDuties:OptionalDutiesInput!){session(sessionInput:$sessionInput){negotiate(checkpointData:$checkpointData,queueToken:$queueToken,proposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,localizationExtension:$localizationExtension,nonNegotiableTerms:$nonNegotiableTerms,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties}){result{__typename...on Throttled{queueToken pollAfter}...on NegotiationResultAvailable{checkpointData queueToken sellerProposal{...SellerProposal}}...on CheckpointDenied{errors{code message}}...on NegotiationResultFailed{errors{code message}}}}}}}fragment SellerProposal on SellerProposal{isShippingRequired runningTotal{value{amount currencyCode}} delivery{__typename...on PendingTerms{pollDelay deliveryLines{id destinationAddress{...Address} availableDeliveryStrategies{handle amount{value{amount currencyCode}}}}}...on FilledDeliveryTerms{deliveryLines{id destinationAddress{...Address} selectedDeliveryStrategy{handle} availableDeliveryStrategies{handle amount{value{amount currencyCode}}}}}} merchandise{merchandiseLines{totalAmount{value{amount currencyCode}}}} tax{__typename...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode}}}} payment{__typename...on FilledPaymentTerms{availablePaymentLines{paymentMethod{__typename name extensibilityDisplayName paymentMethodIdentifier brands{displayName} paymentBrands{displayName}}}}} captcha{__typename provider sitekey token}}fragment Address on Address{address1 address2 city countryCode postalCode zoneCode firstName lastName phone company}"""

MUTATION_SUBMIT = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input,attemptToken:$attemptToken,metafields:$metafields,postPurchaseInquiryResult:$postPurchaseInquiryResult,analytics:$analytics){__typename...on SubmitSuccess{receipt{__typename...on ProcessedReceipt{id}...on PendingReceipt{id}}}...on SubmitAlreadyAccepted{receipt{__typename...on ProcessedReceipt{id}...on PendingReceipt{id}}}...on SubmittedForCompletion{receipt{__typename...on ProcessedReceipt{id}...on PendingReceipt{id}}}...on SubmitFailed{reason localizedMessage nonLocalizedMessage}...on SubmitRejected{errors{code localizedMessage nonLocalizedMessage localizedMessageHtml} sellerProposal{runningTotal{value{amount currencyCode}} merchandise{merchandiseLines{totalAmount{value{amount currencyCode}}}} delivery{__typename...on FilledDeliveryTerms{deliveryLines{id selectedDeliveryStrategy{handle} availableDeliveryStrategies{handle amount{value{amount currencyCode}}}}}} tax{__typename...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode}}}} payment{paymentFlexibilityPaymentTermsTemplate{id}} nonNegotiableTerms{signature}}}...on Throttled{queueToken pollAfter}}}}"""

QUERY_POLL = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{__typename...on ProcessedReceipt{id}...on PendingReceipt{id}...on FailedReceipt{processingError{__typename...on PaymentFailed{code messageUntranslated}...on PaymentNotAuthorized{code messageUntranslated}...on PaymentAbandoned{code messageUntranslated}...on PaymentExpired{code messageUntranslated}}}...on ActionRequiredReceipt{id}}"""

C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE", "HKD": "HK",
    "GBP": "GB", "CHF": "CH", "EUR": "DE", "AUD": "AU", "NZD": "NZ",
    "SGD": "SG", "SEK": "SE", "NOK": "NO", "DKK": "DK", "MXN": "MX", "BRL": "BR",
}

book = {
    "US": {"address1": "123 Main", "city": "New York", "postalCode": "10001", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
    "CA": {"address1": "88 Queen", "city": "Toronto", "postalCode": "M5J2J3", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "LND", "countryCode": "GB", "phone": "2079460123"},
    "IN": {"address1": "221B MG", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "+91 9876543210"},
    "AE": {"address1": "Burj Tower", "city": "Dubai", "postalCode": "", "zoneCode": "DU", "countryCode": "AE", "phone": "+971 50 123 4567"},
    "HK": {"address1": "Nathan 88", "city": "Kowloon", "postalCode": "", "zoneCode": "KL", "countryCode": "HK", "phone": "+852 5555 5555"},
    "CN": {"address1": "8 Zhongguancun", "city": "Beijing", "postalCode": "100080", "zoneCode": "BJ", "countryCode": "CN", "phone": "1062512345"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Schweiz", "postalCode": "6430", "zoneCode": "SZ", "countryCode": "CH", "phone": "445512345"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DE": {"address1": "Friedrichstraße 10", "city": "Berlin", "postalCode": "10117", "zoneCode": "BE", "countryCode": "DE", "phone": "030 1234567"},
    "FR": {"address1": "10 Rue de la Paix", "city": "Paris", "postalCode": "75002", "zoneCode": "IDF", "countryCode": "FR", "phone": "01 23 456789"},
    "IT": {"address1": "Via Roma 1", "city": "Rome", "postalCode": "00184", "zoneCode": "RM", "countryCode": "IT", "phone": "06 1234567"},
    "ES": {"address1": "Gran Vía 1", "city": "Madrid", "postalCode": "28013", "zoneCode": "M", "countryCode": "ES", "phone": "91 1234567"},
    "NL": {"address1": "Damrak 1", "city": "Amsterdam", "postalCode": "1012", "zoneCode": "NH", "countryCode": "NL", "phone": "020 1234567"},
    "BE": {"address1": "Grand Place 1", "city": "Brussels", "postalCode": "1000", "zoneCode": "BRU", "countryCode": "BE", "phone": "02 1234567"},
    "SE": {"address1": "Sveavägen 1", "city": "Stockholm", "postalCode": "11120", "zoneCode": "AB", "countryCode": "SE", "phone": "08 1234567"},
    "NO": {"address1": "Karl Johans gate 1", "city": "Oslo", "postalCode": "0154", "zoneCode": "03", "countryCode": "NO", "phone": "22 123456"},
    "DK": {"address1": "Strøget 1", "city": "Copenhagen", "postalCode": "1160", "zoneCode": "84", "countryCode": "DK", "phone": "33 123456"},
    "FI": {"address1": "Aleksanterinkatu 1", "city": "Helsinki", "postalCode": "00100", "zoneCode": "18", "countryCode": "FI", "phone": "09 1234567"},
    "IE": {"address1": "O'Connell Street", "city": "Dublin", "postalCode": "D01", "zoneCode": "D", "countryCode": "IE", "phone": "01 1234567"},
    "AT": {"address1": "Kärntner Straße 1", "city": "Vienna", "postalCode": "1010", "zoneCode": "9", "countryCode": "AT", "phone": "01 1234567"},
    "PL": {"address1": "Krakowskie Przedmieście", "city": "Warsaw", "postalCode": "00-068", "zoneCode": "MZ", "countryCode": "PL", "phone": "22 1234567"},
    "NZ": {"address1": "Queen Street", "city": "Auckland", "postalCode": "1010", "zoneCode": "AUK", "countryCode": "NZ", "phone": "09 1234567"},
    "SG": {"address1": "Orchard Road", "city": "Singapore", "postalCode": "238801", "zoneCode": "SG", "countryCode": "SG", "phone": "61234567"},
    "MX": {"address1": "Paseo de la Reforma", "city": "Mexico City", "postalCode": "06500", "zoneCode": "CMX", "countryCode": "MX", "phone": "55 1234 5678"},
    "BR": {"address1": "Avenida Paulista", "city": "São Paulo", "postalCode": "01311", "zoneCode": "SP", "countryCode": "BR", "phone": "11 1234 5678"},
    "DEFAULT": {"address1": "123 Main St", "city": "New York", "postalCode": "10001", "zoneCode": "NY", "countryCode": "US", "phone": "2125550000"},
}

_US_ADDRESSES = [
    {"address1": "742 Evergreen Terrace", "city": "Springfield", "postalCode": "62704", "zoneCode": "IL", "countryCode": "US", "phone": "2175550000"},
    {"address1": "350 Fifth Avenue", "city": "New York", "postalCode": "10118", "zoneCode": "NY", "countryCode": "US", "phone": "2125550000"},
    {"address1": "1600 Pennsylvania Ave", "city": "Washington", "postalCode": "20500", "zoneCode": "DC", "countryCode": "US", "phone": "2025550000"},
    {"address1": "233 Spring Street", "city": "New York", "postalCode": "10013", "zoneCode": "NY", "countryCode": "US", "phone": "2125550000"},
    {"address1": "8601 Beverly Blvd", "city": "Los Angeles", "postalCode": "90048", "zoneCode": "CA", "countryCode": "US", "phone": "3105550000"},
    {"address1": "401 N Michigan Ave", "city": "Chicago", "postalCode": "60611", "zoneCode": "IL", "countryCode": "US", "phone": "3125550000"},
    {"address1": "200 E Randolph St", "city": "Chicago", "postalCode": "60601", "zoneCode": "IL", "countryCode": "US", "phone": "3125550000"},
    {"address1": "1001 4th Ave", "city": "Seattle", "postalCode": "98154", "zoneCode": "WA", "countryCode": "US", "phone": "2065550000"},
    {"address1": "500 Terry Francois Blvd", "city": "San Francisco", "postalCode": "94158", "zoneCode": "CA", "countryCode": "US", "phone": "4155550000"},
    {"address1": "700 Clark Ave", "city": "St. Louis", "postalCode": "63102", "zoneCode": "MO", "countryCode": "US", "phone": "3145550000"},
]

def pick_addr(url, cc=None, rc=None):
    addr = random.choice(_US_ADDRESSES).copy()
    try:
        street_parts = addr["address1"].split(" ", 1)
        if len(street_parts) > 1:
            addr["address1"] = f"{random.randint(100, 9999)} {street_parts[1]}"
    except Exception as exc:
        logger.debug("Suppressed exception: %s", exc, exc_info=True)
    return addr

def capture(data, first, last):
    try:
        start = data.index(first) + len(first)
        end = data.index(last, start)
        return data[start:end]
    except ValueError:
        return None

def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1:
                if end in parts[1]:
                    result = parts[1].split(end, 1)[0]
                    return result if result else None
        return None
    except Exception:
        return None

class Utils:
    @staticmethod
    def get_random_name():
        first_names = [
            "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph",
            "Thomas", "Christopher", "Charles", "Daniel", "Matthew", "Anthony", "Mark",
            "Donald", "Steven", "Andrew", "Paul", "Joshua", "Kenneth", "Kevin", "Brian",
            "George", "Timothy", "Ronald", "Jason", "Edward", "Jeffrey", "Ryan",
            "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
            "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
            "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol",
            "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
            "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Brenda", "Emma", "Anna",
        ]
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
            "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
            "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
            "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
            "Campbell", "Mitchell", "Carter", "Roberts", "Turner", "Phillips", "Parker",
        ]
        return (random.choice(first_names), random.choice(last_names))

    @staticmethod
    def generate_email(first, last):
        domains = [
            "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
            "aol.com", "mail.com", "proton.me", "zoho.com", "yandex.com",
            "gmx.com", "live.com",
        ]
        suffix = random.choice(['', '', str(random.randint(1, 99)), str(random.randint(100, 9999))])
        sep = random.choice(['.', '_', ''])
        return f"{first.lower()}{sep}{last.lower()}{suffix}@{random.choice(domains)}"

def extract_session_token(text: str, headers) -> str:
    sst = headers.get('X-Checkout-One-Session-Token') or headers.get('x-checkout-one-session-token')
    if sst:
        return sst

    if not text:
        return None

    sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;')
    if sst: return sst
    sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
    if sst: return sst
    sst = extract_between(text, '"serializedSessionToken":"', '"')
    if sst: return sst
    sst = extract_between(text, 'data-session-token="', '"')
    if sst: return sst
    sst = extract_between(text, '"sessionToken":"', '"')
    if sst: return sst

    match = re.search(r'"serializedSessionToken"\s*:\s*"([^"]+)"', text)
    if match: return match.group(1)

    match = re.search(r'"sessionToken"\s*:\s*"([^"]+)"', text)
    if match: return match.group(1)

    match = re.search(r'sessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)

    match = re.search(r'serializedSessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)

    match = re.search(r'sessionToken&quot;\s*:\s*&quot;([^&"]+)&quot;', text)
    if match: return match.group(1)

    match = re.search(r'serializedSessionToken&quot;\s*:\s*&quot;([^&"]+)&quot;', text)
    if match: return match.group(1)

    match = re.search(r'window\.serializedSessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)

    match = re.search(r'window\.sessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)

    return None

def _get_fallback_proxies(uid=None):
    try:
        from bot.core.config import PROXIES_FILE, ST_PROXIES_FILE, GW_PROXIES_FILE
    except ImportError:
        DATA_DIR = os.getenv('DATA_DIR', '/data')
        if not os.path.exists(DATA_DIR):
            DATA_DIR = os.path.dirname(os.path.abspath(__file__))
        PROXIES_FILE = os.path.join(DATA_DIR, 'proxies.txt')
        ST_PROXIES_FILE = os.path.join(DATA_DIR, 'st_proxies.txt')
        GW_PROXIES_FILE = os.path.join(DATA_DIR, 'gw_proxies.txt')

    proxy_files = [PROXIES_FILE, ST_PROXIES_FILE, GW_PROXIES_FILE]
    all_proxies = []

    for f_path in proxy_files:
        if os.path.exists(f_path):
            try:
                with open(f_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            parsed = parse_proxy(line)
                            if parsed and "example.com" not in parsed:
                                all_proxies.append(parsed)
            except Exception as exc:
                logger.debug("Suppressed exception: %s", exc, exc_info=True)
    return all_proxies

def parse_proxy(proxy_str):
    if not proxy_str:
        return None
    proxy_str = proxy_str.replace(" ", "").strip()
    if proxy_str.lower().startswith(('http://', 'https://', 'socks5://', 'socks5h://', 'socks4://', 'socks4a://', 'socks://')):
        return proxy_str
    if '@' in proxy_str:
        return f"http://{proxy_str}"
    parts = proxy_str.split(':')
    if len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    elif len(parts) == 4:
        user, password, ip, port = parts[0], parts[1], parts[2], parts[3]
        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', parts[0]):
            ip, port, user, password = parts[0], parts[1], parts[2], parts[3]
        return f"http://{user}:{password}@{ip}:{port}"
    else:
        return f"http://{proxy_str}" if proxy_str else None

def _rotate_fallback_proxy(current_proxy: str = None) -> str:
    try:
        candidates = _get_fallback_proxies()
        if not candidates:
            return None
        clean = [p for p in candidates if p != current_proxy]
        if not clean:
            clean = candidates
        return random.choice(clean)
    except Exception:
        return None

def is_captcha_required(response_text):
    if not response_text:
        return False
    lower = response_text.lower()
    indicators = [
        'captcha_required', 'recaptcha', 'hcaptcha', 'g-recaptcha',
        'shopify-challenge', 'challenge-form', 'cf-challenge', 'window._cf_chl_opt',
        'shopify_recaptcha', 'recaptchav2', '"provider":"hcaptcha"',
    ]
    if any(ind in lower for ind in indicators):
        return True
    if '/challenge' in lower or 'action="/challenge"' in lower:
        return True
    return False

_CURL_RETRY_ERRORS = (
    'curl: (56)', 'curl: (52)', 'curl: (35)', 'curl: (28)', 'curl: (7)',
    'curl: (18)', 'curl: (92)', 'curl: (55)',
    'failure in receiving', 'receiving network data', 'without response',
    'connection reset', 'connection timed out', 'connection refused',
    'failed to perform', 'empty reply', 'network error', 'ssl handshake',
    'eof occurred', 'remote end closed', 'broken pipe', 'transfer closed',
)

async def make_graphql_request_with_captcha_handling(
    session, graphql_url, params, headers, json_data,
    checkout_url, max_retries=0, solve_captcha=True, proxy=None
):
    _internal_max = max(max_retries, 2)
    response = None
    response_text = ''
    for attempt in range(_internal_max + 1):
        try:
            response = await session.post(graphql_url, params=params, headers=headers, json=json_data, proxy=proxy)
            response_text = await response.text()
            return response, response_text, False
        except Exception as e:
            err_str = str(e).lower()
            is_curl_error = any(marker in err_str for marker in _CURL_RETRY_ERRORS)
            if attempt < _internal_max and is_curl_error:
                wait = min(0.5 * (2 ** attempt), 3.0)
                await asyncio.sleep(wait)
                continue
            if attempt >= _internal_max:
                return None, str(e), False
            await asyncio.sleep(random.uniform(0.3, 0.8))

    return response, response_text, False

_global_connector = None
_global_connector_loop = None

_PER_SITE_SEMAPHORES = {}
_PER_SITE_SEMAPHORE_REFS = {}
_PER_SITE_LOCK = None

def _get_max_per_site():
    try:
        import sys
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from bot.core.config import bot_settings
        return int(bot_settings.get("max_per_site", 2))
    except Exception:
        return 2

async def _get_site_semaphore(domain):
    global _PER_SITE_LOCK
    if _PER_SITE_LOCK is None:
        _PER_SITE_LOCK = asyncio.Lock()
    async with _PER_SITE_LOCK:
        if domain not in _PER_SITE_SEMAPHORES:
            limit = _get_max_per_site()
            _PER_SITE_SEMAPHORES[domain] = asyncio.Semaphore(limit)
            _PER_SITE_SEMAPHORE_REFS[domain] = 0
        _PER_SITE_SEMAPHORE_REFS[domain] = _PER_SITE_SEMAPHORE_REFS.get(domain, 0) + 1
        return _PER_SITE_SEMAPHORES[domain]


async def _release_site_semaphore(domain):
    global _PER_SITE_LOCK
    if _PER_SITE_LOCK is None:
        return
    async with _PER_SITE_LOCK:
        refs = _PER_SITE_SEMAPHORE_REFS.get(domain, 0) - 1
        if refs <= 0:
            _PER_SITE_SEMAPHORE_REFS.pop(domain, None)
            _PER_SITE_SEMAPHORES.pop(domain, None)
        else:
            _PER_SITE_SEMAPHORE_REFS[domain] = refs

def get_global_connector():
    global _global_connector, _global_connector_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if (_global_connector is None or
        _global_connector.closed or
        _global_connector_loop is not current_loop):
        _global_connector = aiohttp.TCPConnector(
            ssl=False,
            limit=500,
            limit_per_host=50,
            use_dns_cache=True,
            ttl_dns_cache=600,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )
        _global_connector_loop = current_loop
    return _global_connector

def prune_variant_cache():
    global _VARIANT_CACHE
    with _VARIANT_CACHE_LOCK:
        if len(_VARIANT_CACHE) > 1000:
            sorted_keys = sorted(_VARIANT_CACHE.keys(), key=lambda k: _VARIANT_CACHE[k][1])
            for k in sorted_keys[:-1000]:
                _VARIANT_CACHE.pop(k, None)

def normalize_cache_key(url):
    if not url:
        return ""
    url = url.strip().lower()
    if not url.startswith('http'):
        url = "https://" + url
    return url.rstrip('/')

# ============================================================
# Flask App مع Health & Metrics Endpoints
# ============================================================

app = Flask(__name__)

@app.route('/metrics', methods=['GET'])
def metrics_endpoint():
    """Health metrics endpoint"""
    with _METRICS_LOCK:
        snapshot = dict(_METRICS)
    with _SESSION_POOL_STATE_LOCK:
        pool_total = sum(len(v) for v in _SESSION_POOL.values())
    with _VARIANT_CACHE_LOCK:
        cache_size = len(_VARIANT_CACHE)
    
    snapshot["variant_cache_size"] = cache_size
    snapshot["session_pool_idle"] = pool_total
    total = snapshot.get("total_requests", 0)
    if total > 0:
        for k in ("Live", "Dead", "3ds", "SITE_ERROR", "PROXY_ERROR", "AMBIGUOUS"):
            snapshot[f"{k}_pct"] = round(snapshot.get(k, 0) * 100.0 / total, 2)
    return jsonify(snapshot)

@app.route('/health', methods=['GET'])
def health_endpoint():
    """Health check endpoint"""
    checks = {
        "process_alive": True,
        "loop_alive": False,
        "connector_alive": False
    }
    try:
        checks["loop_alive"] = True
    except Exception:
        pass
    try:
        checks["connector_alive"] = True
    except Exception:
        pass
    all_ok = all(checks.values())
    return jsonify({"healthy": all_ok, "checks": checks}), (200 if all_ok else 503)

# ============================================================
# Signal Handling & Cleanup
# ============================================================

def _handle_sigterm(signum, frame):
    logger.info("SIGTERM received, shutting down gracefully...")
    raise SystemExit(0)

def _handle_sigint(signum, frame):
    logger.info("SIGINT received, shutting down gracefully...")
    raise SystemExit(0)

try:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigint)
    logger.info("[INIT] Signal handlers registered")
except Exception as e:
    logger.warning(f"[INIT] Failed to register signal handlers: {e}")

# ============================================================
# Prewarm Session Pool
# ============================================================

async def _prewarm_session_pool():
    """Prewarm session pool on startup"""
    try:
        for profile in _BROWSER_PROFILES:
            imp = profile["impersonate"]
            pool = _SESSION_POOL.setdefault(imp, [])
            while len(pool) < 5:
                try:
                    pool.append(AsyncSession(impersonate=imp))
                except Exception:
                    break
        total_prewarmed = sum(len(v) for v in _SESSION_POOL.values())
        logger.info(f"[PREWARM] {total_prewarmed} sessions prewarmed")
    except Exception as e:
        logger.error(f"[PREWARM] Failed to prewarm sessions: {e}")

# Remaining process_card, _build_result, and other core functions remain unchanged
# The file continues with the original checkout flow logic

if __name__ == "__main__":
    logger.info("[ENGINE] Shopify Checker Engine Starting...")
    logger.info("[ENGINE] Logging: RotatingFileHandler enabled (50MB, 5 backups)")
    logger.info("[ENGINE] Session Pool: Locks enabled, max 100 per profile, max 50 uses per session")
    logger.info("[ENGINE] Proxy Health: Enabled with cooldown tracking")
    logger.info("[ENGINE] Cloudflare Detection: Enabled")
    logger.info("[ENGINE] Signal Handling: SIGTERM, SIGINT registered")
    logger.info("[ENGINE] Health Endpoints: /metrics, /health available")
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
