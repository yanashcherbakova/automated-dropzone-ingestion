VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD"}

CURRENCY_MAPPING = {
    "US$": "USD",
    "DOLLAR": "USD",
    "DOLLARS": "USD",

    "EURO": "EUR",
    "EUROS": "EUR",
    "€": "EUR",

    "£": "GBP",
    "GBR": "GBP",

    "YEN": "JPY",
    "¥": "JPY",

    "CA$": "CAD",
    "CDN": "CAD",
}

CURRENCY_VARIANTS = [
    "USD", "usd", " USD ", "US$",
    "EUR", "eur", "EURO", "€",
    "GBP", "gbp", "GBR", "£",
    "JPY", "jpy", "YEN", "¥",
    "CAD", "cad", "CA$", "cdn",

    "US D", "U SD", "EU R",
    "XXX", "ZZZ", "123", "0",
    "", " ", None
]

CANONICAL_STATUS = {"SUCCESS", "FAILED", "PENDING", "CANCELLED"}

STATUS_MAPPING = {
    "success": "SUCCESS", "ok": "SUCCESS", "completed": "SUCCESS",
    "failed": "FAILED", "fail": "FAILED", "error": "FAILED", "declined": "FAILED",
    "pending": "PENDING", "in_progress": "PENDING",
    "cancel": "CANCELLED", "canceled": "CANCELLED", "cancelled": "CANCELLED",
}

STATUS_VARIANTS = [
    "SUCCESS", "success", "ok", "completed",
    "FAILED", "failed", "fail", "error", "declined",
    "PENDING", "pending", "in_progress",
    "CANCELLED", "cancel", "canceled", "cancelled",
    "unknown", "", None
]

CANONICAL_PAYMENT_METHODS = {
    "CARD", "BANK_TRANSFER", "PAYPAL", "APPLE_PAY", "GOOGLE_PAY"
}

PAYMENT_METHOD_MAPPING = {
    "card": "CARD", "credit_card": "CARD", "debit": "CARD",
    "bank": "BANK_TRANSFER", "wire": "BANK_TRANSFER", "bank_transfer": "BANK_TRANSFER",
    "paypal": "PAYPAL",
    "applepay": "APPLE_PAY", "apple_pay": "APPLE_PAY",
    "googlepay": "GOOGLE_PAY", "google_pay": "GOOGLE_PAY",
}

PAYMENT_METHOD_VARIANTS = [
    "card", "credit_card", "debit",
    "bank", "wire",
    "paypal", "paypal ",
    "applepay", "ApplePay",
    "googlepay",
    "unknown", "", None
]
