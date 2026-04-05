"""Default collector queries and topic definitions."""

from __future__ import annotations

GOOGLE_NEWS_SEARCHES = [
    '"global economy" OR inflation OR recession OR "interest rates" OR "central bank" OR GDP',
    '"federal reserve" OR ECB OR "Bank of England" OR "Bank of Japan" OR PBOC',
    "trade OR tariffs OR exports OR imports OR sanctions OR supply chains",
    '"oil prices" OR commodities OR OPEC OR gas OR copper OR wheat',
    "jobs OR unemployment OR payrolls OR wages OR hiring",
    '"housing market" OR consumer spending OR retail sales OR debt',
]

GDELT_SEARCHES = [
    '("global economy" OR inflation OR recession OR GDP OR unemployment OR wages OR "central bank" OR "interest rate" OR trade OR tariffs OR "oil prices" OR "financial markets")',
]

API_SEARCHES = [
    '"global economy" OR inflation OR recession OR "central bank"',
    "trade OR tariffs OR exports OR imports",
    '"oil prices" OR commodities OR "financial markets"',
    "jobs OR unemployment OR wages OR payrolls",
]

TOPIC_KEYWORDS = {
    "Inflation & Rates": [
        "inflation",
        "interest rate",
        "interest rates",
        "fed",
        "federal reserve",
        "ecb",
        "bank of england",
        "bank of japan",
        "central bank",
        "monetary policy",
        "cpi",
    ],
    "Growth & Recession": [
        "gdp",
        "growth",
        "recession",
        "slowdown",
        "contraction",
        "expansion",
        "productivity",
        "outlook",
    ],
    "Labor & Consumers": [
        "jobs",
        "payroll",
        "payrolls",
        "employment",
        "unemployment",
        "wages",
        "consumer",
        "retail",
        "housing",
        "spending",
        "debt",
    ],
    "Trade & Policy": [
        "trade",
        "tariff",
        "tariffs",
        "exports",
        "imports",
        "sanction",
        "sanctions",
        "fiscal",
        "budget",
        "tax",
    ],
    "Markets & Commodities": [
        "stocks",
        "shares",
        "bond",
        "bonds",
        "yield",
        "yields",
        "oil",
        "gas",
        "commodity",
        "commodities",
        "gold",
        "copper",
        "wheat",
        "currency",
        "dollar",
    ],
}

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "s",
    "that",
    "the",
    "their",
    "this",
    "to",
    "up",
    "with",
}
