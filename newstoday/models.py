"""Core models and normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from hashlib import sha256
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
}

AGGREGATOR_HOSTS = {
    "news.google.com",
}


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def get_text(self) -> str:
        return "".join(self.parts)


def strip_html(value: str | None) -> str:
    if not value:
        return ""
    parser = _HTMLStripper()
    parser.feed(value)
    return " ".join(unescape(parser.get_text()).split())


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_published_at(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    raw = value.strip()
    if raw.endswith("Z") and len(raw) == 16 and "T" in raw:
        return datetime.strptime(raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    if raw.endswith("Z") and len(raw) == 20 and raw[4] == "-":
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    try:
        return ensure_utc(datetime.fromisoformat(raw))
    except ValueError:
        pass
    try:
        return ensure_utc(parsedate_to_datetime(raw))
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


def normalize_title(title: str, source_name: str | None = None) -> str:
    cleaned = " ".join(unescape(title or "").split())
    if not source_name:
        return cleaned
    suffix = f" - {source_name}".lower()
    if cleaned.lower().endswith(suffix):
        return cleaned[: -len(suffix)].rstrip()
    return cleaned


def canonicalize_url(url: str) -> str:
    parsed = urlparse((url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return (url or "").strip()
    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if key.lower() not in TRACKING_PARAMS and not key.lower().startswith("utm_")
    ]
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
        query=urlencode(sorted(query_items), doseq=True),
    )
    return urlunparse(normalized)


def host_for_url(url: str) -> str:
    return urlparse(url).netloc.lower()


def is_aggregator_url(url: str) -> bool:
    return host_for_url(url) in AGGREGATOR_HOSTS


def title_fingerprint(title: str, source_name: str, published_at: datetime) -> str:
    basis = "|".join(
        [
            " ".join(title.lower().split()),
            " ".join((source_name or "").lower().split()),
            ensure_utc(published_at).date().isoformat(),
        ]
    )
    return sha256(basis.encode("utf-8")).hexdigest()


def url_fingerprint(url: str) -> str:
    basis = canonicalize_url(url) or url
    return sha256(basis.encode("utf-8")).hexdigest()


def choose_better_url(current_url: str, incoming_url: str) -> str:
    if not current_url:
        return incoming_url
    if is_aggregator_url(current_url) and not is_aggregator_url(incoming_url):
        return incoming_url
    return current_url


@dataclass(slots=True)
class Article:
    source_type: str
    source_name: str
    title: str
    url: str
    published_at: datetime
    description: str = ""
    language: str = ""
    country: str = ""
    categories: list[str] = field(default_factory=list)
    authors: list[str] = field(default_factory=list)
    image_url: str = ""
    content: str = ""
    external_id: str = ""
    query: str = ""
    raw_payload: dict[str, Any] = field(default_factory=dict)
    collected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def normalized_title(self) -> str:
        return normalize_title(self.title, self.source_name)

    def normalized_published_at(self) -> datetime:
        return ensure_utc(self.published_at)

    def to_record(self) -> dict[str, Any]:
        published_at = self.normalized_published_at()
        normalized_title_value = self.normalized_title()
        title_fp = title_fingerprint(normalized_title_value, self.source_name, published_at)
        canonical_url = canonicalize_url(self.url)
        return {
            "fingerprint": url_fingerprint(canonical_url or self.url)
            if canonical_url and not is_aggregator_url(canonical_url)
            else title_fp,
            "title_fingerprint": title_fp,
            "canonical_url": canonical_url,
            "source_type": self.source_type,
            "source_name": self.source_name,
            "external_id": self.external_id,
            "title": normalized_title_value,
            "description": " ".join((self.description or "").split()),
            "url": self.url.strip(),
            "published_at": published_at.isoformat(),
            "collected_at": ensure_utc(self.collected_at).isoformat(),
            "language": self.language,
            "country": self.country,
            "categories": list(dict.fromkeys(filter(None, self.categories))),
            "authors": list(dict.fromkeys(filter(None, self.authors))),
            "image_url": self.image_url,
            "content": self.content,
            "query": self.query,
            "raw_payload": self.raw_payload,
        }
