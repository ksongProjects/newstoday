"""News source collectors."""

from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Iterable

from .defaults import API_SEARCHES, GDELT_SEARCHES, GOOGLE_NEWS_SEARCHES
from .models import Article, parse_published_at, strip_html

USER_AGENT = "NewsToday/0.1 (+https://example.invalid)"


class SourceError(RuntimeError):
    """Raised when a source request fails."""


class Collector(ABC):
    name: str

    def __init__(self, *, max_per_query: int = 100) -> None:
        self.max_per_query = max_per_query

    @abstractmethod
    def collect(self, *, hours: int) -> list[Article]:
        raise NotImplementedError

    def _json_get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
        attempts: int = 3,
    ) -> dict:
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                    **(headers or {}),
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout, TimeoutError) as exc:
                last_exc = exc
                if attempt == attempts:
                    break
                time.sleep(min(2 ** (attempt - 1), 4))
        raise SourceError(f"{self.name}: request failed for {url}: {last_exc}") from last_exc

    def _text_get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
        attempts: int = 3,
    ) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/rss+xml, application/xml, text/xml, text/plain;q=0.9, */*;q=0.8",
                    **(headers or {}),
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return response.read().decode("utf-8", errors="replace")
            except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout, TimeoutError) as exc:
                last_exc = exc
                if attempt == attempts:
                    break
                time.sleep(min(2 ** (attempt - 1), 4))
        raise SourceError(f"{self.name}: request failed for {url}: {last_exc}") from last_exc


class GoogleNewsRSSCollector(Collector):
    name = "google_news_rss"

    def collect(self, *, hours: int) -> list[Article]:
        window = f"{hours}h" if hours < 24 else f"{max(1, hours // 24)}d"
        articles: list[Article] = []
        for query in GOOGLE_NEWS_SEARCHES:
            search = f"({query}) when:{window}"
            params = urllib.parse.urlencode(
                {"q": search, "hl": "en-US", "gl": "US", "ceid": "US:en"}
            )
            url = f"https://news.google.com/rss/search?{params}"
            xml_text = self._text_get(url, timeout=25)
            root = ET.fromstring(xml_text)
            for item in root.findall("./channel/item")[: self.max_per_query]:
                source_element = item.find("source")
                source_name = (source_element.text or "Google News").strip() if source_element is not None else "Google News"
                source_url = source_element.attrib.get("url", "") if source_element is not None else ""
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                description = strip_html(item.findtext("description"))
                if source_name and description.endswith(source_name):
                    description = description[: -len(source_name)].rstrip(" -|")
                articles.append(
                    Article(
                        source_type="rss",
                        source_name=source_name,
                        title=title,
                        url=link,
                        published_at=parse_published_at(item.findtext("pubDate")),
                        description=description,
                        language="en",
                        query=query,
                        raw_payload={
                            "source_url": source_url,
                            "guid": item.findtext("guid", ""),
                        },
                    )
                )
            time.sleep(0.2)
        return articles


class GDELTCollector(Collector):
    name = "gdelt"

    def collect(self, *, hours: int) -> list[Article]:
        articles: list[Article] = []
        timespan = f"{hours}h" if hours < 24 else f"{max(1, hours // 24)}d"
        per_query = min(self.max_per_query, 50)
        failures: list[str] = []
        for query in GDELT_SEARCHES:
            params = urllib.parse.urlencode(
                {
                    "query": query,
                    "mode": "artlist",
                    "maxrecords": str(per_query),
                    "format": "json",
                    "sort": "datedesc",
                    "timespan": timespan,
                }
            )
            url = f"http://api.gdeltproject.org/api/v2/doc/doc?{params}"
            try:
                payload = self._json_get(url, timeout=10, attempts=1)
            except SourceError as exc:
                failures.append(str(exc))
                continue
            for item in payload.get("articles", []):
                articles.append(
                    Article(
                        source_type="api",
                        source_name=item.get("domain") or "GDELT",
                        title=item.get("title") or "",
                        url=item.get("url") or item.get("url_mobile") or "",
                        published_at=parse_published_at(item.get("seendate")),
                        language=(item.get("language") or "").lower(),
                        country=item.get("sourcecountry") or "",
                        image_url=item.get("socialimage") or "",
                        query=query,
                        raw_payload=item,
                    )
                )
            time.sleep(0.3)
        if not articles and failures:
            raise SourceError(failures[0])
        return articles


class GNewsCollector(Collector):
    name = "gnews"

    def __init__(self, api_key: str, *, max_per_query: int = 10) -> None:
        super().__init__(max_per_query=max_per_query)
        self.api_key = api_key

    def collect(self, *, hours: int) -> list[Article]:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        articles: list[Article] = []
        for query in API_SEARCHES:
            params = urllib.parse.urlencode(
                {
                    "q": query,
                    "lang": "en",
                    "max": str(min(self.max_per_query, 10)),
                    "from": since,
                    "apikey": self.api_key,
                }
            )
            url = f"https://gnews.io/api/v4/search?{params}"
            payload = self._json_get(url)
            for item in payload.get("articles", []):
                source = item.get("source") or {}
                articles.append(
                    Article(
                        source_type="api",
                        source_name=source.get("name") or "GNews",
                        title=item.get("title") or "",
                        url=item.get("url") or "",
                        published_at=parse_published_at(item.get("publishedAt")),
                        description=item.get("description") or "",
                        language="en",
                        image_url=item.get("image") or "",
                        content=item.get("content") or "",
                        query=query,
                        raw_payload=item,
                    )
                )
            time.sleep(0.25)
        return articles


class NewsDataCollector(Collector):
    name = "newsdata"

    def __init__(self, api_key: str, *, max_per_query: int = 30) -> None:
        super().__init__(max_per_query=max_per_query)
        self.api_key = api_key

    def collect(self, *, hours: int) -> list[Article]:
        articles: list[Article] = []
        max_pages = max(1, self.max_per_query // 10)
        for query in API_SEARCHES:
            next_page = ""
            for _ in range(max_pages):
                params = {
                    "apikey": self.api_key,
                    "q": query,
                    "language": "en",
                    "category": "business",
                }
                if next_page:
                    params["page"] = next_page
                url = f"https://newsdata.io/api/1/latest?{urllib.parse.urlencode(params)}"
                payload = self._json_get(url)
                for item in payload.get("results", []):
                    creator = item.get("creator")
                    authors = creator if isinstance(creator, list) else ([creator] if creator else [])
                    country = item.get("country")
                    if isinstance(country, list):
                        country = ",".join(country)
                    categories = item.get("category") or []
                    articles.append(
                        Article(
                            source_type="api",
                            source_name=item.get("source_name") or "NewsData.io",
                            title=item.get("title") or "",
                            url=item.get("link") or "",
                            published_at=parse_published_at(item.get("pubDate")),
                            description=strip_html(item.get("description")),
                            language=item.get("language") or "en",
                            country=country or "",
                            categories=categories,
                            authors=authors,
                            image_url=item.get("image_url") or "",
                            content=item.get("content") or "",
                            external_id=item.get("article_id") or "",
                            query=query,
                            raw_payload=item,
                        )
                    )
                next_page = payload.get("nextPage") or ""
                if not next_page:
                    break
                time.sleep(0.25)
        return self._filter_recent(articles, hours)

    def _filter_recent(self, articles: Iterable[Article], hours: int) -> list[Article]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        return [article for article in articles if article.normalized_published_at() >= cutoff]


class CurrentsCollector(Collector):
    name = "currents"

    def __init__(self, api_key: str, *, max_per_query: int = 80) -> None:
        super().__init__(max_per_query=max_per_query)
        self.api_key = api_key

    def collect(self, *, hours: int) -> list[Article]:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        articles: list[Article] = []
        page_size = min(self.max_per_query, 50)
        for query in API_SEARCHES:
            params = urllib.parse.urlencode(
                {
                    "keywords": query,
                    "language": "en",
                    "start_date": since,
                    "page_size": str(page_size),
                    "category": "finance",
                }
            )
            url = f"https://api.currentsapi.services/v1/search?{params}"
            payload = self._json_get(url, headers={"Authorization": self.api_key})
            for item in payload.get("news", []):
                categories = item.get("category")
                if isinstance(categories, str):
                    categories = [categories]
                articles.append(
                    Article(
                        source_type="api",
                        source_name=item.get("author") or item.get("id") or "Currents",
                        title=item.get("title") or "",
                        url=item.get("url") or "",
                        published_at=parse_published_at(item.get("published")),
                        description=strip_html(item.get("description")),
                        language=item.get("language") or "en",
                        country=item.get("country") or "",
                        categories=categories or [],
                        image_url=item.get("image") or "",
                        content=item.get("description") or "",
                        external_id=item.get("id") or "",
                        query=query,
                        raw_payload=item,
                    )
                )
            time.sleep(0.25)
        return articles


class AlphaVantageCollector(Collector):
    name = "alphavantage"

    def __init__(self, api_key: str, *, max_per_query: int = 1000) -> None:
        super().__init__(max_per_query=max_per_query)
        self.api_key = api_key

    def collect(self, *, hours: int) -> list[Article]:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y%m%dT%H%M")
        params = urllib.parse.urlencode(
            {
                "function": "NEWS_SENTIMENT",
                "topics": "economy_macro,economy_monetary,economy_fiscal,financial_markets,finance,energy_transportation",
                "time_from": since,
                "sort": "LATEST",
                "limit": str(min(self.max_per_query, 1000)),
                "apikey": self.api_key,
            }
        )
        url = f"https://www.alphavantage.co/query?{params}"
        payload = self._json_get(url)
        feed = payload.get("feed", [])
        articles: list[Article] = []
        for item in feed:
            source = item.get("source") or "Alpha Vantage"
            topics = [topic.get("topic", "") for topic in item.get("topics", []) if topic.get("topic")]
            authors = item.get("authors") or []
            articles.append(
                Article(
                    source_type="api",
                    source_name=source,
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    published_at=parse_published_at(item.get("time_published")),
                    description=item.get("summary") or "",
                    language="en",
                    categories=topics,
                    authors=authors,
                    image_url=item.get("banner_image") or "",
                    content=item.get("summary") or "",
                    external_id=item.get("overall_sentiment_label") or "",
                    query="economy topics",
                    raw_payload=item,
                )
            )
        return articles


def enabled_collectors(*, max_per_query: int = 100) -> list[Collector]:
    collectors: list[Collector] = [
        GoogleNewsRSSCollector(max_per_query=max_per_query),
        GDELTCollector(max_per_query=max_per_query),
    ]
    gnews_key = os.getenv("GNEWS_API_KEY", "").strip()
    newsdata_key = os.getenv("NEWSDATA_API_KEY", "").strip()
    currents_key = os.getenv("CURRENTS_API_KEY", "").strip()
    alphavantage_key = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()

    if gnews_key:
        collectors.append(GNewsCollector(gnews_key, max_per_query=min(max_per_query, 10)))
    if newsdata_key:
        collectors.append(NewsDataCollector(newsdata_key, max_per_query=max_per_query))
    if currents_key:
        collectors.append(CurrentsCollector(currents_key, max_per_query=max_per_query))
    if alphavantage_key:
        collectors.append(AlphaVantageCollector(alphavantage_key, max_per_query=max_per_query))
    return collectors
