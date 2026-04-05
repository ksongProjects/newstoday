"""Report generation."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .defaults import STOPWORDS, TOPIC_KEYWORDS

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9'-]{2,}")
HEADLINE_KEYWORDS = {
    "economy",
    "economic",
    "inflation",
    "interest rate",
    "interest rates",
    "fed",
    "federal reserve",
    "central bank",
    "recession",
    "gdp",
    "growth",
    "trade",
    "tariff",
    "tariffs",
    "exports",
    "imports",
    "jobs",
    "employment",
    "unemployment",
    "wages",
    "consumer",
    "housing",
    "oil prices",
    "opec",
    "commodity",
    "commodities",
    "bond",
    "bonds",
    "yield",
    "yields",
    "stock market",
    "financial markets",
    "currency",
    "dollar",
}


@dataclass(slots=True)
class ReportResult:
    output_path: Path
    article_count: int


def generate_report(
    *,
    articles: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    report_date: date,
    timezone_name: str,
    output_dir: str | Path,
) -> ReportResult:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"daily-news-{report_date.isoformat()}.md"
    markdown = render_report(
        articles=articles,
        runs=runs,
        report_date=report_date,
        timezone_name=timezone_name,
    )
    output_path.write_text(markdown, encoding="utf-8")
    return ReportResult(output_path=output_path, article_count=len(articles))


def render_report(
    *,
    articles: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    report_date: date,
    timezone_name: str,
) -> str:
    zone = ZoneInfo(timezone_name)
    generated_at = datetime.now(zone).strftime("%Y-%m-%d %H:%M %Z")
    source_counts = Counter(article["source_name"] for article in articles)
    country_counts = Counter(article["country"] for article in articles if article["country"])
    trending_terms = top_terms(articles)
    topic_groups = group_by_topic(articles)
    top_headlines = pick_top_headlines(articles, limit=20)

    lines = [
        f"# Daily Economic World News Report - {report_date.isoformat()}",
        "",
        f"Generated: {generated_at}",
        f"Articles in report window: {len(articles)}",
        f"Unique publisher labels: {len(source_counts)}",
        "",
        "## Collector Status",
    ]

    if runs:
        for run in runs:
            lines.append(
                f"- `{run['source_name']}`: {run['status']} | fetched {run['fetched_count']} | "
                f"inserted {run['inserted_count']} | updated {run['updated_count']} | skipped {run['skipped_count']}"
            )
    else:
        lines.append("- No recent collection runs recorded.")

    lines.extend(["", "## Top Headlines"])
    if top_headlines:
        for article in top_headlines:
            timestamp = datetime.fromisoformat(article["published_at"]).astimezone(zone).strftime("%H:%M")
            lines.append(
                f"- [{article['title']}]({article['url']}) | {article['source_name']} | {timestamp}"
            )
    else:
        lines.append("- No articles found for this date.")

    lines.extend(["", "## Topic Watch"])
    if topic_groups:
        for topic_name, topic_articles in topic_groups.items():
            lines.append(f"### {topic_name} ({len(topic_articles)})")
            for article in topic_articles[:8]:
                lines.append(f"- [{article['title']}]({article['url']}) | {article['source_name']}")
            lines.append("")
    else:
        lines.append("- No topic clusters were detected.")
        lines.append("")

    lines.append("## Trending Terms")
    if trending_terms:
        lines.append("- " + ", ".join(f"{term} ({count})" for term, count in trending_terms))
    else:
        lines.append("- No recurring terms detected.")

    lines.extend(["", "## Source Mix"])
    if source_counts:
        for source_name, count in source_counts.most_common(15):
            lines.append(f"- {source_name}: {count}")
    else:
        lines.append("- No sources found.")

    lines.extend(["", "## Country Signals"])
    if country_counts:
        for country, count in country_counts.most_common(10):
            lines.append(f"- {country}: {count}")
    else:
        lines.append("- Country metadata was sparse in this run.")

    return "\n".join(lines).rstrip() + "\n"


def pick_top_headlines(articles: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    seen_sources: Counter[str] = Counter()
    chosen: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    ranked_articles = sorted(
        articles,
        key=lambda item: (relevance_score(item), item["published_at"]),
        reverse=True,
    )
    for article in ranked_articles:
        title_key = article["title"].strip().lower()
        if not title_key or title_key in seen_titles:
            continue
        if relevance_score(article) <= 0:
            continue
        if seen_sources[article["source_name"]] >= 3:
            continue
        chosen.append(article)
        seen_titles.add(title_key)
        seen_sources[article["source_name"]] += 1
        if len(chosen) >= limit:
            break
    return chosen


def group_by_topic(articles: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for article in articles:
        haystack = f"{article['title']} {article['description']}".lower()
        for topic_name, keywords in TOPIC_KEYWORDS.items():
            if any(matches_keyword(haystack, keyword) for keyword in keywords):
                grouped[topic_name].append(article)
    return dict(grouped)


def top_terms(articles: list[dict[str, Any]], *, limit: int = 12) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for article in articles:
        text = f"{article['title']} {article['description']}"
        for token in TOKEN_RE.findall(text):
            word = token.lower()
            if word in STOPWORDS or word.isdigit():
                continue
            counter[word] += 1
    return counter.most_common(limit)


def relevance_score(article: dict[str, Any]) -> int:
    haystack = f"{article.get('title', '')} {article.get('description', '')}".lower()
    return sum(1 for keyword in HEADLINE_KEYWORDS if matches_keyword(haystack, keyword))


def matches_keyword(text: str, keyword: str) -> bool:
    pattern = r"\b" + re.escape(keyword.lower()).replace(r"\ ", r"\s+") + r"\b"
    return re.search(pattern, text) is not None
