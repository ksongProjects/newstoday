"""SQLite storage for articles and collection runs."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .models import Article, choose_better_url


@dataclass(slots=True)
class UpsertStats:
    inserted: int = 0
    updated: int = 0
    skipped: int = 0


class NewsStorage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()

    def close(self) -> None:
        self.conn.close()

    def _create_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT NOT NULL UNIQUE,
                    title_fingerprint TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    url TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    language TEXT NOT NULL,
                    country TEXT NOT NULL,
                    categories_json TEXT NOT NULL,
                    authors_json TEXT NOT NULL,
                    image_url TEXT NOT NULL,
                    content TEXT NOT NULL,
                    query_text TEXT NOT NULL,
                    raw_payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_articles_published_at
                    ON articles (published_at);

                CREATE INDEX IF NOT EXISTS idx_articles_title_fingerprint
                    ON articles (title_fingerprint);

                CREATE TABLE IF NOT EXISTS collection_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    fetched_count INTEGER NOT NULL,
                    inserted_count INTEGER NOT NULL,
                    updated_count INTEGER NOT NULL,
                    skipped_count INTEGER NOT NULL,
                    message TEXT NOT NULL
                );
                """
            )

    def upsert_articles(self, articles: list[Article]) -> UpsertStats:
        stats = UpsertStats()
        with self.conn:
            for article in articles:
                record = article.to_record()
                existing = self.conn.execute(
                    """
                    SELECT *
                    FROM articles
                    WHERE fingerprint = ?
                       OR title_fingerprint = ?
                    LIMIT 1
                    """,
                    (record["fingerprint"], record["title_fingerprint"]),
                ).fetchone()
                if existing is None:
                    self.conn.execute(
                        """
                        INSERT INTO articles (
                            fingerprint,
                            title_fingerprint,
                            canonical_url,
                            source_type,
                            source_name,
                            external_id,
                            title,
                            description,
                            url,
                            published_at,
                            collected_at,
                            language,
                            country,
                            categories_json,
                            authors_json,
                            image_url,
                            content,
                            query_text,
                            raw_payload_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record["fingerprint"],
                            record["title_fingerprint"],
                            record["canonical_url"],
                            record["source_type"],
                            record["source_name"],
                            record["external_id"],
                            record["title"],
                            record["description"],
                            record["url"],
                            record["published_at"],
                            record["collected_at"],
                            record["language"],
                            record["country"],
                            json.dumps(record["categories"], ensure_ascii=True),
                            json.dumps(record["authors"], ensure_ascii=True),
                            record["image_url"],
                            record["content"],
                            record["query"],
                            json.dumps(record["raw_payload"], ensure_ascii=True),
                        ),
                    )
                    stats.inserted += 1
                    continue

                updated_row = self._merged_row(existing, record)
                if updated_row is None:
                    stats.skipped += 1
                    continue

                self.conn.execute(
                    """
                    UPDATE articles
                    SET fingerprint = ?,
                        title_fingerprint = ?,
                        canonical_url = ?,
                        source_type = ?,
                        source_name = ?,
                        external_id = ?,
                        title = ?,
                        description = ?,
                        url = ?,
                        published_at = ?,
                        collected_at = ?,
                        language = ?,
                        country = ?,
                        categories_json = ?,
                        authors_json = ?,
                        image_url = ?,
                        content = ?,
                        query_text = ?,
                        raw_payload_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        updated_row["fingerprint"],
                        updated_row["title_fingerprint"],
                        updated_row["canonical_url"],
                        updated_row["source_type"],
                        updated_row["source_name"],
                        updated_row["external_id"],
                        updated_row["title"],
                        updated_row["description"],
                        updated_row["url"],
                        updated_row["published_at"],
                        updated_row["collected_at"],
                        updated_row["language"],
                        updated_row["country"],
                        json.dumps(updated_row["categories"], ensure_ascii=True),
                        json.dumps(updated_row["authors"], ensure_ascii=True),
                        updated_row["image_url"],
                        updated_row["content"],
                        updated_row["query"],
                        json.dumps(updated_row["raw_payload"], ensure_ascii=True),
                        existing["id"],
                    ),
                )
                stats.updated += 1
        return stats

    def _merged_row(self, existing: sqlite3.Row, incoming: dict[str, Any]) -> dict[str, Any] | None:
        existing_payload = json.loads(existing["raw_payload_json"]) if existing["raw_payload_json"] else {}
        existing_categories = json.loads(existing["categories_json"]) if existing["categories_json"] else []
        existing_authors = json.loads(existing["authors_json"]) if existing["authors_json"] else []

        merged = {
            "fingerprint": existing["fingerprint"],
            "title_fingerprint": existing["title_fingerprint"] or incoming["title_fingerprint"],
            "canonical_url": existing["canonical_url"] or incoming["canonical_url"],
            "source_type": existing["source_type"] or incoming["source_type"],
            "source_name": existing["source_name"] or incoming["source_name"],
            "external_id": existing["external_id"] or incoming["external_id"],
            "title": existing["title"] or incoming["title"],
            "description": existing["description"] or incoming["description"],
            "url": choose_better_url(existing["url"], incoming["url"]),
            "published_at": existing["published_at"] or incoming["published_at"],
            "collected_at": incoming["collected_at"],
            "language": existing["language"] or incoming["language"],
            "country": existing["country"] or incoming["country"],
            "categories": list(dict.fromkeys(existing_categories + incoming["categories"])),
            "authors": list(dict.fromkeys(existing_authors + incoming["authors"])),
            "image_url": existing["image_url"] or incoming["image_url"],
            "content": existing["content"] or incoming["content"],
            "query": existing["query_text"] or incoming["query"],
            "raw_payload": existing_payload or incoming["raw_payload"],
        }

        if (
            merged["url"] == existing["url"]
            and merged["description"] == existing["description"]
            and merged["content"] == existing["content"]
            and merged["image_url"] == existing["image_url"]
            and merged["query"] == existing["query_text"]
            and merged["canonical_url"] == existing["canonical_url"]
            and merged["categories"] == existing_categories
            and merged["authors"] == existing_authors
        ):
            return None
        return merged

    def record_run(
        self,
        *,
        source_name: str,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        fetched_count: int,
        inserted_count: int,
        updated_count: int,
        skipped_count: int,
        message: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO collection_runs (
                    source_name,
                    started_at,
                    finished_at,
                    status,
                    fetched_count,
                    inserted_count,
                    updated_count,
                    skipped_count,
                    message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_name,
                    started_at.astimezone(timezone.utc).isoformat(),
                    finished_at.astimezone(timezone.utc).isoformat(),
                    status,
                    fetched_count,
                    inserted_count,
                    updated_count,
                    skipped_count,
                    message,
                ),
            )

    def fetch_articles_for_date(self, report_date: date, timezone_name: str) -> list[dict[str, Any]]:
        zone = ZoneInfo(timezone_name)
        start_local = datetime.combine(report_date, time.min, tzinfo=zone)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).isoformat()
        end_utc = end_local.astimezone(timezone.utc).isoformat()
        rows = self.conn.execute(
            """
            SELECT *
            FROM articles
            WHERE published_at >= ?
              AND published_at < ?
            ORDER BY published_at DESC
            """,
            (start_utc, end_utc),
        ).fetchall()
        return [self._row_to_article_dict(row) for row in rows]

    def recent_runs(self, since_hours: int = 30) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        rows = self.conn.execute(
            """
            SELECT *
            FROM collection_runs
            WHERE started_at >= ?
            ORDER BY started_at DESC
            """,
            (cutoff,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _row_to_article_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["categories"] = json.loads(item.pop("categories_json"))
        item["authors"] = json.loads(item.pop("authors_json"))
        item["raw_payload"] = json.loads(item.pop("raw_payload_json"))
        item["query"] = item.pop("query_text")
        return item
