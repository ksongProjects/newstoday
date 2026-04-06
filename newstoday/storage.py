"""SQLite storage for YouTube videos and collection runs."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .models import VideoRecord


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
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL UNIQUE,
                    channel_id TEXT NOT NULL,
                    channel_title TEXT NOT NULL,
                    channel_lookup TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    url TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    duration_iso TEXT NOT NULL,
                    duration_seconds INTEGER NOT NULL,
                    view_count INTEGER NOT NULL,
                    thumbnail_url TEXT NOT NULL,
                    live_status TEXT NOT NULL,
                    transcript_status TEXT NOT NULL,
                    transcript_language TEXT NOT NULL,
                    transcript_language_code TEXT NOT NULL,
                    transcript_is_generated INTEGER NOT NULL,
                    transcript_is_translated INTEGER NOT NULL,
                    transcript_error TEXT NOT NULL,
                    transcript_text TEXT NOT NULL,
                    transcript_segments_json TEXT NOT NULL,
                    ai_summary_points_json TEXT NOT NULL DEFAULT '[]',
                    ai_summary_model TEXT NOT NULL DEFAULT '',
                    ai_summary_generated_at TEXT NOT NULL DEFAULT '',
                    ai_summary_error TEXT NOT NULL DEFAULT '',
                    raw_payload_json TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_videos_published_at
                    ON videos (published_at);

                CREATE INDEX IF NOT EXISTS idx_videos_channel_title
                    ON videos (channel_title);

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
            self._ensure_video_columns()

    def _ensure_video_columns(self) -> None:
        existing_columns = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(videos)").fetchall()
        }
        required_columns = {
            "ai_summary_points_json": "TEXT NOT NULL DEFAULT '[]'",
            "ai_summary_model": "TEXT NOT NULL DEFAULT ''",
            "ai_summary_generated_at": "TEXT NOT NULL DEFAULT ''",
            "ai_summary_error": "TEXT NOT NULL DEFAULT ''",
        }
        for column_name, column_definition in required_columns.items():
            if column_name in existing_columns:
                continue
            self.conn.execute(f"ALTER TABLE videos ADD COLUMN {column_name} {column_definition}")

    def upsert_videos(self, videos: list[VideoRecord]) -> UpsertStats:
        stats = UpsertStats()
        with self.conn:
            for video in videos:
                record = video.to_record()
                existing = self.conn.execute(
                    "SELECT * FROM videos WHERE video_id = ? LIMIT 1",
                    (record["video_id"],),
                ).fetchone()
                if existing is None:
                    self.conn.execute(
                        """
                        INSERT INTO videos (
                            video_id,
                            channel_id,
                            channel_title,
                            channel_lookup,
                            title,
                            description,
                            url,
                            published_at,
                            duration_iso,
                            duration_seconds,
                            view_count,
                            thumbnail_url,
                            live_status,
                            transcript_status,
                            transcript_language,
                            transcript_language_code,
                            transcript_is_generated,
                            transcript_is_translated,
                            transcript_error,
                            transcript_text,
                            transcript_segments_json,
                            ai_summary_points_json,
                            ai_summary_model,
                            ai_summary_generated_at,
                            ai_summary_error,
                            raw_payload_json,
                            collected_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record["video_id"],
                            record["channel_id"],
                            record["channel_title"],
                            record["channel_lookup"],
                            record["title"],
                            record["description"],
                            record["url"],
                            record["published_at"],
                            record["duration_iso"],
                            record["duration_seconds"],
                            record["view_count"],
                            record["thumbnail_url"],
                            record["live_status"],
                            record["transcript_status"],
                            record["transcript_language"],
                            record["transcript_language_code"],
                            record["transcript_is_generated"],
                            record["transcript_is_translated"],
                            record["transcript_error"],
                            record["transcript_text"],
                            json.dumps(record["transcript_segments"], ensure_ascii=True),
                            json.dumps(record["ai_summary_points"], ensure_ascii=True),
                            record["ai_summary_model"],
                            record["ai_summary_generated_at"],
                            record["ai_summary_error"],
                            json.dumps(record["raw_payload"], ensure_ascii=True),
                            record["collected_at"],
                        ),
                    )
                    stats.inserted += 1
                    continue

                merged = self._merged_row(existing, record)
                if merged is None:
                    stats.skipped += 1
                    continue

                self.conn.execute(
                    """
                    UPDATE videos
                    SET channel_id = ?,
                        channel_title = ?,
                        channel_lookup = ?,
                        title = ?,
                        description = ?,
                        url = ?,
                        published_at = ?,
                        duration_iso = ?,
                        duration_seconds = ?,
                        view_count = ?,
                        thumbnail_url = ?,
                        live_status = ?,
                        transcript_status = ?,
                        transcript_language = ?,
                        transcript_language_code = ?,
                        transcript_is_generated = ?,
                        transcript_is_translated = ?,
                        transcript_error = ?,
                        transcript_text = ?,
                        transcript_segments_json = ?,
                        ai_summary_points_json = ?,
                        ai_summary_model = ?,
                        ai_summary_generated_at = ?,
                        ai_summary_error = ?,
                        raw_payload_json = ?,
                        collected_at = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        merged["channel_id"],
                        merged["channel_title"],
                        merged["channel_lookup"],
                        merged["title"],
                        merged["description"],
                        merged["url"],
                        merged["published_at"],
                        merged["duration_iso"],
                        merged["duration_seconds"],
                        merged["view_count"],
                        merged["thumbnail_url"],
                        merged["live_status"],
                        merged["transcript_status"],
                        merged["transcript_language"],
                        merged["transcript_language_code"],
                        merged["transcript_is_generated"],
                        merged["transcript_is_translated"],
                        merged["transcript_error"],
                        merged["transcript_text"],
                        json.dumps(merged["transcript_segments"], ensure_ascii=True),
                        json.dumps(merged["ai_summary_points"], ensure_ascii=True),
                        merged["ai_summary_model"],
                        merged["ai_summary_generated_at"],
                        merged["ai_summary_error"],
                        json.dumps(merged["raw_payload"], ensure_ascii=True),
                        merged["collected_at"],
                        existing["id"],
                    ),
                )
                stats.updated += 1
        return stats

    def _merged_row(self, existing: sqlite3.Row, incoming: dict[str, Any]) -> dict[str, Any] | None:
        existing_payload = json.loads(existing["raw_payload_json"]) if existing["raw_payload_json"] else {}
        existing_segments = (
            json.loads(existing["transcript_segments_json"]) if existing["transcript_segments_json"] else []
        )
        existing_ai_summary_points = (
            json.loads(existing["ai_summary_points_json"]) if existing["ai_summary_points_json"] else []
        )

        merged = {
            "channel_id": incoming["channel_id"] or existing["channel_id"],
            "channel_title": incoming["channel_title"] or existing["channel_title"],
            "channel_lookup": incoming["channel_lookup"] or existing["channel_lookup"],
            "title": incoming["title"] or existing["title"],
            "description": incoming["description"] or existing["description"],
            "url": incoming["url"] or existing["url"],
            "published_at": incoming["published_at"] or existing["published_at"],
            "duration_iso": incoming["duration_iso"] or existing["duration_iso"],
            "duration_seconds": max(int(existing["duration_seconds"]), int(incoming["duration_seconds"])),
            "view_count": max(int(existing["view_count"]), int(incoming["view_count"])),
            "thumbnail_url": incoming["thumbnail_url"] or existing["thumbnail_url"],
            "live_status": incoming["live_status"] or existing["live_status"],
            "transcript_status": existing["transcript_status"],
            "transcript_language": existing["transcript_language"],
            "transcript_language_code": existing["transcript_language_code"],
            "transcript_is_generated": existing["transcript_is_generated"],
            "transcript_is_translated": existing["transcript_is_translated"],
            "transcript_error": existing["transcript_error"],
            "transcript_text": existing["transcript_text"],
            "transcript_segments": existing_segments,
            "ai_summary_points": existing_ai_summary_points,
            "ai_summary_model": existing["ai_summary_model"],
            "ai_summary_generated_at": existing["ai_summary_generated_at"],
            "ai_summary_error": existing["ai_summary_error"],
            "raw_payload": incoming["raw_payload"] or existing_payload,
            "collected_at": incoming["collected_at"],
        }

        incoming_transcript_better = self._transcript_score(incoming) > self._transcript_score(existing)
        if incoming_transcript_better:
            merged["transcript_status"] = incoming["transcript_status"]
            merged["transcript_language"] = incoming["transcript_language"]
            merged["transcript_language_code"] = incoming["transcript_language_code"]
            merged["transcript_is_generated"] = incoming["transcript_is_generated"]
            merged["transcript_is_translated"] = incoming["transcript_is_translated"]
            merged["transcript_error"] = incoming["transcript_error"]
            merged["transcript_text"] = incoming["transcript_text"]
            merged["transcript_segments"] = incoming["transcript_segments"]
            merged["ai_summary_points"] = incoming["ai_summary_points"]
            merged["ai_summary_model"] = incoming["ai_summary_model"]
            merged["ai_summary_generated_at"] = incoming["ai_summary_generated_at"]
            merged["ai_summary_error"] = incoming["ai_summary_error"]
        elif self._ai_summary_score(incoming) > self._ai_summary_score(existing):
            merged["ai_summary_points"] = incoming["ai_summary_points"]
            merged["ai_summary_model"] = incoming["ai_summary_model"]
            merged["ai_summary_generated_at"] = incoming["ai_summary_generated_at"]
            merged["ai_summary_error"] = incoming["ai_summary_error"]

        if (
            merged["channel_title"] == existing["channel_title"]
            and merged["title"] == existing["title"]
            and merged["description"] == existing["description"]
            and merged["view_count"] == existing["view_count"]
            and merged["thumbnail_url"] == existing["thumbnail_url"]
            and merged["transcript_status"] == existing["transcript_status"]
            and merged["transcript_text"] == existing["transcript_text"]
            and merged["transcript_error"] == existing["transcript_error"]
            and merged["transcript_segments"] == existing_segments
            and merged["ai_summary_points"] == existing_ai_summary_points
            and merged["ai_summary_model"] == existing["ai_summary_model"]
            and merged["ai_summary_generated_at"] == existing["ai_summary_generated_at"]
            and merged["ai_summary_error"] == existing["ai_summary_error"]
            and merged["duration_seconds"] == existing["duration_seconds"]
        ):
            return None
        return merged

    def _transcript_score(self, record: dict[str, Any] | sqlite3.Row) -> tuple[int, int]:
        transcript_text = record["transcript_text"] or ""
        status = record["transcript_status"] or ""
        has_text = 1 if transcript_text else 0
        status_rank = 2 if status == "ok" else 1 if status == "empty" else 0
        return (status_rank, len(transcript_text) if has_text else 0)

    def _ai_summary_score(self, record: dict[str, Any] | sqlite3.Row) -> tuple[int, int, int]:
        summary_points = self._summary_points_from_record(record)
        summary_error = str(record["ai_summary_error"] or "")
        joined_length = sum(len(item) for item in summary_points)
        return (1 if summary_points else 0, 1 if summary_error else 0, joined_length)

    def _summary_points_from_record(self, record: dict[str, Any] | sqlite3.Row) -> list[str]:
        if isinstance(record, sqlite3.Row):
            raw_value = record["ai_summary_points_json"] if "ai_summary_points_json" in record.keys() else "[]"
            return list(json.loads(raw_value or "[]"))
        return list(record.get("ai_summary_points", []) or [])

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

    def fetch_videos_for_date(self, report_date: date, timezone_name: str) -> list[dict[str, Any]]:
        zone = ZoneInfo(timezone_name)
        start_local = datetime.combine(report_date, time.min, tzinfo=zone)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).isoformat()
        end_utc = end_local.astimezone(timezone.utc).isoformat()
        rows = self.conn.execute(
            """
            SELECT *
            FROM videos
            WHERE published_at >= ?
              AND published_at < ?
            ORDER BY published_at DESC
            """,
            (start_utc, end_utc),
        ).fetchall()
        return [self._row_to_video_dict(row) for row in rows]

    def fetch_recent_videos(self, since_hours: int = 24) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        rows = self.conn.execute(
            """
            SELECT *
            FROM videos
            WHERE published_at >= ?
            ORDER BY published_at DESC
            """,
            (cutoff,),
        ).fetchall()
        return [self._row_to_video_dict(row) for row in rows]

    def fetch_videos_in_range(self, start_at: datetime, end_at: datetime) -> list[dict[str, Any]]:
        start_utc = start_at.astimezone(timezone.utc).isoformat()
        end_utc = end_at.astimezone(timezone.utc).isoformat()
        rows = self.conn.execute(
            """
            SELECT *
            FROM videos
            WHERE published_at >= ?
              AND published_at < ?
            ORDER BY published_at DESC
            """,
            (start_utc, end_utc),
        ).fetchall()
        return [self._row_to_video_dict(row) for row in rows]

    def fetch_videos_by_ids(self, video_ids: list[str]) -> list[dict[str, Any]]:
        normalized_ids = [str(video_id).strip() for video_id in video_ids if str(video_id).strip()]
        if not normalized_ids:
            return []
        placeholders = ",".join("?" for _ in normalized_ids)
        rows = self.conn.execute(
            f"""
            SELECT *
            FROM videos
            WHERE video_id IN ({placeholders})
            ORDER BY published_at DESC
            """,
            normalized_ids,
        ).fetchall()
        return [self._row_to_video_dict(row) for row in rows]

    def clear_transcripts(self, video_ids: list[str]) -> int:
        normalized_ids = [str(video_id).strip() for video_id in video_ids if str(video_id).strip()]
        if not normalized_ids:
            return 0
        placeholders = ",".join("?" for _ in normalized_ids)
        with self.conn:
            cursor = self.conn.execute(
                f"""
                UPDATE videos
                SET transcript_status = 'pending',
                    transcript_language = '',
                    transcript_language_code = '',
                    transcript_is_generated = 0,
                    transcript_is_translated = 0,
                    transcript_error = '',
                    transcript_text = '',
                    transcript_segments_json = '[]',
                    ai_summary_points_json = '[]',
                    ai_summary_model = '',
                    ai_summary_generated_at = '',
                    ai_summary_error = '',
                    updated_at = CURRENT_TIMESTAMP
                WHERE video_id IN ({placeholders})
                """,
                normalized_ids,
            )
        return int(cursor.rowcount or 0)

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

    def _row_to_video_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["transcript_segments"] = json.loads(item.pop("transcript_segments_json"))
        item["ai_summary_points"] = json.loads(item.pop("ai_summary_points_json"))
        item["raw_payload"] = json.loads(item.pop("raw_payload_json"))
        item["transcript_is_generated"] = bool(item["transcript_is_generated"])
        item["transcript_is_translated"] = bool(item["transcript_is_translated"])
        return item
