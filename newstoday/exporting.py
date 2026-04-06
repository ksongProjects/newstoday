"""Transcript export helpers for UI downloads and downstream pipelines."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import format_duration
from .reporting import build_summary_points, classify_topics

EXPORT_SCHEMA_VERSION = "newstoday.transcripts.v1"


def build_export_rows(video_rows: list[dict[str, Any]], *, timezone_name: str = "UTC") -> list[dict[str, Any]]:
    zone = resolve_timezone(timezone_name)
    rows: list[dict[str, Any]] = []
    for row in sorted(video_rows, key=lambda item: item.get("published_at", ""), reverse=True):
        topics = classify_topics(f"{row.get('title', '')} {row.get('description', '')} {row.get('transcript_text', '')}")
        summary_points = build_summary_points(row)
        transcript_text = str(row.get("transcript_text", "") or "")
        published_at = str(row.get("published_at", "") or "")
        published_local = ""
        if published_at:
            published_local = datetime.fromisoformat(published_at.replace("Z", "+00:00")).astimezone(zone).isoformat()
        rows.append(
            {
                "video_id": str(row.get("video_id", "") or ""),
                "url": str(row.get("url", "") or ""),
                "channel_id": str(row.get("channel_id", "") or ""),
                "channel_title": str(row.get("channel_title", "") or ""),
                "channel_lookup": str(row.get("channel_lookup", "") or ""),
                "title": str(row.get("title", "") or ""),
                "description": str(row.get("description", "") or ""),
                "published_at": published_at,
                "published_local": published_local,
                "duration_seconds": int(row.get("duration_seconds", 0) or 0),
                "duration_display": format_duration(int(row.get("duration_seconds", 0) or 0)),
                "view_count": int(row.get("view_count", 0) or 0),
                "transcript_status": str(row.get("transcript_status", "") or ""),
                "transcript_language": str(row.get("transcript_language", "") or ""),
                "transcript_language_code": str(row.get("transcript_language_code", "") or ""),
                "transcript_is_generated": bool(row.get("transcript_is_generated", False)),
                "transcript_is_translated": bool(row.get("transcript_is_translated", False)),
                "transcript_error": str(row.get("transcript_error", "") or ""),
                "transcript_text": transcript_text,
                "transcript_word_count": len(transcript_text.split()),
                "transcript_char_count": len(transcript_text),
                "segment_count": len(row.get("transcript_segments", []) or []),
                "topics": topics,
                "summary_points": summary_points,
                "transcript_segments": list(row.get("transcript_segments", []) or []),
            }
        )
    return rows


def export_transcripts_json(video_rows: list[dict[str, Any]], *, timezone_name: str = "UTC") -> bytes:
    payload = {
        "schema_version": EXPORT_SCHEMA_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "timezone": timezone_name,
        "count": len(video_rows),
        "items": build_export_rows(video_rows, timezone_name=timezone_name),
    }
    return json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")


def export_transcripts_csv(video_rows: list[dict[str, Any]], *, timezone_name: str = "UTC") -> bytes:
    rows = build_export_rows(video_rows, timezone_name=timezone_name)
    buffer = io.StringIO(newline="")
    fieldnames = [
        "video_id",
        "url",
        "channel_id",
        "channel_title",
        "channel_lookup",
        "title",
        "description",
        "published_at",
        "published_local",
        "duration_seconds",
        "duration_display",
        "view_count",
        "transcript_status",
        "transcript_language",
        "transcript_language_code",
        "transcript_is_generated",
        "transcript_is_translated",
        "transcript_error",
        "transcript_word_count",
        "transcript_char_count",
        "segment_count",
        "topics",
        "summary_points",
        "transcript_text",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "video_id": row["video_id"],
                "url": row["url"],
                "channel_id": row["channel_id"],
                "channel_title": row["channel_title"],
                "channel_lookup": row["channel_lookup"],
                "title": row["title"],
                "description": row["description"],
                "published_at": row["published_at"],
                "published_local": row["published_local"],
                "duration_seconds": row["duration_seconds"],
                "duration_display": row["duration_display"],
                "view_count": row["view_count"],
                "transcript_status": row["transcript_status"],
                "transcript_language": row["transcript_language"],
                "transcript_language_code": row["transcript_language_code"],
                "transcript_is_generated": row["transcript_is_generated"],
                "transcript_is_translated": row["transcript_is_translated"],
                "transcript_error": row["transcript_error"],
                "transcript_word_count": row["transcript_word_count"],
                "transcript_char_count": row["transcript_char_count"],
                "segment_count": row["segment_count"],
                "topics": " | ".join(row["topics"]),
                "summary_points": " | ".join(row["summary_points"]),
                "transcript_text": row["transcript_text"],
            }
        )
    return buffer.getvalue().encode("utf-8")


def resolve_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")
