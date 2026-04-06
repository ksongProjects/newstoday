"""YouTube models and normalization helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

DURATION_RE = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?$"
)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_published_at(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).replace("\n", " ").replace("\r", " ").split())


def parse_duration_seconds(duration: str | None) -> int:
    if not duration:
        return 0
    match = DURATION_RE.match(duration)
    if not match:
        return 0
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return (((days * 24) + hours) * 60 + minutes) * 60 + seconds


def format_duration(seconds: int) -> str:
    seconds = max(int(seconds), 0)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def build_video_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


@dataclass(slots=True)
class ChannelTarget:
    label: str = ""
    handle: str = ""
    channel_id: str = ""
    username: str = ""

    def display_name(self) -> str:
        return self.label or self.handle or self.channel_id or self.username

    def key(self) -> str:
        return (self.channel_id or self.handle or self.username or self.label).strip()

    def matches(self, raw_value: str) -> bool:
        candidate = raw_value.strip().lower()
        return candidate in {
            self.label.strip().lower(),
            self.handle.strip().lower(),
            self.channel_id.strip().lower(),
            self.username.strip().lower(),
        }


@dataclass(slots=True)
class VideoRecord:
    video_id: str
    channel_id: str
    channel_title: str
    channel_lookup: str
    title: str
    description: str
    published_at: datetime
    duration_iso: str = ""
    duration_seconds: int = 0
    view_count: int = 0
    thumbnail_url: str = ""
    live_status: str = "none"
    transcript_status: str = "missing"
    transcript_language: str = ""
    transcript_language_code: str = ""
    transcript_is_generated: bool = False
    transcript_is_translated: bool = False
    transcript_error: str = ""
    transcript_text: str = ""
    transcript_segments: list[dict[str, Any]] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)
    collected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def url(self) -> str:
        return build_video_url(self.video_id)

    def to_record(self) -> dict[str, Any]:
        return {
            "video_id": self.video_id,
            "channel_id": self.channel_id,
            "channel_title": normalize_text(self.channel_title),
            "channel_lookup": normalize_text(self.channel_lookup),
            "title": normalize_text(self.title),
            "description": normalize_text(self.description),
            "url": self.url(),
            "published_at": ensure_utc(self.published_at).isoformat(),
            "duration_iso": self.duration_iso,
            "duration_seconds": int(self.duration_seconds),
            "view_count": int(self.view_count),
            "thumbnail_url": self.thumbnail_url,
            "live_status": self.live_status,
            "transcript_status": self.transcript_status,
            "transcript_language": self.transcript_language,
            "transcript_language_code": self.transcript_language_code,
            "transcript_is_generated": 1 if self.transcript_is_generated else 0,
            "transcript_is_translated": 1 if self.transcript_is_translated else 0,
            "transcript_error": normalize_text(self.transcript_error),
            "transcript_text": normalize_text(self.transcript_text),
            "transcript_segments": self.transcript_segments,
            "raw_payload": self.raw_payload,
            "collected_at": ensure_utc(self.collected_at).isoformat(),
        }


def video_record_from_mapping(data: Mapping[str, Any]) -> VideoRecord:
    return VideoRecord(
        video_id=str(data.get("video_id", "")),
        channel_id=str(data.get("channel_id", "")),
        channel_title=str(data.get("channel_title", "")),
        channel_lookup=str(data.get("channel_lookup", "")),
        title=str(data.get("title", "")),
        description=str(data.get("description", "")),
        published_at=parse_published_at(str(data.get("published_at", ""))),
        duration_iso=str(data.get("duration_iso", "")),
        duration_seconds=int(data.get("duration_seconds", 0) or 0),
        view_count=int(data.get("view_count", 0) or 0),
        thumbnail_url=str(data.get("thumbnail_url", "")),
        live_status=str(data.get("live_status", "none") or "none"),
        transcript_status=str(data.get("transcript_status", "pending") or "pending"),
        transcript_language=str(data.get("transcript_language", "")),
        transcript_language_code=str(data.get("transcript_language_code", "")),
        transcript_is_generated=bool(data.get("transcript_is_generated", False)),
        transcript_is_translated=bool(data.get("transcript_is_translated", False)),
        transcript_error=str(data.get("transcript_error", "")),
        transcript_text=str(data.get("transcript_text", "")),
        transcript_segments=list(data.get("transcript_segments", []) or []),
        raw_payload=dict(data.get("raw_payload", {}) or {}),
        collected_at=parse_published_at(str(data.get("collected_at", ""))) if data.get("collected_at") else datetime.now(timezone.utc),
    )
