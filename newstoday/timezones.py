"""Shared timezone helpers for compact UTC offset selection."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UTC_OFFSET_RE = re.compile(r"^UTC(?P<sign>[+-])(?P<hours>\d{2})(?::?(?P<minutes>\d{2}))?$")
TIMEZONE_OPTIONS = tuple(
    [f"UTC{offset:+03d}" for offset in range(-12, 15)]
)


def resolve_timezone(timezone_name: str) -> tzinfo:
    normalized = normalize_timezone_name(timezone_name)
    match = UTC_OFFSET_RE.match(normalized)
    if not match:
        return timezone.utc
    sign = -1 if match.group("sign") == "-" else 1
    hours = int(match.group("hours"))
    minutes = int(match.group("minutes") or 0)
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def normalize_timezone_name(timezone_name: str) -> str:
    cleaned = str(timezone_name or "").strip()
    if cleaned in TIMEZONE_OPTIONS:
        return cleaned
    if cleaned.upper() == "UTC":
        return "UTC+00"

    compact = _normalize_utc_offset_label(cleaned)
    if compact in TIMEZONE_OPTIONS:
        return compact

    try:
        zone = ZoneInfo(cleaned)
    except ZoneInfoNotFoundError:
        return "UTC+00"
    return _offset_label_from_zone(zone)


def _normalize_utc_offset_label(value: str) -> str:
    raw = value.strip().upper()
    if raw.startswith("UTC"):
        raw = raw[3:]
    if not raw:
        return "UTC+00"
    if raw[0] not in "+-":
        return "UTC+00"
    sign = raw[0]
    number = raw[1:]
    if ":" in number:
        try:
            hours_text, minutes_text = number.split(":", 1)
            hours = int(hours_text)
            minutes = int(minutes_text)
        except ValueError:
            return "UTC+00"
        if minutes != 0:
            return "UTC+00"
        return f"UTC{sign}{hours:02d}"
    try:
        hours = int(number)
    except ValueError:
        return "UTC+00"
    return f"UTC{sign}{hours:02d}"


def _offset_label_from_zone(zone: ZoneInfo) -> str:
    offset = datetime.now(timezone.utc).astimezone(zone).utcoffset()
    if offset is None:
        return "UTC+00"
    total_seconds = int(offset.total_seconds())
    if total_seconds % 3600 != 0:
        return "UTC+00"
    hours = total_seconds // 3600
    if hours < -12 or hours > 14:
        return "UTC+00"
    return f"UTC{hours:+03d}"
