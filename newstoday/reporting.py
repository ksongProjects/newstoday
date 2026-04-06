"""Transcript-based report generation for YouTube news videos."""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .defaults import HEADLINE_KEYWORDS, NOISE_SNIPPETS, PROMO_PHRASES, STOPWORDS, TOPIC_KEYWORDS
from .models import format_duration, normalize_text

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9'-]{2,}")
SENTENCE_BREAK_RE = re.compile(r"(?<=[.!?])\s+")


@dataclass(slots=True)
class ReportResult:
    output_path: Path
    video_count: int


def generate_report(
    *,
    videos: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    report_date: date,
    timezone_name: str,
    output_dir: str | Path,
) -> ReportResult:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"daily-video-news-{report_date.isoformat()}.md"
    markdown = render_report(
        videos=videos,
        runs=runs,
        report_date=report_date,
        timezone_name=timezone_name,
    )
    output_path.write_text(markdown, encoding="utf-8")
    return ReportResult(output_path=output_path, video_count=len(videos))


def render_report(
    *,
    videos: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    report_date: date,
    timezone_name: str,
) -> str:
    zone = ZoneInfo(timezone_name)
    generated_at = datetime.now(zone).strftime("%Y-%m-%d %H:%M %Z")
    annotated_videos = [annotate_video(video) for video in videos]
    transcript_ready = [video for video in annotated_videos if video["transcript_status"] == "ok"]
    top_videos = pick_top_videos(annotated_videos, limit=10)
    topic_groups = group_by_topic(transcript_ready)
    trending_terms = top_terms(transcript_ready, limit=15)
    channel_counts = Counter(video["channel_title"] for video in annotated_videos)
    transcript_status_counts = Counter(video["transcript_status"] for video in annotated_videos)
    transcript_gaps = [video for video in annotated_videos if video["transcript_status"] != "ok"][:12]

    lines = [
        f"# Daily YouTube Video News - {report_date.isoformat()}",
        "",
        f"Generated: {generated_at}",
        f"Videos in report window: {len(annotated_videos)}",
        f"Transcript-backed videos: {len(transcript_ready)}",
        f"Channels covered: {len(channel_counts)}",
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

    lines.extend(["", "## Daily Video News"])
    if top_videos:
        for video in top_videos:
            lines.extend(render_video_block(video, timezone_name))
    else:
        lines.append("- No transcript-backed videos found for this date.")

    lines.extend(["", "## Topic Watch"])
    if topic_groups:
        for topic_name, topic_videos in topic_groups.items():
            lines.append(f"### {topic_name} ({len(topic_videos)})")
            for video in topic_videos[:6]:
                summary = video["summary_points"][0] if video["summary_points"] else "Transcript collected."
                lines.append(f"- [{video['title']}]({video['url']}) | {video['channel_title']} | {summary}")
            lines.append("")
    else:
        lines.append("- No transcript topic clusters were detected.")
        lines.append("")

    lines.append("## Transcript Coverage")
    if transcript_status_counts:
        for status, count in transcript_status_counts.most_common():
            lines.append(f"- {status}: {count}")
    else:
        lines.append("- No videos were collected.")

    lines.extend(["", "## Transcript Gaps"])
    if transcript_gaps:
        for video in transcript_gaps:
            lines.append(
                f"- [{video['title']}]({video['url']}) | {video['channel_title']} | "
                f"{video['transcript_status']}: {video['transcript_error'] or 'transcript unavailable'}"
            )
    else:
        lines.append("- No transcript gaps in this report window.")

    lines.extend(["", "## Channel Mix"])
    if channel_counts:
        for channel_title, count in channel_counts.most_common(15):
            lines.append(f"- {channel_title}: {count}")
    else:
        lines.append("- No channels found.")

    lines.extend(["", "## Trending Terms"])
    if trending_terms:
        lines.append("- " + ", ".join(f"{term} ({count})" for term, count in trending_terms))
    else:
        lines.append("- No recurring transcript terms detected.")

    return "\n".join(lines).rstrip() + "\n"


def render_video_block(video: dict[str, Any], timezone_name: str) -> list[str]:
    zone = ZoneInfo(timezone_name)
    published = datetime.fromisoformat(video["published_at"]).astimezone(zone).strftime("%H:%M")
    transcript_label = "generated" if video["transcript_is_generated"] else "manual"
    if video["transcript_is_translated"]:
        transcript_label += ", translated"
    summary_points = video["summary_points"] or ["Transcript collected, but no high-signal summary lines were extracted."]
    topics = ", ".join(video["topic_labels"]) if video["topic_labels"] else "General news"
    lines = [
        f"### [{video['title']}]({video['url']})",
        (
            f"Channel: {video['channel_title']} | Published: {published} | Duration: "
            f"{format_duration(video['duration_seconds'])} | Views: {video['view_count']:,} | "
            f"Transcript: {transcript_label} ({video['transcript_language_code'] or 'n/a'})"
        ),
    ]
    for point in summary_points:
        lines.append(f"- {point}")
    lines.append(f"- Key topics: {topics}")
    lines.append("")
    return lines


def annotate_video(video: dict[str, Any]) -> dict[str, Any]:
    annotated = dict(video)
    analysis_text = build_analysis_text(video)
    annotated["topic_labels"] = classify_topics(analysis_text)
    annotated["summary_points"] = build_summary_points(video)
    annotated["relevance_score"] = relevance_score(annotated)
    return annotated


def build_analysis_text(video: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in [
            normalize_text(video.get("title")),
            normalize_text(video.get("description")),
            normalize_text(video.get("transcript_text")),
        ]
        if part
    )


def build_summary_points(video: dict[str, Any], *, limit: int = 3) -> list[str]:
    segments = candidate_segments(video)
    if not segments:
        return []

    title_terms = {term for term, _ in extract_terms(f"{video.get('title', '')} {video.get('description', '')}", limit=8)}
    scored_segments: list[tuple[int, int, str]] = []
    for index, segment in enumerate(segments):
        score = score_segment(segment, title_terms)
        if score > 0:
            scored_segments.append((score, index, segment))

    if not scored_segments:
        return segments[:limit]

    chosen = sorted(scored_segments, key=lambda item: (-item[0], item[1]))[:limit]
    chosen.sort(key=lambda item: item[1])
    return [segment for _, _, segment in chosen]


def candidate_segments(video: dict[str, Any]) -> list[str]:
    raw_segments = video.get("transcript_segments") or []
    if raw_segments:
        grouped: list[str] = []
        current_parts: list[str] = []
        current_length = 0
        for raw_segment in raw_segments:
            text = clean_segment_text(raw_segment.get("text", ""))
            if not text:
                continue
            current_parts.append(text)
            current_length += len(text)
            if current_length >= 180 or text.endswith((".", "!", "?")):
                grouped.append(normalize_text(" ".join(current_parts)))
                current_parts = []
                current_length = 0
        if current_parts:
            grouped.append(normalize_text(" ".join(current_parts)))
        return dedupe_segments(grouped)

    transcript_text = normalize_text(video.get("transcript_text"))
    if not transcript_text:
        return []
    return dedupe_segments(
        [
            normalize_text(piece)
            for piece in SENTENCE_BREAK_RE.split(transcript_text)
            if normalize_text(piece)
        ]
    )


def clean_segment_text(text: str) -> str:
    normalized = normalize_text(text)
    if not normalized:
        return ""
    lowered = normalized.lower()
    if lowered in NOISE_SNIPPETS:
        return ""
    if lowered.startswith("[") and lowered.endswith("]") and len(lowered) <= 24:
        return ""
    return normalized


def dedupe_segments(segments: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for segment in segments:
        lowered = segment.lower()
        if lowered in seen or len(segment) < 40:
            continue
        seen.add(lowered)
        deduped.append(segment)
    return deduped


def score_segment(segment: str, title_terms: set[str]) -> int:
    lowered = segment.lower()
    if any(phrase in lowered for phrase in PROMO_PHRASES):
        return -1
    score = 0
    score += sum(2 for keyword in HEADLINE_KEYWORDS if matches_keyword(lowered, keyword))
    score += sum(1 for term in title_terms if term and matches_keyword(lowered, term))
    if 60 <= len(segment) <= 280:
        score += 2
    if any(char.isdigit() for char in segment):
        score += 1
    return score


def pick_top_videos(videos: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    chosen: list[dict[str, Any]] = []
    seen_channels: Counter[str] = Counter()
    seen_titles: set[str] = set()
    ranked = sorted(
        videos,
        key=lambda item: (
            item["transcript_status"] == "ok",
            item["relevance_score"],
            item["published_at"],
        ),
        reverse=True,
    )
    for video in ranked:
        title_key = video["title"].strip().lower()
        if title_key in seen_titles:
            continue
        if video["transcript_status"] != "ok":
            continue
        if seen_channels[video["channel_title"]] >= 2:
            continue
        chosen.append(video)
        seen_titles.add(title_key)
        seen_channels[video["channel_title"]] += 1
        if len(chosen) >= limit:
            break
    return chosen


def group_by_topic(videos: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for video in videos:
        for topic_name in video.get("topic_labels", []):
            grouped[topic_name].append(video)
    return dict(grouped)


def classify_topics(text: str) -> list[str]:
    lowered = text.lower()
    labels: list[str] = []
    for topic_name, keywords in TOPIC_KEYWORDS.items():
        if any(matches_keyword(lowered, keyword) for keyword in keywords):
            labels.append(topic_name)
    return labels


def top_terms(videos: list[dict[str, Any]], *, limit: int = 12) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for video in videos:
        text = " ".join(
            [
                normalize_text(video.get("title")),
                normalize_text(video.get("description")),
                " ".join(video.get("summary_points", [])),
            ]
        )
        for token in TOKEN_RE.findall(text):
            term = token.lower()
            if term in STOPWORDS or term.isdigit():
                continue
            counter[term] += 1
    return counter.most_common(limit)


def extract_terms(text: str, *, limit: int) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for token in TOKEN_RE.findall(text):
        term = token.lower()
        if term in STOPWORDS or term.isdigit():
            continue
        counter[term] += 1
    return counter.most_common(limit)


def relevance_score(video: dict[str, Any]) -> int:
    analysis_text = build_analysis_text(video).lower()
    keyword_matches = sum(1 for keyword in HEADLINE_KEYWORDS if matches_keyword(analysis_text, keyword))
    topic_bonus = len(video.get("topic_labels", [])) * 3
    transcript_bonus = 10 if video.get("transcript_status") == "ok" else 0
    duration_bonus = 1 if int(video.get("duration_seconds", 0)) >= 60 else 0
    view_bonus = 0
    if int(video.get("view_count", 0)) > 0:
        view_bonus = min(5, int(math.log10(max(int(video["view_count"]), 1))))
    return transcript_bonus + topic_bonus + keyword_matches + duration_bonus + view_bonus


def matches_keyword(text: str, keyword: str) -> bool:
    pattern = r"\b" + re.escape(keyword.lower()).replace(r"\ ", r"\s+") + r"\b"
    return re.search(pattern, text) is not None
