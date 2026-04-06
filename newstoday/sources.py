"""YouTube channel collection and transcript fetching."""

from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    CouldNotRetrieveTranscript,
    InvalidVideoId,
    IpBlocked,
    NoTranscriptFound,
    PoTokenRequired,
    RequestBlocked,
    TranscriptsDisabled,
    TranslationLanguageNotAvailable,
    VideoUnavailable,
    YouTubeTranscriptApiException,
)

from .defaults import DEFAULT_CHANNELS, DEFAULT_TRANSCRIPT_LANGUAGES
from .models import (
    ChannelTarget,
    VideoRecord,
    normalize_text,
    parse_duration_seconds,
    parse_published_at,
)

USER_AGENT = "NewsToday/0.2 (+https://example.invalid)"


class SourceError(RuntimeError):
    """Raised when a source request fails."""


class YouTubeDataClient:
    base_url = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key.strip()
        if not self.api_key:
            raise SourceError("YOUTUBE_API_KEY is required for YouTube collection.")

    def _get(self, endpoint: str, params: dict[str, str], *, timeout: int = 30, attempts: int = 2) -> dict:
        query = dict(params)
        query["key"] = self.api_key
        url = f"{self.base_url}/{endpoint}?{urllib.parse.urlencode(query)}"
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
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
        raise SourceError(f"YouTube API request failed for {endpoint}: {last_exc}") from last_exc

    def resolve_channel(self, target: ChannelTarget) -> dict[str, Any]:
        if target.channel_id:
            payload = self._get("channels", {"part": "snippet,contentDetails,statistics", "id": target.channel_id})
            items = payload.get("items", [])
            if items:
                return items[0]
        if target.username:
            payload = self._get("channels", {"part": "snippet,contentDetails,statistics", "forUsername": target.username})
            items = payload.get("items", [])
            if items:
                return items[0]
        if target.handle:
            for handle_value in (target.handle, target.handle.lstrip("@")):
                try:
                    payload = self._get(
                        "channels",
                        {"part": "snippet,contentDetails,statistics", "forHandle": handle_value},
                        timeout=15,
                        attempts=1,
                    )
                except SourceError:
                    payload = {}
                items = payload.get("items", [])
                if items:
                    return items[0]
            resolved = self._resolve_channel_with_search(target)
            if resolved:
                return resolved
        raise SourceError(f"Unable to resolve YouTube channel target: {target.display_name()}")

    def _resolve_channel_with_search(self, target: ChannelTarget) -> dict[str, Any] | None:
        queries = [value for value in [target.handle, target.handle.lstrip("@"), target.label] if value]
        fallback: dict[str, Any] | None = None
        for query in queries:
            payload = self._get(
                "search",
                {
                    "part": "snippet",
                    "type": "channel",
                    "q": query,
                    "maxResults": "5",
                },
                timeout=15,
                attempts=1,
            )
            for item in payload.get("items", []):
                channel_id = (
                    item.get("id", {}).get("channelId")
                    or item.get("snippet", {}).get("channelId")
                    or ""
                )
                if not channel_id:
                    continue
                candidate = self._get("channels", {"part": "snippet,contentDetails,statistics", "id": channel_id})
                candidate_items = candidate.get("items", [])
                if not candidate_items:
                    continue
                channel = candidate_items[0]
                if self._channel_matches_target(channel, target):
                    return channel
                if fallback is None:
                    fallback = channel
        return fallback

    def _channel_matches_target(self, channel: dict[str, Any], target: ChannelTarget) -> bool:
        snippet = channel.get("snippet", {})
        title = normalize_text(snippet.get("title")).lower()
        custom_url = normalize_text(snippet.get("customUrl")).lower().lstrip("@")
        handle = target.handle.lower().lstrip("@")
        if target.channel_id and channel.get("id", "").lower() == target.channel_id.lower():
            return True
        if handle and custom_url and handle == custom_url:
            return True
        if target.label and title == target.label.lower():
            return True
        return False

    def fetch_recent_uploads(
        self,
        *,
        uploads_playlist_id: str,
        cutoff: datetime,
        max_videos: int,
    ) -> list[dict[str, Any]]:
        return self.fetch_uploads_window(
            uploads_playlist_id=uploads_playlist_id,
            start_at=cutoff,
            end_at=None,
            max_videos=max_videos,
        )

    def fetch_uploads_window(
        self,
        *,
        uploads_playlist_id: str,
        start_at: datetime,
        end_at: datetime | None,
        max_videos: int,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        page_token = ""
        stop_paging = False
        while len(items) < max_videos and not stop_paging:
            params = {
                "part": "snippet,contentDetails",
                "playlistId": uploads_playlist_id,
                "maxResults": "50",
            }
            if page_token:
                params["pageToken"] = page_token
            payload = self._get("playlistItems", params, timeout=20, attempts=2)
            for item in payload.get("items", []):
                content_details = item.get("contentDetails", {})
                video_id = content_details.get("videoId") or ""
                if not video_id or video_id in seen_ids:
                    continue
                published_raw = content_details.get("videoPublishedAt") or item.get("snippet", {}).get("publishedAt")
                published_at = parse_published_at(published_raw)
                if published_at < start_at:
                    stop_paging = True
                    break
                if end_at is not None and published_at >= end_at:
                    continue
                items.append(item)
                seen_ids.add(video_id)
                if len(items) >= max_videos:
                    break
            page_token = payload.get("nextPageToken", "")
            if not page_token:
                break
            time.sleep(0.1)
        return items

    def fetch_video_details(self, video_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        ids = [video_id for video_id in video_ids if video_id]
        results: dict[str, dict[str, Any]] = {}
        for index in range(0, len(ids), 50):
            chunk = ids[index : index + 50]
            payload = self._get(
                "videos",
                {
                    "part": "snippet,contentDetails,statistics,status,liveStreamingDetails",
                    "id": ",".join(chunk),
                },
                timeout=20,
                attempts=2,
            )
            for item in payload.get("items", []):
                if item.get("id"):
                    results[item["id"]] = item
            time.sleep(0.1)
        return results

    def search_channels(
        self,
        *,
        query: str,
        max_results: int = 10,
        page_token: str = "",
    ) -> dict[str, Any]:
        params = {
            "part": "snippet",
            "type": "channel",
            "q": query,
            "maxResults": str(min(max(max_results, 1), 25)),
        }
        if page_token:
            params["pageToken"] = page_token
        payload = self._get("search", params, timeout=20, attempts=2)
        channel_ids = [
            item.get("id", {}).get("channelId", "")
            for item in payload.get("items", [])
            if item.get("id", {}).get("channelId", "")
        ]
        details_map: dict[str, dict[str, Any]] = {}
        if channel_ids:
            details_payload = self._get(
                "channels",
                {
                    "part": "snippet,statistics,contentDetails",
                    "id": ",".join(channel_ids),
                },
                timeout=20,
                attempts=2,
            )
            details_map = {
                item.get("id", ""): item
                for item in details_payload.get("items", [])
                if item.get("id")
            }
        results = []
        for item in payload.get("items", []):
            channel_id = item.get("id", {}).get("channelId", "")
            detail = details_map.get(channel_id, {})
            snippet = detail.get("snippet", {}) or item.get("snippet", {})
            results.append(
                {
                    "channel_id": channel_id,
                    "title": normalize_text(snippet.get("title")),
                    "handle": "@" + normalize_text(snippet.get("customUrl", "")).lstrip("@")
                    if normalize_text(snippet.get("customUrl"))
                    else "",
                    "description": normalize_text(snippet.get("description")),
                    "thumbnail_url": (
                        snippet.get("thumbnails", {}).get("default", {}).get("url", "")
                    ),
                    "video_count": int(detail.get("statistics", {}).get("videoCount", 0) or 0),
                    "subscriber_count": int(detail.get("statistics", {}).get("subscriberCount", 0) or 0),
                }
            )
        return {
            "results": results,
            "next_page_token": payload.get("nextPageToken", ""),
            "prev_page_token": payload.get("prevPageToken", ""),
        }


class TranscriptFetcher:
    def __init__(self, languages: Iterable[str]) -> None:
        cleaned = [language.strip() for language in languages if language and language.strip()]
        self.languages = tuple(cleaned or DEFAULT_TRANSCRIPT_LANGUAGES)
        self.translation_target = self.languages[0].split("-", 1)[0]
        self.api = YouTubeTranscriptApi()

    def fetch(self, video_id: str) -> dict[str, Any]:
        try:
            transcript_list = self.api.list(video_id)
            transcript = self._pick_transcript(transcript_list)
            if transcript is None:
                return self._missing_result("No transcript found.")
            fetched = transcript.fetch(preserve_formatting=False)
            raw_segments = fetched.to_raw_data()
            text = normalize_text(" ".join(segment.get("text", "") for segment in raw_segments))
            return {
                "status": "ok" if text else "empty",
                "language": fetched.language,
                "language_code": fetched.language_code,
                "is_generated": bool(fetched.is_generated),
                "is_translated": bool(getattr(transcript, "_news_today_translated", False)),
                "error": "",
                "text": text,
                "segments": raw_segments,
            }
        except NoTranscriptFound:
            return self._missing_result("No transcript found.")
        except TranscriptsDisabled:
            return self._missing_result("Transcripts are disabled.", status="disabled")
        except VideoUnavailable:
            return self._missing_result("Video unavailable.", status="unavailable")
        except InvalidVideoId:
            return self._missing_result("Invalid video id.", status="invalid")
        except (RequestBlocked, IpBlocked, PoTokenRequired) as exc:
            return self._missing_result(str(exc), status="blocked")
        except (CouldNotRetrieveTranscript, TranslationLanguageNotAvailable, YouTubeTranscriptApiException) as exc:
            return self._missing_result(str(exc), status="error")

    def _pick_transcript(self, transcript_list: Any) -> Any | None:
        for finder_name in ("find_manually_created_transcript", "find_generated_transcript", "find_transcript"):
            finder = getattr(transcript_list, finder_name)
            try:
                transcript = finder(list(self.languages))
                setattr(transcript, "_news_today_translated", False)
                return transcript
            except NoTranscriptFound:
                continue

        for transcript in transcript_list:
            if not getattr(transcript, "is_translatable", False):
                continue
            try:
                translated = transcript.translate(self.translation_target)
                setattr(translated, "_news_today_translated", True)
                return translated
            except TranslationLanguageNotAvailable:
                continue
        return None

    def _missing_result(self, error: str, *, status: str = "missing") -> dict[str, Any]:
        return {
            "status": status,
            "language": "",
            "language_code": "",
            "is_generated": False,
            "is_translated": False,
            "error": normalize_text(error),
            "text": "",
            "segments": [],
        }


class YouTubeNewsCollector:
    name = "youtube"

    def __init__(
        self,
        *,
        api_key: str,
        channels_file: str | None = None,
        targets: list[ChannelTarget] | None = None,
        max_videos_per_channel: int = 10,
        transcript_languages: Iterable[str] = DEFAULT_TRANSCRIPT_LANGUAGES,
    ) -> None:
        self.client = YouTubeDataClient(api_key)
        self.transcripts = TranscriptFetcher(transcript_languages)
        self.max_videos_per_channel = max(1, int(max_videos_per_channel))
        self.targets = dedupe_channel_targets(targets or load_channel_targets(channels_file))

    def collect(self, *, hours: int, selected_channels: list[str] | None = None) -> list[VideoRecord]:
        videos = self.collect_metadata(hours=hours, selected_channels=selected_channels)
        return self.enrich_transcripts(videos)

    def collect_metadata(
        self,
        *,
        hours: int | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        selected_channels: list[str] | None = None,
    ) -> list[VideoRecord]:
        now = datetime.now(timezone.utc)
        range_start = start_at.astimezone(timezone.utc) if start_at is not None else now - timedelta(hours=int(hours or 24))
        range_end = end_at.astimezone(timezone.utc) if end_at is not None else None
        if range_end is not None and range_end <= range_start:
            raise SourceError("Video range end must be after the start.")
        targets = select_channel_targets(self.targets, selected_channels)
        videos: list[VideoRecord] = []
        failures: list[str] = []

        for target in targets:
            try:
                channel = self.client.resolve_channel(target)
                uploads_playlist = (
                    channel.get("contentDetails", {})
                    .get("relatedPlaylists", {})
                    .get("uploads", "")
                )
                if not uploads_playlist:
                    raise SourceError(f"No uploads playlist found for {target.display_name()}.")
                playlist_items = self.client.fetch_uploads_window(
                    uploads_playlist_id=uploads_playlist,
                    start_at=range_start,
                    end_at=range_end,
                    max_videos=self.max_videos_per_channel,
                )
                details_map = self.client.fetch_video_details(
                    item.get("contentDetails", {}).get("videoId", "") for item in playlist_items
                )
                for playlist_item in playlist_items:
                    video_id = playlist_item.get("contentDetails", {}).get("videoId", "")
                    details = details_map.get(video_id)
                    if not details:
                        continue
                    record = self._build_video_record(
                        target=target,
                        channel=channel,
                        playlist_item=playlist_item,
                        details=details,
                        fetch_transcript=False,
                    )
                    if record is not None:
                        videos.append(record)
            except SourceError as exc:
                failures.append(str(exc))
                continue

        if not videos and failures:
            raise SourceError(failures[0])
        return videos

    def enrich_transcripts(
        self,
        videos: Iterable[VideoRecord],
        *,
        selected_video_ids: list[str] | None = None,
    ) -> list[VideoRecord]:
        selected = {video_id.strip() for video_id in (selected_video_ids or []) if video_id and video_id.strip()}
        enriched: list[VideoRecord] = []
        for video in videos:
            if selected and video.video_id not in selected:
                enriched.append(video)
                continue
            transcript_result = self.transcripts.fetch(video.video_id)
            enriched.append(
                VideoRecord(
                    video_id=video.video_id,
                    channel_id=video.channel_id,
                    channel_title=video.channel_title,
                    channel_lookup=video.channel_lookup,
                    title=video.title,
                    description=video.description,
                    published_at=video.published_at,
                    duration_iso=video.duration_iso,
                    duration_seconds=video.duration_seconds,
                    view_count=video.view_count,
                    thumbnail_url=video.thumbnail_url,
                    live_status=video.live_status,
                    transcript_status=transcript_result["status"],
                    transcript_language=transcript_result["language"],
                    transcript_language_code=transcript_result["language_code"],
                    transcript_is_generated=bool(transcript_result["is_generated"]),
                    transcript_is_translated=bool(transcript_result["is_translated"]),
                    transcript_error=transcript_result["error"],
                    transcript_text=transcript_result["text"],
                    transcript_segments=transcript_result["segments"],
                    raw_payload=video.raw_payload,
                    collected_at=datetime.now(timezone.utc),
                )
            )
        return enriched

    def _build_video_record(
        self,
        *,
        target: ChannelTarget,
        channel: dict[str, Any],
        playlist_item: dict[str, Any],
        details: dict[str, Any],
        fetch_transcript: bool,
    ) -> VideoRecord | None:
        snippet = details.get("snippet", {})
        status = details.get("status", {})
        content_details = details.get("contentDetails", {})
        live_status = normalize_text(snippet.get("liveBroadcastContent") or "none").lower() or "none"
        if live_status in {"live", "upcoming"}:
            return None
        if normalize_text(status.get("privacyStatus")).lower() not in {"", "public"}:
            return None

        transcript_result = (
            self.transcripts.fetch(details.get("id", ""))
            if fetch_transcript
            else {
                "status": "pending",
                "language": "",
                "language_code": "",
                "is_generated": False,
                "is_translated": False,
                "error": "",
                "text": "",
                "segments": [],
            }
        )
        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = (
            thumbnails.get("maxres", {}).get("url")
            or thumbnails.get("high", {}).get("url")
            or thumbnails.get("medium", {}).get("url")
            or thumbnails.get("default", {}).get("url")
            or ""
        )

        return VideoRecord(
            video_id=details.get("id", ""),
            channel_id=channel.get("id", ""),
            channel_title=snippet.get("channelTitle") or channel.get("snippet", {}).get("title", ""),
            channel_lookup=target.key(),
            title=snippet.get("title", ""),
            description=snippet.get("description", ""),
            published_at=parse_published_at(snippet.get("publishedAt")),
            duration_iso=content_details.get("duration", ""),
            duration_seconds=parse_duration_seconds(content_details.get("duration")),
            view_count=int(details.get("statistics", {}).get("viewCount", 0) or 0),
            thumbnail_url=thumbnail_url,
            live_status=live_status,
            transcript_status=transcript_result["status"],
            transcript_language=transcript_result["language"],
            transcript_language_code=transcript_result["language_code"],
            transcript_is_generated=bool(transcript_result["is_generated"]),
            transcript_is_translated=bool(transcript_result["is_translated"]),
            transcript_error=transcript_result["error"],
            transcript_text=transcript_result["text"],
            transcript_segments=transcript_result["segments"],
            raw_payload={
                "target": {
                    "label": target.label,
                    "handle": target.handle,
                    "channel_id": target.channel_id,
                    "username": target.username,
                },
                "channel": channel,
                "playlist_item": playlist_item,
                "video": details,
            },
        )


def load_channel_targets(channels_file: str | None) -> list[ChannelTarget]:
    if not channels_file:
        return [ChannelTarget(**item) for item in DEFAULT_CHANNELS]

    path = Path(channels_file)
    if not path.exists():
        raise SourceError(f"Channels file not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise SourceError("Channels file must contain a JSON list.")

    targets: list[ChannelTarget] = []
    for item in payload:
        if isinstance(item, str):
            targets.append(channel_target_from_value(item))
            continue
        if isinstance(item, dict):
            targets.append(
                ChannelTarget(
                    label=normalize_text(item.get("label")),
                    handle=normalize_text(item.get("handle")),
                    channel_id=normalize_text(item.get("channel_id")),
                    username=normalize_text(item.get("username")),
                )
            )
            continue
        raise SourceError("Each channel entry must be a string or object.")
    return dedupe_channel_targets(targets)


def dedupe_channel_targets(targets: Iterable[ChannelTarget]) -> list[ChannelTarget]:
    deduped: list[ChannelTarget] = []
    seen: set[str] = set()
    for target in targets:
        key = target.key().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(target)
    return deduped


def select_channel_targets(targets: list[ChannelTarget], selected_channels: list[str] | None) -> list[ChannelTarget]:
    if not selected_channels:
        return targets

    chosen: list[ChannelTarget] = []
    remaining = [value for value in selected_channels if value and value.strip()]
    for target in targets:
        for raw_value in list(remaining):
            if target.matches(raw_value):
                chosen.append(target)
                remaining.remove(raw_value)
                break

    for raw_value in remaining:
        chosen.append(channel_target_from_value(raw_value))
    return dedupe_channel_targets(chosen)


def channel_target_from_value(value: str) -> ChannelTarget:
    raw = value.strip()
    lowered = raw.lower()
    if "/channel/" in lowered:
        channel_id = raw.split("/channel/", 1)[1].split("/", 1)[0]
        return ChannelTarget(channel_id=channel_id)
    if "/@" in raw:
        handle = "@" + raw.split("/@", 1)[1].split("/", 1)[0]
        return ChannelTarget(handle=handle)
    if raw.startswith("@"):
        return ChannelTarget(handle=raw)
    if raw.startswith("UC"):
        return ChannelTarget(channel_id=raw)
    return ChannelTarget(label=raw)
