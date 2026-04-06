"""Gemini-backed transcript summarization helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping
from urllib import error, request

from .models import normalize_text

GEMINI_SUMMARY_MODEL = "gemini-2.5-flash"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GEMINI_SUMMARY_MAX_POINTS = 24

GEMINI_SYSTEM_INSTRUCTION = (
    "You are a professional financial journalist and macroeconomics analyst. "
    "Read the provided YouTube transcript and extract only the highest-signal, factual points that matter "
    "for world economics, finance, markets, monetary policy, trade, commodities, companies, and stocks. "
    "Prioritize central bank actions, inflation, rates, jobs, growth, fiscal policy, trade, earnings, "
    "sector moves, market reactions, named companies, and specific numbers. Ignore greetings, sponsorship "
    "copy, scene-setting filler, and generic banter. Do not speculate and do not add facts not supported "
    "by the transcript."
)

GEMINI_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary_points": {
            "type": "array",
            "description": (
                "Ordered newsroom-ready key points focused on economics, finance, markets, policy, "
                "commodities, companies, and stocks. Include all materially important facts from the transcript."
            ),
            "minItems": 1,
            "maxItems": GEMINI_SUMMARY_MAX_POINTS,
            "items": {
                "type": "string",
                "description": (
                    "One concise, factual key point grounded in the transcript. Mention concrete numbers, "
                    "entities, indices, rates, or market moves when they appear."
                ),
            },
        }
    },
    "required": ["summary_points"],
    "additionalProperties": False,
}


class GeminiSummaryError(RuntimeError):
    """Raised when Gemini summarization fails."""


@dataclass(slots=True)
class GeminiSummaryResult:
    summary_points: list[str]
    model: str


def summarize_transcript(
    video: Mapping[str, Any],
    *,
    api_key: str,
    model: str = GEMINI_SUMMARY_MODEL,
    timeout_seconds: int = 45,
) -> GeminiSummaryResult:
    """Generate economics-focused summary points for a transcript."""

    if not api_key.strip():
        raise GeminiSummaryError("Gemini API key is missing.")

    transcript_text = normalize_text(str(video.get("transcript_text", "") or ""))
    if not transcript_text:
        raise GeminiSummaryError("Transcript text is empty.")

    payload = {
        "system_instruction": {"parts": [{"text": GEMINI_SYSTEM_INSTRUCTION}]},
        "contents": [{"parts": [{"text": build_summary_prompt(video, transcript_text=transcript_text)}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseJsonSchema": GEMINI_RESPONSE_SCHEMA,
        },
    }
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    api_request = request.Request(
        GEMINI_API_URL.format(model=model),
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key.strip(),
        },
        method="POST",
    )

    try:
        with request.urlopen(api_request, timeout=timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        raise GeminiSummaryError(_http_error_message(exc)) from exc
    except error.URLError as exc:
        reason = exc.reason if getattr(exc, "reason", None) else "network error"
        raise GeminiSummaryError(f"Gemini request failed: {reason}") from exc
    except json.JSONDecodeError as exc:
        raise GeminiSummaryError("Gemini returned an unreadable response.") from exc

    text = _response_text(response_payload)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise GeminiSummaryError("Gemini did not return valid JSON summary data.") from exc

    summary_points = sanitize_summary_points(parsed.get("summary_points", []))
    if not summary_points:
        raise GeminiSummaryError("Gemini returned no usable summary points.")
    return GeminiSummaryResult(summary_points=summary_points, model=model)


def build_summary_prompt(video: Mapping[str, Any], *, transcript_text: str) -> str:
    title = normalize_text(str(video.get("title", "") or ""))
    description = normalize_text(str(video.get("description", "") or ""))
    channel_title = normalize_text(str(video.get("channel_title", "") or ""))
    published_at = normalize_text(str(video.get("published_at", "") or ""))
    transcript_language = normalize_text(str(video.get("transcript_language_code", "") or ""))

    return "\n".join(
        [
            "Summarize this YouTube news transcript for a professional economics and markets desk.",
            "Return all materially important facts that appear in the transcript, ordered from most important to least important.",
            "Do not stop at three points. Keep going until you have covered the full set of economically or financially meaningful facts.",
            "If the transcript spans multiple stories, keep the bullets focused on the economically or financially material ones.",
            "Split distinct facts into separate bullets when that improves clarity.",
            "",
            f"Channel: {channel_title or 'Unknown'}",
            f"Title: {title or 'Untitled'}",
            f"Published: {published_at or 'Unknown'}",
            f"Transcript language: {transcript_language or 'Unknown'}",
            f"Description: {description or 'None'}",
            "",
            "Transcript:",
            transcript_text,
        ]
    ).strip()


def sanitize_summary_points(raw_points: Any, *, limit: int = GEMINI_SUMMARY_MAX_POINTS) -> list[str]:
    if not isinstance(raw_points, list):
        return []

    points: list[str] = []
    seen: set[str] = set()
    for item in raw_points:
        text = normalize_text(str(item or ""))
        if not text:
            continue
        normalized_key = text.lower()
        if normalized_key in seen:
            continue
        if text[-1] not in ".!?":
            text += "."
        seen.add(normalized_key)
        points.append(text)
        if len(points) >= limit:
            break
    return points


def _response_text(payload: Mapping[str, Any]) -> str:
    candidates = payload.get("candidates") or []
    for candidate in candidates:
        content = candidate.get("content") or {}
        parts = content.get("parts") or []
        text = "".join(
            normalize_text(str(part.get("text", "")))
            for part in parts
            if isinstance(part, Mapping) and part.get("text")
        ).strip()
        if text:
            return text

    prompt_feedback = payload.get("promptFeedback") or {}
    block_reason = normalize_text(str(prompt_feedback.get("blockReason", "") or ""))
    if block_reason:
        raise GeminiSummaryError(f"Gemini blocked the request: {block_reason.lower()}.")
    raise GeminiSummaryError("Gemini returned no summary content.")


def _http_error_message(exc: error.HTTPError) -> str:
    body = exc.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        payload = {}
    message = normalize_text(str((payload.get("error") or {}).get("message", "") or ""))
    if message:
        return f"Gemini request failed: {message}"
    return f"Gemini request failed with HTTP {exc.code}."
