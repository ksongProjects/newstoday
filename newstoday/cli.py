"""Command-line entrypoints for the YouTube news pipeline."""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timezone

from .defaults import DEFAULT_TRANSCRIPT_LANGUAGES
from .reporting import generate_report
from .sources import SourceError, YouTubeNewsCollector
from .storage import NewsStorage


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        return
    args.func(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect headline news from YouTube channels, transcribe videos, and build daily reports."
    )
    subparsers = parser.add_subparsers(dest="command")

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=os.getenv("NEWSTODAY_DB_PATH", "data/news.db"))
    common.add_argument("--hours", type=int, default=int(os.getenv("NEWSTODAY_DEFAULT_HOURS", "24")))
    common.add_argument(
        "--timezone",
        default=os.getenv("NEWSTODAY_TIMEZONE", "UTC"),
        help="Timezone name for report date boundaries.",
    )
    common.add_argument(
        "--channels-file",
        default=os.getenv("NEWSTODAY_CHANNELS_FILE") or None,
        help="Optional JSON file describing channels to monitor.",
    )
    common.add_argument(
        "--channel",
        action="append",
        dest="channels",
        help="Limit collection to one or more configured channels, handles, or channel IDs.",
    )
    common.add_argument(
        "--max-videos-per-channel",
        type=int,
        default=int(os.getenv("NEWSTODAY_MAX_VIDEOS_PER_CHANNEL", "10")),
        help="Maximum recent uploads to inspect per channel.",
    )
    common.add_argument(
        "--transcript-language",
        action="append",
        dest="transcript_languages",
        help="Preferred transcript language code. Repeat to set fallback order.",
    )

    collect = subparsers.add_parser("collect", parents=[common], help="Collect YouTube videos into SQLite.")
    collect.set_defaults(func=run_collect)

    report = subparsers.add_parser("report", parents=[common], help="Generate a Markdown daily report.")
    report.add_argument("--date", default=date.today().isoformat(), help="Report date in YYYY-MM-DD.")
    report.add_argument("--report-dir", default=os.getenv("NEWSTODAY_REPORT_DIR", "reports"))
    report.set_defaults(func=run_report)

    daily = subparsers.add_parser("daily", parents=[common], help="Collect and then generate a report.")
    daily.add_argument("--date", default=date.today().isoformat(), help="Report date in YYYY-MM-DD.")
    daily.add_argument("--report-dir", default=os.getenv("NEWSTODAY_REPORT_DIR", "reports"))
    daily.set_defaults(func=run_daily)
    return parser


def run_collect(args: argparse.Namespace) -> None:
    storage = NewsStorage(args.db)
    try:
        try:
            collector = YouTubeNewsCollector(
                api_key=os.getenv("YOUTUBE_API_KEY", ""),
                channels_file=args.channels_file,
                max_videos_per_channel=args.max_videos_per_channel,
                transcript_languages=resolve_transcript_languages(args.transcript_languages),
            )
        except SourceError as exc:
            raise SystemExit(str(exc)) from exc
        started_at = datetime.now(timezone.utc)
        try:
            videos = collector.collect(hours=args.hours, selected_channels=args.channels)
            stats = storage.upsert_videos(videos)
            finished_at = datetime.now(timezone.utc)
            transcript_ok = sum(1 for video in videos if video.transcript_status == "ok")
            transcript_missing = len(videos) - transcript_ok
            storage.record_run(
                source_name=collector.name,
                started_at=started_at,
                finished_at=finished_at,
                status="ok",
                fetched_count=len(videos),
                inserted_count=stats.inserted,
                updated_count=stats.updated,
                skipped_count=stats.skipped,
                message=f"transcripts_ok={transcript_ok} transcripts_missing={transcript_missing}",
            )
            print(
                f"{collector.name}: fetched={len(videos)} inserted={stats.inserted} "
                f"updated={stats.updated} skipped={stats.skipped} transcripts_ok={transcript_ok}"
            )
        except SourceError as exc:
            finished_at = datetime.now(timezone.utc)
            storage.record_run(
                source_name="youtube",
                started_at=started_at,
                finished_at=finished_at,
                status="error",
                fetched_count=0,
                inserted_count=0,
                updated_count=0,
                skipped_count=0,
                message=str(exc),
            )
            raise SystemExit(str(exc)) from exc
    finally:
        storage.close()


def run_report(args: argparse.Namespace) -> None:
    report_date = date.fromisoformat(args.date)
    storage = NewsStorage(args.db)
    try:
        videos = storage.fetch_videos_for_date(report_date, args.timezone)
        runs = storage.recent_runs()
        result = generate_report(
            videos=videos,
            runs=runs,
            report_date=report_date,
            timezone_name=args.timezone,
            output_dir=args.report_dir,
        )
        print(f"report={result.output_path} videos={result.video_count}")
    finally:
        storage.close()


def run_daily(args: argparse.Namespace) -> None:
    run_collect(args)
    run_report(args)


def resolve_transcript_languages(raw_languages: list[str] | None) -> list[str]:
    if raw_languages:
        return [value.strip() for value in raw_languages if value and value.strip()]

    env_value = os.getenv("NEWSTODAY_TRANSCRIPT_LANGUAGES", "")
    if env_value.strip():
        return [value.strip() for value in env_value.split(",") if value.strip()]
    return list(DEFAULT_TRANSCRIPT_LANGUAGES)


if __name__ == "__main__":
    main()
