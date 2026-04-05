"""Command-line entrypoints."""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timezone

from .reporting import generate_report
from .sources import SourceError, enabled_collectors
from .storage import NewsStorage


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        return
    args.func(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect economic world news and build daily reports.")
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
        "--source",
        action="append",
        dest="sources",
        help="Limit collection to one or more source names.",
    )
    common.add_argument(
        "--max-per-query",
        type=int,
        default=100,
        help="Soft per-query cap. Some APIs enforce lower hard limits.",
    )

    collect = subparsers.add_parser("collect", parents=[common], help="Collect news into SQLite.")
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
        collectors = select_collectors(args.sources, max_per_query=args.max_per_query)
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        for collector in collectors:
            started_at = datetime.now(timezone.utc)
            try:
                articles = collector.collect(hours=args.hours)
                stats = storage.upsert_articles(articles)
                finished_at = datetime.now(timezone.utc)
                storage.record_run(
                    source_name=collector.name,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="ok",
                    fetched_count=len(articles),
                    inserted_count=stats.inserted,
                    updated_count=stats.updated,
                    skipped_count=stats.skipped,
                    message="collection completed",
                )
                total_inserted += stats.inserted
                total_updated += stats.updated
                total_skipped += stats.skipped
                print(
                    f"{collector.name}: fetched={len(articles)} inserted={stats.inserted} "
                    f"updated={stats.updated} skipped={stats.skipped}"
                )
            except SourceError as exc:
                finished_at = datetime.now(timezone.utc)
                storage.record_run(
                    source_name=collector.name,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="error",
                    fetched_count=0,
                    inserted_count=0,
                    updated_count=0,
                    skipped_count=0,
                    message=str(exc),
                )
                print(f"{collector.name}: error={exc}")
        print(
            f"collection complete: inserted={total_inserted} updated={total_updated} skipped={total_skipped}"
        )
    finally:
        storage.close()


def run_report(args: argparse.Namespace) -> None:
    report_date = date.fromisoformat(args.date)
    storage = NewsStorage(args.db)
    try:
        articles = storage.fetch_articles_for_date(report_date, args.timezone)
        runs = storage.recent_runs()
        result = generate_report(
            articles=articles,
            runs=runs,
            report_date=report_date,
            timezone_name=args.timezone,
            output_dir=args.report_dir,
        )
        print(f"report={result.output_path} articles={result.article_count}")
    finally:
        storage.close()


def run_daily(args: argparse.Namespace) -> None:
    run_collect(args)
    run_report(args)


def select_collectors(selected_sources: list[str] | None, *, max_per_query: int) -> list:
    collectors = enabled_collectors(max_per_query=max_per_query)
    if not selected_sources:
        return collectors
    selected = {name.strip().lower() for name in selected_sources if name and name.strip()}
    return [collector for collector in collectors if collector.name.lower() in selected]


if __name__ == "__main__":
    main()
