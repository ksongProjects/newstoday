# NewsToday

NewsToday is a lightweight Python collector for economic world news. It is built to pull from zero-cost sources by default and layer in additional free-tier APIs when you add API keys.

Out of the box it collects from:

- Google News RSS query feeds
- GDELT DOC 2.0 article search on a best-effort basis

Optional free-tier adapters are included for:

- GNews
- NewsData.io
- Currents
- Alpha Vantage News & Sentiment

The collector stores normalized articles in SQLite, deduplicates them across sources and queries, and generates a daily Markdown report with headline coverage, topic buckets, trending terms, and source mix.

## Quick Start

```powershell
python -m newstoday.cli collect
python -m newstoday.cli report
python -m newstoday.cli daily
```

To use optional keyed providers, set environment variables from `.env.example` first.

## What Gets Built

- `data/news.db`: SQLite database with articles and collection runs
- `reports/daily-news-YYYY-MM-DD.md`: daily report in Markdown

## CLI

```powershell
python -m newstoday.cli collect --hours 24
python -m newstoday.cli report --date 2026-04-05
python -m newstoday.cli daily --hours 24
python -m newstoday.cli collect --source google_news_rss --source gdelt
```

### Commands

- `collect`: pull articles from enabled sources into SQLite
- `report`: build a report from articles already stored in SQLite
- `daily`: run `collect` and then `report`

### Useful Options

- `--db`: override SQLite path
- `--report-dir`: override output directory
- `--hours`: lookback window for collection
- `--date`: target report date in `YYYY-MM-DD`
- `--timezone`: report timezone, defaults to `NEWSTODAY_TIMEZONE` or `UTC`
- `--source`: limit collection to one or more named sources

## Source Strategy

The default profile is designed to maximize coverage while staying free:

1. Google News RSS search feeds fan out across multiple economic search queries and return up to 100 items per feed.
2. GDELT adds broad international article coverage across many publishers and languages.
3. Optional keyed sources add more JSON-native feeds and larger daily quotas if you want extra volume.

Because the same story can appear through multiple searches and providers, NewsToday deduplicates articles using canonical URLs plus title and source fingerprints.

## Environment Variables

```text
NEWSTODAY_DB_PATH
NEWSTODAY_REPORT_DIR
NEWSTODAY_TIMEZONE
NEWSTODAY_DEFAULT_HOURS
GNEWS_API_KEY
NEWSDATA_API_KEY
CURRENTS_API_KEY
ALPHAVANTAGE_API_KEY
```

## Scheduling

On Windows Task Scheduler, you can run a daily job such as:

```powershell
python -m newstoday.cli daily --hours 24
```

## Notes

- Google News RSS links are sometimes redirect URLs; the report still links correctly.
- GDELT works more reliably over `http` than `https` in some environments, so the adapter uses the `http` endpoint intentionally.
- GDELT can be intermittent, so the collector treats it as supplemental and fails fast instead of blocking the whole daily run.
- Free-tier APIs often delay some articles by around 12 hours. The zero-key sources help fill that gap.

## Free API References

- [GDELT DOC 2.0 API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/amp/)
- [GNews Pricing](https://gnews.io/pricing)
- [Currents Documentation](https://currentsapi.services/en/docs/)
- [Alpha Vantage Documentation](https://www.alphavantage.co/documentation/)
- [NewsData.io Latest Endpoint](https://newsdata.io/blog/latest-news-endpoint/)
- [NewsData.io Pricing](https://newsdata.io/blog/pricing-plan-in-newsdata-io/)
