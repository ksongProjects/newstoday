# NewsToday

NewsToday is a YouTube-first headline news collector. It monitors curated news channels, pulls recent uploads with the YouTube Data API, fetches transcripts with `youtube-transcript-api`, stores the results in SQLite, and generates a daily Markdown report centered on transcript-backed summaries.

It also includes a dense dark Streamlit UI for:

- entering the YouTube API key for the current session
- searching YouTube channels and adding them directly to the watchlist
- editing, enabling, and removing channels directly in the UI
- loading recent uploads without immediately transcribing everything
- selecting only the videos you want to transcribe
- reviewing summaries and full transcripts in-app
- exporting selected or filtered transcript results as JSON or CSV

## What It Does

- Collects recent uploads from known YouTube news channels
- Fetches video metadata like title, publish time, duration, and view count
- Pulls transcripts when captions are available
- Builds transcript-based summary bullets for headline videos
- Generates a daily report with a dedicated video-news section, topic watch, and transcript gap tracking

## Requirements

- Python 3.10+
- A YouTube Data API key in `YOUTUBE_API_KEY`

Install dependencies:

```powershell
pip install -e .
```

## Launch The UI

```powershell
python -m streamlit run newstoday/ui.py
```

The UI is intentionally dark, compact, and data-dense so you can keep more channels, videos, and transcript detail on screen at once.

## Quick Start

```powershell
python -m newstoday.cli collect
python -m newstoday.cli report
python -m newstoday.cli daily
```

### Example

```powershell
$env:YOUTUBE_API_KEY="your-key"
python -m newstoday.cli daily --hours 24 --timezone America/Vancouver
```

## Channel Configuration

The Streamlit UI manages the watchlist directly in session state, so you can search YouTube and add or remove channels without creating a channels file.

If you do not provide a channels file to the CLI, NewsToday uses a small built-in starter list of business and world-news channels.

If you want explicit channel control for CLI runs, copy `channels.example.json` to `channels.json`, edit it, and point `NEWSTODAY_CHANNELS_FILE` at it or pass `--channels-file channels.json`.

Each entry can be:

- A handle string like `"@Reuters"`
- A channel id like `"UC..."` 
- An object such as `{"label": "Reuters", "handle": "@Reuters"}`

## CLI

```powershell
python -m newstoday.cli collect --hours 24 --max-videos-per-channel 12
python -m newstoday.cli collect --channel @Reuters --channel @BloombergTelevision
python -m newstoday.cli report --date 2026-04-05
python -m newstoday.cli daily --channels-file channels.json
```

### Commands

- `collect`: poll channels, fetch transcripts, and upsert videos into SQLite
- `report`: generate a Markdown report from stored videos
- `daily`: run collection and then generate the report

### Useful Options

- `--db`: override SQLite path
- `--report-dir`: override output directory
- `--hours`: lookback window for collection
- `--channel`: limit collection to specific handles, labels, or channel IDs
- `--channels-file`: JSON file with curated channels
- `--max-videos-per-channel`: cap uploads per channel per run
- `--transcript-language`: set transcript language priority order

## Output

- `data/news.db`: SQLite database with videos and collection runs
- `reports/daily-video-news-YYYY-MM-DD.md`: daily YouTube news report

## Report Structure

The daily report includes:

- Collector status
- Daily video news
- Topic watch
- Transcript coverage
- Transcript gaps
- Channel mix
- Trending terms

## Notes

- Upload playlist polling is much cheaper than broad YouTube search for routine collection.
- Transcript availability is inconsistent. Some videos will be missing captions, delayed, or blocked.
- `youtube-transcript-api` uses YouTube transcript endpoints rather than the Data API caption download flow, which makes it practical for third-party news channels but also less predictable than official metadata endpoints.

## References

- [YouTube Data API channels.list](https://developers.google.com/youtube/v3/docs/channels/list)
- [YouTube Data API playlistItems.list](https://developers.google.com/youtube/v3/docs/playlistItems/list)
- [YouTube Data API videos.list](https://developers.google.com/youtube/v3/docs/videos/list)
- [YouTube quota costs](https://developers.google.com/youtube/v3/determine_quota_cost)
- [youtube-transcript-api on PyPI](https://pypi.org/project/youtube-transcript-api/)
