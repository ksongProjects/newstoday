"""Dense dark Streamlit UI for YouTube news collection and transcription."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import math
import os
from typing import Any

import pandas as pd
import streamlit as st

from newstoday.ai import GEMINI_SUMMARY_MODEL, GeminiSummaryError, summarize_transcript
from newstoday.defaults import DEFAULT_CHANNELS, DEFAULT_TRANSCRIPT_LANGUAGES
from newstoday.exporting import export_transcripts_csv, export_transcripts_json
from newstoday.models import ChannelTarget, VideoRecord, format_duration, video_record_from_mapping
from newstoday.reporting import classify_topics, summary_points_for_video, summary_source_for_video
from newstoday.sources import (
    SourceError,
    TranscriptFetcher,
    YouTubeDataClient,
    YouTubeNewsCollector,
)
from newstoday.storage import NewsStorage
from newstoday.timezones import TIMEZONE_OPTIONS, normalize_timezone_name, resolve_timezone

CHANNEL_COLUMNS = ["selected", "enabled", "label", "handle", "channel_id", "username"]
TRANSCRIPT_TABLE_COLUMNS = ["published", "channel", "title", "lang", "topics", "summary", "video_id"]
LANGUAGE_OPTIONS = {"English": list(DEFAULT_TRANSCRIPT_LANGUAGES)}
CHANNEL_PAGE_SIZE = 8
VIDEO_PAGE_SIZE = 20
TRANSCRIPT_PAGE_SIZE = 10
VIDEO_TABLE_COLUMNS = [
    "selected",
    "published",
    "channel",
    "title",
    "duration",
    "views",
    "transcript",
    "lang",
    "topics",
    "summary",
    "video_id",
    "watch",
]


def main() -> None:
    st.set_page_config(
        page_title="NewsToday Console",
        page_icon="YT",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_dense_dark_css()

    defaults = sidebar_settings()
    initialize_state()

    st.title("NewsToday Console")
    st.caption("YouTube-only headline workflow: channels -> videos -> transcripts.")

    tabs = st.tabs(["Channels", "Videos", "Transcripts"])
    with tabs[0]:
        render_channels_tab(defaults)
    with tabs[1]:
        render_videos_tab(defaults)
    with tabs[2]:
        render_transcripts_tab(defaults)


def sidebar_settings() -> dict[str, Any]:
    st.sidebar.header("Controls")
    api_key = st.sidebar.text_input(
        "YouTube API Key",
        value=os.getenv("YOUTUBE_API_KEY", ""),
        type="password",
        help="Used for channel resolution and recent upload metadata. Kept in the current browser session only.",
    )
    db_path = st.sidebar.text_input(
        "SQLite DB",
        value=os.getenv("NEWSTODAY_DB_PATH", "data/news.db"),
    )
    default_timezone = normalize_timezone_name(os.getenv("NEWSTODAY_TIMEZONE", "UTC"))
    timezone_name = st.sidebar.selectbox(
        "Timezone",
        options=TIMEZONE_OPTIONS,
        index=TIMEZONE_OPTIONS.index(default_timezone),
        key="timezone_select",
    )
    max_videos_per_channel = st.sidebar.number_input(
        "Max Videos / Channel",
        min_value=1,
        max_value=50,
        value=int(os.getenv("NEWSTODAY_MAX_VIDEOS_PER_CHANNEL", "10")),
        step=1,
    )
    transcript_language = st.sidebar.selectbox(
        "Transcript Language",
        options=list(LANGUAGE_OPTIONS.keys()),
        index=0,
        help="Only English is enabled for now.",
    )
    st.sidebar.caption("API key is required for channel search and metadata loading.")
    st.sidebar.markdown("**Gemini Summaries**")
    gemini_api_key = st.sidebar.text_input(
        "Gemini API Key",
        value=os.getenv("GEMINI_API_KEY", ""),
        type="password",
        help=(
            "Optional. When set, NewsToday can generate transcript summaries with Gemini 2.5 Flash and save "
            "them alongside the transcript."
        ),
    )
    st.sidebar.caption(
        "Optional. Uses gemini-2.5-flash for world economics, finance, commodities, and stocks key points."
    )

    return {
        "api_key": api_key.strip(),
        "db_path": db_path.strip(),
        "timezone": timezone_name,
        "max_videos_per_channel": int(max_videos_per_channel),
        "transcript_languages": LANGUAGE_OPTIONS[transcript_language],
        "gemini_api_key": gemini_api_key.strip(),
        "gemini_model": GEMINI_SUMMARY_MODEL,
    }


def initialize_state() -> None:
    if "channel_rows" not in st.session_state:
        st.session_state.channel_rows = default_channel_rows()
    if "video_rows" not in st.session_state:
        st.session_state.video_rows = []
    if "channel_page" not in st.session_state:
        st.session_state.channel_page = 1
    if "video_page" not in st.session_state:
        st.session_state.video_page = 1
    if "transcript_page" not in st.session_state:
        st.session_state.transcript_page = 1
    if "channel_search_query" not in st.session_state:
        st.session_state.channel_search_query = ""
    if "channel_search_results" not in st.session_state:
        st.session_state.channel_search_results = []
    if "channel_search_next_token" not in st.session_state:
        st.session_state.channel_search_next_token = ""
    if "channel_search_prev_token" not in st.session_state:
        st.session_state.channel_search_prev_token = ""
    if "channel_search_page" not in st.session_state:
        st.session_state.channel_search_page = 1
    if "selected_transcript_video_id" not in st.session_state:
        st.session_state.selected_transcript_video_id = ""


def render_channels_tab(settings: dict[str, Any]) -> None:
    st.subheader("Channel Discovery")
    search_cols = st.columns([2.5, 0.8, 0.8, 0.8, 0.8, 1.5])
    search_query = search_cols[0].text_input(
        "Search YouTube Channels",
        value=st.session_state.channel_search_query,
        placeholder="Reuters, CNBC, Bloomberg, BBC...",
        key="channel_search_input",
    )
    search_clicked = search_cols[1].button("Search", width="stretch")
    clear_clicked = search_cols[2].button(
        "Clear",
        width="stretch",
        disabled=not (st.session_state.channel_search_query or st.session_state.channel_search_results),
    )
    prev_clicked = search_cols[3].button(
        "Prev",
        width="stretch",
        disabled=not st.session_state.channel_search_prev_token,
    )
    next_clicked = search_cols[4].button(
        "Next",
        width="stretch",
        disabled=not st.session_state.channel_search_next_token,
    )
    search_cols[5].caption(
        f"Search page {st.session_state.channel_search_page} | "
        f"{len(st.session_state.channel_search_results)} results"
    )

    if search_clicked:
        run_channel_search(settings["api_key"], search_query, page_token="", next_page=1)
    if clear_clicked:
        clear_channel_search()
    if prev_clicked and st.session_state.channel_search_prev_token:
        run_channel_search(
            settings["api_key"],
            st.session_state.channel_search_query,
            page_token=st.session_state.channel_search_prev_token,
            next_page=max(1, int(st.session_state.channel_search_page) - 1),
        )
    if next_clicked and st.session_state.channel_search_next_token:
        run_channel_search(
            settings["api_key"],
            st.session_state.channel_search_query,
            page_token=st.session_state.channel_search_next_token,
            next_page=int(st.session_state.channel_search_page) + 1,
        )

    render_channel_search_results()

    st.subheader("Channel Watchlist")
    current_df = channels_df_from_rows(st.session_state.channel_rows)
    enabled_count = int(current_df["enabled"].sum()) if not current_df.empty else 0
    metrics = st.columns(4)
    metrics[0].metric("Rows", len(current_df))
    metrics[1].metric("Enabled", enabled_count)
    metrics[2].metric("Handles", sum(bool(value) for value in current_df["handle"]))
    metrics[3].metric("Channel IDs", sum(bool(value) for value in current_df["channel_id"]))

    actions = st.columns([1.1, 1.1, 1.1, 4.7])
    if actions[0].button("Load Defaults", width="stretch"):
        st.session_state.channel_rows = default_channel_rows()
        st.session_state.channel_page = 1
        st.rerun()
    if actions[1].button("Remove Selected", width="stretch"):
        kept_rows = [row for row in st.session_state.channel_rows if not row.get("selected")]
        if len(kept_rows) != len(st.session_state.channel_rows):
            st.session_state.channel_rows = kept_rows
            st.session_state.channel_page = min(
                st.session_state.channel_page,
                total_pages(len(st.session_state.channel_rows), CHANNEL_PAGE_SIZE),
            )
            st.rerun()
    if actions[2].button("Clear All", width="stretch"):
        st.session_state.channel_rows = []
        st.session_state.channel_page = 1
        st.rerun()
    actions[3].caption("Add channels from YouTube search above, then enable, edit, or remove them here.")

    page_df, current_page, _ = paginated_dataframe(
        current_df,
        state_key="channel_page",
        page_size=CHANNEL_PAGE_SIZE,
        label="Watchlist Pages",
    )
    edited_page = st.data_editor(
        page_df,
        hide_index=True,
        width="stretch",
        key=f"channels_editor_page_{current_page}",
        column_config={
            "selected": st.column_config.CheckboxColumn("Rm", width="small"),
            "enabled": st.column_config.CheckboxColumn("On", width="small"),
            "label": st.column_config.TextColumn("Label", width="medium"),
            "handle": st.column_config.TextColumn("Handle", width="medium"),
            "channel_id": st.column_config.TextColumn("Channel ID", width="medium"),
            "username": st.column_config.TextColumn("Username", width="medium"),
        },
        num_rows="fixed",
    )
    edited_page = normalize_channels_df(edited_page)
    if sync_channel_page_edits(current_df, edited_page):
        st.rerun()

    st.caption(
        f"Showing channels {(current_page - 1) * CHANNEL_PAGE_SIZE + 1}"
        f"-{min(current_page * CHANNEL_PAGE_SIZE, len(current_df)) if len(current_df) else 0}"
        f" of {len(current_df)}"
    )


def render_videos_tab(settings: dict[str, Any]) -> None:
    st.subheader("Video Queue")
    targets = channel_targets_from_rows(st.session_state.channel_rows)
    ensure_video_date_range_state(settings["timezone"])

    controls = st.columns([1.2, 1.2, 1.1, 1.1, 1.1, 3.3])
    start_date = controls[0].date_input(
        "Start Date",
        key="video_range_start",
    )
    end_date = controls[1].date_input(
        "End Date",
        key="video_range_end",
    )
    load_clicked = controls[2].button("Load Videos", width="stretch")
    load_db_clicked = controls[3].button("Load DB", width="stretch")
    clear_clicked = controls[4].button("Clear Queue", width="stretch")
    controls[5].caption("Load YouTube metadata or stored rows for an exact local date range.")

    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return

    range_start, range_end = build_video_range_bounds(
        start_date=start_date,
        end_date=end_date,
        timezone_name=settings["timezone"],
    )

    if clear_clicked:
        st.session_state.video_rows = []
        st.session_state.video_page = 1
        st.rerun()

    if load_db_clicked:
        storage = NewsStorage(settings["db_path"])
        try:
            rows = storage.fetch_videos_in_range(range_start, range_end)
        finally:
            storage.close()
        st.session_state.video_rows = preserve_video_selection(st.session_state.video_rows, rows)
        st.session_state.video_page = 1
        st.rerun()

    if load_clicked:
        if not settings["api_key"]:
            st.error("Enter a YouTube API key before loading videos.")
        elif not targets:
            st.error("Add and enable at least one channel.")
        else:
            with st.spinner("Loading recent uploads from YouTube..."):
                collector = YouTubeNewsCollector(
                    api_key=settings["api_key"],
                    targets=targets,
                    max_videos_per_channel=settings["max_videos_per_channel"],
                    transcript_languages=settings["transcript_languages"],
                )
                try:
                    records = collector.collect_metadata(start_at=range_start, end_at=range_end)
                except SourceError as exc:
                    st.error(str(exc))
                    records = []
                if records:
                    storage = NewsStorage(settings["db_path"])
                    try:
                        storage.upsert_videos(records)
                        merged_rows = storage.fetch_videos_by_ids([record.video_id for record in records])
                    finally:
                        storage.close()
                    st.session_state.video_rows = preserve_video_selection(
                        st.session_state.video_rows,
                        merged_rows,
                    )
                    st.session_state.video_page = 1
                    existing_transcripts = sum(row.get("transcript_status") == "ok" for row in merged_rows)
                    if existing_transcripts:
                        st.success(
                            f"Loaded {len(merged_rows)} videos in the selected range. "
                            f"{existing_transcripts} already have saved transcripts."
                        )
                    else:
                        st.success(f"Loaded {len(merged_rows)} videos in the selected range.")
                else:
                    st.warning("No videos found in the selected date range.")

    metrics = build_video_metrics(st.session_state.video_rows)
    metric_cols = st.columns(5)
    metric_cols[0].metric("Loaded", metrics["total"])
    metric_cols[1].metric("Selected", metrics["selected"])
    metric_cols[2].metric("Transcribed", metrics["ok"])
    metric_cols[3].metric("Pending", metrics["pending"])
    metric_cols[4].metric("Gaps", metrics["gaps"])

    filters = st.columns([2, 2, 2, 3])
    search_text = filters[0].text_input(
        "Search",
        value="",
        placeholder="title / channel / topic",
        key="video_search_text",
    )
    channel_options = sorted({row.get("channel_title", "") for row in st.session_state.video_rows if row.get("channel_title")})
    selected_channels = filters[1].multiselect(
        "Channels",
        options=channel_options,
        default=[],
        key="video_channel_filter",
    )
    status_options = sorted({row.get("transcript_status", "pending") for row in st.session_state.video_rows})
    selected_statuses = filters[2].multiselect(
        "Transcript",
        options=status_options,
        default=status_options,
        key="video_status_filter",
    )
    show_only_selected = filters[3].checkbox("Show selected only", value=False, key="video_show_only_selected")

    table_df = build_video_table_df(
        st.session_state.video_rows,
        timezone_name=settings["timezone"],
        search_text=search_text,
        selected_channels=selected_channels,
        selected_statuses=selected_statuses,
        show_only_selected=show_only_selected,
    )
    page_df, current_page, _ = paginated_dataframe(
        table_df,
        state_key="video_page",
        page_size=VIDEO_PAGE_SIZE,
        label="Video Pages",
    )

    edited_table = st.data_editor(
        page_df[VIDEO_TABLE_COLUMNS] if not page_df.empty else page_df,
        hide_index=True,
        width="stretch",
        key=f"videos_editor_page_{current_page}",
        column_config={
            "selected": st.column_config.CheckboxColumn("Tx", width="small"),
            "published": st.column_config.TextColumn("Published", width="small"),
            "channel": st.column_config.TextColumn("Channel", width="small"),
            "title": st.column_config.TextColumn("Title", width="large"),
            "duration": st.column_config.TextColumn("Dur", width="small"),
            "views": st.column_config.NumberColumn("Views", format="%d", width="small"),
            "transcript": st.column_config.TextColumn("Transcript", width="small"),
            "lang": st.column_config.TextColumn("Lang", width="small"),
            "topics": st.column_config.TextColumn("Topics", width="medium"),
            "summary": st.column_config.TextColumn("Summary", width="large"),
            "video_id": st.column_config.TextColumn("Video ID", width="medium"),
            "watch": st.column_config.LinkColumn("Watch", display_text="open", width="small"),
        },
        disabled=["published", "channel", "title", "duration", "views", "transcript", "lang", "topics", "summary", "video_id", "watch"],
    )
    if apply_video_selection(edited_table):
        st.rerun()

    st.caption(
        f"Showing videos {(current_page - 1) * VIDEO_PAGE_SIZE + 1}"
        f"-{min(current_page * VIDEO_PAGE_SIZE, len(table_df)) if len(table_df) else 0}"
        f" of {len(table_df)}"
    )

    selected_ids = [row["video_id"] for row in st.session_state.video_rows if row.get("selected")]
    tx_cols = st.columns([1.5, 1.4, 1.4, 2.7])
    transcribe_label = "Transcribe + Summarize" if settings["gemini_api_key"] else "Transcribe Selected"
    transcribe_clicked = tx_cols[0].button(transcribe_label, width="stretch")
    summarize_clicked = tx_cols[1].button(
        "Summarize Selected",
        width="stretch",
        disabled=not settings["gemini_api_key"],
    )
    clear_transcripts_clicked = tx_cols[2].button("Clear Transcripts", width="stretch")
    tx_cols[3].caption(
        "Selected rows stay checked after updates. When a Gemini key is set, transcription also writes "
        "gemini-2.5-flash economics and markets bullets."
    )

    if transcribe_clicked:
        if not selected_ids:
            st.warning("Select at least one video to transcribe.")
        else:
            transcribe_selected_videos(
                selected_ids=selected_ids,
                transcript_languages=settings["transcript_languages"],
                db_path=settings["db_path"],
                gemini_api_key=settings["gemini_api_key"],
                gemini_model=settings["gemini_model"],
            )
            st.session_state.transcript_page = 1
            st.rerun()

    if summarize_clicked:
        if not selected_ids:
            st.warning("Select at least one video to summarize.")
        else:
            summarize_selected_videos(
                selected_ids=selected_ids,
                db_path=settings["db_path"],
                gemini_api_key=settings["gemini_api_key"],
                gemini_model=settings["gemini_model"],
            )
            st.session_state.transcript_page = 1
            st.rerun()

    if clear_transcripts_clicked:
        if not selected_ids:
            st.warning("Select at least one video to clear transcript data.")
        else:
            cleared_count = clear_selected_transcripts(selected_ids=selected_ids, db_path=settings["db_path"])
            st.session_state.transcript_page = 1
            if cleared_count:
                st.success(f"Cleared transcript data for {cleared_count} selected videos.")
            else:
                st.info("No transcript data needed to be cleared.")
            st.rerun()


def render_transcripts_tab(settings: dict[str, Any]) -> None:
    st.subheader("Transcript Review")
    ok_rows = [row for row in st.session_state.video_rows if row.get("transcript_status") == "ok"]
    if not ok_rows:
        st.info("No transcript-backed videos loaded yet.")
        return

    filters = st.columns([2.2, 2.0, 3.8])
    search_text = filters[0].text_input(
        "Transcript Search",
        value="",
        placeholder="title / transcript text",
        key="transcript_search_text",
    )
    channel_options = sorted({row.get("channel_title", "") for row in ok_rows if row.get("channel_title")})
    selected_channels = filters[1].multiselect(
        "Channels",
        options=channel_options,
        default=[],
        key="transcript_channel_filter",
    )
    filters[2].caption("Select a transcript from the current page to inspect the details below.")

    transcript_df = build_transcript_table_df(
        ok_rows,
        timezone_name=settings["timezone"],
        search_text=search_text,
        selected_channels=selected_channels,
    )
    filtered_video_ids = set(transcript_df["video_id"].tolist()) if not transcript_df.empty else set()
    filtered_rows = [row for row in ok_rows if row.get("video_id") in filtered_video_ids]
    page_df, current_page, _ = paginated_dataframe(
        transcript_df,
        state_key="transcript_page",
        page_size=TRANSCRIPT_PAGE_SIZE,
        label="Transcript Pages",
    )
    st.dataframe(
        page_df[TRANSCRIPT_TABLE_COLUMNS] if not page_df.empty else page_df,
        width="stretch",
        hide_index=True,
    )
    st.caption(
        f"Showing transcripts {(current_page - 1) * TRANSCRIPT_PAGE_SIZE + 1}"
        f"-{min(current_page * TRANSCRIPT_PAGE_SIZE, len(transcript_df)) if len(transcript_df) else 0}"
        f" of {len(transcript_df)}"
    )

    available_ids = page_df["video_id"].tolist() if not page_df.empty else []
    if not available_ids:
        st.info("No transcript rows match the current filters.")
        return
    if st.session_state.selected_transcript_video_id not in available_ids:
        st.session_state.selected_transcript_video_id = available_ids[0]

    current_index = available_ids.index(st.session_state.selected_transcript_video_id)
    selected_id = st.selectbox(
        "Transcript On Current Page",
        options=available_ids,
        index=current_index,
        format_func=lambda video_id: transcript_option_label(page_df, video_id),
        key="transcript_page_video_select",
    )
    if selected_id != st.session_state.selected_transcript_video_id:
        st.session_state.selected_transcript_video_id = selected_id
        st.rerun()

    row = next(item for item in ok_rows if item["video_id"] == selected_id)
    export_cols = st.columns([1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 2.0])
    export_cols[0].download_button(
        "Selected JSON",
        data=export_transcripts_json([row], timezone_name=settings["timezone"]),
        file_name=f"transcript-{selected_id}.json",
        mime="application/json",
        width="stretch",
    )
    export_cols[1].download_button(
        "Selected CSV",
        data=export_transcripts_csv([row], timezone_name=settings["timezone"]),
        file_name=f"transcript-{selected_id}.csv",
        mime="text/csv",
        width="stretch",
    )
    export_cols[2].download_button(
        "Filtered JSON",
        data=export_transcripts_json(filtered_rows, timezone_name=settings["timezone"]),
        file_name=f"transcripts-filtered-{len(filtered_rows)}.json",
        mime="application/json",
        width="stretch",
        disabled=not filtered_rows,
    )
    export_cols[3].download_button(
        "Filtered CSV",
        data=export_transcripts_csv(filtered_rows, timezone_name=settings["timezone"]),
        file_name=f"transcripts-filtered-{len(filtered_rows)}.csv",
        mime="text/csv",
        width="stretch",
        disabled=not filtered_rows,
    )
    summarize_current_clicked = export_cols[4].button(
        "Summarize This",
        width="stretch",
        disabled=not settings["gemini_api_key"],
    )
    remove_current_clicked = export_cols[5].button("Remove This", width="stretch")
    export_cols[6].caption(
        "Exports use a single canonical transcript schema. Gemini summaries are included when they exist."
    )

    if summarize_current_clicked:
        summarize_selected_videos(
            selected_ids=[selected_id],
            db_path=settings["db_path"],
            gemini_api_key=settings["gemini_api_key"],
            gemini_model=settings["gemini_model"],
        )
        st.rerun()

    if remove_current_clicked:
        cleared_count = clear_selected_transcripts(selected_ids=[selected_id], db_path=settings["db_path"])
        if cleared_count:
            st.session_state.transcript_page = 1
            st.success("Removed the selected transcript.")
        else:
            st.info("That transcript was already cleared.")
        st.rerun()

    summary_points = summary_points_for_video(row)
    summary_source = summary_source_for_video(row)
    topics = classify_topics(f"{row.get('title', '')} {row.get('description', '')} {row.get('transcript_text', '')}")
    top = st.columns([3.0, 1.2, 1.2, 1.2])
    top[0].markdown(f"**[{row['title']}]({row['url']})**")
    top[1].metric("Duration", format_duration(int(row.get("duration_seconds", 0))))
    top[2].metric("Views", f"{int(row.get('view_count', 0)):,}")
    top[3].metric("Topics", len(topics))

    st.caption(
        f"{row.get('channel_title', '')} | transcript={row.get('transcript_status')} "
        f"| language={row.get('transcript_language_code') or 'n/a'}"
    )
    if topics:
        st.markdown("**Topics:** " + ", ".join(topics))
    st.caption(f"Summary source: {summary_source}")
    if row.get("ai_summary_error"):
        st.caption(f"Gemini summary issue: {row.get('ai_summary_error')}")

    left, right = st.columns([1.2, 2.8])
    with left:
        st.markdown("**Summary Points**")
        for point in summary_points or ["No summary points extracted."]:
            st.write(f"- {point}")
    with right:
        st.markdown("**Full Transcript**")
        st.text_area(
            "Transcript",
            value=row.get("transcript_text", ""),
            height=480,
            key=f"transcript_text_{selected_id}",
            label_visibility="collapsed",
        )


def transcribe_selected_videos(
    *,
    selected_ids: list[str],
    transcript_languages: list[str],
    db_path: str,
    gemini_api_key: str,
    gemini_model: str,
) -> None:
    fetcher = TranscriptFetcher(transcript_languages)
    updates: list[VideoRecord] = []
    progress = st.progress(0.0, text="Starting transcript fetch...")
    selected_rows = [item for item in st.session_state.video_rows if item.get("video_id") in selected_ids]
    total = len(selected_rows)

    for index, row in enumerate(selected_rows, start=1):
        transcript_result = fetcher.fetch(row["video_id"])
        updated = dict(row)
        updated["transcript_status"] = transcript_result["status"]
        updated["transcript_language"] = transcript_result["language"]
        updated["transcript_language_code"] = transcript_result["language_code"]
        updated["transcript_is_generated"] = bool(transcript_result["is_generated"])
        updated["transcript_is_translated"] = bool(transcript_result["is_translated"])
        updated["transcript_error"] = transcript_result["error"]
        updated["transcript_text"] = transcript_result["text"]
        updated["transcript_segments"] = transcript_result["segments"]
        reset_ai_summary(updated)
        if gemini_api_key and updated["transcript_status"] == "ok" and updated["transcript_text"]:
            try:
                summary = summarize_transcript(updated, api_key=gemini_api_key, model=gemini_model)
                updated["ai_summary_points"] = summary.summary_points
                updated["ai_summary_model"] = summary.model
                updated["ai_summary_generated_at"] = datetime.now(timezone.utc).isoformat()
                updated["ai_summary_error"] = ""
            except GeminiSummaryError as exc:
                updated["ai_summary_model"] = gemini_model
                updated["ai_summary_error"] = str(exc)
        updates.append(video_record_from_mapping(updated))
        progress_label = "Transcribed"
        if gemini_api_key and updated["transcript_status"] == "ok":
            progress_label = "Transcribed and summarized"
        progress.progress(index / total, text=f"{progress_label} {index}/{total}: {row['title'][:80]}")

    if updates:
        storage = NewsStorage(db_path)
        try:
            storage.upsert_videos(updates)
        finally:
            storage.close()
        st.session_state.video_rows = preserve_video_selection(
            st.session_state.video_rows,
            [update.to_record() for update in updates],
        )


def summarize_selected_videos(*, selected_ids: list[str], db_path: str, gemini_api_key: str, gemini_model: str) -> None:
    if not gemini_api_key:
        st.error("Enter a Gemini API key before summarizing transcripts.")
        return

    selected_rows = [
        dict(item)
        for item in st.session_state.video_rows
        if item.get("video_id") in selected_ids and item.get("transcript_status") == "ok" and item.get("transcript_text")
    ]
    if not selected_rows:
        st.warning("Select at least one transcript-backed video to summarize.")
        return

    updates: list[VideoRecord] = []
    progress = st.progress(0.0, text="Starting Gemini summaries...")
    total = len(selected_rows)
    for index, row in enumerate(selected_rows, start=1):
        updated = dict(row)
        reset_ai_summary(updated)
        try:
            summary = summarize_transcript(updated, api_key=gemini_api_key, model=gemini_model)
            updated["ai_summary_points"] = summary.summary_points
            updated["ai_summary_model"] = summary.model
            updated["ai_summary_generated_at"] = datetime.now(timezone.utc).isoformat()
            updated["ai_summary_error"] = ""
        except GeminiSummaryError as exc:
            updated["ai_summary_model"] = gemini_model
            updated["ai_summary_error"] = str(exc)
        updates.append(video_record_from_mapping(updated))
        progress.progress(index / total, text=f"Summarized {index}/{total}: {row['title'][:80]}")

    storage = NewsStorage(db_path)
    try:
        storage.upsert_videos(updates)
    finally:
        storage.close()
    st.session_state.video_rows = preserve_video_selection(
        st.session_state.video_rows,
        [update.to_record() for update in updates],
    )


def reset_ai_summary(row: dict[str, Any]) -> None:
    row["ai_summary_points"] = []
    row["ai_summary_model"] = ""
    row["ai_summary_generated_at"] = ""
    row["ai_summary_error"] = ""


def clear_selected_transcripts(*, selected_ids: list[str], db_path: str) -> int:
    storage = NewsStorage(db_path)
    try:
        cleared_count = storage.clear_transcripts(selected_ids)
        refreshed_rows = storage.fetch_videos_by_ids(selected_ids)
    finally:
        storage.close()
    st.session_state.video_rows = preserve_video_selection(st.session_state.video_rows, refreshed_rows)
    return cleared_count


def ensure_video_date_range_state(timezone_name: str) -> None:
    if "video_range_start" in st.session_state and "video_range_end" in st.session_state:
        return
    zone = resolve_timezone(timezone_name)
    today_local = datetime.now(zone).date()
    default_hours = int(os.getenv("NEWSTODAY_DEFAULT_HOURS", "24"))
    default_days = int(os.getenv("NEWSTODAY_DEFAULT_DAYS", str(max(1, math.ceil(default_hours / 24)))))
    start_date = today_local - timedelta(days=max(default_days - 1, 0))
    st.session_state.video_range_start = start_date
    st.session_state.video_range_end = today_local


def build_video_range_bounds(*, start_date: date, end_date: date, timezone_name: str) -> tuple[datetime, datetime]:
    zone = resolve_timezone(timezone_name)
    start_local = datetime.combine(start_date, time.min, tzinfo=zone)
    end_local = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=zone)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def run_channel_search(api_key: str, query: str, *, page_token: str, next_page: int) -> None:
    if not api_key:
        st.error("Enter a YouTube API key before searching channels.")
        return
    cleaned_query = query.strip()
    if not cleaned_query:
        st.error("Enter a search term.")
        return
    with st.spinner("Searching YouTube channels..."):
        client = YouTubeDataClient(api_key)
        try:
            result = client.search_channels(query=cleaned_query, max_results=10, page_token=page_token)
        except SourceError as exc:
            st.error(str(exc))
            return
    st.session_state.channel_search_query = cleaned_query
    st.session_state.channel_search_results = result["results"]
    st.session_state.channel_search_next_token = result["next_page_token"]
    st.session_state.channel_search_prev_token = result["prev_page_token"]
    st.session_state.channel_search_page = next_page
    st.rerun()


def clear_channel_search() -> None:
    st.session_state.channel_search_query = ""
    st.session_state.channel_search_results = []
    st.session_state.channel_search_next_token = ""
    st.session_state.channel_search_prev_token = ""
    st.session_state.channel_search_page = 1
    st.session_state.channel_search_input = ""
    st.rerun()


def render_channel_search_results() -> None:
    results = st.session_state.channel_search_results
    if not results:
        st.caption("Search for a channel above to add it to the watchlist.")
        return

    st.markdown("**Search Results**")
    existing_rows = st.session_state.channel_rows
    for index, item in enumerate(results):
        candidate = channel_row_from_search_result(item)
        already_added = any(channel_rows_match(row, candidate) for row in existing_rows)
        cols = st.columns([3.7, 1.4, 0.8, 0.7])
        title = item.get("title", "") or candidate["label"]
        subtitle = " | ".join(
            part
            for part in [
                candidate["handle"],
                candidate["channel_id"],
                f"videos={item.get('video_count', 0):,}",
                f"subs={item.get('subscriber_count', 0):,}",
            ]
            if part
        )
        cols[0].markdown(f"**{title}**  \n{subtitle}")
        cols[1].caption(item.get("description", "")[:140] or "No description")
        if cols[2].button(
            "Added" if already_added else "Add",
            key=f"add_channel_{index}",
            width="stretch",
            disabled=already_added,
        ):
            st.session_state.channel_rows = upsert_channel_row(st.session_state.channel_rows, item)
            st.session_state.channel_page = total_pages(len(st.session_state.channel_rows), CHANNEL_PAGE_SIZE)
            st.rerun()
        cols[3].markdown(
            f"[open](https://www.youtube.com/channel/{candidate['channel_id']})" if candidate["channel_id"] else ""
        )


def build_video_metrics(video_rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "total": len(video_rows),
        "selected": sum(bool(row.get("selected")) for row in video_rows),
        "ok": sum(row.get("transcript_status") == "ok" for row in video_rows),
        "pending": sum(row.get("transcript_status") == "pending" for row in video_rows),
        "gaps": sum(row.get("transcript_status") not in {"ok", "pending"} for row in video_rows),
    }


def build_transcript_table_df(
    transcript_rows: list[dict[str, Any]],
    *,
    timezone_name: str,
    search_text: str,
    selected_channels: list[str],
) -> pd.DataFrame:
    zone = resolve_timezone(timezone_name)
    search = search_text.strip().lower()
    allowed_channels = set(selected_channels)
    rows: list[dict[str, Any]] = []

    for row in sorted(transcript_rows, key=lambda item: item.get("published_at", ""), reverse=True):
        topics = classify_topics(f"{row.get('title', '')} {row.get('description', '')} {row.get('transcript_text', '')}")
        summary_points = summary_points_for_video(row)
        haystack = " ".join(
            [
                row.get("channel_title", ""),
                row.get("title", ""),
                row.get("transcript_text", ""),
                " ".join(topics),
            ]
        ).lower()
        if search and search not in haystack:
            continue
        if allowed_channels and row.get("channel_title") not in allowed_channels:
            continue
        published = pd.to_datetime(row.get("published_at")).tz_convert(zone).strftime("%m-%d %H:%M")
        rows.append(
            {
                "published": published,
                "channel": row.get("channel_title", ""),
                "title": row.get("title", ""),
                "lang": row.get("transcript_language_code", ""),
                "topics": ", ".join(topics[:3]),
                "summary": " | ".join(summary_points[:2]),
                "video_id": row.get("video_id", ""),
            }
        )
    return pd.DataFrame(rows, columns=TRANSCRIPT_TABLE_COLUMNS)


def build_video_table_df(
    video_rows: list[dict[str, Any]],
    *,
    timezone_name: str,
    search_text: str,
    selected_channels: list[str],
    selected_statuses: list[str],
    show_only_selected: bool,
) -> pd.DataFrame:
    zone = resolve_timezone(timezone_name)
    rows: list[dict[str, Any]] = []
    search = search_text.strip().lower()
    allowed_channels = set(selected_channels)
    allowed_statuses = set(selected_statuses)

    for row in sorted(video_rows, key=lambda item: item.get("published_at", ""), reverse=True):
        topics = classify_topics(f"{row.get('title', '')} {row.get('description', '')} {row.get('transcript_text', '')}")
        summary_points = summary_points_for_video(row)
        haystack = " ".join(
            [
                row.get("channel_title", ""),
                row.get("title", ""),
                row.get("description", ""),
                " ".join(topics),
                " ".join(summary_points),
            ]
        ).lower()
        if search and search not in haystack:
            continue
        if allowed_channels and row.get("channel_title") not in allowed_channels:
            continue
        if allowed_statuses and row.get("transcript_status") not in allowed_statuses:
            continue
        if show_only_selected and not row.get("selected"):
            continue
        published = pd.to_datetime(row.get("published_at")).tz_convert(zone).strftime("%m-%d %H:%M")
        rows.append(
            {
                "selected": bool(row.get("selected")),
                "published": published,
                "channel": row.get("channel_title", ""),
                "title": row.get("title", ""),
                "duration": format_duration(int(row.get("duration_seconds", 0))),
                "views": int(row.get("view_count", 0) or 0),
                "transcript": display_transcript_status(row.get("transcript_status", "")),
                "lang": row.get("transcript_language_code", ""),
                "topics": ", ".join(topics[:3]),
                "summary": " | ".join(summary_points[:2]),
                "video_id": row.get("video_id", ""),
                "watch": row.get("url", ""),
            }
        )
    return pd.DataFrame(rows, columns=VIDEO_TABLE_COLUMNS)


def paginated_dataframe(df: pd.DataFrame, *, state_key: str, page_size: int, label: str) -> tuple[pd.DataFrame, int, int]:
    item_count = len(df)
    page_count = total_pages(item_count, page_size)
    current_page = int(st.session_state.get(state_key, 1))
    current_page = max(1, min(current_page, page_count))
    st.session_state[state_key] = current_page

    controls = st.columns([1.0, 1.0, 1.0, 4.0])
    if controls[0].button("Prev", key=f"{state_key}_prev", width="stretch", disabled=current_page <= 1):
        st.session_state[state_key] = current_page - 1
        st.rerun()
    if controls[1].button("Next", key=f"{state_key}_next", width="stretch", disabled=current_page >= page_count):
        st.session_state[state_key] = current_page + 1
        st.rerun()
    controls[2].metric("Page", f"{current_page}/{page_count}")
    controls[3].caption(f"{label} | rows per page: {page_size}")

    start = (current_page - 1) * page_size
    end = start + page_size
    return df.iloc[start:end].copy(), current_page, page_count


def total_pages(item_count: int, page_size: int) -> int:
    return max(1, math.ceil(max(item_count, 1) / page_size))


def apply_video_selection(edited_table: pd.DataFrame) -> bool:
    if edited_table.empty:
        return False
    selection_map = {
        str(row["video_id"]): bool(row["selected"])
        for _, row in edited_table.iterrows()
    }
    changed = False
    for row in st.session_state.video_rows:
        video_id = str(row.get("video_id", ""))
        if video_id in selection_map:
            new_value = selection_map[video_id]
            if bool(row.get("selected")) != new_value:
                row["selected"] = new_value
                changed = True
    return changed


def preserve_video_selection(existing_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selection_map = {row.get("video_id", ""): bool(row.get("selected")) for row in existing_rows}
    combined: dict[str, dict[str, Any]] = {row.get("video_id", ""): dict(row) for row in existing_rows if row.get("video_id")}
    for row in new_rows:
        video_id = row.get("video_id", "")
        if not video_id:
            continue
        merged = dict(row)
        merged["selected"] = selection_map.get(video_id, bool(row.get("selected", False)))
        combined[video_id] = merged
    return list(sorted(combined.values(), key=lambda item: item.get("published_at", ""), reverse=True))


def transcript_option_label(page_df: pd.DataFrame, video_id: str) -> str:
    row = page_df.loc[page_df["video_id"] == video_id].iloc[0]
    return f"{row['published']} | {row['channel']} | {row['title']}"


def display_transcript_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "ok":
        return "saved"
    if normalized == "pending":
        return "not fetched"
    if normalized in {"missing", "disabled", "unavailable"}:
        return "no transcript"
    if normalized == "empty":
        return "empty"
    if normalized in {"blocked", "error", "invalid"}:
        return "error"
    return normalized or "unknown"


def normalize_channels_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=CHANNEL_COLUMNS)
    normalized = df.copy()
    for column in CHANNEL_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = False if column == "selected" else (True if column == "enabled" else "")
    normalized = normalized[CHANNEL_COLUMNS]
    normalized["selected"] = normalized["selected"].map(lambda value: coerce_bool(value, default=False))
    normalized["enabled"] = normalized["enabled"].map(lambda value: coerce_bool(value, default=True))
    for column in ["label", "handle", "channel_id", "username"]:
        normalized[column] = normalized[column].fillna("").astype(str).map(str.strip)
    return normalized


def channels_df_from_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return normalize_channels_df(pd.DataFrame(rows, columns=CHANNEL_COLUMNS))


def channels_rows_from_df(df: pd.DataFrame) -> list[dict[str, Any]]:
    normalized = normalize_channels_df(df)
    rows: list[dict[str, Any]] = []
    for _, row in normalized.iterrows():
        rows.append(
            {
                "selected": bool(row.get("selected", False)),
                "enabled": bool(row.get("enabled", True)),
                "label": str(row.get("label", "")).strip(),
                "handle": str(row.get("handle", "")).strip(),
                "channel_id": str(row.get("channel_id", "")).strip(),
                "username": str(row.get("username", "")).strip(),
            }
        )
    return rows


def default_channel_rows() -> list[dict[str, Any]]:
    return [
        {
            "selected": False,
            "enabled": True,
            "label": item.get("label", ""),
            "handle": item.get("handle", ""),
            "channel_id": item.get("channel_id", ""),
            "username": item.get("username", ""),
        }
        for item in DEFAULT_CHANNELS
    ]


def sync_channel_page_edits(full_df: pd.DataFrame, edited_page: pd.DataFrame) -> bool:
    if edited_page is None or full_df.empty:
        return False

    normalized_page = normalize_channels_df(edited_page)
    updated_df = full_df.copy()
    for row_index in normalized_page.index:
        if row_index not in updated_df.index:
            continue
        updated_df.loc[row_index, CHANNEL_COLUMNS] = normalized_page.loc[row_index, CHANNEL_COLUMNS]

    new_rows = channels_rows_from_df(updated_df)
    if new_rows == st.session_state.channel_rows:
        return False

    st.session_state.channel_rows = new_rows
    return True


def upsert_channel_row(rows: list[dict[str, Any]], item: dict[str, Any]) -> list[dict[str, Any]]:
    candidate = channel_row_from_search_result(item)
    updated_rows = [dict(row) for row in rows]

    for index, row in enumerate(updated_rows):
        if not channel_rows_match(row, candidate):
            continue
        merged = dict(row)
        for field in ["label", "handle", "channel_id", "username"]:
            if candidate[field]:
                merged[field] = candidate[field]
        merged["enabled"] = True
        merged["selected"] = False
        updated_rows[index] = channels_rows_from_df(pd.DataFrame([merged]))[0]
        return updated_rows

    updated_rows.append(candidate)
    return updated_rows


def channel_row_from_search_result(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "selected": False,
        "enabled": True,
        "label": str(item.get("title", "") or item.get("label", "")).strip(),
        "handle": str(item.get("handle", "")).strip(),
        "channel_id": str(item.get("channel_id", "")).strip(),
        "username": str(item.get("username", "")).strip(),
    }


def channel_rows_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    for field in ["channel_id", "handle", "username", "label"]:
        left_value = normalize_identifier(left.get(field, ""))
        right_value = normalize_identifier(right.get(field, ""))
        if left_value and right_value and left_value == right_value:
            return True
    return False


def normalize_identifier(value: str) -> str:
    return str(value or "").strip().lower()


def coerce_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return default
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return bool(value)


def channel_targets_from_rows(rows: list[dict[str, Any]]) -> list[ChannelTarget]:
    targets: list[ChannelTarget] = []
    for row in rows:
        if not row.get("enabled", True):
            continue
        if not any(str(row.get(field, "")).strip() for field in ["label", "handle", "channel_id", "username"]):
            continue
        targets.append(
            ChannelTarget(
                label=str(row.get("label", "")).strip(),
                handle=str(row.get("handle", "")).strip(),
                channel_id=str(row.get("channel_id", "")).strip(),
                username=str(row.get("username", "")).strip(),
            )
        )
    return targets


def inject_dense_dark_css() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: #0b0f14;
            color: #d7dee7;
        }
        [data-testid="stSidebar"] {
            background: #0f141b;
            border-right: 1px solid #1b2330;
        }
        .block-container {
            padding-top: 0.8rem;
            padding-bottom: 0.8rem;
            padding-left: 1rem;
            padding-right: 1rem;
            max-width: 100%;
        }
        h1, h2, h3, h4, p, label, span, div, input, textarea {
            font-size: 12px !important;
        }
        h1 { font-size: 22px !important; }
        h2 { font-size: 16px !important; }
        h3 { font-size: 14px !important; }
        .stMetric {
            background: #101722;
            border: 1px solid #1d2633;
            padding: 0.35rem 0.5rem;
            border-radius: 6px;
        }
        .stMetric [data-testid="stMetricValue"] {
            font-size: 18px !important;
        }
        div[data-baseweb="input"] input,
        div[data-baseweb="base-input"] textarea,
        .stTextInput input,
        .stTextArea textarea {
            background: #0f141b !important;
            color: #d7dee7 !important;
            font-size: 12px !important;
        }
        div[data-testid="stDataFrame"] * {
            font-size: 11px !important;
        }
        div[data-testid="stDataEditor"] * {
            font-size: 11px !important;
        }
        button[kind="secondary"],
        button[kind="primary"] {
            min-height: 2rem;
            padding-top: 0.1rem;
            padding-bottom: 0.1rem;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.25rem;
        }
        .stTabs [data-baseweb="tab"] {
            height: 2rem;
            padding-left: 0.6rem;
            padding-right: 0.6rem;
        }
        code {
            color: #9bd1ff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
