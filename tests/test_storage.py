from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from newstoday.models import VideoRecord
from newstoday.storage import NewsStorage


class StorageTests(unittest.TestCase):
    def test_storage_updates_video_when_transcript_improves(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "news.db"
            storage = NewsStorage(db_path)
            published_at = datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc)
            try:
                stats_one = storage.upsert_videos(
                    [
                        VideoRecord(
                            video_id="abc123",
                            channel_id="UC123",
                            channel_title="Reuters",
                            channel_lookup="@Reuters",
                            title="Markets open lower",
                            description="Opening headlines",
                            published_at=published_at,
                            transcript_status="missing",
                        )
                    ]
                )
                stats_two = storage.upsert_videos(
                    [
                        VideoRecord(
                            video_id="abc123",
                            channel_id="UC123",
                            channel_title="Reuters",
                            channel_lookup="@Reuters",
                            title="Markets open lower",
                            description="Opening headlines",
                            published_at=published_at,
                            transcript_status="ok",
                            transcript_text="Stocks fall as investors brace for inflation data.",
                            transcript_segments=[{"text": "Stocks fall as investors brace for inflation data.", "start": 0, "duration": 3}],
                            ai_summary_points=["Markets focused on inflation data and the policy outlook."],
                            ai_summary_model="gemini-2.5-flash",
                            ai_summary_generated_at="2026-04-05T12:05:00+00:00",
                        )
                    ]
                )
                stored = storage.fetch_videos_for_date(published_at.date(), "UTC")
                self.assertEqual(stats_one.inserted, 1)
                self.assertEqual(stats_two.updated, 1)
                self.assertEqual(stored[0]["transcript_status"], "ok")
                self.assertIn("inflation", stored[0]["transcript_text"].lower())
                self.assertEqual(stored[0]["ai_summary_model"], "gemini-2.5-flash")
                self.assertEqual(stored[0]["ai_summary_points"], ["Markets focused on inflation data and the policy outlook."])
                by_id = storage.fetch_videos_by_ids(["abc123"])
                self.assertEqual(len(by_id), 1)
                self.assertEqual(by_id[0]["video_id"], "abc123")
                self.assertEqual(by_id[0]["transcript_status"], "ok")
            finally:
                storage.close()

    def test_storage_fetches_by_range_and_can_clear_transcripts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "news.db"
            storage = NewsStorage(db_path)
            published_at = datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc)
            older_published_at = published_at - timedelta(days=3)
            try:
                storage.upsert_videos(
                    [
                        VideoRecord(
                            video_id="range-1",
                            channel_id="UC123",
                            channel_title="Reuters",
                            channel_lookup="@Reuters",
                            title="Markets open lower",
                            description="Opening headlines",
                            published_at=published_at,
                            transcript_status="ok",
                            transcript_text="Stocks fall as investors brace for inflation data.",
                            transcript_segments=[{"text": "Stocks fall as investors brace for inflation data.", "start": 0, "duration": 3}],
                            ai_summary_points=["Markets focused on inflation data and policy risk."],
                            ai_summary_model="gemini-2.5-flash",
                            ai_summary_generated_at="2026-04-05T12:05:00+00:00",
                        ),
                        VideoRecord(
                            video_id="range-2",
                            channel_id="UC123",
                            channel_title="Reuters",
                            channel_lookup="@Reuters",
                            title="Older headline",
                            description="Older headlines",
                            published_at=older_published_at,
                            transcript_status="ok",
                            transcript_text="An older transcript.",
                            transcript_segments=[{"text": "An older transcript.", "start": 0, "duration": 2}],
                        ),
                    ]
                )
                in_range = storage.fetch_videos_in_range(
                    published_at - timedelta(hours=1),
                    published_at + timedelta(hours=1),
                )
                cleared = storage.clear_transcripts(["range-1"])
                refreshed = storage.fetch_videos_by_ids(["range-1"])

                self.assertEqual(len(in_range), 1)
                self.assertEqual(in_range[0]["video_id"], "range-1")
                self.assertEqual(cleared, 1)
                self.assertEqual(refreshed[0]["transcript_status"], "pending")
                self.assertEqual(refreshed[0]["transcript_text"], "")
                self.assertEqual(refreshed[0]["transcript_segments"], [])
                self.assertEqual(refreshed[0]["ai_summary_points"], [])
                self.assertEqual(refreshed[0]["ai_summary_model"], "")
            finally:
                storage.close()


if __name__ == "__main__":
    unittest.main()
