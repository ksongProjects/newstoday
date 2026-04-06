from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
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
                        )
                    ]
                )
                stored = storage.fetch_videos_for_date(published_at.date(), "UTC")
                self.assertEqual(stats_one.inserted, 1)
                self.assertEqual(stats_two.updated, 1)
                self.assertEqual(stored[0]["transcript_status"], "ok")
                self.assertIn("inflation", stored[0]["transcript_text"].lower())
                by_id = storage.fetch_videos_by_ids(["abc123"])
                self.assertEqual(len(by_id), 1)
                self.assertEqual(by_id[0]["video_id"], "abc123")
                self.assertEqual(by_id[0]["transcript_status"], "ok")
            finally:
                storage.close()


if __name__ == "__main__":
    unittest.main()
