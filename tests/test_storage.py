from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from newstoday.models import Article, canonicalize_url
from newstoday.storage import NewsStorage


class StorageTests(unittest.TestCase):
    def test_canonicalize_url_removes_tracking_params(self) -> None:
        url = "https://example.com/story?utm_source=x&id=7&fbclid=abc"
        self.assertEqual(canonicalize_url(url), "https://example.com/story?id=7")

    def test_storage_deduplicates_on_title_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "news.db"
            storage = NewsStorage(db_path)
            published_at = datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc)
            try:
                stats_one = storage.upsert_articles(
                    [
                        Article(
                            source_type="rss",
                            source_name="Reuters",
                            title="Inflation cools in March",
                            url="https://news.google.com/story-one",
                            published_at=published_at,
                        )
                    ]
                )
                stats_two = storage.upsert_articles(
                    [
                        Article(
                            source_type="api",
                            source_name="Reuters",
                            title="Inflation cools in March",
                            url="https://www.reuters.com/world/economy/inflation-cools/",
                            published_at=published_at,
                        )
                    ]
                )
                self.assertEqual(stats_one.inserted, 1)
                self.assertEqual(stats_two.updated, 1)
            finally:
                storage.close()


if __name__ == "__main__":
    unittest.main()
