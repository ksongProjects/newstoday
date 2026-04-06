from __future__ import annotations

import json
import unittest

from newstoday.exporting import export_transcripts_csv, export_transcripts_json


class ExportingTests(unittest.TestCase):
    def test_json_export_wraps_items_with_schema(self) -> None:
        payload = json.loads(
            export_transcripts_json(
                [
                    {
                        "video_id": "abc123",
                        "url": "https://www.youtube.com/watch?v=abc123",
                        "channel_id": "UC123",
                        "channel_title": "Reuters",
                        "channel_lookup": "@Reuters",
                        "title": "Markets open lower",
                        "description": "Opening headlines",
                        "published_at": "2026-04-05T12:00:00+00:00",
                        "duration_seconds": 95,
                        "view_count": 2500,
                        "transcript_status": "ok",
                        "transcript_language": "English",
                        "transcript_language_code": "en",
                        "transcript_text": "Stocks fall as investors brace for inflation data.",
                        "transcript_segments": [{"text": "Stocks fall as investors brace for inflation data.", "start": 0, "duration": 3}],
                    }
                ],
                timezone_name="UTC",
            ).decode("utf-8")
        )
        self.assertEqual(payload["schema_version"], "newstoday.transcripts.v1")
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["video_id"], "abc123")
        self.assertIn("inflation", payload["items"][0]["transcript_text"].lower())

    def test_csv_export_flattens_topics_and_summary_points(self) -> None:
        csv_text = export_transcripts_csv(
            [
                {
                    "video_id": "abc123",
                    "url": "https://www.youtube.com/watch?v=abc123",
                    "channel_id": "UC123",
                    "channel_title": "Reuters",
                    "channel_lookup": "@Reuters",
                    "title": "Markets open lower",
                    "description": "Opening headlines",
                    "published_at": "2026-04-05T12:00:00+00:00",
                    "duration_seconds": 95,
                    "view_count": 2500,
                    "transcript_status": "ok",
                    "transcript_language": "English",
                    "transcript_language_code": "en",
                    "transcript_text": "Stocks fall as investors brace for inflation data.",
                    "transcript_segments": [{"text": "Stocks fall as investors brace for inflation data.", "start": 0, "duration": 3}],
                }
            ],
            timezone_name="UTC",
        ).decode("utf-8")
        self.assertIn("video_id,url,channel_id", csv_text)
        self.assertIn("abc123", csv_text)
        self.assertIn("inflation", csv_text.lower())


if __name__ == "__main__":
    unittest.main()
