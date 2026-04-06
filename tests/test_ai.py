from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from newstoday.ai import GEMINI_SUMMARY_MODEL, GEMINI_SUMMARY_MAX_POINTS, GeminiSummaryError, sanitize_summary_points, summarize_transcript


class GeminiSummaryTests(unittest.TestCase):
    @patch("newstoday.ai.request.urlopen")
    def test_summarize_transcript_uses_structured_json_response(self, mock_urlopen: MagicMock) -> None:
        response_payload = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": json.dumps(
                                    {
                                        "summary_points": [
                                            "The Fed signal and inflation path dominated the market outlook.",
                                            "Stocks reacted to expectations for the next CPI release.",
                                        ]
                                    }
                                )
                            }
                        ]
                    }
                }
            ]
        }
        response = MagicMock()
        response.read.return_value = json.dumps(response_payload).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = response

        result = summarize_transcript(
            {
                "channel_title": "Reuters",
                "title": "Inflation and jobs top the market agenda",
                "description": "A roundup of macro headlines.",
                "transcript_language_code": "en",
                "transcript_text": "Inflation is elevated and markets are watching the next CPI print.",
            },
            api_key="test-key",
        )

        self.assertEqual(result.model, GEMINI_SUMMARY_MODEL)
        self.assertEqual(
            result.summary_points,
            [
                "The Fed signal and inflation path dominated the market outlook.",
                "Stocks reacted to expectations for the next CPI release.",
            ],
        )
        sent_request = mock_urlopen.call_args.args[0]
        self.assertIn(GEMINI_SUMMARY_MODEL, sent_request.full_url)
        body = json.loads(sent_request.data.decode("utf-8"))
        self.assertEqual(body["generationConfig"]["responseMimeType"], "application/json")
        self.assertIn("responseJsonSchema", body["generationConfig"])

    def test_summarize_transcript_requires_transcript_text(self) -> None:
        with self.assertRaises(GeminiSummaryError):
            summarize_transcript({"title": "No transcript"}, api_key="test-key")

    def test_sanitize_summary_points_keeps_more_than_three_items(self) -> None:
        points = sanitize_summary_points(
            [
                "Point one",
                "Point two",
                "Point three",
                "Point four",
            ]
        )
        self.assertEqual(len(points), 4)
        self.assertLessEqual(len(points), GEMINI_SUMMARY_MAX_POINTS)


if __name__ == "__main__":
    unittest.main()
