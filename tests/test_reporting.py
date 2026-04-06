from __future__ import annotations

import unittest
from datetime import date

from newstoday.reporting import build_summary_points, classify_topics, render_report, summary_points_for_video


def sample_video(**overrides: object) -> dict:
    video = {
        "video_id": "abc123",
        "channel_title": "Reuters",
        "title": "Inflation and jobs top the market agenda",
        "description": "A roundup of the latest inflation and labor headlines.",
        "url": "https://www.youtube.com/watch?v=abc123",
        "published_at": "2026-04-05T12:00:00+00:00",
        "duration_seconds": 420,
        "view_count": 250000,
        "transcript_status": "ok",
        "transcript_language_code": "en",
        "transcript_is_generated": True,
        "transcript_is_translated": False,
        "transcript_text": (
            "Inflation is staying elevated as central bank officials weigh rate cuts. "
            "Employers are still hiring, but wage growth is slowing. "
            "Markets are watching the next CPI release for signs of cooling prices."
        ),
        "transcript_segments": [
            {"text": "Inflation is staying elevated as central bank officials weigh rate cuts.", "start": 0, "duration": 5},
            {"text": "Employers are still hiring, but wage growth is slowing.", "start": 5, "duration": 4},
            {"text": "Markets are watching the next CPI release for signs of cooling prices.", "start": 9, "duration": 4},
        ],
        "transcript_error": "",
    }
    video.update(overrides)
    return video


class ReportingTests(unittest.TestCase):
    def test_build_summary_points_extracts_transcript_lines(self) -> None:
        points = build_summary_points(sample_video())
        self.assertTrue(points)
        self.assertIn("Inflation", points[0])

    def test_classify_topics_detects_rate_story(self) -> None:
        labels = classify_topics(sample_video()["transcript_text"])
        self.assertIn("Inflation & Rates", labels)

    def test_summary_points_prefer_stored_ai_summary(self) -> None:
        points = summary_points_for_video(
            sample_video(
                ai_summary_points=["Fed language and inflation data dominated the market outlook."],
                ai_summary_model="gemini-2.5-flash",
            )
        )
        self.assertEqual(points, ["Fed language and inflation data dominated the market outlook."])

    def test_summary_points_for_video_no_longer_defaults_to_three_items(self) -> None:
        points = summary_points_for_video(
            sample_video(
                ai_summary_points=[
                    "Inflation remained elevated in the latest discussion.",
                    "Central bank officials were weighing possible rate cuts.",
                    "Hiring continued but wage growth slowed.",
                    "Markets were watching the next CPI release.",
                ],
                ai_summary_model="gemini-2.5-flash",
            )
        )
        self.assertEqual(len(points), 4)

    def test_render_report_includes_daily_video_news_section(self) -> None:
        markdown = render_report(
            videos=[sample_video()],
            runs=[],
            report_date=date(2026, 4, 5),
            timezone_name="UTC",
        )
        self.assertIn("## Daily Video News", markdown)
        self.assertIn("Inflation and jobs top the market agenda", markdown)

    def test_render_report_uses_ai_summary_when_present(self) -> None:
        markdown = render_report(
            videos=[
                sample_video(
                    ai_summary_points=["Markets focused on inflation, rate cuts, and the next CPI print."],
                    ai_summary_model="gemini-2.5-flash",
                )
            ],
            runs=[],
            report_date=date(2026, 4, 5),
            timezone_name="UTC",
        )
        self.assertIn("Markets focused on inflation, rate cuts, and the next CPI print.", markdown)


if __name__ == "__main__":
    unittest.main()
