from __future__ import annotations

import unittest
from datetime import date

from newstoday.reporting import group_by_topic, render_report, top_terms


def sample_article(title: str, description: str, source_name: str = "Example") -> dict:
    return {
        "title": title,
        "description": description,
        "source_name": source_name,
        "url": "https://example.com/story",
        "published_at": "2026-04-05T12:00:00+00:00",
        "country": "US",
    }


class ReportingTests(unittest.TestCase):
    def test_group_by_topic_detects_rate_story(self) -> None:
        articles = [sample_article("Central bank weighs interest rates", "Inflation remains sticky")]
        grouped = group_by_topic(articles)
        self.assertIn("Inflation & Rates", grouped)

    def test_top_terms_returns_repeated_words(self) -> None:
        articles = [
            sample_article("Inflation slows in major economies", "Inflation data cools"),
            sample_article("Jobs market stays firm", "Jobs and wages remain strong"),
        ]
        terms = dict(top_terms(articles, limit=5))
        self.assertIn("inflation", terms)
        self.assertIn("jobs", terms)

    def test_render_report_includes_headline_section(self) -> None:
        markdown = render_report(
            articles=[sample_article("Trade tensions rise", "Tariffs are back")],
            runs=[],
            report_date=date(2026, 4, 5),
            timezone_name="UTC",
        )
        self.assertIn("## Top Headlines", markdown)
        self.assertIn("Trade tensions rise", markdown)


if __name__ == "__main__":
    unittest.main()
