from __future__ import annotations

import unittest

from newstoday.models import format_duration, parse_duration_seconds


class ModelTests(unittest.TestCase):
    def test_parse_duration_seconds_handles_hours_minutes_seconds(self) -> None:
        self.assertEqual(parse_duration_seconds("PT1H2M3S"), 3723)

    def test_format_duration_uses_compact_clock(self) -> None:
        self.assertEqual(format_duration(3723), "1:02:03")
        self.assertEqual(format_duration(59), "0:59")


if __name__ == "__main__":
    unittest.main()

