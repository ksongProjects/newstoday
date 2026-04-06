from __future__ import annotations

import unittest
from datetime import datetime

from newstoday.timezones import normalize_timezone_name, resolve_timezone


class TimezoneTests(unittest.TestCase):
    def test_normalize_timezone_name_supports_utc_and_offsets(self) -> None:
        self.assertEqual(normalize_timezone_name("UTC"), "UTC+00")
        self.assertEqual(normalize_timezone_name("UTC-7"), "UTC-07")
        self.assertEqual(normalize_timezone_name("+9"), "UTC+09")

    def test_resolve_timezone_returns_fixed_offset(self) -> None:
        tz = resolve_timezone("UTC-08")
        offset = datetime(2026, 4, 5, 12, 0, tzinfo=tz).utcoffset()
        self.assertEqual(offset.total_seconds(), -8 * 3600)


if __name__ == "__main__":
    unittest.main()
