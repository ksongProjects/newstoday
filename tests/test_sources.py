from __future__ import annotations

import unittest

from newstoday.sources import channel_target_from_value, select_channel_targets
from newstoday.models import ChannelTarget


class SourceTests(unittest.TestCase):
    def test_channel_target_from_value_understands_handle_and_channel_id(self) -> None:
        self.assertEqual(channel_target_from_value("@Reuters").handle, "@Reuters")
        self.assertEqual(channel_target_from_value("UC1234567890").channel_id, "UC1234567890")

    def test_select_channel_targets_keeps_matching_targets(self) -> None:
        targets = [
            ChannelTarget(label="Reuters", handle="@Reuters"),
            ChannelTarget(label="Bloomberg Television", handle="@BloombergTelevision"),
        ]
        selected = select_channel_targets(targets, ["@Reuters"])
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].handle, "@Reuters")


if __name__ == "__main__":
    unittest.main()

