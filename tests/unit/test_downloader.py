import unittest
from pathlib import Path

from satellite.downloader.extract_reanalysis import _DATA_DIR, _format_dates


class TestExtractMethods(unittest.TestCase):
    def test_default_data_directory(self):
        data_dir = Path().home() / "copernicus_data"
        self.assertEqual(_DATA_DIR, data_dir)

    def test_date_formatting(self):
        self.assertEqual(_format_dates("2023-01-01"), ("2023", "01", "01"))
        self.assertEqual(
            _format_dates("2023-01-01", "2023-01-02"),
            ("2023", ["01"], ["01", "02"]),
        )
        self.assertEqual(
            _format_dates("2023-01-01", "2023-01-31"),
            (
                "2023",
                ["01"],
                [
                    "01",
                    "02",
                    "03",
                    "04",
                    "05",
                    "06",
                    "07",
                    "08",
                    "09",
                    "10",
                    "11",
                    "12",
                    "13",
                    "14",
                    "15",
                    "16",
                    "17",
                    "18",
                    "19",
                    "20",
                    "21",
                    "22",
                    "23",
                    "24",
                    "25",
                    "26",
                    "27",
                    "28",
                    "29",
                    "30",
                    "31",
                ],
            ),
        )
        self.assertEqual(
            _format_dates("2023-01-01", "2023-06-15"),
            (
                "2023",
                ["01", "02", "03", "04", "05", "06"],
                [
                    "01",
                    "02",
                    "03",
                    "04",
                    "05",
                    "06",
                    "07",
                    "08",
                    "09",
                    "10",
                    "11",
                    "12",
                    "13",
                    "14",
                    "15",
                    "16",
                    "17",
                    "18",
                    "19",
                    "20",
                    "21",
                    "22",
                    "23",
                    "24",
                    "25",
                    "26",
                    "27",
                    "28",
                    "29",
                    "30",
                    "31",
                ],
            ),
        )

    ...
