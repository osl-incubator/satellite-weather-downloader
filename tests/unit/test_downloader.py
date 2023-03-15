import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock

from satellite.downloader.extract_reanalysis import _BR_AREA, _DATA_DIR, _format_dates

class TestExtractMethods(unittest.TestCase):
    def test_brazil_coordinates_to_copernicus_api(self):
        self.assertEquals(
            _BR_AREA, {'N': 5.5, 'W': -74.0, 'S': -33.75, 'E': -32.25}
        )

    def test_default_data_directory(self):
        self.assertEquals(_DATA_DIR, Path().home / 'copernicus_data')

    def test_date_formatting(self):
        self.assertEquals(_format_dates('2023-01-01'), ('2023', '01', '01'))
        self.assertEquals(
            _format_dates('2023-01-01', '2023-01-02'),
            ('2023', ['01'], ['01', '02']),
        )
        self.assertEquals(
            _format_dates('2023-01-01', '2023-01-31'),
            (
                '2023',
                ['01'],
                [
                    '01',
                    '02',
                    '03',
                    '04',
                    '05',
                    '06',
                    '07',
                    '08',
                    '09',
                    '10',
                    '11',
                    '12',
                    '13',
                    '14',
                    '15',
                    '16',
                    '17',
                    '18',
                    '19',
                    '20',
                    '21',
                    '22',
                    '23',
                    '24',
                    '25',
                    '26',
                    '27',
                    '28',
                    '29',
                    '30',
                    '31',
                ],
            ),
        )
        self.assertEquals(
            _format_dates('2023-01-01', '2023-06-15'),
            (
                '2023',
                ['01', '02', '03', '04', '05', '06'],
                [
                    '01',
                    '02',
                    '03',
                    '04',
                    '05',
                    '06',
                    '07',
                    '08',
                    '09',
                    '10',
                    '11',
                    '12',
                    '13',
                    '14',
                    '15',
                    '16',
                    '17',
                    '18',
                    '19',
                    '20',
                    '21',
                    '22',
                    '23',
                    '24',
                    '25',
                    '26',
                    '27',
                    '28',
                    '29',
                    '30',
                    '31',
                ],
            ),
        )

    ...


if __name__ == '__main__':
    unittest.main()
