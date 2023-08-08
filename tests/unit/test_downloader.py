import datetime
from pathlib import Path
import time
import unittest

import dotenv
import loguru

from satellite.downloader import download_br_netcdf
from satellite.downloader.extract_reanalysis import _BR_AREA, _DATA_DIR, _format_dates
from satellite.weather import load_dataset
from satellite.weather._brazil.extract_latlons import MUNICIPIOS


class TestDownloaderAndWeatherBrasil(unittest.TestCase):
    expected_columns = [
        "date", 
        "temp_max", 
        "precip_max", 
        "umid_max", 
        "pressao_max",
        "temp_med", 
        "precip_med", 
        "umid_med", 
        "pressao_med",
        "temp_min", 
        "precip_min", 
        "umid_min", 
        "pressao_min",
        "precip_tot", 
        "geocodigo"
    ]

    muns = [mun['geocodigo'] for mun in MUNICIPIOS]

    def setUp(self):
        # TODO: check the availability of testing with real download calls
        dotenv.load_dotenv()
        self.daily_last_update = download_br_netcdf()
        self.weekly_file = download_br_netcdf("2023-01-01", "2023-01-07")
        self.montly_file = download_br_netcdf("2023-01-01", "2023-01-31")


    def test_download_last_update(self):
        today = datetime.datetime.now().date()
        lu = today - datetime.timedelta(days=8) # Last update
        
        file = Path(self.daily_last_update)
        expected_file = _DATA_DIR / f"BR_{lu.year}{lu.month:02d}{lu.day:02d}.nc"

        self.assertTrue(file.exists())
        self.assertEqual(str(file), str(expected_file))

    def test_download_weekly_file(self):
        file = Path(self.weekly_file)
        expected_file = _DATA_DIR / "BR_20230101_20230107.nc"

        self.assertTrue(file.exists())
        self.assertEqual(str(file), str(expected_file))

    def test_download_monthly_file(self):
        file = Path(self.montly_file)
        expected_file = _DATA_DIR / "BR_20230101_20230131.nc"

        self.assertTrue(file.exists())
        self.assertEqual(str(file), str(expected_file))

    def test_load_datasets(self):
        for file in [self.daily_last_update, self.weekly_file, self.montly_file]:
            ds = load_dataset(file)
            self.assertEqual(len(ds), 4)
            del ds

    def test_copebr_dataframe_last_update_one_geocode(self):
        st = time.time()
        ds = load_dataset(self.daily_last_update)
        df = ds.copebr.to_dataframe(3304557)
        et = time.time()
        tt = et - st
        loguru.logger.info(
            f"took {tt:.4f} seconds"
        )
        self.assertEqual(self.expected_columns, list(df.columns))
        self.assertEqual(len(df), 1)
        self.assertFalse(df.empty)
        del ds, df

    def test_copebr_dataframe_weekly_file_one_geocode(self):
        st = time.time()
        ds = load_dataset(self.daily_last_update)
        df = ds.copebr.to_dataframe(3304557)
        et = time.time()
        tt = et - st
        loguru.logger.info(
            f"took {tt:.4f} seconds"
        )
        self.assertEqual(self.expected_columns, list(df.columns))
        self.assertEqual(len(df), 7)
        self.assertFalse(df.empty)
        del ds, df

    def test_copebr_dataframe_monthly_file_one_geocode(self):
        st = time.time()
        ds = load_dataset(self.daily_last_update)
        df = ds.copebr.to_dataframe(3304557)
        et = time.time()
        tt = et - st
        loguru.logger.info(
            f"took {tt:.4f} seconds"
        )
        self.assertEqual(self.expected_columns, list(df.columns))
        self.assertEqual(len(df), 1)
        self.assertFalse(df.empty)
        del ds, df


class TestExtractMethods(unittest.TestCase):
    def test_brazil_coordinates_to_copernicus_api(self):
        self.assertEqual(
            _BR_AREA, {'N': 5.5, 'W': -74.0, 'S': -33.75, 'E': -32.25}
        )

    def test_default_data_directory(self):
        data_dir = Path().home() / 'copernicus_data'
        self.assertEqual(_DATA_DIR, data_dir)

    def test_date_formatting(self):
        self.assertEqual(_format_dates('2023-01-01'), ('2023', '01', '01'))
        self.assertEqual(
            _format_dates('2023-01-01', '2023-01-02'),
            ('2023', ['01'], ['01', '02']),
        )
        self.assertEqual(
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
        self.assertEqual(
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
