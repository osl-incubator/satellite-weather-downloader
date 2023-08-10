import loguru
from pathlib import Path
import numpy as np
from numpy import dtype
import xarray as xr
import pandas as pd
import dask

from cProfile import Profile
import unittest
from pstats import Stats

from satellite import weather

logger = loguru.logger


class TestWeatherCopebr(unittest.TestCase):
    def setUp(self) -> None:
        self.file = Path(__file__).parent / 'data' / 'BR_20230101.nc'
        self.geocodes =  weather.copebr.brazil.extract_latlons.COORDS_BY_GEOCODE
        self.dataset = weather.load_dataset(str(self.file))

    def test_get_latlons_from_geocode(self):
        profiler = Profile()
        profiler.enable()

        latlons = weather.copebr.get_latlons(3304557)

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertEqual(latlons, ([-22.75, -23.0], [-43.0, -43.25]))
        self.assertEqual(
            weather.copebr.get_latlons(4108304), ([-25.5], [-54.5, -54.75])
        )
        self.assertRaises(
            ValueError, weather.copebr.get_latlons, 123
        )

    def test_load_netcdf_daily_file(self):
        profiler = Profile()
        profiler.enable()

        dataset = weather.load_dataset(str(self.file))

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(dataset) == xr.core.dataset.Dataset)
        self.assertEqual(list(dataset.keys()), ['t2m', 'tp', 'd2m', 'msl'])
        self.assertEqual(list(dataset.coords), ['longitude', 'latitude', 'time'])

    def test_reducing_dataset_by_min(self):
        profiler = Profile()
        profiler.enable()

        ds_min = weather.copebr.reduce_by(self.dataset, np.min, "min")

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertEqual(
            list(ds_min.keys()), ['t2m_min', 'tp_min', 'd2m_min', 'msl_min']
        )
        self.assertEqual(
            [float(var) for var in tuple(ds_min.variables.values())],
            [261.238525390625, 9.313225746154785e-10, 234.1446533203125, 100014.875]
        )

    def test_converting_dataset_to_br_units(self):
        profiler = Profile()
        profiler.enable()

        br = weather.copebr.convert_to_br_units(self.dataset)

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(br) == xr.core.dataset.Dataset)
        self.assertEqual(list(br.keys()), ['temp', 'precip', 'umid', 'pressao'])
        self.assertEqual(list(br.coords), ['longitude', 'latitude', 'time'])

    def test_slicing_dataset_by_coord(self):
        lats, lons = weather.copebr.get_latlons(3304557)
        profiler = Profile()
        profiler.enable()

        ds_slice = weather.copebr.slice_dataset_by_coord(self.dataset, lats, lons)

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(ds_slice) == xr.core.dataset.Dataset)
        self.assertEqual(len(ds_slice.time), 8)
        self.assertTrue(all([x > 0 for x in list(ds_slice.t2m.values)]))

    def test_slicing_dataset_by_geocode(self):
        profiler = Profile()
        profiler.enable()

        ds_rio = weather.copebr.geocode_ds(self.dataset, 3304557)

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(ds_rio) == xr.core.dataset.Dataset)
        self.assertEqual(
            list(ds_rio.keys()), 
            [
                'temp_max', 'precip_max', 'umid_max',
                'pressao_max', 'temp_med', 'precip_med',
                'umid_med', 'pressao_med', 'temp_min',
                'precip_min', 'umid_min', 'pressao_min', 'precip_tot'
            ]
        )

    def test_geocode_dataset_to_dataframe(self):
        profiler = Profile()
        profiler.enable()

        df = weather.copebr.geocode_to_dataframe(self.dataset, 3304557)

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(df) == pd.core.frame.DataFrame)
        self.assertFalse(df.empty)
        self.assertEqual(
            list(df.columns), 
            [
                'temp_max', 'precip_max', 'umid_max',
                'pressao_max', 'temp_med', 'precip_med',
                'umid_med', 'pressao_med', 'temp_min',
                'precip_min', 'umid_min', 'pressao_min', 
                'precip_tot', 'geocodigo'
            ]
        )

    def test_final_dataframe_with_50_geocodes(self):
        profiler = Profile()
        profiler.enable()

        df = weather.copebr.final_dataframe(
            self.dataset, list(self.geocodes.keys())[:50]
        )

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(df) == dask.dataframe.core.DataFrame)
        self.assertEqual(
            list(df.columns), 
            [
                'date', 'temp_max', 'precip_max', 'umid_max',
                'pressao_max', 'temp_med', 'precip_med',
                'umid_med', 'pressao_med', 'temp_min',
                'precip_min', 'umid_min', 'pressao_min', 
                'precip_tot', 'geocodigo'
            ]
        )
        self.assertEqual(
            dict(df.dtypes),
            {
                'date': dtype('<M8[ns]'),
                'temp_max': dtype('float32'),
                'precip_max': dtype('float32'),
                'umid_max': dtype('float32'),
                'pressao_max': dtype('float32'),
                'temp_med': dtype('float32'),
                'precip_med': dtype('float32'),
                'umid_med': dtype('float32'),
                'pressao_med': dtype('float32'),
                'temp_min': dtype('float32'),
                'precip_min': dtype('float32'),
                'umid_min': dtype('float32'),
                'pressao_min': dtype('float32'),
                'precip_tot': dtype('float32'),
                'geocodigo': dtype('int64')
            }
        )

    def test_final_dataframe_with_500_geocodes(self):
        profiler = Profile()
        profiler.enable()

        df = weather.copebr.final_dataframe(
            self.dataset, list(self.geocodes.keys())[:500]
        )

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(df) == dask.dataframe.core.DataFrame)
