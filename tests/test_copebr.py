import unittest
from cProfile import Profile
from pathlib import Path
from pstats import Stats

import loguru
import numpy as np
import pandas as pd
import xarray as xr
from satellite import DataSet, ADM2, ADM0

logger = loguru.logger


class TestWeatherCopebr(unittest.TestCase):
    def setUp(self) -> None:
        self.file = Path(__file__).parent / "data" / "BR_20230101.nc"
        self.dataset = DataSet.from_netcdf((str(self.file)))

    def test_get_latlons_from_geocode(self):
        profiler = Profile()
        profiler.enable()

        adm = ADM2.get(code=3304557, adm0="BRA")

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertEqual(adm.code, "3304557")
        self.assertEqual(adm.adm0.name, ADM0.get(code="BRA").name)

    def test_load_netcdf_daily_file(self):
        profiler = Profile()
        profiler.enable()

        dataset = DataSet.from_netcdf((str(self.file)))

        profiler.disable()
        stats = Stats(profiler)
        logger.info("")
        stats.sort_stats("cumtime").print_stats(10)

        self.assertTrue(type(dataset) == xr.core.dataset.Dataset)
        self.assertEqual(list(dataset.keys()), ['t2m', 'tp', 'd2m', 'msl'])
        self.assertEqual(list(dataset.coords), ["longitude", "latitude", "time"])
