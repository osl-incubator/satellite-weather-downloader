# type: ignore[attr-defined]
"""satellite_weather_downloader Weather Collection Python package"""
# TODO: docstrings
import xarray as xr

from .copebr import * # noqa
from .dsei import * # noqa


def load_dataset(file_path: str) -> xr.Dataset:
    with xr.open_dataset(file_path, engine='netcdf4') as ds:
        return ds
