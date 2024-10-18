from pathlib import Path
import logging
import xarray as xr
import zipfile

from satellite.weather.extensions import copebr
from satellite.weather.types import Locale


class Weather:
    def __init__(self, fpath: str, locale: Locale):
        self._ds = _load_dataset(fpath)

    @property
    def cope(self):
        return self._ds.cope


def _load_dataset(file_path: str) -> xr.Dataset:
    file = Path(file_path)

    if not file.exists():
        raise FileNotFoundError(file_path)

    if file.suffix == ".nc":
        with xr.open_dataset(file_path, engine="netcdf4") as ds:
            return ds

    if file.suffix == ".zip":
        with zipfile.ZipFile(file_path) as zfile:
            files = zfile.namelist()
            if len(files) > 1:
                logging.warning(
                    f"More than one NetCDF file were found in {file_path}. "
                    "You may want to extract them manually to load the entire "
                    "dataset"
                )
            with zfile.open(files[0]) as ds:
                return xr.open_dataset(ds, engine="netcdf4")

    raise NotImplementedError(f"Unknown file type '{file.suffix}'")
