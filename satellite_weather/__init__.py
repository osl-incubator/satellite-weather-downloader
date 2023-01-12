# type: ignore[attr-defined]
"""satellite_weather_downloader Weather Collection Python package"""
from importlib import metadata as importlib_metadata
from xr_extensions import CopeBRDatasetExtension
import satellite_weather as weather
import xarray as xr


def load_dataset(file_path: str) -> xr.Dataset:
    with xr.open_dataset(file_path, engine='netcdf4') as ds:
        return ds


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return '1.4.0'  # changed by semantic-release


version: str = get_version()
__version__: str = version
