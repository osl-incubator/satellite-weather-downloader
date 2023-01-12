# type: ignore[attr-defined]
"""satellite_weather_downloader Weather Downloader Python package"""
from importlib import metadata as importlib_metadata
from pathlib import Path


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return '1.4.0'  # changed by semantic-release


version: str = get_version()
__version__: str = version


