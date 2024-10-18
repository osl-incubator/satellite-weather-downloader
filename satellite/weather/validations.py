from pathlib import Path
from typing import Optional

from satellite.weather.constants import LOCALES, DATASET_FPATH_SUFFIX


def validate_locale(locale: Optional[str]) -> str | None:
    if locale:
        assert locale in LOCALES, (
            f"Unknown locale '{locale}'. Options: {LOCALES.keys()}"
        )
    return locale


def validate_xr_dataset_file_path(fpath: str) -> str:
    file = Path(fpath)
    assert file.suffix in DATASET_FPATH_SUFFIX, (
        f"Unknown file type {file.suffix}. "
        f"Supported file types: {DATASET_FPATH_SUFFIX}"
    )
    return fpath
