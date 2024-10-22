from pathlib import Path

from satellite.weather.orm.constants import ADM0_OPTIONS, DATASET_FPATH_SUFFIX


def validate_adm0_options(code: str) -> str:
    assert code in ADM0_OPTIONS, (
        f"Unknown ADM0 '{code}'. Options: {ADM0_OPTIONS.keys()}"
    )
    return code


def validate_xr_dataset_file_path(fpath: str) -> str:
    file = Path(fpath)
    assert file.suffix in DATASET_FPATH_SUFFIX, (
        f"Unknown file type {file.suffix}. "
        f"Supported file types: {DATASET_FPATH_SUFFIX}"
    )
    return fpath
