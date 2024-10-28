from typing import Optional, Dict, Literal
from datetime import datetime, timedelta
from pathlib import Path

import xarray as xr

from satellite.models import ERA5LandRequest, DataSet


def reanalysis_era5_land(
    output: Optional[str] = None,
    api_token: Optional[str] = None,
    product_type: list[str] = ["reanalysis"],
    variable: list[str] = [
        "2m_temperature",
        "total_precipitation",
        "2m_dewpoint_temperature",
        "surface_pressure",
    ],
    date: str = str((datetime.now() - timedelta(days=6)).date()),
    time: list[str] = [
        "00:00",
        "03:00",
        "06:00",
        "09:00",
        "12:00",
        "15:00",
        "18:00",
        "21:00",
    ],
    locale: Optional[Literal["BRA", "ARG"]] = None,
    area: Optional[Dict[Literal["N", "S", "W", "E"], float]] = None,
    format: Literal["grib", "netcdf"] = "netcdf",
    download_format: Literal["zip", "unarchived"] = "zip",
) -> xr.Dataset:
    request = dict(
        product_type=product_type,
        variable=variable,
        date=date,
        time=time,
        locale=locale,
        area=area,
        format=format,
        download_format=download_format,
    )
    if output and Path(output).is_file():
        return DataSet.from_netcdf(output)
    return DataSet.from_netcdf(
        ERA5LandRequest(api_key=api_token, request=request).download(output)
    )
