from typing import Optional
from pathlib import Path

import xarray as xr
from cdsapi.api import Client

from satellite.models import ERA5LandRequest, DataSet


def reanalysis_era5_land(
    output: Optional[str] = None,
    client: Optional[Client] = None,
    **kwargs,
) -> xr.Dataset:
    if output and Path(output).is_file():
        return DataSet.from_netcdf(output)
    return DataSet.from_netcdf(
        ERA5LandRequest(client=client, request=kwargs).download(output)
    )
