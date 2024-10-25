from typing import Optional

import xarray as xr
from cdsapi.api import Client

from satellite.models import ERA5LandRequest, DataSet


def reanalysis_era5_land(
    client: Optional[Client] = None,
    output: Optional[str] = None,
    **kwargs,
) -> xr.Dataset:
    return DataSet.from_netcdf4(
        ERA5LandRequest(client=client, request=kwargs).download(output)
    )
