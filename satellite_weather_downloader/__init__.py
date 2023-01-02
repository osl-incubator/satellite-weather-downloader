# type: ignore[attr-defined]
"""satellite_weather_downloader Python package"""

import sys
import xarray as xr
import metpy.calc as mpcalc
import array
from metpy.units import units
from importlib import metadata as importlib_metadata


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "1.4.0"  # changed by semantic-release


version: str = get_version()
__version__: str = version


# Xarray Dataset Methods

def load_dataset(file_path: str) -> xr.Dataset:
    with xr.open_dataset(file_path, engine="netcdf4") as ds:
        return ds


def slice_dataset_by_coord(
    dataset: xr.Dataset, 
    lat: list[int],
    lon: list[int]
) -> xr.Dataset:
    """
    Slices a dataset using latitudes and longitudes, returns a dataset 
    with the mean values between the coordinates.
    """
    ds = dataset.sel(latitude=lat, longitude=lon, method='nearest')
    return ds


def extract_daily_average_values(dataset: xr.Dataset) -> xr.DataArray():
    """
    Groups Dataset by dayofyear (1, 2...365) and returns its
    max, mean and mean. Used to group the 8 different times
    along the day.
    """
    ds = dataset.groupby('time.dayofyear')
    return ds.min(), ds.mean(), ds.max()


def convert_to_br_units(dataset: xr.Dataset) -> xr.Dataset:
    """
    Parse the units according to Brazil's standard unit measures.
    Rename their unit names and long names as well. 
    """
    ds = dataset
    vars = list(ds.data_vars.keys())

    if 't2m' in vars:
        #Convert Kelvin to Celsius degrees
        ds['t2m'] = ds.t2m - 273.15
        ds['t2m'].attrs['units'] = 'degC'
        ds['t2m'].attrs['long_name'] = 'Temperature'
    if 'tp' in vars:
        #Convert meters to millimeters
        ds['tp'] = ds.tp * 1000
        ds['tp'].attrs['units'] = 'mm'
        ds['tp'].attrs['long_name'] = 'Total Precipitation'
    if 'msl' in vars:
        #Convert Pa to ATM
        ds['msl'] = ds.msl * 0.00000986923
        ds['msl'].attrs['units'] = 'atm'
        ds['msl'].attrs['long_name'] = 'Sea Level Pressure'
    if 'd2m' in vars:
        #Calculate Relative Humidity percentage and add to Dataset 
        ds['d2m'] = ds.d2m - 273.15
        rh = mpcalc.relative_humidity_from_dewpoint(
                ds['t2m'] * units.degC, ds['d2m'] * units.degC) * 100
        ds['rh'] = rh
        ds['rh'].attrs['units'] = 'pct'
        ds['rh'].attrs['long_name'] = 'Relative Humidity'

    return ds

