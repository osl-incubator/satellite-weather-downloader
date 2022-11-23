"""
This module is responsible for retrieving the data from Copernicus.
All data collected comes from "ERA5 hourly data on single levels
from 1959 to present" dataset, which can be found here:
https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels
Copernicus allows the user retrieve data in Grib or NetCDF file format,
that contains geolocation data, besides the selected data to be requested
from the API. The data will later be compiled in a single DataFrame that
contains the following data:

date       : datetime object.
geocode    : geocode from a specific brazilian city, according to IBGE format.
temp_min   : Minimum┐
temp_med   : Average├─ temperature in `celcius degrees` given a geocode coord.
temp_max   : Maximum┘
precip_min : Minimum┐
precip_med : Average├─ of total precipitation in `mm` given a geocode coord.
precip_max : Maximum┘
pressao_min: Minimum┐
pressao_med: Average├─ sea level pressure in `hPa` given a geocode coord.
pressao_max: Maximum┘
umid_min   : Minimum┐
umid_med   : Average├─ percentage of relative humidity given a geocode coord.
umid_max   : Maximum┘

Data collected for these measures are obtained by retrieving the following
variables from the API:

2m temperature (t2m)
Total precipitation (tp)
Mean sea level pressure (msl)
2m dewpoint temperature (d2m)

Data will be collected within a 3 hour range along each day of the month,
the highest and lowest values are stored and the mean is taken with all
the values from the day.

Methods
-------

    download() : Send a request to Copernicus API with the parameters of
                 the city specified by its geocode. The data can be retrieved
                 for certain day of the year and within a time range. As much
                 bigger the time interval chosen, as long will take to download
                 the requested data.
                 @warning: for some reason, even if requested by Copernicus
                           website, trying to retrieve a date range with the
                           current month and the last days of the past month
                           is not possible. For data with a month range, do
                           not choose the current one.
                 @warning: date 2022-08-01 is corrupting all data retrieved.

    _parse_data(data, columns_name) : Groups data by date and aggregate values
                                      to max, min and avg values.

    _retrieve_data(row) : Uses numpy.mean() to aggregate all data from the four
                          coordinate points collected from a specific geocode
                          coordinate.
                          @see `extract_coodinates` module.

    to_dataframe(file) : Will handle the NetCDF file and retrieve its values
                         using numpy `load_dataset()` method and return a
                         DataFrame with the format above.
"""

import logging
import re
from datetime import datetime, timedelta
from functools import lru_cache, reduce
from pathlib import Path
from typing import Optional, Tuple, Union

import metpy.calc as mpcalc
import numpy as np
import pandas as pd
import xarray as xr
from metpy.units import units
from satellite_weather_downloader.utils import (
    connection,
    extract_coordinates,
    extract_latlons,
)
from satellite_weather_downloader.utils.globals import BR_AREA, DATA_DIR

help_d = "Use `help(extract_reanalysis.download_netcdf())` for more info."

date_today = datetime.now()
date_format = "%Y-%m-%d"
date_re_format = r"\d{4}-\d{2}-\d{2}"
date_iso_format = "YYYY-MM-DD"

# dataset has maximum of 7 days of update delay.
# in order to prevent requesting invalid dates,
# the max date is 7 days ago, from today's date
max_update_delay = date_today - timedelta(days=7)
max_update_delay_f = datetime.strftime(max_update_delay, date_format)


def download_netcdf(
    date: Optional[str] = None,
    date_end: Optional[str] = None,
    data_dir: Optional[str] = None,
    uid: Optional[str] = None,
    key: Optional[str] = None,
):
    """
    Creates the request for Copernicus API. Extracts the latitude and
    longitude for a given geocode of a brazilian city, calculates the
    area in which the data will be retrieved and send the request via
    `cdsapi.Client()`. Data can be retrieved for a specific date or a
    date range, usage:

    download_netcdf() -> downloads the last available date
    download_netcdf(date='2022-10-04') or
    download_netcdf(date='2022-10-04', date_end='2022-10-01')

    Using this way, a request will be done to extract the data related
    to the area of Brazil. A file in the NetCDF format will be downloaded
    as specified in `data_dir` attribute, with the format '*.nc',
    returned as a variable to be used in `netcdf_to_dataframe()` method.

    Attrs:
        date (opt(str)): Format 'YYYY-MM-DD'. Date of data to be retrieved.
        date_end (opt(str)): Format 'YYYY-MM-DD'. Used along with `date`
                              to define a date range.
        data_dir (opt(str)): Path in which the NetCDF file will be downloaded.
                             Default dir is specified in `globals.DATA_DIR`.
        uid (opt(str)): UID from Copernicus User page, it will be used with
                        `connection.connect()` method.
        key (opt(str)): API Key from Copernicus User page, it will be used with
                        `connection.connect()` method.

    Returns:
        String corresponding to path: `data_dir/filename`, that can later be used
        to transform into DataFrame with the `netcdf_to_dataframe()` method.
    """

    conn = connection.connect(uid, key)

    if data_dir:
        data_dir = Path(str(data_dir))

    else:
        data_dir = DATA_DIR

    data_dir.mkdir(parents=True, exist_ok=True)

    if date and not date_end:
        year, month, day = _format_dates(date)
        filename = f"BR_{date}.nc"

    elif all([date, date_end]):
        year, month, day = _format_dates(date, date_end)
        filename = f"BR_{date}_{date_end}.nc"

    elif not date and not date_end:
        logging.warning(
            "No date provided, downloading last"
            + f" available date: {max_update_delay_f}"
        )
        year, month, day = _format_dates(max_update_delay_f)
        filename = f"BR_{max_update_delay_f}.nc"

    else:
        raise Exception(
            f"""
            Bad usage.
            {help_d}
        """
        )

    filename = filename.replace("-", "")
    file = f"{data_dir}/{filename}"
    if Path(file).exists():
        return file

    else:
        try:
            conn.retrieve(
                "reanalysis-era5-single-levels",
                {
                    "product_type": "reanalysis",
                    "variable": [
                        "2m_temperature",
                        "total_precipitation",
                        "2m_dewpoint_temperature",
                        "mean_sea_level_pressure",
                    ],
                    "year": year,
                    "month": month,
                    "day": day,
                    "time": [
                        "00:00",
                        "03:00",
                        "06:00",
                        "09:00",
                        "12:00",
                        "15:00",
                        "18:00",
                        "21:00",
                    ],
                    "area": list(BR_AREA.values()),
                    "format": "netcdf",
                },
                str(file),
            )
            logging.info(f"NetCDF {filename} downloaded at {data_dir}.")

            return str(file)

        except Exception as e:
            logging.error(e)

@lru_cache
def netcdf_to_dataframe(
    file_path: str,
    geocode: Union[str, int],
    raw: bool = False,
):
    """
    Returns a DataFrame with the values related to the geocode of a brazilian
    city according to IBGE's format, given a NetCDF file. It will use
    `extract_latlons` and `extract_coordinates` to define the closest area of
    the geocode passed to the method. Extract the values using xarray DataSet
    and return the average values related to the geocode area. More information
    about how the area is extracted can be found in the `extract_coordinates`
    module.
    (geocodes source https://ibge.gov.br/explica/codigos-dos-municipios.php)

    Attrs:
        file_path (str)     : The path related to a NetCDF file, returned by
                              `download_netcdf()` method.
        geocode (str or int): Geocode of a city in Brazil according to IBGE.
        raw (bool)          : If raw is set to True, the DataFrame returned will
                              contain data in 3 hours intervals. Default return
                              will aggregate these values into 24 hours interval.
    """

    # Load netCDF file into xarray dataset
    ds = xr.load_dataset(file_path, engine="netcdf4")

    # Parse units to br's units
    ds["t2m"] = ds.t2m - 273.15
    ds["tp"] = ds.tp * 1000
    ds["msl"] = ds.msl / 100
    ds["d2m"] = ds.d2m - 273.15

    # relative_humidity = temperature/dewpoint_temperature
    rh = (
        mpcalc.relative_humidity_from_dewpoint(
            ds["t2m"] * units.degC, ds["d2m"] * units.degC
        )
        * 100
    )

    # dewpoint sacrifice to store relative humidity in DataSet
    # d2m is now relative humidity
    ds["d2m"] = rh

    lat, lon = extract_latlons.from_geocode(int(geocode))
    N, S, E, W = extract_coordinates.from_latlon(lat, lon)

    lats = [N, S]
    lons = [E, W]

    t2m_area = ds["t2m"].sel(longitude=lons, latitude=lats, method="nearest")
    tp_area = ds["tp"].sel(longitude=lons, latitude=lats, method="nearest")
    rh_area = ds["d2m"].sel(longitude=lons, latitude=lats, method="nearest")
    msl_area = ds["msl"].sel(longitude=lons, latitude=lats, method="nearest")

    if raw:
        temperature = pd.DataFrame([_retrieve_data(v) for v in t2m_area])
        precipitation = pd.DataFrame([_retrieve_data(v) for v in tp_area])
        rel_hum = pd.DataFrame([_retrieve_data(v) for v in rh_area])
        pressure = pd.DataFrame([_retrieve_data(v) for v in msl_area])

        temperature.columns = temperature.columns.str.replace("var", "temp")
        precipitation.columns = precipitation.columns.str.replace("var", "precip")
        rel_hum.columns = rel_hum.columns.str.replace("var", "umid")
        pressure.columns = pressure.columns.str.replace("var", "pressao")

        dfs = [temperature, precipitation, pressure, rel_hum]
        merged_dfs = reduce(lambda l, r: pd.merge(l, r, on=["date"]), dfs)

        geocodes = [geocode for r in range(len(merged_dfs))]
        merged_dfs.insert(0, "geocodigo", geocodes)

        return merged_dfs

    temperature = _parse_data([_retrieve_data(v) for v in t2m_area], "temp")
    precipitation = _parse_data([_retrieve_data(v) for v in tp_area], "precip")
    rel_hum = _parse_data([_retrieve_data(v) for v in rh_area], "umid")
    pressure = _parse_data([_retrieve_data(v) for v in msl_area], "pressao")

    dfs = [temperature, precipitation, pressure, rel_hum]
    merged_dfs = reduce(lambda l, r: pd.merge(l, r, on=["date"]), dfs)

    geocodes = [geocode for r in range(len(merged_dfs))]
    merged_dfs.insert(0, "geocodigo", geocodes)

    return merged_dfs


def _format_dates(
    date: str,
    date_end: Optional[str] = None,
) -> Tuple[Union[str, list], Union[str, list], Union[str, list]]:
    """
    Returns the days, months and years by given a date or
    a date range.

    Attrs:
        date (str)    : Initial date.
        date_end (str): If provided, defines a date range to be extracted.

    Returns:
        year (str or list) : The year(s) related to date provided.
        month (str or list): The month(s) related to date provided.
        day (str or list)  : The day(s) related to date provided.
    """

    ini_date = datetime.strptime(date, date_format)
    year, month, day = date.split("-")

    if ini_date > max_update_delay:
        raise Exception(
            f"""
                Invalid date. The last update date is:
                {max_update_delay_f}
                {help_d}
        """
        )

    # check for right initial date format
    if not re.match(date_re_format, date):
        raise Exception(
            f"""
                Invalid initial date. Format:
                {date_iso_format}
                {help_d}
        """
        )

    # an end date can be passed to define the date range
    # if there is no end date, only the day specified on
    # `date` will be downloaded
    if date_end:
        end_date = datetime.strptime(date_end, date_format)

        # check for right end date format
        if not re.match(date_re_format, date_end):
            raise Exception(
                f"""
                    Invalid end date. Format:
                    {date_iso_format}
                    {help_d}
            """
            )

        # safety limit for Copernicus limit and file size: 1 year
        max_api_query = timedelta(days=367)
        if end_date - ini_date > max_api_query:
            raise Exception(
                f"""
                    Maximum query reached (limit: {max_api_query.days} days).
                    {help_d}
            """
            )

        # end date can't be bigger than initial date
        if end_date < ini_date:
            raise Exception(
                f"""
                    Please select a valid date range.
                    {help_d}
            """
            )

        # the date range will be responsible for match the requests
        # if the date is across months. For example a week that ends
        # after the month.
        df = pd.date_range(start=date, end=date_end)
        year_set = set()
        month_set = set()
        day_set = set()
        for date in df:
            date_f = str(date)
            iso_form = date_f.split(" ")[0]
            year_, month_, day_ = iso_form.split("-")
            year_set.add(year_)
            month_set.add(month_)
            day_set.add(day_)

        # parsing the correct types
        month = list(month_set)
        day = list(day_set)
        # sorting them (can't do inline)
        month.sort()
        day.sort()

        if len(year_set) == 1:
            year = str(year_set.pop())
        else:
            year = list(year)
            year.sort()

    return year, month, day


def _parse_data(data, column_name):
    """
    Group min, max and avg values into a DataFrame and changes the columns to
    AlertaDengue's format.

    Attrs:
        data (list)       : List of values retrieved by xarray DataSet.
        column_name (str) : Name of the column to be updated into the
                            DataFrame

    Returns:
        result (DataFrame): DataFrame ready to be merged into final DataFrame.
    """
    df = pd.DataFrame(data)
    df["date"] = df["date"].dt.floor("D")
    result = df.groupby("date").agg(
        var_min=("var", "min"), var_med=("var", "mean"), var_max=("var", "max")
    )

    result.columns = result.columns.str.replace("var", column_name)
    return result


def _retrieve_data(row):
    """
    Parse row to the right encode type and returns the average value.

    Attrs:
        row (dataset row) : A row in xarray DataSet to be parsed.
    """
    parsed_date = row.time.values.astype("M8[ms]").astype("O")
    avg_vals = np.mean(row.values)
    return dict(date=parsed_date, var=avg_vals)
