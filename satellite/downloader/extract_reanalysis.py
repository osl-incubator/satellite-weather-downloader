"""
This module is responsible for retrieving the data from Copernicus.
All data collected comes from "ERA5 hourly data on single levels
from 1959 to present" dataset, which can be found here:
https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels
Copernicus allows the user retrieve data in Grib or NetCDF file format,
that contains geolocation data, besides the selected data to be requested
from the API. Data collected for these measures are obtained by retrieving
the following variables from the API:

    2m temperature (t2m)
    Total precipitation (tp)
    Mean sea level pressure (msl)
    2m dewpoint temperature (d2m)

Data will be collected within a 3 hour range along each day of the month,
the highest and lowest values are stored and the mean is taken with all
the values from the day.

Methods
-------

download_br_netcdf() : Send a request to Copernicus API with the parameters of
                    the city specified by its geocode. The data can be retrieved
                    for certain day of the year and within a time range. As much
                    bigger the time interval chosen, as long will take to download
                    the requested data.
                    @warning: for some reason, even if requested by Copernicus
                                website, trying to retrieve a date range with the
                                current month and the last days of the past month
                                is not possible. Avoid using the current month.
"""

import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, Union

import pandas as pd
import urllib3
from cdsapi.api import Client

_BR_AREA = {'N': 5.5, 'W': -74.0, 'S': -33.75, 'E': -32.25}
_DATA_DIR = Path.home() / 'copernicus_data'

_HELP = 'Use `help(extract_reanalysis.download_br_netcdf)` for more info.'

_CUR_DATE = datetime.now()
_DATE_FORMAT = '%Y-%m-%d'
_RE_FORMAT = r'\d{4}-\d{2}-\d{2}'
_ISO_FORMAT = 'YYYY-MM-DD'

# dataset has maximum of 8 days of update delay.
# in order to prevent requesting invalid dates,
# the max date is 8 days ago, from today's date
_MAX_DELAY = _CUR_DATE - timedelta(days=8)
_MAX_DELAY_F = datetime.strftime(_MAX_DELAY, _DATE_FORMAT)

# TODO: make download_br_netcdf accepts date and datetime types.
def download_br_netcdf(
    date: Optional[str] = None,
    date_end: Optional[str] = None,
    data_dir: Optional[str] = _DATA_DIR,
    user_key: Optional[str] = None,
):
    """
    Creates the request for Copernicus API. Extracts the latitude and
    longitude for a given geocode of a brazilian city, calculates the
    area in which the data will be retrieved and send the request via
    `cdsapi.Client()`. Data can be retrieved for a specific date or a
    date range, usage:

    download_br_netcdf() -> downloads the last available date
    download_br_netcdf(date='2022-10-04') or
    download_br_netcdf(date='2022-10-04', date_end='2022-10-01')

    Using this way, a request will be done to extract the data related
    to the area of Brazil. A file in the NetCDF format will be downloaded
    as specified in `data_dir` attribute, with the format '*.nc',
    returned as a variable to be used in `netcdf_to_dataframe()` method.

    Attrs:
        date (opt(str)): Format 'YYYY-MM-DD'. Date of data to be retrieved.
        date_end (opt(str)): Format 'YYYY-MM-DD'. Used along with `date`
                              to define a date range.
        data_dir (opt(str)): Path in which the NetCDF file will be downloaded.
                             Default dir is `$HOME/copernicus_data/`.

    Returns:
        String corresponding to path: `data_dir/filename`, that can later be used
        to transform into a `xarray.Dataset` with the CopeBRDatasetExtension located
        in `satellite_weather` module.
    """
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    if not user_key:
        cdsapi_key = os.getenv('CDSAPI_KEY')
    else:
        cdsapi_key = user_key
        
    if not cdsapi_key:
        raise EnvironmentError(
            'Environment variable CDSAPI_KEY not found in the system.\n'
            'Execute `$ export CDSAPI_KEY="<MY_UID>:<MY_API_KEY>" to fix.\n'
            'These credentials are found in your Copernicus User Page: \n'
            'https://cds.climate.copernicus.eu/user/<MY_USER>'
        )

    conn = Client(
        url='https://cds.climate.copernicus.eu/api/v2',
        key=cdsapi_key,
    )

    if date and not date_end:
        year, month, day = _format_dates(date)
        filename = f'BR_{date}.nc'

    elif all([date, date_end]):
        year, month, day = _format_dates(date, date_end)
        filename = f'BR_{date}_{date_end}.nc'

    elif not date and not date_end:
        logging.warning(
            'No date provided, downloading last'
            + f' available date: {_MAX_DELAY_F}'
        )
        year, month, day = _format_dates(_MAX_DELAY_F)
        filename = f'BR_{_MAX_DELAY_F}.nc'

    else:
        raise Exception(
            f"""
            Bad usage.
            {_HELP}
        """
        )

    filename = filename.replace('-', '')
    file = f'{data_dir}/{filename}'
    if Path(file).exists():
        return file

    else:
        try:
            urllib3.disable_warnings()
            conn.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': [
                        '2m_temperature',
                        'total_precipitation',
                        '2m_dewpoint_temperature',
                        'mean_sea_level_pressure',
                    ],
                    'year': year,
                    'month': month,
                    'day': day,
                    'time': [
                        '00:00',
                        '03:00',
                        '06:00',
                        '09:00',
                        '12:00',
                        '15:00',
                        '18:00',
                        '21:00',
                    ],
                    'area': list(_BR_AREA.values()),
                    'format': 'netcdf',
                },
                str(file),
            )
            logging.info(f'NetCDF {filename} downloaded at {data_dir}.')
            return str(file)

        except Exception as e:
            logging.error(e)
            raise e


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

    ini_date = datetime.strptime(date, _DATE_FORMAT)
    year, month, day = date.split('-')

    if ini_date > _MAX_DELAY:
        raise Exception(
            f"""
                Invalid date. The last update date is:
                {_MAX_DELAY_F}
                {_HELP}
        """
        )

    # check for right initial date format
    if not re.match(_RE_FORMAT, date):
        raise Exception(
            f"""
                Invalid initial date. Format:
                {_ISO_FORMAT}
                {_HELP}
        """
        )

    # an end date can be passed to define the date range
    # if there is no end date, only the day specified on
    # `date` will be downloaded
    if date_end:
        end_date = datetime.strptime(date_end, _DATE_FORMAT)

        # check for right end date format
        if not re.match(_RE_FORMAT, date_end):
            raise Exception(
                f"""
                    Invalid end date. Format:
                    {_ISO_FORMAT}
                    {_HELP}
            """
            )

        # safety limit for Copernicus limit and file size: 1 year
        max_api_query = timedelta(days=367)
        if end_date - ini_date > max_api_query:
            raise Exception(
                f"""
                    Maximum query reached (limit: {max_api_query.days} days).
                    {_HELP}
            """
            )

        # end date can't be bigger than initial date
        if end_date < ini_date:
            raise Exception(
                f"""
                    Please select a valid date range.
                    {_HELP}
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
            iso_form = date_f.split(' ')[0]
            year_, month_, day_ = iso_form.split('-')
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
            year = list(year_set)
            year.sort()

    return year, month, day
