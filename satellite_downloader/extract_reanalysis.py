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
                    @warning: date 2022-08-01 is corrupting all data retrieved.
"""

import re
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union

import connection

BR_AREA = {'N': 5.5, 'W': -74.0, 'S': -33.75, 'E': -32.25}
DATA_DIR = Path.home() / 'copernicus_data'

help_d = 'Use `help(extract_reanalysis.download_br_netcdf)` for more info.'

date_today = datetime.now()
date_format = '%Y-%m-%d'
date_re_format = r'\d{4}-\d{2}-\d{2}'
date_iso_format = 'YYYY-MM-DD'

# dataset has maximum of 8 days of update delay.
# in order to prevent requesting invalid dates,
# the max date is 8 days ago, from today's date
max_update_delay = date_today - timedelta(days=8)
max_update_delay_f = datetime.strftime(max_update_delay, date_format)

# TODO: make download_br_netcdf accepts date and datetime types. 
def download_br_netcdf(
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
        uid (opt(str)): UID from Copernicus User page, it will be used with
                        `connection.connect()` method.
        key (opt(str)): API Key from Copernicus User page, it will be used with
                        `connection.connect()` method.

    Returns:
        String corresponding to path: `data_dir/filename`, that can later be used
        to transform into a `xarray.Dataset` with the CopeBRDatasetExtension located
        in `satellite_weather` module.
    """

    conn = connection.connect(uid, key)

    if data_dir:
        data_dir = Path(str(data_dir))

    else:
        data_dir = DATA_DIR

    data_dir.mkdir(parents=True, exist_ok=True)

    if date and not date_end:
        year, month, day = _format_dates(date)
        filename = f'BR_{date}.nc'

    elif all([date, date_end]):
        year, month, day = _format_dates(date, date_end)
        filename = f'BR_{date}_{date_end}.nc'

    elif not date and not date_end:
        logging.warning(
            'No date provided, downloading last'
            + f' available date: {max_update_delay_f}'
        )
        year, month, day = _format_dates(max_update_delay_f)
        filename = f'BR_{max_update_delay_f}.nc'

    else:
        raise Exception(
            f"""
            Bad usage.
            {help_d}
        """
        )

    filename = filename.replace('-', '')
    file = f'{data_dir}/{filename}'
    if Path(file).exists():
        return file

    else:
        try:
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
                    'area': list(BR_AREA.values()),
                    'format': 'netcdf',
                },
                str(file),
            )
            logging.info(f'NetCDF {filename} downloaded at {data_dir}.')

            return str(file)

        except Exception as e:
            logging.error(e)


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
    year, month, day = date.split('-')

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
