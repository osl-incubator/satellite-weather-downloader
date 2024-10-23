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

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Literal

from dotenv import load_dotenv
from cdsapi.api import Client
from requests.exceptions import RequestException

_GLOBE_AREA = {"N": 90.0, "W": -180.0, "S": -90.0, "E": 180.0}
_DATA_DIR = Path.home() / "copernicus_data"
_LOCALES = ["BR", "AR"]

_HELP = "Use `help(extract_reanalysis.download_br_netcdf)` for more info."

_CUR_DATE = datetime.now()
_DATE_FORMAT = "%Y-%m-%d"
_RE_FORMAT = r"\d{4}-\d{2}-\d{2}"
_ISO_FORMAT = "YYYY-MM-DD"

# dataset has maximum of 6 days of update delay.
# in order to prevent requesting invalid dates,
# the max date is 6 days ago, from today's date
_MIN_DELAY = _CUR_DATE - timedelta(days=6)
_MIN_DELAY_F = datetime.strftime(_MIN_DELAY, _DATE_FORMAT)

load_dotenv()


def download_netcdf(
    filename: str = None,
    date: Optional[str] = None,
    date_end: Optional[str] = None,
    locale: Optional[Literal["BR", "AR"]] = None,
    area: Optional[dict] = None,
    output_dir: Optional[str] = str(_DATA_DIR),
    user_key: Optional[str] = None,
    verbose: bool = False,
):
    """
    Creates the request for Copernicus API. Extracts the latitude and
    longitude for a given geocode of a brazilian city, calculates the
    area in which the data will be retrieved and send the request via
    `cdsapi.Client()`. Data can be retrieved for a specific date or a
    date range, usage:

    download_netcdf('filename') -> downloads the last available date worldwide

    download_netcdf('filename', date='2022-10-04')

    download_netcdf('filename', date='2022-10-04', date_end='2022-10-01')

    download_netcdf(
        'filename',
        date='2022-10-04',
        date_end='2022-10-01',
        area={'N': 5.5, 'W': -74.0, 'S': -33.75, 'E': -32.25}
    )

    Using this way, a request will be done to extract the data related
    to the area of Brazil. A file in the NetCDF format will be downloaded
    as specified in `data_dir` attribute, with the format '*.nc',
    returned as a variable to be used in `netcdf_to_dataframe()` method.

    Attrs:
        filename: Name of the file (without type; it will be `.nc`)
        date (opt(str)): Format 'YYYY-MM-DD'. Date of data to be retrieved.
        date_end (opt(str)): Format 'YYYY-MM-DD'. Used along with `date`
                              to define a date range.
        area (opt(dict)): Area coordinates in lagitude and longitude
            Format {'N': float, 'W': float, 'S': float, 'E': float}
            Default {'N': 90.0, 'W': -180.0, 'S': -90.0, 'E': -180.0}
        data_dir (opt(str)): Path in which the NetCDF file will be downloaded.
                             Default dir is `$HOME/copernicus_data/`.
        user_key (opt(str)): Credentials for retrieving Copernicus data. Format:
                             "<MY_UID>:<MY_API_KEY>"

    Returns:
        String corresponding to path: `data_dir/filename`, that can later be used
        to transform into a `xarray.Dataset` with the CopeBRDatasetExtension located
        in `satellite.weather` module.
    """
    Path(str(output_dir)).mkdir(parents=True, exist_ok=True)

    if not user_key:
        cdsapi_token = os.getenv("CDSAPI_TOKEN")
    else:
        cdsapi_token = user_key

    if not cdsapi_token:
        raise EnvironmentError(
            "Environment variable CDSAPI_KEY not found in the system.\n"
            'Execute `$ export CDSAPI_TOKEN="<MY_UID_TOKEN>" to fix.\n'
            "These credentials are found in your Copernicus User Page: \n"
            "https://cds.climate.copernicus.eu/user/USER"
        )

    if locale and locale not in _LOCALES:
        raise ValueError(f"locale {locale} not supported. Options: {_LOCALES}")

    if not area:
        match locale:
            case None:
                area = _GLOBE_AREA
            case "BR":
                area = {"N": 5.5, "W": -74.0, "S": -33.75, "E": -32.25}
            case "AR":
                area = {"N": -21.0, "W": -74.0, "S": -56.0, "E": -53.0}

    if date and not date_end:
        filename = f"{locale or 'WW'}_{date}"
        date_req = str(date)
    elif all([date, date_end]):
        filename = f"{locale or 'WW'}_{date}_{date_end}"
        date_req = f"{date}/{date_end}"
    elif not any([date, date_end]):
        if verbose:
            logging.warning(
                "No date provided, downloading last" f" available date: {_MIN_DELAY_F}"
            )
        date = _MIN_DELAY_F
        date_req = str(date)
        filename = f"{locale or 'WW'}_{date}"
    else:
        raise Exception(
            f"""
            Bad usage.
            {_HELP}
        """
        )

    if set(area.keys()) != set(["N", "W", "S", "E"]):
        raise KeyError(
            """
            Wrong area format;
            Default: {"N": 90.0, "W": -180.0, "S": -90.0, "E": -180.0}
        """
        )

    if not all([isinstance(v, (int, float)) for v in area.values()]):
        raise ValueError("Coordinate values must be rather int or float values")

    if abs(area["N"]) > 90 or abs(area["S"]) > 90:
        raise ValueError("Latitude must be between -90 and 90")

    if abs(area["W"]) > 180 or abs(area["E"]) > 180:
        raise ValueError("Longitude must be between -180 and 180")

    file = Path(output_dir) / f"{filename}.zip"

    if file.exists():
        return str(file)

    conn = Client(
        url="https://cds.climate.copernicus.eu/api",
        key=cdsapi_token,
    )

    try:
        conn.retrieve(
            "reanalysis-era5-land",
            {
                "product_type": ["reanalysis"],
                "variable": [
                    "2m_temperature",
                    "total_precipitation",
                    "2m_dewpoint_temperature",
                    "surface_pressure",
                ],
                "date": date_req,
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
                "area": [area["N"], area["W"], area["S"], area["E"]],
                "format": "netcdf",
                "download_format": "zip",
            },
            str(file),
        )
    except (RequestException, KeyboardInterrupt) as e:
        file.unlink(missing_ok=True)
        raise e

    return str(file)
