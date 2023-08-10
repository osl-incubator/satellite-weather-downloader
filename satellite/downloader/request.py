import os
from string import ascii_letters
from typing import Optional, Union

from cdsapi.api import Client
from loguru import logger

from .reanalysis.prompt import reanalysis_prompt


# TODO: Allow area slicing in request
def ERA5_reanalysis(
    filename: str,
    product_type: Optional[str] = None,
    variable: Optional[Union[str, list]] = None,
    year: Optional[Union[str, list]] = None,
    month: Optional[Union[str, list]] = None,
    day: Optional[Union[str, list]] = None,
    time: Optional[Union[str, list]] = None,
    # area: Optional[list] = None,
    format: Optional[str] = None,
) -> str:
    """
    https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels
    Creates the request to Copernicus Reanalysis using CLI or by method call.
    Although the `copebr` extension in `satellite_weather` does not have support to all
    dataset variables, the request allows all options to be passed in, according to
    Copernicus API:
        product_type (str): data format type.
        variable (str or list): variable(s) available from the API
        year (str or list): year(s) since 1959. Format: f'{year:04d}'
        month (str or list): month(s) of the year. Format: f'{day:02d}'
        day (str or list): day(s) of the month. Format: f'{day:02d}'
        time (str or list): 24 hours available by day
        area (list): #Not available yet
        format (str): netcdf or grib
        filename (str): the name of the file when downloaded
    If one of these variables are not passed, it enters in the interactive shell and
    asks for the missing inputs. The request will fail if the data requested is too
    big. Please check the link above to more information.
    """
    allowed_chars = ascii_letters + "/-_"
    for char in filename:
        if char not in allowed_chars:
            raise ValueError(f"Invalid character {char}")

    cdsapi_key = os.getenv("CDSAPI_KEY")
    if not cdsapi_key:
        raise EnvironmentError(
            "Environment variable CDSAPI_KEY not found in the system.\n"
            'Execute `$ export CDSAPI_KEY="{MY_UID}:{MY_KEY}" to fix.\n'
            "These credentials are found in your Copernicus User Page \n"
            "https://cds.climate.copernicus.eu/user/{MY_USER}"
        )

    conn = Client(
        url="https://cds.climate.copernicus.eu/api/v2",
        key=cdsapi_key,
    )

    options = reanalysis_prompt(
        product_type=product_type,
        variable=variable,
        year=year,
        month=month,
        day=day,
        time=time,
        # area=area,
        format=format,
    )

    _no_suffix = filename.split(".")[0]  # Forcing correct suffix
    filename = _no_suffix + ".nc" if options["format"] == "netcdf" else ".grib"

    try:
        conn.retrieve("reanalysis-era5-single-levels", options, filename)
    except Exception as e:
        logger.error(e)
        raise e
