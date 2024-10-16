"""
Retrieve latitude and longitude from `municipios.json`.

Latitude and longitude variables are retrieved using
their geocode reference of a brazilian city as specified
by IBGE:
https://ibge.gov.br/explica/codigos-dos-municipios.php

Methods
-------

from_geocode(geocode):
                Returns a tuple (latitude, longitude)
"""

from pathlib import Path
from functools import lru_cache

import pandas as pd


@lru_cache
def _read_locale_json(locale: str) -> pd.DataFrame:
    return pd.read_json(
        f"{Path(__file__).parent.parent}/locales/{locale}/muns.json"
    )


def from_geocode(geocode: int | str, locale: str) -> tuple:
    """
    Returns latitude and longitude given a city geocode.

    Params:
        geocode (int) : Geocode from a brazilian's city in IBGE's geocode
                        format.
        locale (str)  : Country abbreviation. Example: 'BR'

    Returns:
        lat (float)   : Latitude of geocode in degrees between -90 and 90. 
                        Represents the North and South coordinates.

        lon (float)   : Longitude of geocode in degrees
                        between -180 and 180. Represents
                        the West and East coordinates.
    """

    df = _read_locale_json(locale)
    lat_lon = df.loc[  # noqa
        df["geocode"] == int(geocode), ["latitude", "longitude"]
    ].squeeze()

    if lat_lon.empty:
        raise ValueError(f"Geocode {geocode} not found")

    lat, lon = lat_lon['latitude'], lat_lon['longitude']

    return lat, lon
