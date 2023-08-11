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
import json
from pathlib import Path

with open(f"{Path(__file__).parent}/municipios.json") as muns:
    _mun_decoded = muns.read().encode().decode("utf-8-sig")
    MUNICIPALITIES = json.loads(_mun_decoded)

COORDS_BY_GEOCODE = {
    mun["geocodigo"]: (mun["latitude"], mun["longitude"]) for mun in MUNICIPALITIES
}


def from_geocode(geocode: int) -> tuple:
    """
    Returns latitude and longitude given a city geocode.

    Params:
        geocode (int) : Geocode from a brazilian's city
                        in IBGE's geocode format.

    Returns:
        lat (float)   : Latitude of geocode in degrees
                        between -90 and 90. Represents
                        the North and South coordinates.

        lon (float)   : Longitude of geocode in degrees
                        between -180 and 180. Represents
                        the West and East coordinates.
    """

    if geocode not in COORDS_BY_GEOCODE:
        raise ValueError(f"Geocode {geocode} not found")

    lat, lon = COORDS_BY_GEOCODE[geocode]
    return lat, lon
