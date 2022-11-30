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

from collections import defaultdict
from satellite_weather_downloader.utils.globals import PROJECT_DIR

mun_json = open(
    f"{PROJECT_DIR}/satellite_weather_downloader/utils/municipios.json"
)
mun_decoded = mun_json.read().encode().decode("utf-8-sig")
municipios = json.loads(mun_decoded)
mun_json.close()


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

    coords = defaultdict()

    for mun in municipios:
        coords[mun['geocodigo']] = (mun['latitude'], mun['longitude'])
    
    lat, lon = coords[int(geocode)]
    
    return lat, lon
