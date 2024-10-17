"""
Find the four closest coordinates given a Latitude and Longitude.

NetCDF files store data in intervals of 0.25 degree, or 28 kilometers. Copernicus
API allows a coordinates slice when retrieving the file. Because this distance
range, some coordinates found when rounding the degrees can be distant from the
city coordinates. To manage that, the four closest points from the latitude and
longitude of the city are retrieved, later on the weather variables will be an
average from these four coordinates.

Methods:
    from_latlon(latitude, longitude) : Returns North, South, East and West given a
                                   coordinate.
"""

from functools import lru_cache

import numpy as np


@lru_cache(maxsize=None)
def _longitudes():
    return list(np.arange(-90.0, 90.25, 0.25))


@lru_cache(maxsize=None)
def _latitudes():
    return list(np.arange(-180.0, 180.25, 0.25))


def from_latlon(lat, lon) -> tuple:
    """
    Firstly, the closest coordinate to the city is found. It is then calculated the
    relative position in the quadrant, to define the other three coordinates which
    will later become the average values to a specific city. Take the example the
    center of a city that is represented by the dot:

             N
      ┌──────┬──────┐
      │      │      │
      │  2       1  │
      │      │      │
    W │ ─── ─┼─ ─── │ E
      │      │      │
      │  3       4  │
      │ .    │      │
      └──────┴──────┘
      ▲      S
      │
    closest coord

    Other three coordinates are taken to a measure as close as possible in case the
    closest coordinate is far off the center of the city.
    Let's take, for instance, Rio de Janeiro. Rio's center coordinates are:
    latitude: -22.876652
    longitude: -43.227875
    The closest data point collected would be the coordinate: (-23.0, -43.25). In some
    cases, the closest point is still far from the city itself, or could be in the sea,
    for example. Because of this, other three coordinates will be inserted and then
    the average value is returned from a certain date, defined in
    `extract_reanalysis.download()` method. The coordinates returned from Rio would be:
    [-23.0, -43.25] = S, W
    [-22.75, -43.0] = N, E
    """
    closest_lat = min(_latitudes(), key=lambda x: abs(x - lat))
    closest_lon = min(_longitudes(), key=lambda x: abs(x - lon))

    lat_diff = lat - closest_lat
    lon_diff = lon - closest_lon

    # 1st quadrant
    if lat_diff < 0 and lon_diff < 0:
        north, south, east, west = (
            closest_lat,
            closest_lat - 0.25,
            closest_lon,
            closest_lon - 0.25,
        )
    # 2nd quadrant
    elif lat_diff > 0 and lon_diff < 0:
        north, south, east, west = (
            closest_lat + 0.25,
            closest_lat,
            closest_lon,
            closest_lon - 0.25,
        )
    # 3rd quadrant
    elif lat_diff > 0 and lon_diff > 0:
        north, south, east, west = (
            closest_lat + 0.25,
            closest_lat,
            closest_lon + 0.25,
            closest_lon,
        )
    # 4th quadrant
    else:
        north, south, east, west = (
            closest_lat,
            closest_lat - 0.25,
            closest_lon + 0.25,
            closest_lon,
        )

    return north, south, east, west
