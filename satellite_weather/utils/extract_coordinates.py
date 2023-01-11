"""
Find the four closest coordinates given a Latitude and Longitude.

NetCDF files store data in intervals of 0.25 degree, or 28 kilometers. Copernicus
API allows a coordinates slice when retrieving the file. Because this distance
range, some coordinates found when rounding the degrees can be distant from the
city coordinates. To manage that, the four closest points from the latitude and
longitude of the city are retrieved, later on the weather variables will be an
average from these four coordinates. More information about how this method works
can be found in the jupyter notebook of the project.

Methods:
    do_area(latitude, longitude) : Returns North, South, East and West given a
                                   coordinate.
"""

from satellite_weather.utils.globals import LATITUDES, LONGITUDES


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
    closest_lat = min(LATITUDES, key=lambda x: abs(x - lat))
    closest_lon = min(LONGITUDES, key=lambda x: abs(x - lon))

    first_quadr = lat - closest_lat < 0 and lon - closest_lon < 0
    second_quadr = lat - closest_lat > 0 and lon - closest_lon < 0
    third_quadr = lat - closest_lat > 0 and lon - closest_lon > 0
    fourth_quadr = lat - closest_lat < 0 and lon - closest_lon > 0

    if first_quadr:
        north, east = closest_lat, closest_lon
        i_south = [i - 1 for i, x in enumerate(LATITUDES) if x == north].pop()
        i_west = [i - 1 for i, y in enumerate(LONGITUDES) if y == east].pop()
        south, west = LATITUDES[i_south], LONGITUDES[i_west]

    if second_quadr:
        north, west = closest_lat, closest_lon
        i_south = [i - 1 for i, x in enumerate(LATITUDES) if x == north].pop()
        i_east = [i + 1 for i, y in enumerate(LONGITUDES) if y == west].pop()
        south, east = LATITUDES[i_south], LONGITUDES[i_east]

    if third_quadr:
        south, west = closest_lat, closest_lon
        i_north = [i + 1 for i, x in enumerate(LATITUDES) if x == south].pop()
        i_east = [i + 1 for i, y in enumerate(LONGITUDES) if y == west].pop()
        north, east = LATITUDES[i_north], LONGITUDES[i_east]

    if fourth_quadr:
        south, east = closest_lat, closest_lon
        i_north = [i - 1 for i, x in enumerate(LATITUDES) if x == south].pop()
        i_west = [i - 1 for i, y in enumerate(LONGITUDES) if y == east].pop()
        north, west = LATITUDES[i_north], LONGITUDES[i_west]

    return north, south, east, west
