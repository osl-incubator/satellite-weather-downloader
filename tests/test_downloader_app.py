import satellite_weather as sat
from satellite_weather import __version__


def test_version():
    assert __version__ == '0.1.0'  # changed by semantic-release


def test_import_package():
    from satellite_downloader import extract_reanalysis

    assert (
        extract_reanalysis.__name__
        == 'satellite_weather_downloader.extract_reanalysis'
    )


def test_extract_latlons_from_geocode_n_extract_coord_from_latlons():
    from satellite_weather.utils.extract_coordinates import from_latlon
    from satellite_weather.utils.extract_latlons import from_geocode

    latlon_rio_de_janeiro = from_geocode(3304557)
    latlon_florianopolis = from_geocode(4205407)

    coord_rio_de_janeiro = from_latlon(
        latlon_rio_de_janeiro[0], latlon_rio_de_janeiro[1]
    )
    coord_florianopolis = from_latlon(
        latlon_florianopolis[0], latlon_florianopolis[1]
    )

    assert latlon_rio_de_janeiro == (-22.9129, -43.2003)
    assert latlon_florianopolis == (-27.5945, -48.5477)
    assert coord_rio_de_janeiro == (-22.75, -23.0, -43.0, -43.25)
    assert coord_florianopolis == (-27.5, -27.75, -48.5, -48.75)


def test_download_netcdf_file():
    ...
