import numpy as np
import pandas as pd
import xarray as xr
import metpy.calc as mpcalc
from metpy.units import units
from satellite_weather.utils import (
    extract_coordinates,
    extract_latlons,
)

xr.set_options(keep_attrs=True)


@xr.register_dataset_accessor('copebr')
class CopeBRDatasetExtension:
    """
    This class is an `xr.Dataset` extension. It works as a dataset
    layer with the purpose of enhancing the dataset with new methods.
    The expect input dataset is an `netCDF4` file from Copernicus API;
    this extension will work on certain data variables, the method that
    extracts with the correct parameters can be found in `extract_reanalysis`
    module.

    Usage:

        ```
        import satellite_weather_downloader as sat
        ds = sat.load_dataset('file/path')
        RJ_geocode = 3304557
        rio_df = ds.copebr.to_dataframe(RJ_geocode)
        rio_ds = ds.copebr.ds_from_geocode(RJ_geocode)
        ```

    The original dataset will be parsed into Brazilian's data format and can
    be sliced by a Geocode from any City in Brazil, according to IBGE geocodes.
    The expect output when the requested data is not `raw` is:

    date       : datetime object.
    temp_min   : Minimum┐
    temp_med   : Average├─ temperature in `celcius degrees` given a geocode.
    temp_max   : Maximum┘
    precip_min : Minimum┐
    precip_med : Average├─ of total precipitation in `mm` given a geocode.
    precip_max : Maximum┘
    pressao_min: Minimum┐
    pressao_med: Average├─ sea level pressure in `hPa` given a geocode.
    pressao_max: Maximum┘
    umid_min   : Minimum┐
    umid_med   : Average├─ percentage of relative humidity given a geocode.
    umid_max   : Maximum┘
    """

    def __init__(self, xarray_ds: xr.Dataset) -> None:
        self._ds = xarray_ds

    def ds_from_geocode(self, geocode: int, raw=False) -> xr.Dataset:
        """
        This is the most important method of the extension. It will
        slice the dataset according to the geocode provided, do the
        math and the parse of the units to Br's format, and reduce by
        min, mean and max by day, if the `raw` is false.
        Attrs:
            geocode (str or int): Geocode of a city in Brazil according to IBGE.
            raw (bool)          : If raw is set to True, the DataFrame returned will
                                  contain data in 3 hours intervals. Default return
                                  will aggregate these values into 24 hours interval.
        Returns:
            xr.Dataset: The final dataset with the data parsed into Br's format.
                        if not `raw`, will group the data by day, taking it's 
                        min, mean and max values. If `raw`, the data corresponds
                        to a 3h interval range for each day in the dataset.
        """
        lats, lons = self._get_latlons(geocode)

        geocode_ds = self._convert_to_br_units(
            self._slice_dataset_by_coord(
                dataset=self._ds, lats=lats, lons=lons
            )
        )

        if raw:
            return geocode_ds

        gb = geocode_ds.resample(time='1D')

        gmin, gmean, gmax = (
            self._reduce_by(gb, np.min, 'min'),
            self._reduce_by(gb, np.mean, 'med'),
            self._reduce_by(gb, np.max, 'max'),
        )

        final_ds = xr.combine_by_coords([gmin, gmean, gmax], data_vars='all')

        return final_ds

    def to_dataframe(self, geocode: int, raw=False) -> pd.DataFrame:
        """
        Returns a DataFrame with the values related to the geocode of a brazilian
        city according to IBGE's format. Extract the values using `ds_from_geocode()`
        and return `xr.Dataset.to_dataframe()` from Xarray, inserting the geocode into
        the final DataFrame.
        Attrs:
            geocode (str or int): Geocode of a city in Brazil according to IBGE.
            raw (bool)          : If raw is set to True, the DataFrame returned will
                                  contain data in 3 hours intervals. Default return
                                  will aggregate these values into 24 hours interval.
        Returns:
            pd.DataFrame: Similar to `ds.copebr.ds_from_geocode(geocode).to_dataframe()`
                          but with an extra column with the geocode, in order to differ
                          the data when inserting into a database, for instance.
        """
        ds = self.ds_from_geocode(geocode, raw)
        df = ds.to_dataframe()
        geocodes = [geocode for g in range(len(df))]
        df.insert(0, 'geocodigo', geocodes)
        return df

    def _get_latlons(self, geocode: int) -> tuple:
        """
        Extract Latitude and Longitude from a Brazilian's city
        according to IBGE's geocode format.
        """
        lat, lon = extract_latlons.from_geocode(int(geocode))
        N, S, E, W = extract_coordinates.from_latlon(lat, lon)

        lats = [N, S]
        lons = [E, W]

        match geocode:
            case 4108304:   # Foz do Iguaçu
                lats = [-25.5]
                lons = [-54.5, -54.75]

        return lats, lons

    def _slice_dataset_by_coord(
        self, dataset: xr.Dataset, lats: list[int], lons: list[int]
    ) -> xr.Dataset:
        """
        Slices a dataset using latitudes and longitudes, returns a dataset
        with the mean values between the coordinates.
        """
        ds = dataset.sel(latitude=lats, longitude=lons, method='nearest')
        return ds.mean(dim=['latitude', 'longitude'])

    def _convert_to_br_units(self, dataset: xr.Dataset) -> xr.Dataset:
        """
        Parse the units according to Brazil's standard unit measures.
        Rename their unit names and long names as well.
        """
        ds = dataset
        vars = list(ds.data_vars.keys())

        if 't2m' in vars:
            # Convert Kelvin to Celsius degrees
            ds['t2m'] = ds.t2m - 273.15
            ds['t2m'].attrs = {'units': 'degC', 'long_name': 'Temperatura'}
        if 'tp' in vars:
            # Convert meters to millimeters
            ds['tp'] = ds.tp * 1000
            ds['tp'] = ds.tp.round(5)
            ds['tp'].attrs = {'units': 'mm', 'long_name': 'Precipitação'}
        if 'msl' in vars:
            # Convert Pa to ATM
            ds['msl'] = ds.msl * 0.00000986923
            ds['msl'].attrs = {
                'units': 'atm',
                'long_name': 'Pressão ao Nível do Mar',
            }
        if 'd2m' in vars:
            # Calculate Relative Humidity percentage and add to Dataset
            ds['d2m'] = ds.d2m - 273.15
            rh = (
                mpcalc.relative_humidity_from_dewpoint(
                    ds['t2m'] * units.degC, ds['d2m'] * units.degC
                )
                * 100
            )
            # Replacing the variable instead of dropping. d2m won't be used.
            ds['d2m'] = rh
            ds['d2m'].attrs = {
                'units': 'pct',
                'long_name': 'Umidade Relativa do Ar',
            }

        with_br_vars = {
            't2m': 'temp',
            'tp': 'precip',
            'msl': 'pressao',
            'd2m': 'umid',
        }

        return ds.rename(with_br_vars)

    def _reduce_by(self, ds: xr.Dataset, func, prefix: str):
        """
        Applies a function to each coordinate in the dataset and
        replace the `data_vars` names to it's corresponding prefix.
        """
        ds = ds.apply(func=func)
        return ds.rename(
            dict(
                zip(
                    list(ds.data_vars),
                    list(map(lambda x: f'{x}_{prefix}', list(ds.data_vars))),
                )
            )
        )
