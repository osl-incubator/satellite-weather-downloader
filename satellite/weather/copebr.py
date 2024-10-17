from abc import ABC, abstractmethod
from typing import Union

import pandas as pd
import dask
import dask.array as da
import dask.dataframe as dd
import numpy as np
import xarray as xr
from loguru import logger
from epiweeks import Week
from sqlalchemy.engine import Connectable

from .utils import extract_latlons, extract_coordinates

xr.set_options(keep_attrs=True)


class CopeDatasetExtensionBase(ABC):
    """
    This class is an `xr.Dataset` extension base class. It's children will
    works as a dataset layer with the purpose of enhancing the xarray dataset
    with new methods. The expect input dataset is an `netCDF4` file from
    Copernicus API; this extension will work on certain data variables,
    the method that extracts with the correct parameters can be found in
    `extract_reanalysis` module.

    Usage:
    ```
    import satellite.weather as sat
    ds = sat.load_dataset('file/path')
    ds.Cope<ABV>.to_dataframe(geocode)
    ds.Cope<ABV>.geocode_ds(geocode)
    ```

    The expect output when the requested data is not `raw` is:

    date       : datetime object.
    epiweek    : Epidemiological week (format: YYYYWW)
    temp_min   : Minimum┐
    temp_med   : Average├─ temperature in `celcius degrees` given a geocode.
    temp_max   : Maximum┘
    precip_min : Minimum┐
    precip_med : Average├─ of total precipitation in `mm` given a geocode.
    precip_max : Maximum┘
    precip_tot : Total precipitation in `mm` given a geocode.
    pressao_min: Minimum┐
    pressao_med: Average├─ sea level pressure in `hPa` given a geocode.
    pressao_max: Maximum┘
    umid_min   : Minimum┐
    umid_med   : Average├─ percentage of relative humidity given a geocode.
    umid_max   : Maximum┘
    """

    @abstractmethod
    def geocode_ds(self, geocode, raw, **kwargs) -> xr.Dataset:
        pass

    @abstractmethod
    def to_dataframe(self, geocodes, raw, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def to_sql(self, geocodes, con, tablename, schema, raw, **kwargs) -> None:
        """
        Reads the data for each geocode and insert the rows into the
        database one by one, created by sqlalchemy engine with the URI.
        This method is convenient to prevent the memory overhead when
        executing with a large amount of geocodes.
        """
        pass


@xr.register_dataset_accessor("CopeBR")
class CopeBRDatasetExtension:
    def __init__(self, xarray_ds: xr.Dataset):
        self._ds = xarray_ds
        self.locale = "BR"

    def to_dataframe(self, geocodes: Union[list[int], int], raw: bool = False):
        df = _final_dataframe(
            dataset=self._ds, geocodes=geocodes, locale=self.locale, raw=raw
        )
        if isinstance(df, dask.dataframe.DataFrame):
            df = df.compute()
        df = df.reset_index(drop=True)

        return df

    def to_sql(
        self,
        geocodes: Union[list[int], int],
        con: Connectable,
        tablename: str,
        schema: str,
        raw: bool = False,
        verbose: bool = True,
    ) -> None:
        geocodes = [geocodes] if isinstance(geocodes, int) else geocodes
        for geocode in geocodes:
            _geocode_to_sql(
                dataset=self._ds,
                geocode=geocode,
                con=con,
                schema=schema,
                tablename=tablename,
                locale=self.locale,
                raw=raw,
            )
            if verbose:
                logger.info(f"{geocode} updated on {schema}.{tablename}")

    def geocode_ds(self, geocode: int, raw: bool = False):
        return _geocode_ds(ds=self._ds, geocode=geocode, locale=self.locale, raw=raw)


@xr.register_dataset_accessor("CopeAR")
class CopeARDatasetExtension:
    def __init__(self, xarray_ds: xr.Dataset):
        self._ds = xarray_ds
        self.locale = "AR"

    def to_dataframe(self, geocodes: Union[list[str], str], raw: bool = False):
        df = _final_dataframe(
            dataset=self._ds, geocodes=geocodes, locale=self.locale, raw=raw
        )
        if isinstance(df, dask.dataframe.DataFrame):
            df = df.compute()
        df = df.reset_index(drop=True)
        return df

    def to_sql(
        self,
        geocodes: Union[list[str], str],
        con: Connectable,
        tablename: str,
        schema: str,
        raw: bool = False,
        verbose: bool = True,
    ):
        geocodes = [geocodes] if isinstance(geocodes, int) else geocodes
        for geocode in geocodes:
            _geocode_to_sql(
                dataset=self._ds,
                geocode=geocode,
                con=con,
                schema=schema,
                tablename=tablename,
                raw=raw,
            )
            if verbose:
                logger.info(f"{geocode} updated on {schema}.{tablename}")

    def geocode_ds(self, geocode: str, raw: bool = False):
        return _geocode_ds(self._ds, geocode, self.locale, raw)


def _final_dataframe(
    dataset: xr.Dataset,
    geocodes: Union[list[str | int], int | str],
    locale: str,
    raw=False,
) -> pd.DataFrame:
    geocodes = [geocodes] if isinstance(geocodes, int) else geocodes

    dfs = []
    for geocode in geocodes:
        dfs.append(
            _geocode_to_dataframe(
                dataset=dataset, geocode=geocode, locale=locale, raw=raw
            )
        )

    final_df = dd.concat(dfs)

    if final_df.index.name == "time":
        final_df = final_df.reset_index(drop=False)

    if raw:
        final_df = final_df.rename(columns={"time": "datetime"})
    else:
        final_df = final_df.rename(columns={"time": "date"})

    return final_df


def _geocode_to_sql(
    dataset: xr.Dataset,
    geocode: int,
    con: Connectable,
    schema: str,
    tablename: str,
    locale: str,
    raw: bool,
) -> None:
    df = _geocode_to_dataframe(dataset=dataset, geocode=geocode, locale=locale, raw=raw)
    df = df.reset_index(drop=False)
    if raw:
        df = df.rename(columns={"time": "datetime"})
    else:
        df = df.rename(columns={"time": "date"})

    df.to_sql(
        name=tablename,
        schema=schema,
        con=con,
        if_exists="append",
        index=False,
    )
    del df


def _geocode_to_dataframe(
    dataset: xr.Dataset, geocode: int, locale: str, raw: bool = False
) -> pd.DataFrame:
    """
    Returns a DataFrame with the values related to the geocode of a
    city according to each country's standard. Extract the values
    using `ds_from_geocode()` and return `xr.Dataset.to_dataframe()`
    from Xarray, inserting the geocode into the final DataFrame.
    Attrs:
      geocode (str or int): Geocode of a city
      raw (bool)          : If raw is set to True, the DataFrame returned
                            will contain data in 3 hours intervals.
                            Default return will aggregate these values
                            into 24 hours interval.
    Returns:
      pd.DataFrame: Similar to `ds_from_geocode(geocode).to_dataframe()`
                    but with two extra columns with the geocode and epiweek,
                    the integer columns are also rounded to 4 decimals digits
    """
    ds = _geocode_ds(ds=dataset, geocode=geocode, locale=locale, raw=raw)
    df = ds.to_dataframe()
    del ds
    geocode = [geocode for g in range(len(df))]
    df = df.assign(geocode=da.from_array(geocode))
    df = df.assign(epiweek=str(Week.fromdate(df.index.to_pydatetime()[0])))
    columns_to_round = list(set(df.columns).difference(set(["geocode", "epiweek"])))
    df[columns_to_round] = df[columns_to_round].map(lambda x: np.round(x, 4))
    return df


def _geocode_ds(
    ds: xr.Dataset, geocode: int | str, locale: str, raw=False
) -> xr.Dataset:
    """
    This is the most important method of the extension. It will
    slice the dataset according to the geocode provided, do the
    math and the parse of the units to Br's format, and reduce by
    min, mean and max by day, if the `raw` is false.
    Attrs:
        geocode (str|int): Geocode of a city.
        raw (bool)       : If raw is set to True, the DataFrame returned
                           will contain data in 3 hours intervals. Default
                           return will aggregate these values into 24h
                           interval.
        locale (str)     : Country abbreviation. Example: 'BR'
    Returns:
        xr.Dataset: The final dataset with the data parsed into Br's
                    format. If not `raw`, will group the data by day,
                    taking it's min, mean and max values. If `raw`,
                    the data corresponds to a 3h interval range for
                    each day in the dataset.
    """
    lats, lons = _get_latlons(geocode=geocode, locale=locale)

    geocode_ds = _convert_to_br_units(
        _slice_dataset_by_coord(dataset=ds, lats=lats, lons=lons)
    )

    if raw:
        return geocode_ds

    geocode_ds = geocode_ds.sortby("time")
    gb = geocode_ds.resample(time="1D")
    gmin, gmean, gmax, gtot = (
        _reduce_by(gb, np.min, "min"),
        _reduce_by(gb, np.mean, "med"),
        _reduce_by(gb, np.max, "max"),
        _reduce_by(gb, np.sum, "tot"),
    )

    final_ds = xr.combine_by_coords(
        [gmin, gmean, gmax, gtot.precip_tot], data_vars="all"
    )

    return final_ds


def _slice_dataset_by_coord(
    dataset: xr.Dataset, lats: list[int], lons: list[int]
) -> xr.Dataset:
    """
    Slices a dataset using latitudes and longitudes, returns a dataset
    with the mean values between the coordinates.
    """
    ds = dataset.sel(latitude=lats, longitude=lons, method="nearest")
    return ds.mean(dim=["latitude", "longitude"])


def _convert_to_br_units(dataset: xr.Dataset) -> xr.Dataset:
    """
    Parse measure units. Rename their unit names and long names as well.
    """
    ds = dataset
    _vars = list(ds.data_vars.keys())

    if "t2m" in _vars:
        # Convert Kelvin to Celsius degrees
        ds["t2m"] = ds.t2m - 273.15
        ds["t2m"].attrs = {"units": "degC", "long_name": "Temperatura"}

        if "d2m" in _vars:
            # Calculate Relative Humidity percentage and add to Dataset
            ds["d2m"] = ds.d2m - 273.15

            e = 6.112 * np.exp(17.67 * ds.d2m / (ds.d2m + 243.5))
            es = 6.112 * np.exp(17.67 * ds.t2m / (ds.t2m + 243.5))
            rh = (e / es) * 100

            # Replacing the variable instead of dropping. d2m won't be used.
            ds["d2m"] = rh
            ds["d2m"].attrs = {
                "units": "pct",
                "long_name": "Umidade Relativa do Ar",
            }
    if "tp" in _vars:
        # Convert meters to millimeters
        ds["tp"] = ds.tp * 1000
        ds["tp"] = ds.tp.round(5)
        ds["tp"].attrs = {"units": "mm", "long_name": "Precipitação"}
    if "sp" in _vars:
        # Convert Pa to ATM
        ds["sp"] = ds.sp * 0.00000986923
        ds["sp"].attrs = {
            "units": "atm",
            "long_name": "Pressão ao Nível do Mar",
        }

    parsed_vars = {
        "valid_time": "time",
        "t2m": "temp",
        "tp": "precip",
        "sp": "pressao",
        "d2m": "umid",
    }

    return ds.rename(parsed_vars)


def _reduce_by(ds: xr.Dataset, func, prefix: str):
    """
    Applies a function to each coordinate in the dataset and
    replace the `data_vars` names to it's corresponding prefix.
    """
    ds = ds.apply(func=func)
    # ds = ds.map(lambda x: np.round(x, 4))
    return ds.rename(
        dict(
            zip(
                list(ds.data_vars),
                list(map(lambda x: f"{x}_{prefix}", list(ds.data_vars))),
            )
        )
    )


def _get_latlons(geocode: int | str, locale: str) -> tuple[list[float], list[float]]:
    """
    Extract Latitude and Longitude from a geocode of the specific locale.
    """
    lat, lon = extract_latlons.from_geocode(int(geocode), locale)
    N, S, E, W = extract_coordinates.from_latlon(lat, lon)

    lats = [N, S]
    lons = [E, W]

    if locale == "BR":
        match geocode:
            case 4108304:  # Foz do Iguaçu - BR
                lats = [-25.5]
                lons = [-54.5, -54.75]

            case 3548500:  # Santos (SP) - BR
                lats = [-24.0]
                lons = [-46.25, -46.5]

    return lats, lons
