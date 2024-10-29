from abc import ABC, abstractmethod
from typing import Union, Optional

import pandas as pd
import numpy as np
import xarray as xr
import xagg as xa
import trio
from loguru import logger
from epiweeks import Week

from satellite.geo.models import ADM, ADMBase

xr.set_options(keep_attrs=True)


class CopeExtensionBase(ABC):
    """
    This class is an `xr.Dataset` extension base class. It's children will
    works as a dataset layer with the purpose of enhancing the xarray dataset
    with new methods. The expect input dataset is an `netCDF4` file from
    Copernicus API; this extension will work on certain data variables,
    the method that extracts with the correct parameters can be found in
    `extract_reanalysis` module.

    Usage:
    ```
    ds.cope.to_dataframe(ADM)
    ds.cope.adm_ds(ADM)
    ```
    See also: satellite.ADM2

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
    def to_dataframe(self, adms: Union[list[ADM], ADM]) -> pd.DataFrame:
        pass

    @abstractmethod
    def adm_ds(self, adm: ADM) -> xr.Dataset:
        pass

    @abstractmethod
    def to_sql(self, adms, con, tablename, schema, raw, **kwargs) -> None:
        """
        Reads the data for each geocode and insert the rows into the
        database one by one, created by sqlalchemy engine with the URI.
        This method is convenient to prevent the memory overhead when
        executing with a large amount of geocodes.
        """
        pass


@xr.register_dataset_accessor("cope")
class CopeExtension(CopeExtensionBase):
    def __init__(self, xarray_ds: xr.Dataset):
        self._ds = xarray_ds

    def to_dataframe(self, adms: Union[list[ADM], ADM]) -> pd.DataFrame:
        return trio.run(self._ato_dataframe, adms)

    def to_sql(
        self,
        adms: Union[list[int], int],
        con,
        tablename: str,
        schema: Optional[str] = None,
        raw: bool = False,
        verbose: bool = True,
    ) -> None:
        adms = [adms] if isinstance(adms, ADMBase) else adms
        trio.run(self._ato_sql_async, adms, con, tablename, schema, verbose)

    async def _ato_dataframe(self, adms: Union[list[ADM], ADM]) -> pd.DataFrame:
        adms = [adms] if isinstance(adms, ADMBase) else adms
        dfs = []
        async with trio.open_nursery() as nursery:
            for adm in adms:
                nursery.start_soon(self._adm_dataframe, adm, dfs)
        return pd.concat(dfs)

    async def _adm_dataframe(self, adm: ADM, dfs: list) -> None:
        ds = await trio.to_thread.run_sync(self.adm_ds, adm)
        df = ds.to_dataframe().reset_index()
        df = df.assign(epiweek=str(Week.fromdate(pd.to_datetime(df.time)[0])))
        columns_to_round = list(
            set(df.columns).difference(set(["time", "code", "name", "epiweek"]))
        )
        df[columns_to_round] = df[columns_to_round].map(lambda x: np.round(x, 4))
        df = df.drop(columns=["poly_idx", "name"])
        df = df.rename(columns={"time": "date", "code": "geocode"})
        dfs.append(df)

    async def _ato_sql_async(
        self,
        adms: Union[list[int], int],
        con,
        tablename: str,
        schema: Optional[str] = None,
        verbose: bool = True,
    ) -> None:
        async with trio.open_nursery() as nursery:
            for adm in adms:
                nursery.start_soon(
                    self._adm_to_sql, adm, con, tablename, schema, verbose
                )

    async def _adm_to_sql(
        self,
        adm: ADM,
        con,
        tablename: str,
        schema: Optional[str],
        verbose: bool,
    ) -> None:
        df = await self._ato_dataframe(adm)
        df.to_sql(
            name=tablename,
            schema=schema,
            con=con,
            if_exists="append",
            index=False,
        )
        if verbose:
            logger.info(
                f"{adm.code} updated on {schema + '.' if schema else ''}{tablename}"
            )

    def adm_ds(self, adm: ADM):
        return _adm_ds(ds=self._ds, adm=adm)


def _adm_ds(ds: xr.Dataset, adm: ADM) -> xr.Dataset:
    ds = _convert_units(ds)
    weightmap = xa.pixel_overlaps(ds, adm.to_dataframe(), silent=True)
    ds = xa.aggregate(ds, weightmap, silent=True).to_dataset().sortby("time")
    gb = ds.resample(time="1D")
    gmin, gmean, gmax, gtot = (
        _reduce_by(gb, np.min, "min"),
        _reduce_by(gb, np.mean, "med"),
        _reduce_by(gb, np.max, "max"),
        _reduce_by(gb, np.sum, "tot"),
    )
    return xr.combine_by_coords(
        [ds.code, ds.name, gmin, gmean, gmax, gtot.precip_tot], data_vars="all"
    )


def _reduce_by(ds: xr.Dataset, func, prefix: str) -> xr.Dataset:
    ds = ds.apply(func=func).drop_vars(
        ["code", "name", "adm1", "adm0"], errors="ignore"
    )

    return ds.rename(
        dict(
            zip(
                list(ds.data_vars),
                list(map(lambda x: f"{x}_{prefix}", list(ds.data_vars))),
            )
        )
    )


def _convert_units(ds: xr.Dataset) -> xr.Dataset:
    _ds = ds.copy()
    del ds
    _vars = list(_ds.data_vars.keys())

    parsed_vars = {
        "valid_time": "time",
    }

    if "t2m" in _vars:
        _ds["t2m"] = _ds.t2m - 273.15
        _ds["t2m"].attrs = {"units": "degC", "long_name": "Temperatura"}
        parsed_vars["t2m"] = "temp"

        if "d2m" in _vars:
            _ds["d2m"] = _ds.d2m - 273.15

            e = 6.112 * np.exp(17.67 * _ds.d2m / (_ds.d2m + 243.5))
            es = 6.112 * np.exp(17.67 * _ds.t2m / (_ds.t2m + 243.5))
            rh = (e / es) * 100

            _ds["d2m"] = rh
            _ds["d2m"].attrs = {
                "units": "pct",
                "long_name": "Umidade Relativa do Ar",
            }
            parsed_vars["d2m"] = "umid"

    if "tp" in _vars:
        _ds["tp"] = _ds.tp * 1000
        _ds["tp"] = _ds.tp.round(5)
        _ds["tp"].attrs = {"units": "mm", "long_name": "Precipitação"}
        parsed_vars["tp"] = "precip"

    if "sp" in _vars:
        _ds["sp"] = _ds.sp * 0.00000986923
        _ds["sp"].attrs = {
            "units": "atm",
            "long_name": "Pressão ao Nível do Mar",
        }
        parsed_vars["sp"] = "pressao"

    return _ds.rename(parsed_vars)
