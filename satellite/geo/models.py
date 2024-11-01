__all__ = ["ADM0", "ADM1", "ADM2"]

from typing import TypeVar, Type, List, Optional
from inspect import get_annotations
from functools import lru_cache
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd
import geopandas as gpd
import duckdb

from satellite.geo import functional, constants


ADM = TypeVar("ADM", bound="ADMBase")


class ADMBase(ABC):
    __tablename__: str
    __fields__: list[str]  # NOTE: Must be indexed as the same as the attrs

    code: str
    name: str
    adm0: Optional["ADM0"]
    adm1: Optional["ADM1"]
    adm2: Optional["ADM2"]

    def __str__(self) -> str:
        return str(self.code)

    def __repr__(self) -> str:
        return self.name

    @abstractmethod
    def to_dataframe(self) -> gpd.GeoDataFrame: ...

    @classmethod
    def get(cls: Type[ADM], **params) -> Type[ADM]:
        with functional.session() as session:
            res = cls._query(session=session, **params)

            if len(res) > 1:
                raise ValueError(
                    f"{cls.__tablename__} for query " f"{params} found multiple entries"
                )

            data = res.fetchone()

        if not data:
            raise ValueError(f"{cls.__tablename__} for query {params} not found")

        instance = cls.__new__(cls)
        for field, value in zip(cls.__fields__, data):
            setattr(instance, field, value)

        return instance

    @classmethod
    def filter(cls: Type[ADM], **params) -> List[Type[ADM]]:
        # TODO: include ADM class initialization when filtering (recursive err)
        with functional.session() as session:
            res = cls._query(session=session, **params)
            data = res.fetchall()

        adms = []
        for item in data:
            instance = cls.__new__(cls)
            for field, value in zip(cls.__fields__, item):
                setattr(instance, field, value)
            adms.append(instance)

        return adms

    @classmethod
    def _query(cls: Type[ADM], session, **kwargs) -> duckdb.DuckDBPyRelation:
        where_params = " AND ".join([f"{p} = '{v}'" for p, v in kwargs.items()])
        where = f"WHERE {where_params}"
        select = f"SELECT * FROM {cls.__tablename__} "
        query = select + where if kwargs else select
        return session.sql(query)

    @staticmethod
    @lru_cache(maxsize=None)
    def _read_gpkg(fpath: Path, locale=None) -> gpd.GeoDataFrame:
        if locale == "BRA":
            chunks = constants.GPKGS_DIR / "BRA"
            dfs = []
            for chunk in chunks.glob("*.gpkg"):
                dfs.append(gpd.read_file(str(chunk), encoding="utf-8"))
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = gpd.read_file(str(fpath), encoding="utf-8")
        return df

    @classmethod
    def create_table(cls):
        fields = cls._get_class_fields(cls.__fields__)
        columns = []
        for field, _type in fields.items():
            if _type in [ADM0, ADM1, ADM2]:
                _type = str

            query = f"{field} {duckdb.typing.DuckDBPyType(_type)}"

            if _type == ADM0 and field == "code":
                query += " PRIMARY KEY"

            query += " NOT NULL"

            columns.append(query)

            if _type == ADM1:
                columns.append("UNIQUE (code, adm0)")

            if _type == ADM2:
                columns.append("UNIQUE (code, adm1)")

        with functional.session() as session:
            session.execute(
                "CREATE TABLE IF NOT EXISTS "
                f"{cls.__tablename__} ({', '.join(columns)})"
            )
            session.commit()

    @classmethod
    def drop_table(cls):  # TODO: REMOVE IT (READ ONLY TABLE)
        with functional.session() as session:
            session.sql(f"DROP TABLE IF EXISTS {cls.__tablename__}")
            session.commit()

    @classmethod
    def _get_class_fields(cls, fields: list[str] = None) -> dict[str, type]:
        if not fields:
            raise ValueError("a list of fields must be provided")
        return {k: v for k, v in get_annotations(cls).items() if k in fields}


class ADM0(ADMBase):
    __tablename__ = "adm0"
    __fields__ = ["code", "name"]

    code: str
    name: str

    def __init__(self) -> None:
        raise ValueError("bad ADM0 instantiation, use ADM0.get() instead")

    def to_dataframe(self) -> gpd.GeoDataFrame:
        gpkg = constants.GPKGS_DIR / f"{self.code}.gpkg"
        gdf = self._read_gpkg(gpkg, locale=self.code)
        gdf = gdf.dissolve()
        if len(gdf) != 1:
            raise ValueError("expects only one row as output")
        res = gdf.copy().drop(columns=["adm2", "adm1"])
        res.loc[0, "code"] = self.code
        res.loc[0, "name"] = self.name
        return res[self.__fields__ + ["geometry"]]


class ADM1(ADMBase):
    __tablename__ = "adm1"
    __fields__ = ["code", "name", "adm0"]

    code: str
    name: str
    adm0: ADM0

    def __init__(self) -> None:
        raise ValueError("bad ADM1 initialization, use ADM1.get() instead")

    def to_dataframe(self) -> gpd.GeoDataFrame:
        adm0 = self.adm0.code if isinstance(self.adm0, ADM0) else self.adm0
        gpkg = constants.GPKGS_DIR / f"{adm0}.gpkg"
        gdf = self._read_gpkg(gpkg, locale=adm0)
        gdf = gdf[gdf["adm1"] == self.code]
        gdf = gdf.dissolve(by="adm1", as_index=False).reset_index(drop=True)
        if len(gdf) != 1:
            raise ValueError("expects only one row as output")
        res = gdf.copy().drop(columns=["adm2"])
        res = res.rename(columns={"adm1": "code"})
        res.loc[0, "name"] = self.name
        res.loc[0, "adm0"] = adm0
        return res[self.__fields__ + ["geometry"]]

    @classmethod
    def get(cls: Type[ADM], **params) -> Optional[ADM]:
        res = super().get(**params)
        if res:
            res.adm0 = ADM0.get(code=res.adm0)
        return res


class ADM2(ADMBase):
    __tablename__ = "adm2"
    __fields__ = ["code", "name", "adm0", "adm1"]

    code: str
    name: str
    adm0: ADM0
    adm1: ADM1

    def __init__(self) -> None:
        raise ValueError("bad ADM2 initialization, use ADM2.get() instead")

    def to_dataframe(self) -> gpd.GeoDataFrame:
        adm0 = self.adm0.code if isinstance(self.adm0, ADM0) else self.adm0
        adm1 = self.adm1.code if isinstance(self.adm1, ADM1) else self.adm1
        gpkg = constants.GPKGS_DIR / f"{adm0}.gpkg"
        gdf = self._read_gpkg(gpkg, locale=adm0)
        gdf = gdf[(gdf["adm1"] == adm1) & (gdf["adm2"] == self.code)]
        if len(gdf) != 1:
            raise ValueError("expects only one row as output")
        res = gdf.copy().reset_index(drop=True)
        res = res.rename(columns={"adm2": "code"})
        res.loc[0, "name"] = self.name
        res.loc[0, "adm0"] = adm0
        res.loc[0, "adm1"] = adm1
        return res[self.__fields__ + ["geometry"]]

    @classmethod
    def get(cls: Type[ADM], **params) -> Optional[ADM]:
        res = super().get(**params)
        if res:
            res.adm0 = ADM0.get(code=res.adm0)
            res.adm1 = ADM1.get(code=res.adm1, adm0=res.adm0.code)
        return res
