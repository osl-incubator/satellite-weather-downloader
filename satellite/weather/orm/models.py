from abc import ABC, abstractmethod
from typing import TypeVar, Type, List, Optional
import inspect

import pandas as pd
import duckdb

from satellite.weather.orm import functional, types


ADM = TypeVar("ADM", bound="ADMBase")


def init_db():
    for adm in [ADM0, ADM1, ADM2]:
        adm.drop_table()
        adm.create_table()

    df = pd.DataFrame.from_dict({"code": ["BRA"], "name": ["Brazil"]})

    with functional.session() as session:
        session.execute(f"INSERT INTO {ADM0.__tablename__} SELECT * FROM df")
        session.commit()

    bra = ADM0.get(code="BRA")
    assert bra.name == "Brazil"

    df = pd.DataFrame.from_dict(
        {"code": [123], "name": ["SP"], "adm0": ["BRA"]}
    )

    with functional.session() as session:
        session.execute(f"INSERT INTO {ADM1.__tablename__} SELECT * FROM df")
        session.commit()

    sp = ADM1.get(code=123)
    assert sp.name == "SP"
    assert sp.adm0.name == bra.name

    df = pd.DataFrame.from_dict(
        {"code": [12345], "name": ["Foo Bar"], "adm0": ["BRA"], "adm1": [123]}
    )

    with functional.session() as session:
        session.execute(f"INSERT INTO {ADM2.__tablename__} SELECT * FROM df")
        session.commit()

    foo = ADM2.get(code=12345, adm1=123)
    assert foo.name == "Foo Bar"
    assert foo.adm0.name == bra.name
    assert foo.adm1.name == sp.name


class ADMBase(ABC):
    __tablename__: str
    __fields__: list[str]  # NOTE: Must be indexed as the same as the attrs

    code: str | int
    name: str
    adm0: Optional["ADM0"]
    adm1: Optional["ADM1"]
    adm2: Optional["ADM2"]

    def __str__(self) -> str:
        return str(self.code)

    def __repr__(self) -> str:
        return self.name

    @abstractmethod
    def __init__(self) -> None: ...

    @abstractmethod
    def geometry(self): ...

    @classmethod
    def get(cls: Type[ADM], **params) -> Type[ADM]:
        with functional.session() as session:
            res = cls._query(session=session, **params)

            if len(res) > 1:
                raise ValueError(
                    f"{cls} for query {params} found multiple entries"
                )

            data = res.fetchone()

        if not data:
            raise ValueError(f"{cls} for query {params} not found")

        instance = cls.__new__(cls)

        for field, value in zip(cls.__fields__, data):
            setattr(instance, field, value)

        return instance

    @classmethod
    def filter(cls: Type[ADM], **kwargs) -> List[Type[ADM]]:
        ...

    @classmethod
    def _query(cls: Type[ADM], session, **kwargs) -> duckdb.DuckDBPyRelation:
        where_params = (
            " AND ".join([f"{p} = '{v}'" for p, v in kwargs.items()])
        )
        where = f"WHERE {where_params}"
        select = f"SELECT * FROM {cls.__tablename__} "
        query = select + where if kwargs else select
        return session.sql(query)

    @classmethod
    def create_table(cls):
        fields = cls._get_class_fields(cls.__fields__)  # noqa
        columns = []
        for field, _type in fields.items():
            # TODO: should add fk?
            if _type == ADM0:
                _type = str

            if _type in [ADM1, ADM2]:
                _type = int

            query = f"{field} {duckdb.typing.DuckDBPyType(_type)}"

            if field == "code":
                query += " PRIMARY KEY"

            columns.append(query)

        with functional.session() as session:
            session.execute(f"""
                CREATE TABLE {cls.__tablename__} ({", ".join(columns)})
            """)
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
        return {
            k: v for k, v in
            inspect.get_annotations(cls).items()
            if k in fields
        }


class ADM0(ADMBase):
    __tablename__ = "adm0"
    __fields__ = ["code", "name"]

    code: str
    name: str

    def __init__(self) -> None:
        if not all([self.code, self.name]):
            raise ValueError(
                f"Bad {type(self)} instantiation, "
                f"please use {type(self)}.get() instead"
            )

    def geometry(self):
        raise NotImplementedError()


class ADM1(ADMBase):
    __tablename__ = "adm1"
    __fields__ = ["code", "name", "adm0"]

    code: int
    name: str
    adm0: ADM0

    def __init__(self) -> None:
        if not all([self.code, self.name, self.adm0]):
            raise ValueError(
                "Bad ADM1 initialization, please use ADM1.get() instead"
            )

    def geometry(self):
        raise NotImplementedError()

    @classmethod
    def get(cls: Type[ADM], **params) -> Optional[ADM]:
        res = super().get(**params)
        if res:
            res.adm0 = ADM0.get(code=res.adm0)  # noqa
        return res


class ADM2(ADMBase):
    __tablename__ = "adm2"
    __fields__ = ["code", "name", "adm0", "adm1"]

    code: int
    name: str
    adm0: ADM0
    adm1: ADM1

    def __init__(self) -> None:
        if not all([self.code, self.name, self.adm0]):
            raise ValueError(
                "Bad ADM2 initialization, please use ADM2.get() instead"
            )

    def geometry(self):
        raise NotImplementedError()

    @classmethod
    def get(cls: Type[ADM], **params) -> Optional[ADM]:
        res = super().get(**params)
        if res:
            res.adm0 = ADM0.get(code=res.adm0)  # noqa
            res.adm1 = ADM1.get(code=res.adm1, adm0=res.adm0.code)  # noqa
        return res
