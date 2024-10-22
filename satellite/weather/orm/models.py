import inspect
from abc import ABC, abstractmethod

import pandas as pd
import duckdb

from satellite.weather.orm import functional, types


def init_db():
    for adm in [ADM0, ADM1]:
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


class Base(ABC):
    __tablename__: str
    __fields__: list[str]  # NOTE: Must be indexed as the same as the attrs

    code: str | int
    name: str

    def __str__(self) -> str:
        return str(self.code)

    def __repr__(self) -> str:
        return self.name

    @abstractmethod
    def __init__(self) -> None: ...

    @abstractmethod
    def geometry(self): ...

    @classmethod
    def get(cls, **params):
        with functional.session() as session:
            res = session.sql(f"""
                SELECT * FROM {cls.__tablename__} WHERE 
                {" AND ".join([f"{p} = '{v}'" for p, v in params.items()])}
            """)

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
    def create_table(cls):
        fields = cls._get_class_fields(cls.__fields__)  # noqa
        columns = []
        for field, _type in fields.items():
            # TODO: should add fk?
            if _type == ADM0:
                _type = str

            if _type in [ADM1]:
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


class ADM0(Base):
    __tablename__ = "adm0"
    __fields__ = ["code", "name"]

    code: str
    name: str

    def __init__(
        self,
        code: types.ADM0Options,
        name: str,
    ) -> None:
        self.code = code
        self.name = name

    def geometry(self):
        raise NotImplementedError()


class ADM1(Base):
    __tablename__ = "adm1"
    __fields__ = ["code", "name", "adm0"]

    code: int
    name: str
    adm0: ADM0

    def __init__(
        self,
        code: int,
        name: str,
        adm0: types.ADM0Options,
    ) -> None:
        self.code = code
        self.name = name
        self.adm0 = ADM0.get(code=adm0)

    def geometry(self):
        raise NotImplementedError()

#
#
# class ADM2(Base):
#     __tablename__ = "ADM2"
#
#     id = Column(Integer, Sequence("ADM2_id_sequence"), primary_key=True)
#     code = Column(Integer, nullable=False)
#     name = Column(String, nullable=False)
#     adm1_id = Column(
#         Integer,
#         ForeignKey("ADM1.id", ondelete="RESTRICT"),
#         nullable=False
#     )
