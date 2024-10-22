import inspect
from abc import ABC, abstractmethod

import pandas as pd
import duckdb

from satellite.weather.orm import functional, constants, types


def init_db():
    ADM0.drop_table()
    ADM0.create_table()

    df = pd.DataFrame.from_dict({"code": ["BRA"], "full_name": ["Brazil"]})

    with functional.session() as session:
        session.execute(f"INSERT INTO {ADM0.__tablename__} SELECT * FROM df")
        session.commit()

    bra = ADM0.get(code="BRA")
    print(bra.full_name)


class Base(ABC):
    __tablename__: str
    __fields__: list[str]  # NOTE: Must be indexed as the same

    @abstractmethod
    def __init__(self) -> None: ...

    @abstractmethod
    def geometry(self): ...

    @classmethod
    def get(cls, **params):
        with functional.session() as session:
            data = session.sql(f"""
                SELECT * FROM {cls.__tablename__} 
                WHERE {" AND ".join([f"{p} = {v}" for p, v in params.items()])}'
            """).fetchone()
        if not data:
            raise ValueError(f"{cls} for query {params} not found")

        return cls(**data)

    @classmethod
    def create_table(cls):
        fields = cls._get_class_fields(cls.__fields__)  # noqa
        columns = []
        for field, _type in fields.items():
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
    __fields__ = ["code", "full_name"]

    code: str
    full_name: str

    def __init__(self, code: types.ADM0Options, full_name: str) -> None:
        self.code = code
        self.full_name = full_name

#
# class ADM1(Base):
#     __tablename__ = "ADM1"
#
#     id = Column(Integer, Sequence("ADM1_id_sequence"), primary_key=True)
#     code = Column(Integer, nullable=False)
#     full_name = Column(String, nullable=False)
#     adm0_id = Column(
#         Integer,
#         ForeignKey("ADM0.id", ondelete="RESTRICT"),
#         nullable=False
#     )
#
#
# class ADM2(Base):
#     __tablename__ = "ADM2"
#
#     id = Column(Integer, Sequence("ADM2_id_sequence"), primary_key=True)
#     code = Column(Integer, nullable=False)
#     full_name = Column(String, nullable=False)
#     adm1_id = Column(
#         Integer,
#         ForeignKey("ADM1.id", ondelete="RESTRICT"),
#         nullable=False
#     )
