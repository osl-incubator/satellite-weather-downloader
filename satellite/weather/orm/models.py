from sqlalchemy import Column, Integer, String, ForeignKey, Sequence, create_engine, text, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from geoalchemy2 import Geometry as GeoGeometry


DB_FILE = "/home/bida/Projetos/InfoDengue/satellite-weather-downloader/satellite/data/boundaries.duckdb"
Base = declarative_base()


def init_db():
    eng = create_engine(
        f"duckdb:///{DB_FILE}",
        connect_args={'preload_extensions': ['spatial']}
    )

    Base.metadata.drop_all(bind=eng)  # TODO: REMOVE IT
    Base.metadata.create_all(tables=[ADM0, ADM1, ADM2], bind=eng)

    session = Session(bind=eng)
    try:
        session.execute(text("""
            CREATE TABLE geometry (
                id INTEGER PRIMARY KEY,
                adm0_id INTEGER NOT NULL REFERENCES ADM0(id) ON DELETE RESTRICT,
                adm1_id INTEGER REFERENCES ADM1(id) ON DELETE RESTRICT,
                adm2_id INTEGER REFERENCES ADM2(id) ON DELETE RESTRICT,
                geom GEOMETRY NOT NULL
            );
        """))
        session.add(ADM0(code='BR', name='Brazil'))
        session.commit()

        adm0_id = session.query(ADM0).filter_by(code='BR').one().id

        session.add(ADM1(code=1, name='São Paulo', adm0_id=adm0_id))
        session.add(ADM2(code=35, name='Campinas', adm1_id=adm0_id))
        session.commit()

        session.add(Geometry(
            adm0_id=adm0_id,
            geom='POLYGON((-46.65 -23.55, -46.65 -23.4, -46.5 -23.55, -46.65 -23.55))'
        ))
        session.commit()

        return session.query(Geometry).one()
    finally:
        session.close()


class ADM0(Base):
    __tablename__ = "ADM0"

    id = Column(Integer, Sequence("ADM0_id_sequence"), primary_key=True)
    code = Column(String, nullable=False, unique=True)
    full_name = Column(String, nullable=False, unique=True)

    @property
    def name(self):
        return self.__tablename__


class ADM1(Base):
    __tablename__ = "ADM1"

    id = Column(Integer, Sequence("ADM1_id_sequence"), primary_key=True)
    code = Column(Integer, nullable=False)
    full_name = Column(String, nullable=False)
    adm0_id = Column(
        Integer,
        ForeignKey("ADM0.id", ondelete="RESTRICT"),
        nullable=False
    )

    @property
    def name(self):
        return self.__tablename__


class ADM2(Base):
    __tablename__ = "ADM2"

    id = Column(Integer, Sequence("ADM2_id_sequence"), primary_key=True)
    code = Column(Integer, nullable=False)
    full_name = Column(String, nullable=False)
    adm1_id = Column(
        Integer,
        ForeignKey("ADM1.id", ondelete="RESTRICT"),
        nullable=False
    )

    @property
    def name(self):
        return self.__tablename__


class Geometry(Base):
    __tablename__ = "geometry"

    id = Column(Integer, Sequence("geometry_id_sequence"), primary_key=True)
    adm0_id = Column(
        Integer,
        ForeignKey("ADM0.id", ondelete="RESTRICT"),
        nullable=False
    )
    adm1_id = Column(
        Integer,
        ForeignKey("ADM1.id", ondelete="RESTRICT"),
        nullable=True
    )
    adm2_id = Column(
        Integer,
        ForeignKey("ADM2.id", ondelete="RESTRICT"),
        nullable=True
    )
    geom = Column(GeoGeometry, nullable=False)
