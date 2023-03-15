from functools import lru_cache
from pathlib import Path
from typing import Optional, Union
from loguru import logger
import geopandas
import pandas as pd


_SHAPEFILE = Path(__file__).parent / 'areas_dsei.shp'

DSEIs = {
    1: 'Alagoas e Sergipe',
    2: 'Amapá e Norte do Pará',
    3: 'Altamira',
    4: 'Alto Rio Juruá',
    5: 'Alto Rio Purus',
    6: 'Alto Rio Negro',
    7: 'Alto Rio Solimões',
    8: 'Araguaia',
    9: 'Bahia',
    10: 'Ceará',
    11: 'Minas Gerais e Espirito Santo',
    12: 'Vale do Javari',
    13: 'Kaiapó do Pará',
    15: 'Leste de Roraima',
    17: 'Manaus',
    18: 'Guamá-Tocantins',
    19: 'Maranhão',
    20: 'Mato Grosso do Sul',
    21: 'Médio Rio Purus',
    22: 'Parintins',
    23: 'Pernambuco',
    24: 'Porto Velho',
    25: 'Potiguara',
    26: 'Cuiabá',
    27: 'Rio Tapajós',
    28: 'Médio Rio Solimões e Afluentes',
    29: 'Tocantins',
    30: 'Vilhena',
    31: 'Xavante',
    32: 'Xingu',
    33: 'Yanomami',
    34: 'Kaipó do Mato Grosso',
    35: 'Litoral Sul',
    36: 'Interior Sul',
}


DSEI_DF = pd.DataFrame(
    dict(
        DSEI=list(DSEIs.values()), 
        code=list(DSEIs)
    ), 
    index=list(range(len(DSEIs)))
)


@lru_cache
def load_polygons_df() -> geopandas.GeoDataFrame:
    df = geopandas.read_file(
        _SHAPEFILE,
        encoding='iso-8859-2',
        crs='epsg:4326' #WGS84
    )
    return df
