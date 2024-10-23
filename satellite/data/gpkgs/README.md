## Geometry data

The .gpkg files should have the following specifications:
- file name format: `{ADM0 code}.gpkg`
- columns: `adm1` (ADM1 code), `adm2` (ADM2 code) and `geometry` (shape)
- the values in the `adm1` and `adm2` columns should match the values in `ADM.duckdb`/`orm`
- the `geometry` column should reference the smallest region, in this case ADM2


### ARG

source: [PoliticaArgentina/data_warehouse](https://github.com/PoliticaArgentina/data_warehouse/blob/master/geoAr/data_raw/censos/censo_2010.geojson)
