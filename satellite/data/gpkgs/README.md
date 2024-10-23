gpkg files should have the following specifications:

- file name format: `{ADM0 code}.gpkg`
- columns: `adm1` (ADM1 code), `adm2` (ADM2 code) and `geometry` (shape)
- the values in the `adm1` and `adm2` columns should match the values in `ADM.duckdb`
- the `geometry` column should reference the smallest region, in this case ADM2
