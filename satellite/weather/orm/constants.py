from pathlib import Path

ADM_DB = Path(__file__).parent.parent.parent / "data/ADM.duckdb"

# ALPHA 3 CODES
ADM0_OPTIONS = {"BRA": "Brazil", "ARG": "Argentina"}

DATASET_FPATH_SUFFIX = [".zip", ".nc"]  # ".grib"]
