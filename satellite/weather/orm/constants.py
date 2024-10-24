from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent
ADM_DB = BASE_DIR / "data/ADM.duckdb"
GPKGS_DIR = BASE_DIR / "data/gpkgs/"


# ALPHA 3 CODES
ADM0_OPTIONS = {"BRA": "Brazil", "ARG": "Argentina"}

DATASET_FPATH_SUFFIX = [".zip", ".nc"]  # ".grib"]
