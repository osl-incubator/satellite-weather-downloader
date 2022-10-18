"""
Globals variables used within cds_weather app.
"""

import os
from pathlib import Path

import numpy as np

workdir = os.path.dirname(os.path.realpath(__file__))


LONGITUDES = list(np.arange(-90.0, 90.25, 0.25))
LATITUDES = list(np.arange(-180.0, 180.25, 0.25))

PROJECT_DIR = Path(workdir).parent.parent
DATA_DIR = PROJECT_DIR / "data"

CDSAPIRC_PATH = Path.home() / ".cdsapirc"

BR_AREA = {"N": 5.5, "W": -74.0, "S": -33.75, "E": -32.25}
