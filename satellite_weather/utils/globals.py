"""
Globals variables used within cds_weather app.
"""

import os
from pathlib import Path

import numpy as np

workdir = os.path.dirname(os.path.realpath(__file__))


LONGITUDES = list(np.arange(-90.0, 90.25, 0.25))
LATITUDES = list(np.arange(-180.0, 180.25, 0.25))


