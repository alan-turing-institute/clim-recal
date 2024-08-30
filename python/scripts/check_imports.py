from os import PathLike
from pathlib import Path
import rioxarray 
import xarray as xr 
import rasterio
import geopandas as gp
import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray
import argparse

from xarray import cftime_range, CFTimeIndex
from xarray.coding.calendar_ops import convert_calendar
from datetime import timedelta

from xarray.core.types import (
    T_DataArray,
    T_DataArrayOrSet,
    T_Dataset,
)

print("imported all without errors")