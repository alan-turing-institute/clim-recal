import logging, sys

from pycat.io import Dataset
from pycat.esd import QuantileMapping
from ..load_data.data_loader import load_data



obs = Dataset('sample-data', 'observation.nc')
mod = Dataset('sample-data', 'model*.nc')
sce = Dataset('sample-data', 'scenario*.nc')

qm = QuantileMapping(obs, mod, sce)
qm.correct()