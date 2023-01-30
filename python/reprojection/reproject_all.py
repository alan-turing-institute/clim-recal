"""
Call script to convert all nc files in a directory to geotif with BNG projection
arg1 = src
arg2 = dest 

please make sure dest directory exists 
warning, this will overwrite existing data 
"""

from sys import argv as args
from glob import glob 
import numpy as np 
import xarray as xr
import rioxarray
from pathlib import Path
import multiprocessing
from os import cpu_count
from tqdm import tqdm
from os.path import basename


def reproj_to_geotif(x):
    try:
        f = x[0]
        out_dir = x[1]
        ncfile = xr.open_dataset(f, decode_coords="all").drop_dims("bnds").squeeze()
        ncfile = ncfile.rio.set_spatial_dims('grid_longitude', 'grid_latitude')
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        f0_n = f"{out_dir}/{basename(f).replace('.nc','.tif')}"
        ncfile["tasmax"].rio.to_raster(f0_n)
        geotif = xr.open_dataset(f0_n, engine='rasterio')
        geotif.rio.reproject("epsg:27700")['band_data'].rio.to_raster(f0_n)
    except Exception as e:
        print(f"File: {f} produced errors: {e}")
    return 0 

if __name__ == "__main__":

    # find all nc files
    files = glob(f"{args[1]}/*.nc",recursive=True)
    N = len(files)
    args = [ [f,args[2]] for f in files ]
    with multiprocessing.Pool(processes=cpu_count()-1) as pool:
        res = list(tqdm(pool.imap_unordered(reproj_to_geotif, args), total=N))
