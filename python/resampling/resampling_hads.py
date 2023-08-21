'''
This script resamples the UKHADS data to match UKCP18 data.
It resamples spatially, from 1km to 2.2km
It resamples temporally to a 360 day calendar.
'''

import argparse
import pandas as pd
import xarray as xr
import os
import glob
import multiprocessing
from os import cpu_count
from tqdm import tqdm
import scipy
import netCDF4

def enforce_date_dropping(raw_data: xr.Dataset, converted_data: xr.Dataset) -> xr.Dataset:
    """
    Workaround to avoid convert_calendar misbehavior with monthly data files.
    
    For leap years, the conversion assigns dropped data to the previous date instead of deleting it.
    Here we manually delete those dates to avoid duplicates later in the pipeline.
    
    Args:
        raw_data (xr.Dataset): The original data.
        converted_data (xr.Dataset): The data after conversion.
        
    Returns:
        xr.Dataset: The converted data with specific dates dropped.
    """
    month_day_drop = {(1, 31), (4, 1), (6, 1), (8, 1), (9, 31), (12, 1)}
    time_values = pd.DatetimeIndex(raw_data.coords['time'].values)
    
    # Get the indices of the dates to be dropped
    index_to_drop = [i for i, (m, d) in enumerate(zip(time_values.month, time_values.day)) if (m, d) in month_day_drop]
    
    # Filter indices that are within the bounds of the converted_data
    index_to_drop = [i for i in index_to_drop if i < len(converted_data.coords['time'].values)]
    
    if index_to_drop:
        converted_data = converted_data.drop_sel(time=converted_data.coords['time'].values[index_to_drop])
    
    return converted_data
    
def resample_hadukgrid(x):
    '''
    Resamples the UKHADs data to match UKCP18 data both spatially and temporally
    and saves the resampled data to the output directory.
    inputs:
        x: list of inputs
            x[0]: file to be resampled
            x[1]: x_grid    
            x[2]: y_grid    
            x[3]: output_dir    
    '''
    try:
        # due to the multiprocessing implementations inputs come as list
        file = x[0]
        x_grid = x[1]
        y_grid = x[2]
        output_dir = x[3]

        name = os.path.basename(file)
        output_name = f"{'_'.join(name.split('_')[:-1])}_2.2km_resampled_{name.split('_')[-1]}"
        if os.path.exists(os.path.join(output_dir,output_name)):
            print(f"File: {output_name} already exists in this directory. Skipping.")
            return 0

        # files have the variable name as input (e.g. tasmax_hadukgrid_uk_1km_day_20211101-20211130.nc)
        variable = os.path.basename(file).split('_')[0]

        data = xr.open_dataset(file, decode_coords="all")

        # convert to 360 day calendar.
        data_360 = data.convert_calendar(dim='time', calendar='360_day', align_on='year')
        data_360 = enforce_date_dropping(data,data_360)

        # the dataset to be resample must have dimensions named projection_x_coordinate and projection_y_coordinate .
        resampled = data_360[[variable]].interp(projection_x_coordinate=x_grid, projection_y_coordinate=y_grid, method="linear")

        #make sure we keep the original CRS
        resampled.rio.write_crs(data_360.rio.crs,inplace=True)

        # save resampled file
        resampled.to_netcdf(os.path.join(output_dir,output_name))

    except Exception as e:
        print(f"File: {file} produced errors: {e}")
    return 0


if __name__ == "__main__":
    """
    Script to resample UKHADs data from the command line

    """
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding arguments
    parser.add_argument("--input", help="Path where the .nc files to resample is located", required=True, type=str)
    parser.add_argument("--grid_data", help="Path where the .nc file with the grid to resample is located", required=False,type=str, default='../../data/rcp85_land-cpm_uk_2.2km_grid.nc')
    parser.add_argument("--output", help="Path to save the resampled data data", required=False, default=".", type=str)

    parser_args = parser.parse_args()

    # reading baseline grid to resample files to
    grid = xr.open_dataset(parser_args.grid_data)

    try:
        # must have dimensions named projection_x_coordinate and projection_y_coordinate
        x = grid['projection_x_coordinate'][:].values
        y = grid['projection_y_coordinate'][:].values
    except Exception as e:
        print(f"Grid file: {parser_args.grid_data} produced errors: {e}")

    # If output file do not exist create it
    if not os.path.exists(parser_args.output):
        os.makedirs(parser_args.output)

    # find all nc files in input directory
    files = glob.glob(f"{parser_args.input}/*.nc", recursive=True)
    N = len(files)

    args = [[f, x, y, parser_args.output] for f in files]

    with multiprocessing.Pool(processes=cpu_count() - 1) as pool:
        res = list(tqdm(pool.imap_unordered(resample_hadukgrid, args), total=N))
