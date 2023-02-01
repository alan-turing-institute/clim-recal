import argparse
import xarray as xr
import os
import glob
import multiprocessing
from os import cpu_count
from tqdm import tqdm
import scipy
import netCDF4

def resample_hadukgrid(x):
    try:
        # due to the multiprocessing implementations inputs come as list
        file = x[0]
        x_grid = x[1]
        y_grid = x[2]
        output_dir = x[3]

        # files have the variable name as input (e.g. tasmax_hadukgrid_uk_1km_day_20211101-20211130.nc)
        variable = os.path.basename(file).split('_')[0]

        data = xr.open_dataset(file)

        # the dataset to be resample must have dimensions named projection_x_coordinate and projection_y_coordinate .
        resampled = data[[variable]].interp(projection_x_coordinate=x_grid, projection_y_coordinate=y_grid, method="linear")

        # save resampled file
        output_name = f"{os.path.basename(file).split('.')[0]}_2.2km_resampled.nc"

        print (os.path.join(output_dir,output_name))
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
    parser.add_argument("--input", help="Path where the CEDA data to download is located", required=True, type=str)
    parser.add_argument("--output", help="Path to save the downloaded data", required=False, default=".", type=str)
    parser.add_argument("--grid_data", help="Path where the NC file with the grid to resample is located", required=False,
                        type=str, default='../../data/rcp85_land-cpm_uk_2.2km_grid.nc')

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
