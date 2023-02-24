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

        name = os.path.basename(file)
        output_name = f"{'_'.join(name.split('_')[:-1])}_2.2km_resampled_{name.split('_')[-1]}"
        if os.path.exists(os.path.join(output_dir,output_name)):
            print(f"File: {output_name} already exists in this directory. Skipping.")
            return 0

        # files have the variable name as input (e.g. tasmax_hadukgrid_uk_1km_day_20211101-20211130.nc)
        variable = os.path.basename(file).split('_')[0]

        data = xr.open_dataset(file, decode_coords="all")

        # convert to 360 day calendar.
        data = data.convert_calendar(dim='time', calendar='360_day', align_on='year')

        # the dataset to be resample must have dimensions named projection_x_coordinate and projection_y_coordinate .
        resampled = data[[variable]].interp(projection_x_coordinate=x_grid, projection_y_coordinate=y_grid, method="linear")

        #make sure we keep the original CRS
        resampled.rio.write_crs(data.rio.crs,inplace=True)

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
    parser.add_argument("--output", help="Path to save the resampled data data", required=False, default=".", type=str)
    parser.add_argument("--grid_data", help="Path where the .nc file with the grid to resample is located", required=False,
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
