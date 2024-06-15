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


def reproject_hads_to_align_with_target_xr(source_file, target_xr, variable_name):

    xr_data = xr.open_dataset(source_file,decode_coords="all")

    coords = {
        "time": xr_data["time"],
        "projection_y_coordinate": xr_data["projection_y_coordinate"],
        "projection_x_coordinate": xr_data["projection_x_coordinate"],
    }

    without_attributes = xr.DataArray(
        data=xr_data[variable_name].to_numpy(),
        coords=coords,
        name=variable_name
    )
    without_attributes = without_attributes.rio.write_crs(xr_data.rio.crs)
    without_attributes = without_attributes.rio.reproject_match(target_xr)

    return without_attributes


def get_variable_name_from_filename(f_path: Path):
    # for both CPM and HADs files, it is the first part of the filename, upto the first underscore
    return f_path.stem.split("_")[0] 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a single HADs file")
    parser.add_argument("--hads-file", type=Path, required=True, help="Path to the HADs file to process")
    parser.add_argument("--reference-file", type=Path, required=True, help="Path to the file, which will be used as the reference grid to align the HADs data to")
    parser.add_argument("--output-dir", type=Path, required=True, help="Path to the directory where the output file will be saved")
    # Add dry-run option
    parser.add_argument("--dry-run", action="store_true", help="Dry run the script")

    args = parser.parse_args()
    raw_hads_file = args.hads_file
    output_dir = args.output_dir
    target_file_path = args.reference_file
    hads_variable_name = get_variable_name_from_filename(raw_hads_file)
    output_hads_file = output_dir / str(raw_hads_file.name)

    if args.dry_run:
        print(f"Would process `{raw_hads_file}`, with variable `{hads_variable_name}`, reference file {target_file_path} and save to `{output_hads_file}`")
        exit(0)

    # Open the target file once
    target_xr = xr.open_dataset(target_file_path,decode_coords="all")

    # Reproject HADs to match the reference file (typically a converted CPM file)
    converted_hads_xr = reproject_hads_to_align_with_target_xr(raw_hads_file, target_xr, variable_name=hads_variable_name)
    print(f"Saving to {output_hads_file}")
    converted_hads_xr.to_netcdf(output_hads_file, mode="w")
