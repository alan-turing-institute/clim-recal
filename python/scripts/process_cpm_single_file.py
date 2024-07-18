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
from tempfile import TemporaryDirectory
import subprocess

from xarray import cftime_range, CFTimeIndex
from xarray.coding.calendar_ops import convert_calendar
from datetime import timedelta

from xarray.core.types import (
    T_DataArray,
    T_DataArrayOrSet,
    T_Dataset,
)

def reproject_using_gdal(input_file, temp_dir, resample_method_name):

    # Reproject using GDAL
    reprojected_tiff_file = str(Path(temp_dir) / "reprojected_tiff_file.tif")
    reprojected_nc_file = str(Path(temp_dir) / "reprojected_nc_file.nc")
    print(f"Reprojecting {input_file}\n  to {reprojected_tiff_file}\n and {reprojected_nc_file}")

    try:
        # Reproject using GDALWarp
        gdalwarp_cmd = ["gdalwarp", "-t_srs", "EPSG:27700",
                        "-tr", "2200", "2200",
                        "-ovr", "NONE", "-r", resample_method_name, "-of", "GTiff", 
                        "-oo", "VARIABLES_AS_BANDS=YES",
                        "-oo", "GDAL_NETCDF_VERIFY_DIMS=STRICT",
                        input_file, reprojected_tiff_file]

        result1 = subprocess.run(gdalwarp_cmd, check=True, capture_output=True)
        print("gdalwarp output:")
        print(result1.stdout)

        # Now convert the tiff file back to netcdf
        # gdal_translate_cmd = ["gdal_translate", "-of", "netCDF", reprojected_tiff_file, reprojected_nc_file]    
        gdal_translate_cmd = ["gdal_translate", reprojected_tiff_file, reprojected_nc_file]    
        result2 = subprocess.run(gdal_translate_cmd, check=True, capture_output=True)
        print("gdal_translate output:")
        print(result2.stdout)
        print(f"Reprojected file saved to {reprojected_nc_file}")

    except subprocess.CalledProcessError as e:
        print(f"Error reprojecting file:\n"
              f"returncode: {e.returncode}\n"
              f"output: {e.output}\n"
              f"stdout: {e.stdout}\n"
              f"stderr: {e.stderr}\n")
        exit(1)

    return reprojected_nc_file


def print_xr_data_info(xr_data, logging_title: str = None):
    if logging_title:
        print(logging_title)
        print("-" * 80)
    print(f"xr_data.dims: {xr_data.dims}")
    print(f"xr_data.coords: {xr_data.coords.keys()}")


# Time alignment functions
# These have been mostly taken from `xarray.py` and then partially simplified.
def cpm_xarray_to_standard_calendar(cpm_xr_time_series: PathLike):
    cpm_xr_time_series = xr.open_dataset(cpm_xr_time_series, decode_coords="all")
    # print_xr_data_info(cpm_xr_time_series, "As loaded from file")

    cpm_to_std_calendar: T_Dataset = convert_xr_calendar(cpm_xr_time_series)
    # print_xr_data_info(cpm_to_std_calendar, "After convert_xr_calendar")

    # For now, we'll not fix other calendar variables
    if False:
        # Fix other calendar variables
        cpm_to_std_calendar[
            "month_number"
        ] = cpm_to_std_calendar.month_number.interpolate_na(
            "time", fill_value="extrapolate"
        )
        cpm_to_std_calendar["year"] = cpm_to_std_calendar.year.interpolate_na(
            "time", fill_value="extrapolate"
        )
        yyyymmdd_fix: T_DataArray = cpm_to_std_calendar.time.dt.strftime("%Y%m%d")
        cpm_to_std_calendar["yyyymmdd"] = yyyymmdd_fix

        assert cpm_xr_time_series.rio.crs == cpm_to_std_calendar.rio.crs

    return cpm_to_std_calendar


def convert_xr_calendar(xr_time_series: T_Dataset) -> T_DataArrayOrSet:

    calendar_converted_ts: T_DataArrayOrSet = convert_calendar(
        xr_time_series,
        calendar="standard",
        align_on="year",
        missing=np.nan,
        use_cftime=False,
    )

    # calendar_converted_ts = xr_time_series

    return interpolate_xr_ts_nans(
        xr_ts=calendar_converted_ts,
        original_xr_ts=xr_time_series,
        limit=1,
    )


def interpolate_xr_ts_nans(
    xr_ts: T_Dataset,
    original_xr_ts: T_Dataset | None,
    limit: int = 1,
) -> T_Dataset:
    original_xr_ts = original_xr_ts if original_xr_ts else xr_ts

    # Ensure `fill_value` is set to `extrapolate`
    # Without this the `nan` values don't get filled
    kwargs = {}
    kwargs["fill_value"] = "extrapolate"

    interpolated_ts: T_Dataset = xr_ts.interpolate_na(
        dim="time",
        method="linear",
        keep_attrs=True,
        limit=limit,
        **kwargs,
    )

    cftime_col = "time_bnds"
    if cftime_col in interpolated_ts:
        cftime_fix: NDArray = cftime_range_gen(
            interpolated_ts[cftime_col],
        )
        interpolated_ts[cftime_col] = (
            interpolated_ts[cftime_col].dims,
            cftime_fix,
        )

    if original_xr_ts.rio.crs:
        return interpolated_ts.rio.write_crs(xr_ts.rio.crs)
    else:
        return interpolated_ts


def cftime_range_gen(time_data_array: T_DataArray) -> NDArray:
    """Convert a banded time index a banded standard (Gregorian)."""
    date_str_format: str = "%Y-%m-%d"
    assert hasattr(time_data_array, "time")
    time_bnds_fix_range_start: CFTimeIndex = cftime_range(
        time_data_array.time.dt.strftime(date_str_format).values[0],
        time_data_array.time.dt.strftime(date_str_format).values[-1],
    )
    time_bnds_fix_range_end: CFTimeIndex = time_bnds_fix_range_start + timedelta(days=1)
    return np.array((time_bnds_fix_range_start, time_bnds_fix_range_end)).T

def get_variable_name_from_filename(f_path: Path):
    # for both CPM and HADs files, it is the first part of the filename, upto the first underscore
    return f_path.stem.split("_")[0] 

def get_resample_method(variable_name: str):
    resample_methods = {
        "tasmax" : "max",
        "tasmin" : "min",
        "pr" : "med",
        "rainfall" : "med",
    }

    try:
        return resample_methods[variable_name]
    except KeyError as ke:
        raise ValueError(f"No reampling method defined for unknown climate variable name: {variable_name}") from ke

def main(raw_cpm_file, output_cpm_file):
    with TemporaryDirectory() as temp_dir:
        reprojection_resampling_method_name = get_resample_method(get_variable_name_from_filename(raw_cpm_file))

        # Reproject the CPM file
        raw_cpm_file = str(raw_cpm_file)
        reproject_file_path = reproject_using_gdal(raw_cpm_file, temp_dir, reprojection_resampling_method_name)
        # Convert a CPM file to a standard calendar
        temporal_resampled_cpm = cpm_xarray_to_standard_calendar(reproject_file_path)

    print_xr_data_info(temporal_resampled_cpm, "After temporal resampling")
    print(f"Saving to {output_cpm_file}")
    temporal_resampled_cpm.to_netcdf(output_cpm_file, mode="w")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a single CPM file")
    parser.add_argument("--cpm-file", type=Path, required=True, help="Path to the CPM file to process")
    
    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument("--output-dir", type=Path, help="Path to the output directory (the input file name will be used)")
    output_group.add_argument("--output-path", type=Path, help="Full path to the output file")
    # Add dry-run option
    parser.add_argument("--dry-run", action="store_true", help="Dry run the script")

    args = parser.parse_args()
    raw_cpm_file = args.cpm_file

    # Get the full path to the output file, one way or another
    if args.output_path:
        output_cpm_file = args.output_path
        output_dir = args.output_path.parent
    else:
        output_dir = args.output_dir
        output_cpm_file = args.output_dir / str(raw_cpm_file.name)
    
    cpm_variable_name = get_variable_name_from_filename(raw_cpm_file)

    if args.dry_run:
        print(f"Would process `{raw_cpm_file}`, with variable `{cpm_variable_name}` and save to `{output_cpm_file}`")
        exit(0)

    main(raw_cpm_file, output_cpm_file)
