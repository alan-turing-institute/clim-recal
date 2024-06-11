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

def reproject_time_corrected_cpm_to_british_grid(xr_data, variable_name):
    # tasmax_data = xr.open_dataset(file,decode_coords="all", engine="netcdf4")
    xr_data = xr_data.rio.set_spatial_dims("grid_longitude","grid_latitude",True)

    coords = {
        "time": xr_data["time"],
        "grid_latitude": xr_data["grid_latitude"],
        "grid_longitude": xr_data["grid_longitude"],
    }

    # Single ensemble member
    variable_data = xr_data[variable_name][0]

    without_attributes = xr.DataArray(
        data=variable_data.to_numpy(), 
        coords=coords,
        name=variable_name
    )
    without_attributes = without_attributes.rio.write_crs(xr_data.rio.crs)
    without_attributes = without_attributes.rio.reproject("EPSG:27700", resolution=[2200,2200])
    return without_attributes


# Time alignment functions
# These have been mostly taken from `xarray.py` and then partially simplified.
def cpm_xarray_to_standard_calendar(cpm_xr_time_series: PathLike,) -> T_Dataset:
    cpm_xr_time_series = xr.open_dataset(cpm_xr_time_series, decode_coords="all")
    cpm_to_std_calendar: T_Dataset = convert_xr_calendar(cpm_xr_time_series)

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a single CPM file")
    parser.add_argument("--cpm-file", type=Path, required=True, help="Path to the CPM file to process")
    parser.add_argument("--output-dir", type=Path, required=True, help="Path to the output directory")
    # Add dry-run option
    parser.add_argument("--dry-run", action="store_true", help="Dry run the script")

    args = parser.parse_args()
    raw_cpm_file = args.cpm_file
    output_dir = args.output_dir

    # Convert a CPM file to a standard calendar and reproject to British Grid
    output_cpm_file = output_dir / str(raw_cpm_file.name)
    cpm_variable_name = get_variable_name_from_filename(raw_cpm_file)

    if args.dry_run:
        print(f"Would process `{raw_cpm_file}`, with variable `{cpm_variable_name}` and save to `{output_cpm_file}`")
        exit(0)

    temporal_resampled_cpm = cpm_xarray_to_standard_calendar(raw_cpm_file)
    converted_cpm_xr = reproject_time_corrected_cpm_to_british_grid(temporal_resampled_cpm, variable_name=cpm_variable_name)
    print(f"Saving to {output_cpm_file}")
    converted_cpm_xr.to_netcdf(output_cpm_file, mode="w")
