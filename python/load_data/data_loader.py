import glob
import os
from datetime import datetime

import geopandas as gp
import xarray as xr

DateRange = tuple[datetime, datetime]


def load_data(
    input_path: str,
    date_range: DateRange,
    variable: str,
    filter_filenames_on_variable: bool = False,
    run_number: str | None = None,
    filter_filenames_on_run_number: bool = False,
    use_pr: bool = False,
    shapefile_path: str | None = None,
    extension: str = "nc",
) -> xr.DataArray:
    """
    This function takes a date range and a variable and loads and merges
    xarrays based on those parameters.

    If shapefile is provided it crops the data to that region.

    Parameters
    ----------
    input_path 
        Path to where .nc or .tif files are found
    date_range 
        A tuple of datetime objects representing the start and end date
    variable 
        A string representing the variable to be loaded
    filter_filenames_on_variable 
        When True, files in the input_path will be filtered based on
        whether their file name contains "variable" as a substring. When
        False, filtering does not happen.
    run_number 
        A string representing the CPM run number to use
        (out of 13 CPM runs available in the database). Only files
        whose file name contains the substring run_number will be used.
        If `None`, all files in input_path are parsed, regardless of run
        number in filename.
    filter_filenames_on_run_number 
        When True, files in the input_path will be filtered based on
        whether their file name contains "2.2km_" followed by "run_number".
        When False, filtering does not happen. This should only be used for
        CPM files. For HADs files this should always be set to False.
    use_pr 
        If True, replace variable with "pr" string when filtering the file names.
    shapefile_path 
        Path to a shape file used to clip resulting dataset.
    extension 
        Extension of the files to be loaded, it can be .nc or .tif files.

    Returns
    -------
    xr.DataArray 
        A DataArray containing all loaded and merged and clipped data
    """

    if extension not in ("nc", "tif"):
        raise Exception("We only accept .nc or .tif extension for the input data")

    if filter_filenames_on_variable:
        if filter_filenames_on_run_number:
            if use_pr:
                # when run_number is used, use it to select files from CPM file list
                files = glob.glob(
                    f"{input_path}/pr*2.2km_{run_number}_*.{extension}", recursive=True
                )
            else:
                # when run_number is used, use it to select files from CPM file list
                files = glob.glob(
                    f"{input_path}/{variable}*2.2km_{run_number}_*.{extension}",
                    recursive=True,
                )
        else:
            if use_pr:
                # when run_number is not used, select files only based on variable (either CPM or HADs)
                files = glob.glob(f"{input_path}/pr*.{extension}", recursive=True)
            else:
                # when run_number is not used, select files only based on variable (either CPM or HADs)
                files = glob.glob(
                    f"{input_path}/{variable}*.{extension}", recursive=True
                )
    else:
        if filter_filenames_on_run_number:
            # when run_number is used, use it to select files from CPM file list
            files = glob.glob(
                f"{input_path}/*2.2km_{run_number}_*.{extension}", recursive=True
            )
        else:
            # when run_number is not used, select files only based on variable (either CPM or HADs)
            files = glob.glob(f"{input_path}/*.{extension}", recursive=True)

    if len(files) == 0:
        raise Exception(f"No files found in {input_path} with {extension}")

    xa = load_and_merge(date_range, files, variable)

    # clipping
    if shapefile_path:
        print(f"Clipping data using shapefile {shapefile_path}...")
        xa = clip_dataset(xa, variable, shapefile_path)

    return xa


def clip_dataset(xa: xr.DataArray, variable: str, shapefile: str) -> xr.DataArray:
    """Spatially clip `xa` `DataArray` variable via `shapefile.

    Parameters
    ----------
    xa 
        xArray containing a given variable (e.g. rainfall)
    variable 
        A string representing the variable to be loaded
    shapefile 
        Path to a shape file used to clip resulting dataset,
        must be in the same CRS of the input xArray.

    Returns
    -------
    xr.DataArray 
        A clipped xarray dataset

    """
    geodf = gp.read_file(shapefile)

    # assign projection
    xa_mask = (
        xa[variable]
        .rename({"projection_x_coordinate": "x", "projection_y_coordinate": "y"})
        .rio.write_crs("epsg:27700")
    )

    # clip and turn back to Dataset with original coordinate names
    xa = (
        xa_mask.rio.clip(geodf["geometry"])
        .to_dataset()
        .rename(
            {
                "x": "projection_x_coordinate",
                "y": "projection_y_coordinate",
            }
        )
    )

    try:
        # this is creating issues after clipping for hads
        del xa[variable].attrs["grid_mapping"]
    except:
        pass

    return xa


def reformat_file(file: str, variable: str) -> xr.DataArray:
    """Load tif file and reformat xarray into expected format.

    Parameters
    ----------

    file
        Path as a `str` to `tiff` file.
    variable
        A string representing the variable to be loaded

    Returns
    -------
    xr.DataArray 
        A formatted xarray
    """
    print(f"File: {file} needs rasterio library, trying...")
    filename = os.path.basename(file).split("_")

    start = filename[-1].split("-")[0]
    stop = filename[-1].split("-")[1].split(".")[0]
    time_index = xr.cftime_range(
        start, stop, freq="D", calendar="360_day", inclusive="both"
    )

    try:
        with xr.open_dataset(file, engine="rasterio") as x:
            xa = x.rename(
                {
                    "x": "projection_x_coordinate",
                    "y": "projection_y_coordinate",
                    "band": "time",
                    "band_data": variable,
                }
            ).rio.write_crs("epsg:27700")
            xa.coords["time"] = time_index

    except Exception as e:
        with xr.open_rasterio(file) as x:
            xa = x.rename(
                {
                    "x": "projection_x_coordinate",
                    "y": "projection_y_coordinate",
                    "band": "time",
                }
            ).rio.write_crs("epsg:27700")
        xa.coords["time"] = time_index

        xa = xa.transpose(
            "time", "projection_y_coordinate", "projection_x_coordinate"
        ).to_dataset(name=variable)

    return xa


def load_and_merge(
    date_range: DateRange,
    files: list[str],
    variable: str
) -> xr.DataArray:
    """
    Load files into xarrays, select a time range and a variable and merge into a sigle xarray.

    Parameters
    ----------

    date_range 
        A tuple of datetime objects representing the start and end date
    files 
        List of strings with path to files to be loaded.
    variable 
        A string representing the variable to be loaded

    Returns
    -------
    xr.DataArray
        An xarray containing all loaded and merged data
    """

    # Create an empty list to store xarrays
    xarray_list = []
    # Iterate through the variables
    for file in files:
        filename = os.path.basename(file).split("_")
        start_file = datetime.strptime(filename[-1].split("-")[0], "%Y%m%d")
        stop_file = datetime.strptime(
            filename[-1].split("-")[1].split(".")[0], "%Y%m%d"
        )

        start_range = datetime.strptime(date_range[0], "%Y-%m-%d")
        stop_range = datetime.strptime(date_range[1], "%Y-%m-%d")

        if (stop_file < start_range) | (start_file > stop_range):
            continue

        # Load the xarray
        try:
            try:
                print("Loading and selecting ", file)
                with xr.open_dataset(file, engine="netcdf4") as ds:
                    x = ds.load()
                    dv = list(x.data_vars)
                    if (
                        len(dv) > 1
                        and dv[0] == os.path.basename(file)[:-3]
                        and dv[1] == "crs"
                    ):
                        x = x.rename(
                            {
                                "northing": "projection_y_coordinate",
                                "easting": "projection_x_coordinate",
                                os.path.basename(file)[:-3]: variable,
                            }
                        ).rio.write_crs("epsg:27700")
                        x = x.convert_calendar(
                            dim="time", calendar="360_day", align_on="year"
                        )
                    x = x.sel(time=slice(*date_range))
            except Exception as e:
                x = reformat_file(file, variable).sel(time=slice(*date_range))

            # Select the date range
            if x.time.size != 0:
                # Append the xarray to the list
                xarray_list.append(x)
            del x
        except Exception as e:
            print(f"File: {file} produced errors: {e}")

    # Merge all xarrays in the list
    if len(xarray_list) == 0:
        raise RuntimeError(
            "No files passed the time selection. No merged output produced."
        )
    else:
        print("Merging arrays from different files...")
        merged_xarray = xr.concat(xarray_list, dim="time", coords="minimal").sortby(
            "time"
        )

    return merged_xarray
