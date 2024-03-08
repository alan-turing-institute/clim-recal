"""This script resamples the UKHADS data to match UKCP18 data.

It resamples spatially, from 1km to 2.2km.
It resamples temporally to a 365 day calendar.
"""

import argparse
import datetime
import multiprocessing
import os
from dataclasses import dataclass
from glob import glob
from logging import getLogger
from pathlib import Path
from typing import Callable, Final, Iterable

import numpy as np
import pandas as pd
import xarray as xr  # requires rioxarray extension
from numpy import array, random
from osgeo.gdal import GRA_NearestNeighbour, Warp, WarpOptions
from pandas import to_datetime
from tqdm import tqdm
from xarray import DataArray, Dataset

from .utils import ISO_DATE_FORMAT_STR, DateType, date_range_generator

logger = getLogger(__name__)

DropDayType = set[tuple[int, int]]
ChangeDayType = set[tuple[int, int]]

MONTH_DAY_DROP: DropDayType = {(1, 31), (4, 1), (6, 1), (8, 1), (10, 1), (12, 1)}
"""A `set` of tuples of month and day numbers for `enforce_date_changes`."""

MONTH_DAY_ADD: ChangeDayType = {(1, 31), (4, 1), (6, 1), (8, 1), (10, 1)}
"""CONSIDER ADDING DAYS to 360."""

DEFAULT_INTERPOLATION_METHOD: str = "linear"
"""Default method to infer missing estimates in a time series."""


RESAMPLING_PATH: Final[os.PathLike] = Path("Raw/python_refactor/Reprojected_infill")
UK_SPATIAL_PROJECTION: Final[str] = "EPSG:27700"
CPRUK_RESOLUTION: Final[int] = 2200
CPRUK_RESAMPLING_METHOD: Final[str] = GRA_NearestNeighbour
ResamplingArgs = tuple[os.PathLike, np.ndarray, np.ndarray, os.PathLike]
ResamplingCallable = Callable[[list | tuple], int]

GLASGOW_COORDS: Final[tuple[float, float]] = (55.86279, -4.25424)
MANCHESTER_COORDS: Final[tuple[float, float]] = (53.48095, -2.23743)
LONDON_COORDS: Final[tuple[float, float]] = (51.509865, -0.118092)
CITY_COORDS: Final[dict[str, tuple[float, float]]] = {
    "Glasgow": GLASGOW_COORDS,
    "Manchester": MANCHESTER_COORDS,
    "London": LONDON_COORDS,
}
"""Coordinates of Glasgow, Manchester and London as `(lon, lat)` `tuples`."""


def enforce_date_changes(
    raw_data: xr.Dataset,
    converted_data: xr.Dataset,
    month_day_drop: DropDayType = MONTH_DAY_DROP,
) -> xr.Dataset:
    """Workaround convert_calendar misbehavior with monthly data files.

    For leap years, the conversion assigns dropped data to the previous
    date instead of deleting it. Here we manually delete those dates to
    avoid duplicates later in the pipeline. See
    `https://docs.xarray.dev/en/stable/generated/xarray.Dataset.convert_calendar.html#xarray.Dataset.convert_calendar`
    for more information, and for updates on issues see
    `https://github.com/pydata/xarray/issues/8086`

    Parameters
    ----------
    raw_data
        The original data.
    converted_data
        The data after conversion.
    month_day_drop
        Set of `tuples` of numbers: month number and day number.

    Returns
    -------
    The converted data with specific dates dropped.

    Examples
    --------
    >>> enforce_date_changes(
    ...     xarray_spatial_4_days,
    ...     xarray_spatial_4_days)['time'].coords
    Coordinates:
      * time     (time) datetime64[ns] ...1980-11-30 ... 1980-12-04
    >>> ts_4_years: xr.DataArray = enforce_date_changes(
    ...     xarray_spatial_4_years, xarray_spatial_4_years)
    >>> ts_4_years
    <xarray.DataArray 'xa_template' (time: 1437, space: 3)>...
    array([[0.5488135 , 0.71518937, 0.60276338],
           [0.43758721, 0.891773  , 0.96366276],
           [0.38344152, 0.79172504, 0.52889492],
           ...,
           [0.0916689 , 0.62816966, 0.52649637],
           [0.50034874, 0.93687921, 0.88042738],
           [0.71393397, 0.57754071, 0.25236931]])
    Coordinates:
      * time     (time) datetime64[ns] ...1980-11-30 ... 1984-11-29
      * space    (space) <U10 ...'Glasgow' 'Manchester' 'London'
    >>> len(ts_4_years) == 365*4 + 1  # Would keep all days
    False
    >>> len(ts_4_years) == 360*4      # Would enforce all years at 360 days
    False
    >>> len(ts_4_years)               # 3 days fewer than 360 per year
    1437
    """
    time_values = pd.DatetimeIndex(raw_data.coords["time"].values)

    # Get the indices of the dates to be dropped
    index_to_drop = [
        i
        for i, (m, d) in enumerate(zip(time_values.month, time_values.day))
        if (m, d) in month_day_drop
    ]

    # Filter indices that are within the bounds of the converted_data
    index_to_drop = [
        i for i in index_to_drop if i < len(converted_data.coords["time"].values)
    ]

    if index_to_drop:
        converted_data = converted_data.drop_sel(
            time=converted_data.coords["time"].values[index_to_drop]
        )

    return converted_data


def warp_cruk(
    input_path: os.PathLike = RESAMPLING_PATH,
    output_path: os.PathLike = RESAMPLING_PATH,
    output_coord_system: str = UK_SPATIAL_PROJECTION,
    output_x_resolution: int = CPRUK_RESOLUTION,
    output_y_resolution: int = CPRUK_RESOLUTION,
    resampling_method: str = CPRUK_RESAMPLING_METHOD,
    **kwargs,
) -> int:
    """Execute the `gdalwrap` function within `python`.

    This is following a script in the `bash/` folder that uses
    this programme:

    ```bash
    f=$1 # The first argument is the file to reproject
    fn=${f/Raw/Reprojected_infill} # Replace Raw with Reprojected_infill in the filename
    folder=`dirname $fn` # Get the folder name
    mkdir -p $folder # Create the folder if it doesn't exist
    gdalwarp -t_srs 'EPSG:27700' -tr 2200 2200 -r near -overwrite $f "${fn%.nc}.tif" # Reproject the file
    ```

    Parameters
    ----------
    input_path
        Path with `CPRUK` files to resample. `srcDSOrSrcDSTab` in
        `Warp`.
    output_path
        Path to save resampled `input_path` file(s) to. If equal to
        `input_path` then the `overwrite` parameter is called.
        `destNameOrDestDS` in `Warp`.
    output_coord_system
        Coordinate system to convert `input_path` file(s) to.
        `dstSRS` in `WarpOptions`.
    output_x_resolution
        Resolution of `x` cordinates to convert `input_path` file(s) to.
        `xRes` in `WarpOptions`.
    output_y_resolution
        Resolution of `y` cordinates to convert `input_path` file(s) to.
        `yRes` in `WarpOptions`.
    resampling_method
        Sampling method. `resampleAlg` in `WarpOption`. See other options
        in: `https://gdal.org/programs/gdalwarp.html#cmdoption-gdalwarp-r`.
    """
    Path(output_path).mkdir(parents=True, exist_ok=True)
    if input_path == output_path:
        kwargs["overwrite"] = True
    warp_options: WarpOptions = WarpOptions(
        dstSRS=output_coord_system,
        xRes=output_x_resolution,
        yRes=output_y_resolution,
        resampleAlg=resampling_method,
        **kwargs,
    )
    conversion_result = Warp(
        destNameOrDestDS=output_path,
        srcDSOrSrcDSTab=input_path,
        options=warp_options,
    )
    # Todo: future refactors will return something more useful
    # Below is simply meant to match `resample_hadukgrid`
    return 0 if conversion_result else 1


def xarray_example(
    start_date: DateType,
    end_date: DateType,
    coordinates: dict[str, tuple[float, float]] = CITY_COORDS,
    skip_dates: Iterable[datetime.date] | None = None,
    random_seed_int: int | None = None,
    name: str | None = None,
    as_dataset: bool = False,
    **kwargs,
) -> DataArray | Dataset:
    """Generate spatial and temporal `xarray` objects.

    Parameters
    ----------
    start_date
        Start of time series.
    end_date
        End of time series (by default not inclusive).
    coordinates
        A `dict` of region name `str` to `tuple` of
        `(lon, lat)` form.
    skip_dates
        A list of `date` objects to drop/skip between
        `start_date` and `end_date`.
    as_dataset
        Convert output to `Dataset`.
    name
        Name of returned `DataArray` and `Dataset`.
    kwargs
        Additional parameters to pass to `date_range_generator`.

    Returns
    -------
    A `DataArray` of `start_date` to `end_date` date range a
    random variable for coordinates regions
    (Glasgow, Manchester and London as default).

    Examples
    --------
    >>> xarray_example('1980-11-30', '1980-12-5')
    <xarray.DataArray 'xa_template' (time: 5, space: 3)>...
    array([[..., ..., ...],
           [..., ..., ...],
           [..., ..., ...],
           [..., ..., ...],
           [..., ..., ...]])
    Coordinates:
      * time     (time) datetime64[ns] ...1980-11-30 ... 1980-12-04
      * space    (space) <U10 ...'Glasgow' 'Manchester' 'London'
    """
    dates: list[DateType] = list(
        date_range_generator(
            start_date=start_date,
            end_date=end_date,
            start_format_str=ISO_DATE_FORMAT_STR,
            end_format_str=ISO_DATE_FORMAT_STR,
            skip_dates=skip_dates,
            **kwargs,
        )
    )
    if not name:
        name = f"xa_template"
    if isinstance(random_seed_int, int):
        random.seed(random_seed_int)  # ensure results are predictable
    data: array = random.rand(len(dates), len(coordinates))
    spaces: list[str] = list(coordinates.keys())
    # If useful, add lat/lon (currently not working)
    # lat: list[float] = [coord[0] for coord in coordinates.values()]
    # lon: list[float] = [coord[1] for coord in coordinates.values()]
    da: DataArray = DataArray(
        data,
        name=name,
        coords=[
            to_datetime(dates),
            spaces,
        ],
        dims=[
            "time",
            "space",
        ],
        # If useful, add lat/lon (currently not working)
        # coords=[dates, spaces, lon, lat],
        # dims=["time", "space", "lon", "lat"]
    )
    if as_dataset:
        return da.to_dataset()
    else:
        return da


def interp_xr_time_series(
    xr_time_series: xr.Dataset | xr.Dataset,
    check_ts_data: xr.Dataset | xr.Dataset | None = None,
    # month_day_drop: DropDayType = MONTH_DAY_DROP,
    conversion_method: str = DEFAULT_INTERPOLATION_METHOD,
    conversion_func: Callable[[xr.Dataset], xr.Dataset] | None = None,
    keep_attrs: bool = True,
    limit: int = 5,
    fill_na_dates: bool = False,
    **kwargs,
) -> xr.DataArray:
    """Workaround convert_calendar misbehavior with monthly data files.

    For leap years, the conversion assigns dropped data to the previous
    date instead of deleting it. Here we manually delete those dates to
    avoid duplicates later in the pipeline. See
    `https://docs.xarray.dev/en/stable/generated/xarray.Dataset.convert_calendar.html#xarray.Dataset.convert_calendar`
    for more information, and for updates on issues see
    `https://github.com/pydata/xarray/issues/8086`

    Parameters
    ----------
    xr_time_series
        Temporal data set to be converted.
    check_ts_data
        Temporal data to check the converted `raw_data` time series.
    conversion_method
        Function to call to convert the `raw_data` time series.

    Returns
    -------
    The converted data with specific dates dropped.

    Examples
    --------
    >>> filled_2_days: xr.DataArray = interp_xr_time_series(
    ...     xarray_spatial_6_days_2_skipped,
    ...     xarray_spatial_8_days)
    >>> filled_2_days
    <xarray.Dataset> Size: 440B
    Dimensions:      (space: 3, time: 10)
    Coordinates:
      * space        (space) <U10 120B 'Glasgow' 'Manchester' 'London'
      * time         (time) datetime64[ns] 80B 1980-11-30 1980-12-01 ... 1980-12-09
    Data variables:
        xa_template  (time, space) float64 240B 0.5488 0.7152 ... 0.4615 0.7805
    >>> # assert False
    >>> # interp_xr_time_series(xarray_spatial_4_years_360_day, xarray_spatial_4_years)
    """
    array_name: str
    if isinstance(xr_time_series, DataArray):
        array_name = xr_time_series.name or "to_convert"
        xr_time_series = xr_time_series.to_dataset(name=array_name)
    if check_ts_data is not None:
        if isinstance(check_ts_data, DataArray):
            array_name = check_ts_data.name or "to_check"
            check_ts_data = check_ts_data.to_dataset(name=array_name)
        intermediate_ts: xr.DataArray | xr.Dataset = xr_time_series.interp_like(
            check_ts_data
        )
        interpolated_ts: xr.DataArray | xr.Dataset = intermediate_ts.interpolate_na(
            dim="time",
            method=conversion_method,
            keep_attrs=keep_attrs,
            limit=limit,
            **kwargs,
        )
        return interpolated_ts
    elif fill_na_dates:
        logger.debug(
            f"'check_ts_data' is None " f"and 'fill_na' is True, inferring 'NaN'"
        )
        all_dates: Iterable[DateType] = date_range_generator(
            xr_time_series.coords["time"].min().to_dict()["data"],
            xr_time_series.coords["time"].max().to_dict()["data"],
        )
        check_ts_data = xr_time_series.interp(time=all_dates)
    else:
        raise ValueError(f"'check_ts_data' must be set or 'fill_na_dates = True'.")


# def resample_hadukgrid(x: list) -> int:
def resample_hadukgrid(x: list | tuple) -> int:
    """Resample UKHADs data to match UKCP18 spatially and temporally.

    Results are saved to the output directory.

    Parameters
    ----------
    x
        x[0]: file to be resampled
        x[1]: x_grid
        x[2]: y_grid
        x[3]: output_dir

    Returns
    -------
    `0` if resampling is a success `1` if not.

    Raises
    ------
    Exception
        Generic execption for any errors raised.
    """
    try:
        # due to the multiprocessing implementations inputs come as list
        file = x[0]
        x_grid = x[1]
        y_grid = x[2]
        output_dir = x[3]

        name = os.path.basename(file)
        output_name = (
            f"{'_'.join(name.split('_')[:-1])}_2.2km_resampled_{name.split('_')[-1]}"
        )
        if os.path.exists(os.path.join(output_dir, output_name)):
            print(f"File: {output_name} already exists in this directory. Skipping.")
            return 0

        # files have the variable name as input (e.g. tasmax_hadukgrid_uk_1km_day_20211101-20211130.nc)
        variable = os.path.basename(file).split("_")[0]

        data = xr.open_dataset(file, decode_coords="all")

        # # convert to 360 day calendar.
        # data_360 = data.convert_calendar(
        #     dim="time", calendar="360_day", align_on="year"
        # )
        # # apply correction if leap year
        # if data.time.dt.is_leap_year.any():
        #     data_360 = enforce_date_changes(data, data_360)

        # the dataset to be resample must have dimensions named projection_x_coordinate and projection_y_coordinate .
        # resampled = data_360[[variable]].interp(
        #     projection_x_coordinate=x_grid,
        #     projection_y_coordinate=y_grid,
        #     method="linear",
        # )
        resampled = data[[variable]].interp(
            projection_x_coordinate=x_grid,
            projection_y_coordinate=y_grid,
            method="linear",
        )

        # make sure we keep the original CRS
        # resampled.rio.write_crs(data_360.rio.crs, inplace=True)
        resampled.rio.write_crs(data.rio.crs, inplace=True)

        # save resampled file
        resampled.to_netcdf(os.path.join(output_dir, output_name))

    except Exception as e:
        print(f"File: {file} produced errors: {e}")
    return 0


@dataclass
class HADsUKResampleManager:
    input: os.PathLike | None
    output: os.PathLike
    grid_data: os.PathLike | None
    grid: xr.Dataset | None = None
    input_nc_files: Iterable[os.PathLike] | None = None
    cpus: int | None = None
    resampling_func: ResamplingCallable = resample_hadukgrid

    def __len__(self) -> int:
        """Return the length of `self.input_nc_files`."""
        return len(self.input_nc_files) if self.input_nc_files else 0

    def set_input_nc_files(self, new_input_path: os.PathLike | None = None) -> None:
        """Replace `self.input` and process `self.input_nc_files`."""
        if new_input_path:
            self.input = new_input_path
        if not self.input_nc_files:
            self.input_nc_files = tuple(
                glob(f"{parser_args.input}/*.nc", recursive=True)
            )

    def set_grid_x_y(self, new_grid_data: os.PathLike | None = None) -> None:
        if new_grid_data:
            self.grid_data = new_grid_data
        if not self.grid:
            self.grid = xr.open_dataset(self.grid_data)
        try:
            # must have dimensions named projection_x_coordinate and projection_y_coordinate
            self.x: np.ndarray = self.grid["projection_x_coordinate"][:].values
            self.y: np.ndarray = self.grid["projection_y_coordinate"][:].values
        except Exception as e:
            print(f"Grid file: {parser_args.grid_data} produced errors: {e}")

    def __post_init__(self) -> None:
        """Generate related attributes."""
        self.set_grid_x_y()
        self.set_input_nc_files()
        Path(self.output).mkdir(parents=True, exist_ok=True)
        self.total_cpus: int | None = os.cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus

    @property
    def resample_args(self) -> Iterable[ResamplingArgs]:
        """Return args to pass to `self.resample`."""
        if not self.input_nc_files:
            self.set_input_nc_files()
        if not self.x or not self.y:
            self.set_grid_x_y()
        assert self.input_nc_files
        for f in self.input_nc_files:
            yield f, self.x, self.x, self.output

    def resample_multiprocessing(self) -> list[int]:
        """Run `self.resampling_func` via `multiprocessing`."""

        with multiprocessing.Pool(processes=self.cpus) as pool:
            self.results = list(
                tqdm(
                    pool.imap_unordered(self.resampling_func, self.resample_args),
                    total=len(self),
                )
            )
        return self.results

    # if not os.path.exists(parser_args.output):
    #     os.makedirs(parser_args.output)

    # def process_grid_data(self) -> None:
    #     """Process `grid_data` attribute."""
    #     self.grid = xr.open_dataset(self.grid_data)

    #
    # grid = xr.open_dataset(parser_args.grid_data)
    #
    # try:
    #     # must have dimensions named projection_x_coordinate and projection_y_coordinate
    #     x = grid["projection_x_coordinate"][:].values
    #     y = grid["projection_y_coordinate"][:].values
    # except Exception as e:
    #     print(f"Grid file: {parser_args.grid_data} produced errors: {e}")
    #
    # # If output file do not exist create it
    # if not os.path.exists(parser_args.output):
    #     os.makedirs(parser_args.output)
    #
    # # find all nc files in input directory
    # files = glob.glob(f"{parser_args.input}/*.nc", recursive=True)
    # N = len(files)
    #
    # args = [[f, x, y, parser_args.output] for f in files]
    #
    # with multiprocessing.Pool(processes=os.cpu_count() - 1) as pool:
    #     res = list(tqdm(pool.imap_unordered(resample_hadukgrid, args), total=N))


if __name__ == "__main__":
    """
    Script to resample UKHADs data from the command line
    """
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding arguments
    parser.add_argument(
        "--input",
        help="Path where the .nc files to resample is located",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--grid_data",
        help="Path where the .nc file with the grid to resample is located",
        required=False,
        type=str,
        default="../../data/rcp85_land-cpm_uk_2.2km_grid.nc",
    )
    parser.add_argument(
        "--output",
        help="Path to save the resampled data data",
        required=False,
        default=".",
        type=str,
    )
    parser_args = parser.parse_args()
    hads_run_manager = HADsUKResampleManager(
        input=parser_args.input,
        grid_data=parser_args.grid_data,
        output=parser_args.output,
    )
    res = hads_run_manager.resample_multiprocessing()

    # parser_args = parser.parse_args()
    #
    # # reading baseline grid to resample files to
    # grid = xr.open_dataset(parser_args.grid_data)
    #
    # try:
    #     # must have dimensions named projection_x_coordinate and projection_y_coordinate
    #     x = grid["projection_x_coordinate"][:].values
    #     y = grid["projection_y_coordinate"][:].values
    # except Exception as e:
    #     print(f"Grid file: {parser_args.grid_data} produced errors: {e}")
    #
    # # If output file do not exist create it
    # if not os.path.exists(parser_args.output):
    #     os.makedirs(parser_args.output)
    #
    # # find all nc files in input directory
    # files = glob.glob(f"{parser_args.input}/*.nc", recursive=True)
    # N = len(files)
    #
    # args = [[f, x, y, parser_args.output] for f in files]
    #
    # with multiprocessing.Pool(processes=os.cpu_count() - 1) as pool:
    #     res = list(tqdm(pool.imap_unordered(resample_hadukgrid, args), total=N))
