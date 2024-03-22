"""Resample UKHADS data and UKCP18 data.

- UKHADS is resampled spatially from 1km to 2.2km.
- UKCP18 is resampled temporally from a 360 day calendar to a standard (365/366 day) calendar and projected to British National Grid (BNG) (from rotated polar grid).
"""

import argparse
import multiprocessing
import os
from dataclasses import dataclass
from glob import glob
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Iterator, Literal

import numpy as np
import rioxarray  # nopycln: import
from geopandas import GeoDataFrame, read_file
from osgeo.gdal import GRA_NearestNeighbour, Warp, WarpOptions
from tqdm import tqdm
from xarray import DataArray, Dataset, open_dataset
from xarray.coding.calendar_ops import convert_calendar
from xarray.core.types import CFCalendar, InterpOptions

from .utils.xarray import ensure_xr_dataset

logger = getLogger(__name__)

DropDayType = set[tuple[int, int]]
ChangeDayType = set[tuple[int, int]]

MONTH_DAY_DROP: DropDayType = {(1, 31), (4, 1), (6, 1), (8, 1), (10, 1), (12, 1)}
"""A `set` of tuples of month and day numbers for `enforce_date_changes`."""

MONTH_DAY_XARRAY_LEAP_YEAR_DROP: DropDayType = {
    (1, 31),
    (4, 1),
    (6, 1),
    (8, 1),
    (9, 31),
    (12, 1),
}
"""A `set` of month and day tuples dropped for `xarray.day_360` leap years."""

MONTH_DAY_XARRAY_NO_LEAP_YEAR_DROP: DropDayType = {
    (2, 6),
    (4, 20),
    (7, 2),
    (9, 13),
    (11, 25),
}
"""A `set` of month and day tuples dropped for `xarray.day_360` non leap years."""

DEFAULT_INTERPOLATION_METHOD: str = "linear"
"""Default method to infer missing estimates in a time series."""

CFCalendarSTANDARD: Final[str] = "standard"
ConvertCalendarAlignOptions = Literal["date", "year", None]
DEFAULT_CALENDAR_ALIGN: Final[ConvertCalendarAlignOptions] = "year"

RESAMPLING_PATH: Final[PathLike] = Path("Raw/python_refactor/Reprojected_infill")
UK_SPATIAL_PROJECTION: Final[str] = "EPSG:27700"
CPRUK_RESOLUTION: Final[int] = 2200
CPRUK_RESAMPLING_METHOD: Final[str] = GRA_NearestNeighbour
ResamplingArgs = tuple[PathLike, np.ndarray, np.ndarray, PathLike]
ResamplingCallable = Callable[[list | tuple], int]


def warp_cruk(
    input_path: PathLike = RESAMPLING_PATH,
    output_path: PathLike = RESAMPLING_PATH,
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


def convert_xr_calendar(
    xr_time_series: DataArray | Dataset,
    align_on: ConvertCalendarAlignOptions = DEFAULT_CALENDAR_ALIGN,
    calendar: CFCalendar = CFCalendarSTANDARD,
    use_cftime: bool = False,
    missing_value: Any | None = np.nan,
    interpolate_na: bool = False,
    ensure_output_type_is_dataset: bool = False,
    interpolate_method: InterpOptions = DEFAULT_INTERPOLATION_METHOD,
    keep_attrs: bool = True,
    limit: int = 1,
    **kwargs,
) -> Dataset | DataArray:
    """Convert cpm 360 day time series to HADs 365 day time series.

    Notes
    -----
    Short time examples (like 2 skipped out of 8 days) raises:
    `ValueError("date_range_like was unable to generate a range as the source frequency was not inferable."`)

    Parameters
    ----------
    xr_time_series
        A `DataArray` or `Dataset` to convert to `calendar` time.
    align_on
        Whether and how to align `calendar` types.
    calendar
        Type of calendar to convert `xr_time_series` to.
    use_cftime
        Whether to enforce `cftime` vs `datetime64` `time` format.
    missing_value
        Missing value to populate missing date interpolations with.
    interpolate_na
        Whether to apply temporal interpolation for missing values.
    interpolate_method
        Which `InterpOptions` method to apply if `interpolate_na` is `True`.
    keep_attrs
        Whether to keep all attributes on after `interpolate_na`
    limit
        Limit of number of continuous missing day values allowed in `interpolate_na`.
    **kwargs
        Any additional parameters to pass to `interpolate_na`.

    Raises
    ------
    ValueError
        Likely from `xarray` calling `date_range_like`.

    Returns
    -------
    :
        Converted `xr_time_series` to specified `calendar`
        with optional interpolation.

    Examples
    --------
    # Note a new doctest needs to be written to deal
    # with default `year` vs `date` parameters
    >>> xr_360_to_365_datetime64: Dataset = convert_xr_calendar(
    ...     xarray_spatial_4_years_360_day, align_on="date")
    >>> xr_360_to_365_datetime64.sel(
    ...     time=slice("1981-01-30", "1981-02-01"),
    ...     space="Glasgow").day_360
    <xarray.DataArray 'day_360' (time: 3)>...
    Coordinates:
      * time     (time) datetime64[ns] ...1981-01-30 1981-01-31 1981-02-01
        space    <U10 ...'Glasgow'
    >>> xr_360_to_365_datetime64_interp: Dataset = convert_xr_calendar(
    ...     xarray_spatial_4_years_360_day, interpolate_na=True)
    >>> xr_360_to_365_datetime64_interp.sel(
    ...     time=slice("1981-01-30", "1981-02-01"),
    ...     space="Glasgow").day_360
    <xarray.DataArray 'day_360' (time: 3)>...
    array([0.23789282, 0.5356328 , 0.311945  ])
    Coordinates:
      * time     (time) datetime64[ns] ...1981-01-30 1981-01-31 1981-02-01
        space    <U10 ...'Glasgow'
    >>> convert_xr_calendar(xarray_spatial_6_days_2_skipped)
    Traceback (most recent call last):
       ...
    ValueError: `date_range_like` was unable to generate a range as the source frequency was not inferable.
    """
    if ensure_output_type_is_dataset:
        xr_time_series = ensure_xr_dataset(xr_time_series)
    calendar_converted_ts: Dataset | DataArray = convert_calendar(
        xr_time_series,
        calendar,
        align_on=align_on,
        missing=missing_value,
        use_cftime=use_cftime,
    )
    if not interpolate_na:
        return calendar_converted_ts
    else:
        return calendar_converted_ts.interpolate_na(
            dim="time",
            method=interpolate_method,
            keep_attrs=keep_attrs,
            limit=limit,
            **kwargs,
        )


def set_xarray_crm(
    xr_time_series: Dataset,
    crs: str,
    enforce_xarray_spatial_dims: bool = True,
    xr_spatial_xdim: str = "grid_longitude",
    xr_spatial_ydim: str = "grid_latitude",
) -> Dataset:
    if isinstance(xr_time_series, PathLike | str):
        xr_time_series = open_dataset(xr_time_series, decode_coords="all")
    assert isinstance(xr_time_series, Dataset)
    xr_time_series.rio.write_crs(crs, inplace=True)
    if enforce_xarray_spatial_dims:
        xr_time_series.rio.set_spatial_dims(
            x_dim=xr_spatial_xdim, y_dim=xr_spatial_ydim
        )
    return xr_time_series


def crop_nc(
    xr_time_series: Dataset | PathLike,
    crop_geom: PathLike | GeoDataFrame,
    invert=False,
    final_crs: str = UK_SPATIAL_PROJECTION,
    initial_clip_box: bool = False,
    enforce_xarray_spatial_dims: bool = True,
    xr_spatial_xdim: str = "grid_longitude",
    xr_spatial_ydim: str = "grid_latitude",
    **kwargs,
) -> Dataset:
    """Crop `xr_time_series` with `crop_path` `shapefile`.

    Parameters
    ----------
    xr_time_series
        `Dataset` or path to `netcdf` file to load and crop.
    crop_geom
        `GeoDataFrame` or `Path` of file to crop with.
    invert
        Whether to invert the `crop_geom` coordinates.
    final_crs
        Final coordinate system to return cropped `xr_time_series` in.
    initial_clip_box
        Whether to initially clip `xr_time_series` via `crop_geom`
        boundaries. For more details on chained clip approaches see
        https://corteva.github.io/rioxarray/html/examples/clip_geom.html#Clipping-larger-rasters
    enforce_xarray_spatial_dims
        Whether to use `set_spatial_dims` on `xr_time_series` prior to `clip`.
    xr_spatial_xdim
        Column parameter to pass as `xdim` to `set_spatial_dims` if used.
    xr_spatial_ydim
        Column parameter to pass as `ydim` to `set_spatial_dims` if used.
    kwargs
        Any additional parameters to pass to `clip`

    Returns
    -------
    :
        Spatially cropped `xr_time_series` `Dataset` with `final_crs` spatial coords.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip('Can only run with mounted data files')
    >>> cropped = crop_nc(
    ...     data_fixtures_path /
    ...     'tasmax_rcp85_land-cpm_uk_2.2km_01_day_19821201-19831130.nc',
    ...     crop_geom=glasgow_shape_file_path, invert=True)
    >>> cropped.rio.bounds() == glasgow_epsg_27700_bounds
    True
    """
    xr_time_series = set_xarray_crm(
        xr_time_series,
        crs=final_crs,
        enforce_xarray_spatial_dims=enforce_xarray_spatial_dims,
        xr_spatial_xdim=xr_spatial_xdim,
        xr_spatial_ydim=xr_spatial_ydim,
    )
    if isinstance(crop_geom, PathLike):
        crop_geom = read_file(crop_geom)
    assert isinstance(crop_geom, GeoDataFrame)
    crop_geom.set_crs(crs=final_crs, inplace=True)
    if initial_clip_box:
        xr_time_series = xr_time_series.rio.clip_box(
            minx=crop_geom.bounds.minx,
            miny=crop_geom.bounds.miny,
            maxx=crop_geom.bounds.maxx,
            maxy=crop_geom.bounds.maxy,
        )
    return xr_time_series.rio.clip(
        crop_geom.geometry.values, drop=True, invert=invert, **kwargs
    )


def resample_cruk(x: list | tuple) -> int:
    """Resample CRUK data to match `UKHADs` spatially and temporally."""
    pass


def resample_hadukgrid(x: list | tuple) -> int:
    """Resample UKHADs data spatially.

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

        data = open_dataset(file, decode_coords="all")

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
    """Manage resampling HADsUK datafiles for modelling.

    Attributes
    ----------
    input
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    grid_data_path
        `Path` to load to `self.grid`.
    grid
        `Dataset` of grid (either passed via `grid_data_path` or as a parameter).
    input_nc_files
        NCF files to process with `self.grid` etc.
    cpus
        Number of `cpu` cores to use during multiprocessing.
    resampling_func
        Function to call on `self.input_nc_files` with `self.grid`

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip('Can only run with mounted data files')
    >>> hads_resampler: HADsUKResampleManager = HADsUKResampleManager(
    ...     input=data_fixtures_path,
    ...     output=resample_test_output_path,
    ...     grid_data_path=glasgow_shape_file_path,
    ... )
    Grid file: .../tests/data/Glasgow/Glasgow.shp produced errors: 'projection_x_coordinate'
    >>> pprint(hads_resampler.input_nc_files)
    ('tests/data/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19821201-19831130.nc',
     'tests/data/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19841201-19851130.nc',
     'tests/data/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19811201-19821130.nc',
     'tests/data/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19831201-19841130.nc',
     'tests/data/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc')
    """

    input: PathLike | None
    output: PathLike
    grid_data_path: PathLike | None
    grid: Dataset | None = None
    input_nc_files: Iterable[PathLike] | None = None
    cpus: int | None = None
    resampling_func: ResamplingCallable = resample_hadukgrid
    crop: PathLike | GeoDataFrame | None = None
    final_crs: str = UK_SPATIAL_PROJECTION

    def __len__(self) -> int:
        """Return the length of `self.input_nc_files`."""
        return len(self.input_nc_files) if self.input_nc_files else 0

    def set_input_nc_files(self, new_input_path: PathLike | None = None) -> None:
        """Replace `self.input` and process `self.input_nc_files`."""
        if new_input_path:
            self.input = new_input_path
        if not self.input_nc_files:
            self.input_nc_files = tuple(glob(f"{self.input}/*.nc", recursive=True))

    def set_grid_x_y(self, new_grid_data_path: PathLike | None = None) -> None:
        if new_grid_data_path:
            self.grid_data_path = new_grid_data_path
        if not self.grid:
            self.grid = read_file(self.grid_data_path)
        try:
            # must have dimensions named projection_x_coordinate and projection_y_coordinate
            self.x: np.ndarray = self.grid["projection_x_coordinate"][:].values
            self.y: np.ndarray = self.grid["projection_y_coordinate"][:].values
        except Exception as e:
            print(f"Grid file: {self.grid_data_path} produced errors: {e}")

    def __post_init__(self) -> None:
        """Generate related attributes."""
        self.set_grid_x_y()
        self.set_input_nc_files()
        Path(self.output).mkdir(parents=True, exist_ok=True)
        self.total_cpus: int | None = os.cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus

    @property
    def resample_args(self) -> Iterator[ResamplingArgs]:
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


@dataclass
class CPMResampleManager(HADsUKResampleManager):
    resampling_func: ResamplingCallable = resample_hadukgrid

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
