"""Resample UKHADS data and UKCP18 data.

- UKHADS is resampled spatially from 1km to 2.2km.
- UKCP18 is resampled temporally from a 360 day calendar to a standard (365/366 day) calendar and projected to British National Grid (BNG) (from rotated polar grid).

## Notes

"""

from dataclasses import dataclass, field
from datetime import date
from glob import glob
from logging import getLogger
from multiprocessing import Pool
from os import PathLike, cpu_count
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Iterator, Literal, Sequence

import numpy as np
import rioxarray  # nopycln: import
from geopandas import GeoDataFrame, read_file
from numpy.typing import NDArray
from osgeo.gdal import Dataset as GDALDataset
from osgeo.gdal import GDALWarpAppOptions, GRA_NearestNeighbour, Warp, WarpOptions
from rich import print
from tqdm.rich import tqdm, trange
from xarray import DataArray, Dataset, cftime_range, open_dataset
from xarray.coding.calendar_ops import convert_calendar
from xarray.core.types import CFCalendar, InterpOptions

from clim_recal.debiasing.debias_wrapper import VariableOptions

from .utils.core import (
    CLI_DATE_FORMAT_STR,
    ISO_DATE_FORMAT_STR,
    climate_data_mount_path,
    run_callable_attr,
)
from .utils.data import RunOptions, VariableOptions
from .utils.xarray import (
    NETCDF4_XARRAY_ENGINE,
    NETCDF_EXTENSION_STR,
    TIF_EXTENSION_STR,
    GDALFormatsType,
    GDALGeoTiffFormatStr,
    XArrayEngineType,
    ensure_xr_dataset,
)

logger = getLogger(__name__)

DropDayType = set[tuple[int, int]]
ChangeDayType = set[tuple[int, int]]

CLIMATE_DATA_MOUNT_PATH: Path = climate_data_mount_path()
DEFAULT_INTERPOLATION_METHOD: str = "linear"
"""Default method to infer missing estimates in a time series."""

CFCalendarSTANDARD: Final[str] = "standard"
ConvertCalendarAlignOptions = Literal["date", "year", None]
DEFAULT_CALENDAR_ALIGN: Final[ConvertCalendarAlignOptions] = "year"

RESAMPLING_OUTPUT_PATH: Final[PathLike] = (
    CLIMATE_DATA_MOUNT_PATH / "Raw/python_refactor/"
)
RAW_HADS_PATH: Final[PathLike] = CLIMATE_DATA_MOUNT_PATH / "Raw/HadsUKgrid"
RAW_CPM_PATH: Final[PathLike] = CLIMATE_DATA_MOUNT_PATH / "Raw/UKCP2.2"
RAW_HADS_TASMAX_PATH: Final[PathLike] = RAW_HADS_PATH / "tasmax/day"
RAW_CPM_TASMAX_PATH: Final[PathLike] = RAW_CPM_PATH / "tasmax/01/latest"
REPROJECTED_CPM_TASMAX_01_LATEST_INPUT_PATH: Final[PathLike] = (
    CLIMATE_DATA_MOUNT_PATH / "Reprojected_infill/UKCP2.2/tasmax/01/latest"
)

UK_SPATIAL_PROJECTION: Final[str] = "EPSG:27700"
CPRUK_RESOLUTION: Final[int] = 2200
CPRUK_RESAMPLING_METHOD: Final[str] = GRA_NearestNeighbour
ResamplingArgs = tuple[PathLike, np.ndarray, np.ndarray, PathLike]
ResamplingCallable = Callable[[list | tuple], int]
CPM_STANDARD_CALENDAR_PATH: Final[Path] = Path("cpm-standard-calendar")
CPM_SPATIAL_COORDS_PATH: Final[Path] = Path("cpm-to-27700-spatial")
HADS_2_2K_RESOLUTION_PATH: Final[Path] = Path("hads-to-27700-spatial-2.2km")
CPRUK_XDIM: Final[str] = "grid_longitude"
CPRUK_YDIM: Final[str] = "grid_latitude"

HADS_XDIM: Final[str] = "projection_x_coordinate"
HADS_YDIM: Final[str] = "projection_y_coordinate"

DEFAULT_RELATIVE_GRID_DATA_PATH: Final[Path] = (
    Path().absolute() / "../data/rcp85_land-cpm_uk_2.2km_grid.nc"
)

CPM_START_DATE: Final[date] = date(1980, 12, 1)
CPM_END_DATE: Final[date] = date(2060, 11, 30)

HADS_START_DATE: Final[date] = date(1980, 1, 1)
HADS_END_DATE: Final[date] = date(2021, 12, 31)

CPM_OUTPUT_LOCAL_PATH: Final[Path] = Path("cpm")
HADS_OUTPUT_LOCAL_PATH: Final[Path] = Path("hads")


NETCDF_OR_TIF = Literal[TIF_EXTENSION_STR, NETCDF_EXTENSION_STR]


def convert_xr_calendar(
    xr_time_series: DataArray | Dataset | PathLike,
    align_on: ConvertCalendarAlignOptions = DEFAULT_CALENDAR_ALIGN,
    calendar: CFCalendar = CFCalendarSTANDARD,
    use_cftime: bool = False,
    missing_value: Any | None = np.nan,
    interpolate_na: bool = False,
    ensure_output_type_is_dataset: bool = False,
    interpolate_method: InterpOptions = DEFAULT_INTERPOLATION_METHOD,
    keep_attrs: bool = True,
    limit: int = 1,
    engine: XArrayEngineType = NETCDF4_XARRAY_ENGINE,
    extrapolate_fill_value: bool = True,
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
    engine
        Which `XArrayEngineType` to use in parsing files and operations.
    extrapolate_fill_value
        If `True`, then pass `fill_value=extrapolate`. See:
         * https://docs.xarray.dev/en/stable/generated/xarray.Dataset.interpolate_na.html
         * https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html#scipy.interpolate.interp1d
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

    Notes
    -------
    Certain values may fail to interpolate in cases of 360 -> 365/366
    (Gregorian) calendar. Examples include projecting CPM data, which is
    able to fill in measurement values (e.g. `tasmax`) but the `year`
    and `month_number` variables have `nan` values

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
    if isinstance(xr_time_series, PathLike):
        if Path(xr_time_series).suffix.endswith(NETCDF_EXTENSION_STR):
            xr_time_series = open_dataset(
                xr_time_series, decode_coords="all", engine=engine
            )
        else:
            xr_time_series = open_dataset(xr_time_series, engine=engine)
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
        if extrapolate_fill_value:
            kwargs["fill_value"] = "extrapolate"
        return calendar_converted_ts.interpolate_na(
            dim="time",
            method=interpolate_method,
            keep_attrs=keep_attrs,
            limit=limit,
            **kwargs,
        )


def cpm_xarray_to_standard_calendar(cpm_xr_time_series: Dataset | PathLike) -> Dataset:
    """Convert a CPM `nc` file of 360 day calendars to standard calendar.

    Parameters
    ----------
    cpm_xr_time_series
        A raw `xarray` of the form provided by CPM.
    """
    xr_std_calendar: Dataset = convert_xr_calendar(
        cpm_xr_time_series, interpolate_na=True
    )
    time_bnds_fix: DataArray = cftime_range(
        xr_std_calendar.time.dt.strftime(ISO_DATE_FORMAT_STR).values[0],
        xr_std_calendar.time.dt.strftime(ISO_DATE_FORMAT_STR).values[-1],
    )
    xr_std_calendar["time_bnds"] = time_bnds_fix
    xr_std_calendar["month_number"] = xr_std_calendar.month_number.interpolate_na(
        "time", fill_value="extrapolate"
    )
    xr_std_calendar["year"] = xr_std_calendar.year.interpolate_na(
        "time", fill_value="extrapolate"
    )
    yyyymmdd_fix: DataArray = xr_std_calendar.time.dt.strftime(CLI_DATE_FORMAT_STR)
    xr_std_calendar["yyyymmdd"] = yyyymmdd_fix
    return xr_std_calendar


def reproject_coords(
    xr_time_series: Dataset,
    variable_name: str,
    x_grid: NDArray,
    y_grid: NDArray,
    xr_time_series_x_column_name: str,
    xr_time_series_y_column_name: str,
    method: str = "linear",
    engine: XArrayEngineType = NETCDF4_XARRAY_ENGINE,
    **kwargs,
) -> Dataset:
    """Reproject `xr_time_series` to `x_resolution`/`y_resolution`.

    Notes
    -----
    The `rio.reproject` approach commented out below raises
    `ValueError: IndexVariable objects must be 1-dimensional`
    See https://github.com/corteva/rioxarray/discussions/762
    """
    if isinstance(xr_time_series, PathLike | str):
        xr_time_series = open_dataset(
            xr_time_series, decode_coords="all", engine=engine
        )
    assert isinstance(xr_time_series, Dataset)
    # crs = crs if crs else xr_time_series.rio.crs
    # Code commented out below relates to a method following this discussion:
    # https://github.com/corteva/rioxarray/discussions/762
    # if x_resolution and y_resolution:
    #     kwargs['resolution'] = (x_resolution, y_resolution)
    # return xr_time_series.rio.reproject(dst_crs=crs, **kwargs)
    kwargs[xr_time_series_x_column_name] = x_grid
    kwargs[xr_time_series_y_column_name] = y_grid
    return xr_time_series[[variable_name]].interp(method=method, **kwargs)


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
    >>> pytest.skip('Refactor needed, may be removed.')
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> cropped = crop_nc(
    ...     RAW_CPM_TASMAX_PATH /
    ...     'tasmax_rcp85_land-cpm_uk_2.2km_01_day_19821201-19831130.nc',
    ...     crop_geom=glasgow_shape_file_path, invert=True)
    >>> cropped.rio.bounds() == glasgow_epsg_27700_bounds
    True
    """
    # xr_time_series = reproject_xarray_by_crs(
    #     xr_time_series,
    #     crs=final_crs,
    #     enforce_xarray_spatial_dims=enforce_xarray_spatial_dims,
    #     xr_spatial_xdim=xr_spatial_xdim,
    #     xr_spatial_ydim=xr_spatial_ydim,
    # )
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


# Previous approach, kept for reference
# def resample_hadukgrid(x: list | tuple) -> int:
#     """Resample UKHADs data spatially.
#
#     Results are saved to the output directory.
#
#     Parameters
#     ----------
#     x
#         x[0]: file to be resampled
#         x[1]: x_grid
#         x[2]: y_grid
#         x[3]: output_dir
#
#     Returns
#     -------
#     `0` if resampling is a success `1` if not.
#
#     Raises
#     ------
#     Exception
#         Generic execption for any errors raised.
#     """
#     try:
#         # due to the multiprocessing implementations inputs come as list
#         file = x[0]
#         x_grid = x[1]
#         y_grid = x[2]
#         output_dir = x[3]
#
#         name = os.path.basename(file)
#         output_name = (
#             f"{'_'.join(name.split('_')[:-1])}_2.2km_resampled_{name.split('_')[-1]}"
#         )
#         if os.path.exists(os.path.join(output_dir, output_name)):
#             print(f"File: {output_name} already exists in this directory. Skipping.")
#             return 0
#
#         # files have the variable name as input (e.g. tasmax_hadukgrid_uk_1km_day_20211101-20211130.nc)
#         variable = os.path.basename(file).split("_")[0]
#
#         data = open_dataset(file, decode_coords="all")
#
#         # # convert to 360 day calendar.
#         # data_360 = data.convert_calendar(
#         #     dim="time", calendar="360_day", align_on="year"
#         # )
#         # # apply correction if leap year
#         # if data.time.dt.is_leap_year.any():
#         #     data_360 = enforce_date_changes(data, data_360)
#
#         # the dataset to be resample must have dimensions named projection_x_coordinate and projection_y_coordinate .
#         # resampled = data_360[[variable]].interp(
#         #     projection_x_coordinate=x_grid,
#         #     projection_y_coordinate=y_grid,
#         #     method="linear",
#         # )
#         resampled = data[[variable]].interp(
#             projection_x_coordinate=x_grid,
#             projection_y_coordinate=y_grid,
#             method="linear",
#         )
#
#         # make sure we keep the original CRS
#         # resampled.rio.write_crs(data_360.rio.crs, inplace=True)
#         resampled.rio.write_crs(data.rio.crs, inplace=True)
#
#         # save resampled file
#         resampled.to_netcdf(os.path.join(output_dir, output_name))
#
#     except Exception as e:
#         print(f"File: {file} produced errors: {e}")
#     return 0


def reproject_standard_calendar_filename(path: Path) -> Path:
    """Return tweaked `path` to indicate standard day projection."""
    return path.parent / path.name.replace("_day", "_day_std_year")


def reproject_2_2km_filename(path: Path) -> Path:
    """Return tweaked `path` to indicate standard day projection."""
    return path.parent / path.name.replace("_1km", "_2_2km")


def gdal_warp_wrapper(
    input_path: PathLike,
    output_path: PathLike,
    output_crs: str = UK_SPATIAL_PROJECTION,
    output_x_resolution: int = CPRUK_RESOLUTION,
    output_y_resolution: int = CPRUK_RESOLUTION,
    copy_metadata: bool = True,
    return_path: bool = True,
    format: GDALFormatsType | None = GDALGeoTiffFormatStr,
    multithread: bool = True,
    **kwargs,
) -> Path | GDALDataset:
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
    output_crs
        Coordinate system to convert `input_path` file(s) to.
        `dstSRS` in `WarpOptions`.
    format
        Format to convert `input_path` to in `output_path`.
    output_x_resolution
        Resolution of `x` cordinates to convert `input_path` file(s) to.
        `xRes` in `WarpOptions`.
    output_y_resolution
        Resolution of `y` cordinates to convert `input_path` file(s) to.
        `yRes` in `WarpOptions`.
    copy_metadata
        Whether to copy metadata when possible.
    return_path
        Return the resulting path if `True`, else the new `GDALDataset`.
    resampling_method
        Sampling method. `resampleAlg` in `WarpOption`. See other options
        in: `https://gdal.org/programs/gdalwarp.html#cmdoption-gdalwarp-r`.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        assert not Path(output_path).is_dir()
    except AssertionError:
        raise FileExistsError(f"Path exists as a directory: {output_path}")
    if input_path == output_path:
        kwargs["overwrite"] = True
    warp_options: GDALWarpAppOptions = WarpOptions(
        dstSRS=output_crs,
        format=format,
        xRes=output_x_resolution,
        yRes=output_y_resolution,
        copyMetadata=copy_metadata,
        multithread=multithread,
        **kwargs,
    )
    projection: GDALDataset = Warp(
        destNameOrDestDS=output_path, srcDSOrSrcDSTab=input_path, options=warp_options
    )
    assert projection is not None
    return output_path if return_path else projection


def apply_geo_func(
    source_path: PathLike,
    func: Callable[[Dataset], Dataset],
    export_folder: PathLike,
    new_path_name_func: Callable[[Path], Path] | None = None,
    to_netcdf: bool = True,
    include_geo_warp_output_path: bool = False,
    return_results: bool = False,
    **kwargs,
) -> Path | Dataset | GDALDataset:
    """Apply a `Callable` to `netcdf_source` file and export via `to_netcdf`.

    Parameters
    ----------
    source_path
        `netcdf` file to apply `func` to.
    func
        `Callable` to modify `netcdf`.
    export_folder
        Where to save results.
    path_name_replace_tuple
        Optional replacement `str` to apply to `source_path.name` when exporting
    to_netcdf
        Whether to call `to_netcdf` method on `results` `Dataset`.
    """
    export_path: Path = Path(source_path)
    if new_path_name_func:
        export_path = new_path_name_func(export_path)
    export_path = Path(export_folder) / export_path.name
    if include_geo_warp_output_path:
        kwargs["output_path"] = export_path
    results: Dataset | Path | GDALDataset = func(source_path, **kwargs)
    if to_netcdf:
        if isinstance(results, Path):
            results = open_dataset(results)
        if isinstance(results, GDALDataset):
            raise TypeError(
                f"Restuls from 'gdal_warp_wrapper' can't directly export to NetCDF form, only return a Path or GDALDataset"
            )
        assert isinstance(results, Dataset)
        results.to_netcdf(export_path)
    if return_results:
        return results
    else:
        return export_path


@dataclass(kw_only=True)
class HADsResampler:
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    grid_data_path
        `Path` to load to `self.grid`.
    grid
        `Dataset` of grid (either passed via `grid_data_path` or as a parameter).
    input_files
        NCF or TIF files to process with `self.grid` etc.
    resampling_func
        Function to call on `self.input_files` with `self.grid`
    crop
        Path or file to spatially crop `input_files` with.
    final_crs
        Coordinate Reference System (CRS) to return final format in.
    grid_x_column_name
        Column name in `input_files` or `input` for `x` coordinates.
    grid_y_column_name
        Column name in `input_files` or `input` for `y` coordinates.
    input_file_extension
        File extensions to glob `input_files` with.

    Notes
    -----
    - [x] Try time projection first
    - [x] Then space
    - then crop

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> hads_resampler: HADsResampler = HADsResampler(
    ...     output_path=resample_test_hads_output_path,
    ... )
    >>> hads_resampler
    <HADsResampler(...count=504,...
        ...input_path='.../tasmax/day',...
        ...output_path='.../resample/runs/hads')>
    >>> pprint(hads_resampler.input_files)
    (...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc'),
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800201-19800229.nc'),
     ...,
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc'))
    """

    input_path: PathLike | None = RAW_HADS_TASMAX_PATH
    output_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_LOCAL_PATH
    variable_name: VariableOptions = VariableOptions.default()
    grid: PathLike | Dataset = DEFAULT_RELATIVE_GRID_DATA_PATH
    input_files: Iterable[PathLike] | None = None
    cpus: int | None = None
    crop: PathLike | GeoDataFrame | None = None
    final_crs: str = UK_SPATIAL_PROJECTION
    grid_x_column_name: str = HADS_XDIM
    grid_y_column_name: str = HADS_YDIM
    input_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    export_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    cpm_resolution_relative_path: Path = HADS_2_2K_RESOLUTION_PATH
    input_file_x_column_name: str = HADS_XDIM
    input_file_y_column_name: str = HADS_YDIM
    start_index: int = 0
    stop_index: int | None = None

    def __post_init__(self) -> None:
        """Generate related attributes."""
        try:
            assert self.input_path or self.input_files
        except AssertionError:
            raise AttributeError(
                f"'input_path' or 'input_file' are None; at least one must be set."
            )
        self.set_grid_x_y()
        self.set_input_files()
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        self.total_cpus: int | None = cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus

    def __len__(self) -> int:
        """Return the length of `self.input_files`."""
        return (
            len(self.input_files[self.start_index : self.stop_index])
            if isinstance(self.input_files, Sequence)
            else 0
        )

    @property
    def max_count(self) -> int:
        """Maximum length of `self.input_files` ignoring `start_index` and `start_index`."""
        return len(self.input_files) if isinstance(self.input_files, Sequence) else 0

    def __iter__(self) -> Iterator[Path] | None:
        if self.input_files and isinstance(self.input_files, Sequence):
            for file_path in self.input_files[self.start_index : self.stop_index]:
                yield Path(file_path)
        else:
            return None

    def __getitem__(self, key: int | slice) -> Path | tuple[Path] | None:
        if not self.input_files:
            return None
        elif isinstance(key, int):
            return Path(self.input_files[key])
        elif isinstance(key, slice):
            return tuple(Path(path) for path in self.input_files[key])
        else:
            raise IndexError(f"Can only index with 'int', not: '{key}'")

    def set_input_files(self, new_input_path: PathLike | None = None) -> None:
        """Replace `self.input` and process `self.input_files`."""
        if new_input_path:
            self.input = new_input_path
        if not self.input_files or new_input_path:
            self.input_files = tuple(
                Path(path)
                for path in glob(
                    f"{self.input_path}/*.{self.input_file_extension}", recursive=True
                )
            )

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"count={len(self)}, "
            f"max_count={self.max_count}, "
            f"input_path='{self.input_path}', "
            f"output_path='{self.output_path}')>"
        )

    def set_grid(self, new_grid_data_path: PathLike | None = None) -> None:
        """Set check and set (if necessary) `grid` attribute of `self`.

        Parameters
        ----------
        new_grid_data_path
            New `Path` to load to `self.grid`.
        """
        if new_grid_data_path:
            self.grid = new_grid_data_path
        if isinstance(self.grid, PathLike):
            self._grid_path = Path(self.grid)
            self.grid = open_dataset(self.grid)
        assert isinstance(self.grid, Dataset)

    def _get_source_path(
        self, index: int, source_to_index: Sequence | None = None
    ) -> Path:
        """Return a path indexed from `source_to_index` or `self`."""
        if source_to_index is None:
            return self[index]
        elif isinstance(source_to_index, str):
            return getattr(self, source_to_index)[index]
        else:
            return source_to_index[index]

    def _output_path(
        self, relative_output_path: Path, override_export_path: Path | None
    ) -> Path:
        path: PathLike = (
            override_export_path or Path(self.output_path) / relative_output_path
        )
        path.mkdir(exist_ok=True, parents=True)
        return path

    def to_reprojection(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        return_results: bool = False,
        source_to_index: Sequence | None = None,
    ) -> Path | Dataset:
        source_path: Path = self._get_source_path(
            index=index, source_to_index=source_to_index
        )
        path: PathLike = self._output_path(
            self.cpm_resolution_relative_path, override_export_path
        )
        return apply_geo_func(
            source_path=source_path,
            func=reproject_coords,
            export_folder=path,
            # Leaving in case we return to using warp
            # include_geo_warp_output_path=True,
            # to_netcdf=False,
            to_netcdf=True,
            variable_name=self.variable_name,
            x_grid=self.x,
            y_grid=self.y,
            xr_time_series_x_column_name=self.input_file_x_column_name,
            xr_time_series_y_column_name=self.input_file_y_column_name,
            new_path_name_func=reproject_2_2km_filename,
            return_results=return_results,
        )

    def _range_call(
        self,
        method: Callable,
        start: int,
        stop: int | None,
        step: int,
        override_export_path: Path | None = None,
        source_to_index: Iterable | None = None,
    ) -> list[Path | Dataset]:
        export_paths: list[Path] = []
        if stop is None:
            stop = len(self)
        for index in trange(start, stop, step):
            export_paths.append(
                method(
                    index=index,
                    override_export_path=override_export_path,
                    source_to_index=source_to_index,
                )
            )
        return export_paths

    def range_to_reprojection(
        self,
        start: int | None = None,
        stop: int | None = None,
        step: int = 1,
        override_export_path: Path | None = None,
        source_to_index: Sequence | None = None,
    ) -> list[Path]:
        start = start or self.start_index
        stop = stop or self.stop_index
        return self._range_call(
            method=self.to_reprojection,
            start=start,
            stop=stop,
            step=step,
            override_export_path=override_export_path,
            source_to_index=source_to_index,
        )

    def set_grid_x_y(
        self,
        grid_x_column_name: str | None = None,
        grid_y_column_name: str | None = None,
    ) -> None:
        """Set the `x` `y` values via `grid_x_column_name` and `grid_y_column_name`.

        Parameters
        ----------
        grid_x_column_name
            Name of column in `self.grid` `Dataset` to extract to `self.x`.
            If `None` use `self.grid_x_column_name`, else overwrite.
        grid_y_column_name
            Name of column in `self.grid` `Dataset` to extract to `self.y`.
            If `None` use `self.grid_y_column_name`, else overwrite.
        """
        if self.grid is None or isinstance(self.grid, PathLike):
            self.set_grid()
        assert isinstance(self.grid, Dataset)
        self.grid_x_column_name = grid_x_column_name or self.grid_x_column_name
        self.grid_y_column_name = grid_y_column_name or self.grid_y_column_name
        self.x: NDArray = self.grid[self.grid_x_column_name][:].values
        self.y: NDArray = self.grid[self.grid_y_column_name][:].values

    def execute(self, skip_spatial: bool = False, **kwargs) -> list[Path] | None:
        """Run all steps for processing"""
        return self.range_to_reprojection(**kwargs) if not skip_spatial else None


@dataclass(kw_only=True, repr=False)
class CPMResampler(HADsResampler):
    """CPM specific changes to HADsResampler.

    Attributes
    ----------
    input_path
        `Path` to `CPM` files to process.
    output
        `Path` to save processed `CPM` files.
    grid_data_path
        `Path` to load to `self.grid`.
    grid
        `Dataset` of grid (either passed via `grid_data_path` or as a parameter).
    input_files
        NCF or TIF files to process with `self.grid` etc.
    cpus
        Number of `cpu` cores to use during multiprocessing.
    resampling_func
        Function to call on `self.input_files` with `self.grid`
    crop
        Path or file to spatially crop `input_files` with.
    final_crs
        Coordinate Reference System (CRS) to return final format in.
    grid_x_column_name
        Column name in `input_files` or `input` for `x` coordinates.
    grid_y_column_name
        Column name in `input_files` or `input` for `y` coordinates.
    input_file_extension
        File extensions to glob `input_files` with.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> cpm_resampler: CPMResampler = CPMResampler(
    ...     input_path=REPROJECTED_CPM_TASMAX_01_LATEST_INPUT_PATH,
    ...     output_path=resample_test_cpm_output_path,
    ...     input_file_extension=TIF_EXTENSION_STR,
    ... )
    >>> cpm_resampler
    <CPMResampler(...count=100,...
        ...input_path='.../tasmax/01/latest',...
        ...output_path='.../resample/runs/cpm')>
    >>> pprint(cpm_resampler.input_files)
    (...Path('.../tasmax/01/latest/tasmax_...-cpm_uk_2.2km_01_day_19801201-19811130.tif'),
     ...Path('.../tasmax/01/latest/tasmax_...-cpm_uk_2.2km_01_day_19811201-19821130.tif'),
     ...,
     ...Path('.../tasmax/01/latest/tasmax_...-cpm_uk_2.2km_01_day_20791201-20801130.tif'))

    """

    input_path: PathLike | None = RAW_CPM_TASMAX_PATH
    output_path: PathLike = RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_LOCAL_PATH
    standard_calendar_relative_path: Path = CPM_STANDARD_CALENDAR_PATH
    input_file_x_column_name: str = CPRUK_XDIM
    input_file_y_column_name: str = CPRUK_YDIM
    cpm_resolution_relative_path: Path = CPM_SPATIAL_COORDS_PATH

    @property
    def cpm_variable_name(self) -> str:
        return VariableOptions.cpm_value(self.variable_name)

    def to_standard_calendar(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        source_to_index: Sequence | None = None,
    ) -> Path:
        source_path: Path = self._get_source_path(
            index=index, source_to_index=source_to_index
        )
        path: PathLike = self._output_path(
            self.standard_calendar_relative_path, override_export_path
        )
        path.mkdir(exist_ok=True, parents=True)
        return apply_geo_func(
            source_path=source_path,
            func=cpm_xarray_to_standard_calendar,
            export_folder=path,
            new_path_name_func=reproject_standard_calendar_filename,
        )

    def range_to_standard_calendar(
        self,
        start: int | None = None,
        stop: int | None = None,
        step: int = 1,
        override_export_path: Path | None = None,
        source_to_index: Sequence | None = None,
    ) -> list[Path]:
        start = start or self.start_index
        stop = stop or self.stop_index
        return self._range_call(
            method=self.to_standard_calendar,
            start=start,
            stop=stop,
            step=step,
            override_export_path=override_export_path,
            source_to_index=source_to_index,
        )

    def to_reprojection(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        return_results: bool = False,
        source_to_index: Sequence | None = None,
    ) -> Path | Dataset:
        source_path: Path = self._get_source_path(
            index=index, source_to_index=source_to_index
        )
        path: PathLike = self._output_path(
            self.cpm_resolution_relative_path, override_export_path
        )
        return apply_geo_func(
            source_path=source_path,
            func=reproject_coords,
            export_folder=path,
            # Leaving in case we return to using warp
            # include_geo_warp_output_path=True,
            # to_netcdf=False,
            to_netcdf=True,
            variable_name=self.cpm_variable_name,
            x_grid=self.x,
            y_grid=self.y,
            xr_time_series_x_column_name=self.input_file_x_column_name,
            xr_time_series_y_column_name=self.input_file_y_column_name,
            return_results=return_results,
        )

    def execute(
        self, skip_temporal: bool = False, skip_spatial: bool = False, **kwargs
    ) -> list[Path] | None:
        """Run all steps for processing"""
        temporal_reprojected_data: list[Path] | None = None
        spatial_reprojected_data: list[Path] | None = None
        if not skip_temporal:
            temporal_reprojected_data = self.range_to_standard_calendar(**kwargs)
        if not skip_spatial:
            spatial_reprojected_data: list[Path] = self.range_to_reprojection(
                source_to_index=temporal_reprojected_data, **kwargs
            )
        return spatial_reprojected_data


@dataclass(kw_only=True)
class HADsResamplerManager:

    """Class to manage processing HADs resampling.

    Attributes
    ----------
    input_paths
        `Path` or `Paths` to `CPM` files to process. If `Path`, will be propegated with files matching
    output_paths
        `Path` or `Paths` to to save processed `CPM` files to. If `Path` will be propagated to match `input_paths`.
    variables
        Which `VariableOptions` to include.
    sub_path
        `Path` to include at the stem of generating `input_paths`.
    cpus
        Number of `cpu` cores to use during multiprocessing.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> hads_resampler_manager: HADsResamplerManager = HADsResamplerManager(
    ...     variables=VariableOptions.all(),
    ...     output_paths=resample_test_hads_output_path,
    ...     )
    >>> hads_resampler_manager
    <HADsResamplerManager(variables_count=3, input_paths_count=3)>
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_HADS_PATH
    output_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_LOCAL_PATH
    )
    variables: Sequence[VariableOptions | str] = (VariableOptions.default(),)
    sub_path: Path = Path("day")
    start_index: int = 0
    stop_index: int | None = None
    start_date: date = HADS_START_DATE
    end_date: date = HADS_END_DATE
    configs: list[HADsResampler] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    resampler_class: type[HADsResampler] = HADsResampler
    cpus: int | None = None
    _var_path_dict: dict[VariableOptions | str, Sequence[Path]] = field(
        default_factory=dict
    )
    _strict_fail_if_var_in_input_path: bool = True

    class VarirableInBaseImportPathError(Exception):
        """Checking import path validity for `self.variables`."""

        pass

    def __post_init__(self) -> None:
        """Populate config attributes."""
        self.check_paths()
        self.total_cpus: int | None = cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus

    @property
    def input_folder(self) -> Path | None:
        """Return `self._input_path` set by `set_input_paths()`."""
        if hasattr(self, "_input_path"):
            return Path(self._input_path)
        else:
            return None

    @property
    def output_folder(self) -> Path | None:
        """Return `self._output_path` set by `set_output_paths()`."""
        if hasattr(self, "_input_path"):
            return Path(self._input_path)
        else:
            return None

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"variables_count={len(self.variables)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_folder_paths(
        self, path: PathLike, append_var_path_dict: bool = False
    ) -> Iterator[Path]:
        """Return a Generator of paths of `self.variables` and `self.runs`."""
        var_path: Path
        for var in self.variables:
            var_path = Path(path) / var / self.sub_path
            if append_var_path_dict:
                self._var_path_dict[var_path] = var
            yield var_path

    def check_paths(self, run_set_data_paths: bool = True):
        """Check if all `self.input_paths` exist."""

        if run_set_data_paths:
            self.set_input_paths()
            self.set_output_paths()
        assert isinstance(self.input_paths, Iterable)
        assert isinstance(self.output_paths, Iterable)
        assert len(self.input_paths) == len(self.output_paths)
        for path in self.input_paths:
            try:
                assert Path(path).exists()
                assert Path(path).is_dir()
            except AssertionError:
                raise FileExistsError(
                    f"One of 'self.input_paths' in {self} not valid: '{path}'"
                )
            try:
                assert path in self._var_path_dict
            except:
                NotImplemented(
                    f"Syncing `self._var_path_dict` with changes to `self.input_paths`."
                )

    def set_input_paths(self):
        """Propagate `self.input_paths` if needed."""
        if isinstance(self.input_paths, PathLike):
            self._input_path = self.input_paths
            self.input_paths = tuple(
                self._gen_folder_paths(self.input_paths, append_var_path_dict=True)
            )
            if self._strict_fail_if_var_in_input_path:
                for var in self.variables:
                    try:
                        assert var not in str(self._input_path)
                    except AssertionError:
                        raise self.VarirableInBaseImportPathError(
                            f"Folder named '{var}' in self._input_path: "
                            f"'{self._input_path}'. Try passing a parent path or "
                            f"set '_strict_fail_if_var_in_input_path' to 'False'."
                        )

    def set_output_paths(self):
        """Propagate `self.output_paths` if needed."""
        if isinstance(self.output_paths, PathLike):
            self._output_path = self.output_paths
            self.output_paths = tuple(self._gen_folder_paths(self.output_paths))

    def yield_configs(self) -> Iterable[HADsResampler]:
        """Generate a `CPMResampler` or `HADsResampler` for `self.input_paths`."""
        self.check_paths()
        assert isinstance(self.input_paths, Iterable)
        assert isinstance(self.output_paths, Iterable)
        for index, var_path in enumerate(self._var_path_dict.items()):
            yield self.resampler_class(
                input_path=var_path[0],
                output_path=self.output_paths[index],
                variable_name=var_path[1],
                start_index=self.start_index,
                stop_index=self.stop_index,
                **self.config_default_kwargs,
            )

    def __len__(self) -> int:
        """Return the length of `self.input_files`."""
        return (
            len(self.input_paths[self.start_index : self.stop_index])
            if isinstance(self.input_paths, Sequence)
            else 0
        )

    @property
    def max_count(self) -> int:
        """Maximum length of `self.input_files` ignoring `start_index` and `start_index`."""
        return len(self.input_paths) if isinstance(self.input_paths, Sequence) else 0

    def __iter__(self) -> Iterator[Path] | None:
        if isinstance(self.input_paths, Sequence):
            for file_path in self.input_paths[self.start_index : self.stop_index]:
                yield Path(file_path)
        else:
            return None

    def __getitem__(self, key: int | slice) -> Path | tuple[Path, ...] | None:
        if not self.input_paths or not isinstance(self.input_paths, Sequence):
            return None
        elif isinstance(key, int):
            return Path(self.input_paths[key])
        elif isinstance(key, slice):
            return tuple(Path(path) for path in self.input_paths[key])
        else:
            raise IndexError(f"Can only index with 'int', not: '{key}'")

    def execute_resample_configs(
        self, multiprocess: bool = False, cpus: int | None = None
    ) -> tuple[CPMResampler | HADsResampler, ...]:
        """Run all resampler configurations

        Parameters
        ----------
        multiprocess
            If `True` multiprocess running `resample_configs`.

        """
        resamplers: tuple[CPMResampler | HADsResampler, ...] = tuple(
            self.yield_configs()
        )
        results: list[list[Path] | None] = []
        if multiprocess:
            cpus = cpus or self.cpus
            if self.total_cpus and cpus:
                assert cpus <= self.total_cpus
            with Pool(processes=cpus) as pool:
                results = list(
                    tqdm(
                        pool.imap_unordered(run_callable_attr, resamplers),
                        total=len(self),
                    )
                )
            # This currently fails if two tasks are involved
            # , ideally reneamble to support a multiplicative of tasks
            # assert len(results) == len(self)
        else:
            for resampler in resamplers:
                print(resampler)
                results.append(resampler.execute())
        return resamplers


@dataclass(kw_only=True, repr=False)
class CPMResamplerManager(HADsResamplerManager):

    """Class to manage processing CPM resampling.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> cpm_resampler_manager: CPMResamplerManager = CPMResamplerManager(
    ...     stop_index=10,
    ...     output_paths=resample_test_cpm_output_path,
    ...     )
    >>> cpm_resampler_manager
    <CPMResamplerManager(variables_count=1, runs_count=4,
                         input_files_count=4)>
    >>> configs: tuple[CPMResampler, ...] = tuple(
    ...     cpm_resampler_manager.yield_configs())
    >>> pprint(configs)
    (<CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/05/latest',
                   output_path='.../tasmax/05/latest')>,
     <CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/06/latest',
                   output_path='.../tasmax/06/latest')>,
     <CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/07/latest',
                   output_path='.../tasmax/07/latest')>,
     <CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/08/latest',
                   output_path='.../tasmax/08/latest')>)
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_CPM_PATH
    output_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_LOCAL_PATH
    )
    sub_path: Path = Path("latest")
    start_date: date = CPM_START_DATE
    end_date: date = CPM_END_DATE
    configs: list[CPMResampler] = field(default_factory=list)
    resampler_class: type[CPMResampler] = CPMResampler
    runs: Sequence[RunOptions] = RunOptions.preferred()

    # @property
    # def cpm_vars(self) -> tuple[str, ...]:
    #     """Ensure variable paths match `CPM` path names."""
    #     return VariableOptions.cpm_values(self.variables)

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"variables_count={len(self.variables)}, "
            f"runs_count={len(self.runs)}, "
            f"input_files_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_folder_paths(
        self, path: PathLike, append_var_path_dict: bool = False, cpm_paths: bool = True
    ) -> Iterator[Path]:
        """Return a Generator of paths of `self.variables` and `self.runs`."""
        var_path: Path
        for var in self.variables:
            for run_type in self.runs:
                if cpm_paths:
                    var_path: Path = (
                        Path(path)
                        / VariableOptions.cpm_value(var)
                        / run_type
                        / self.sub_path
                    )
                else:
                    var_path: Path = Path(path) / var / run_type / self.sub_path
                if append_var_path_dict:
                    self._var_path_dict[var_path] = var
                yield var_path


# Kept from previous structure for reference
# if __name__ == "__main__":
#     """
#     Script to resample UKHADs data from the command line
#     """
#     # Initialize parser
#     parser = argparse.ArgumentParser()
#
#     # Adding arguments
#     parser.add_argument(
#         "--input-path",
#         help="Path where the .nc files to resample is located",
#         required=True,
#         type=str,
#     )
#     parser.add_argument(
#         "--grid-data-path",
#         help="Path where the .nc file with the grid to resample is located",
#         required=False,
#         type=str,
#         default="../../data/rcp85_land-cpm_uk_2.2km_grid.nc",
#     )
#     parser.add_argument(
#         "--output-path",
#         help="Path to save the resampled data data",
#         required=False,
#         default=".",
#         type=str,
#     )
#     parser_args = parser.parse_args()
#     hads_run_manager = HADsResampler(
#         input_path=parser_args.input_path,
#         grid_data_path=parser_args.grid_data_path,
#         output_path=parser_args.output,
#     )
#     res = hads_run_manager.resample_multiprocessing()
#
#     parser_args = parser.parse_args()
#
#     # reading baseline grid to resample files to
#     grid = xr.open_dataset(parser_args.grid_data)
#
#     try:
#         # must have dimensions named projection_x_coordinate and projection_y_coordinate
#         x = grid["projection_x_coordinate"][:].values
#         y = grid["projection_y_coordinate"][:].values
#     except Exception as e:
#         print(f"Grid file: {parser_args.grid_data} produced errors: {e}")
#
#     # If output file do not exist create it
#     if not os.path.exists(parser_args.output):
#         os.makedirs(parser_args.output)
#
#     # find all nc files in input directory
#     files = glob.glob(f"{parser_args.input}/*.nc", recursive=True)
#     N = len(files)
#
#     args = [[f, x, y, parser_args.output] for f in files]
#
#     with multiprocessing.Pool(processes=os.cpu_count() - 1) as pool:
#         res = list(tqdm(pool.imap_unordered(resample_hadukgrid, args), total=N))
