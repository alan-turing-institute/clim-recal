from dataclasses import dataclass
from datetime import date, datetime, timedelta
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Literal

import numpy as np
import rioxarray  # nopycln: import
from cftime._cftime import Datetime360Day
from geopandas import GeoDataFrame, read_file
from numpy import array, random
from numpy.typing import NDArray
from osgeo.gdal import Dataset as GDALDataset
from osgeo.gdal import GDALWarpAppOptions, Warp, WarpOptions
from pandas import DatetimeIndex, date_range, to_datetime
from xarray import CFTimeIndex, DataArray, Dataset, cftime_range, open_dataset
from xarray.backends.api import ENGINES
from xarray.coding.calendar_ops import convert_calendar
from xarray.core.types import CFCalendar, InterpOptions

from .core import (
    CLI_DATE_FORMAT_STR,
    ISO_DATE_FORMAT_STR,
    DateType,
    climate_data_mount_path,
    date_range_generator,
    date_range_to_str,
    time_str,
)
from .gdal_formats import (
    NETCDF_EXTENSION_STR,
    GDALFormatExtensions,
    GDALFormatsType,
    GDALGeoTiffFormatStr,
)

logger = getLogger(__name__)

DropDayType = set[tuple[int, int]]
ChangeDayType = set[tuple[int, int]]

# MONTH_DAY_DROP: DropDayType = {(1, 31), (4, 1), (6, 1), (8, 1), (10, 1), (12, 1)}
"""A `set` of tuples of month and day numbers for `enforce_date_changes`."""

BRITISH_NATIONAL_GRID_EPSG: Final[str] = "EPSG:27700"

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

GLASGOW_CENTRE_COORDS: Final[tuple[float, float]] = (55.86279, -4.25424)
MANCHESTER_CENTRE_COORDS: Final[tuple[float, float]] = (53.48095, -2.23743)
LONDON_CENTRE_COORDS: Final[tuple[float, float]] = (51.509865, -0.118092)
THREE_CITY_CENTRE_COORDS: Final[dict[str, tuple[float, float]]] = {
    "Glasgow": GLASGOW_CENTRE_COORDS,
    "Manchester": MANCHESTER_CENTRE_COORDS,
    "London": LONDON_CENTRE_COORDS,
}
"""City centre `(lon, lat)` `tuple` coords of `Glasgow`, `Manchester` and `London`."""

XARRAY_EXAMPLE_RANDOM_SEED: Final[int] = 0
# Default 4 year start and end date covering leap year
XARRAY_EXAMPLE_START_DATE_STR: Final[str] = "1980-11-30"
XARRAY_EXAMPLE_END_DATE_4_YEARS: Final[str] = "1984-11-30"


GLASGOW_GEOM_LOCAL_PATH: Final[Path] = Path(
    "shapefiles/three.cities/Glasgow/Glasgow.shp"
)
GLASGOW_GEOM_ABSOLUTE_PATH: Final[Path] = (
    climate_data_mount_path() / GLASGOW_GEOM_LOCAL_PATH
)

BoundsTupleType = tuple[float, float, float, float]
"""`GeoPandas` bounds: (`minx`, `miny`, `maxx`, `maxy`)."""

XArrayEngineType = Literal[*tuple(ENGINES)]
"""Engine types supported by `xarray` as `str`."""

DEFAULT_CALENDAR_ALIGN: Final[ConvertCalendarAlignOptions] = "year"
NETCDF4_XARRAY_ENGINE: Final[str] = "netcdf4"


CPM_365_OR_366_INTERMEDIATE_NC: Final[str] = "cpm-365-or-366.nc"
CPM_365_OR_366_SIMPLIFIED_NC: Final[str] = "cpm-365-or-366-simplified.nc"
CPM_365_OR_366_27700_TIF: Final[str] = "cpm-365-or-366-27700.tif"
CPM_365_OR_366_27700_FINAL: Final[str] = "cpm-365-or-366-27700-final.nc"
CPM_LOCAL_INTERMEDIATE_PATH: Final[Path] = Path("cpm-intermediate-files")

FINAL_RESAMPLE_LAT_COL: Final[str] = "x"
FINAL_RESAMPLE_LON_COL: Final[str] = "y"


def cpm_xarray_to_standard_calendar(
    cpm_xr_time_series: Dataset | PathLike, include_bnds_index: bool = False
) -> Dataset:
    """Convert a CPM `nc` file of 360 day calendars to standard calendar.

    Parameters
    ----------
    cpm_xr_time_series
        A raw `xarray` of the form provided by CPM.
    include_bnds_index
        Whether to fix `bnds` indexing in returned `Dataset`.

    Returns
    -------
    `Dataset` calendar converted to standard (Gregorian).
    """
    if isinstance(cpm_xr_time_series, PathLike):
        cpm_xr_time_series = open_dataset(cpm_xr_time_series, decode_coords="all")
    cpm_to_std_calendar: Dataset = convert_xr_calendar(
        cpm_xr_time_series, interpolate_na=True, check_cftime_cols=("time_bnds",)
    )
    cpm_to_std_calendar[
        "month_number"
    ] = cpm_to_std_calendar.month_number.interpolate_na(
        "time", fill_value="extrapolate"
    )
    cpm_to_std_calendar["year"] = cpm_to_std_calendar.year.interpolate_na(
        "time", fill_value="extrapolate"
    )
    yyyymmdd_fix: DataArray = cpm_to_std_calendar.time.dt.strftime(CLI_DATE_FORMAT_STR)
    cpm_to_std_calendar["yyyymmdd"] = yyyymmdd_fix
    assert cpm_xr_time_series.rio.crs == cpm_to_std_calendar.rio.crs
    if include_bnds_index:
        std_calendar_drop_bnds = cpm_to_std_calendar.drop_dims("bnds")
        cpm_to_std_calendar_fixed_bnds = std_calendar_drop_bnds.expand_dims(
            dim={"bnds": cpm_xr_time_series.bnds}
        )
        return cpm_to_std_calendar_fixed_bnds
    else:
        return cpm_to_std_calendar


def cftime360_to_date(cf_360: Datetime360Day) -> date:
    """Convert a `Datetime360Day` into a `date`.

    Examples
    --------
    >>> cftime360_to_date(Datetime360Day(1980, 1, 1))
    datetime.date(1980, 1, 1)
    """
    return date(cf_360.year, cf_360.month, cf_360.day)


@dataclass
class IntermediateCPMFilesManager:

    """Manage intermediate files and paths for CPM calendar projection.

    Parameters
    ----------
    variable_name
        Name of variable (e.g. `tasmax`). Included in file names.
    output_path
        Folder to save results to.
    subfolder_path
        Path to place intermediate files relative to output_path.
    time_series_start
        Start of time series covered to include in file name.
    time_series_end
        End of time series covered to include in file name.
    file_name_prefix
        str to add at the start of the output file names (after integers).
    subfolder_time_stamp
        Whether to include a time stamp in the subfolder file name.

    Examples
    --------
    >>> test_path = getfixture('tmp_path')
    >>> intermedate_files_manager: IntermediateCPMFilesManager = IntermediateCPMFilesManager(
    ...     file_name_prefix="test-1980",
    ...     variable_name="tasmax",
    ...     output_path=test_path / 'intermediate_test_folder',
    ...     subfolder_path='test_subfolder',
    ...     time_series_start=date(1980, 12, 1),
    ...     time_series_end=date(1981, 11, 30),
    ... )
    >>> str(intermedate_files_manager.output_path)
    '.../intermediate_test_folder'
    >>> str(intermedate_files_manager.intermediate_files_folder)
    '.../test_subfolder'
    >>> str(intermedate_files_manager.final_nc_path)
    '.../test_subfolder/3-test-1980-tasmax-19801201-19811130-cpm-365-or-366-27700-final.nc'
    """

    variable_name: str
    output_path: Path | None
    time_series_start: datetime | date | Datetime360Day
    time_series_end: datetime | date | Datetime360Day
    subfolder_path: Path = CPM_LOCAL_INTERMEDIATE_PATH
    file_name_prefix: str = ""
    subfolder_time_stamp: bool = False

    def __post_init__(self) -> None:
        """Ensure `output_path`, `time_series_start/end` are set correctly."""
        self.output_path = Path(self.output_path) if self.output_path else Path()
        if self.output_path.suffix[1:] in GDALFormatExtensions.values():
            logger.info(
                f"Output path is: '{str(self.output_path)}'\n"
                "Putting intermediate files in parent directory."
            )
            self._passed_output_path = self.output_path
            self.output_path = self.output_path.parent
        if isinstance(self.time_series_start, Datetime360Day):
            self.time_series_start = cftime360_to_date(self.time_series_start)
        if isinstance(self.time_series_end, Datetime360Day):
            self.time_series_end = cftime360_to_date(self.time_series_end)

    def __repr__(self):
        """Summary of config."""
        return (
            f"<IntermediateCPMFilesManager(output_path='{self.output_path}, "
            f"intermediate_files_folder='{self.intermediate_files_folder})>"
        )

    @property
    def date_range_to_str(self) -> str:
        return date_range_to_str(self.time_series_start, self.time_series_end)

    @property
    def prefix_var_name_and_date(self) -> str:
        prefix: str = f"{self.variable_name}-{self.date_range_to_str}-"
        if self.file_name_prefix:
            return f"{self.file_name_prefix}-{prefix}"
        else:
            return prefix

    @property
    def subfolder(self) -> Path:
        if self.subfolder_time_stamp:
            return Path(Path(self.subfolder_path).name + f"-{time_str()}")
        else:
            return self.subfolder_path

    @property
    def intermediate_files_folder(self) -> Path:
        assert self.output_path
        path: Path = self.output_path / self.subfolder
        path.mkdir(exist_ok=True, parents=True)
        assert path.is_dir()
        return path

    @property
    def intermediate_nc_path(self) -> Path:
        return self.intermediate_files_folder / (
            "0-" + self.prefix_var_name_and_date + CPM_365_OR_366_INTERMEDIATE_NC
        )

    @property
    def simplified_nc_path(self) -> Path:
        return self.intermediate_files_folder / (
            "1-" + self.prefix_var_name_and_date + CPM_365_OR_366_SIMPLIFIED_NC
        )

    @property
    def intermediate_warp_path(self) -> Path:
        return self.intermediate_files_folder / (
            "2-" + self.prefix_var_name_and_date + CPM_365_OR_366_27700_TIF
        )

    @property
    def final_nc_path(self) -> Path:
        return self.intermediate_files_folder / (
            "3-" + self.prefix_var_name_and_date + CPM_365_OR_366_27700_FINAL
        )


def cpm_reproject_with_standard_calendar(
    cpm_xr_time_series: Dataset | PathLike,
    variable_name: str,
    output_path: PathLike,
    file_name_prefix: str = "",
    subfolder: PathLike = CPM_LOCAL_INTERMEDIATE_PATH,
    subfolder_time_stamp: bool = False,
    final_x_coord_label: str = FINAL_RESAMPLE_LON_COL,
    final_y_coord_label: str = FINAL_RESAMPLE_LAT_COL,
) -> Dataset:
    """Convert raw `cpm_xr_time_series` to an 365/366 days and 27700 coords.

    Parameters
    ----------
    cpm_xr_time_series
        `Dataset` (or path to load as `Dataset`) expected to be in raw UKCPM
        format, with 360 day years and a rotated coordinate system.
    output_folder
        Path to store all intermediary and final projection.
    file_name_prefix
        `str` to prefix all written files with.

    Returns
    -------
    Final `xarray` `Dataset` after spatial and temporal changes.
    """
    if isinstance(cpm_xr_time_series, PathLike):
        cpm_xr_time_series = open_dataset(cpm_xr_time_series, decode_coords="all")

    intermediate_files: IntermediateCPMFilesManager = IntermediateCPMFilesManager(
        file_name_prefix=file_name_prefix,
        variable_name=variable_name,
        output_path=Path(output_path),
        subfolder_path=Path(subfolder),
        subfolder_time_stamp=subfolder_time_stamp,
        time_series_start=cpm_xr_time_series.time.values[0],
        time_series_end=cpm_xr_time_series.time.values[-1],
    )

    expanded_calendar: Dataset = cpm_xarray_to_standard_calendar(cpm_xr_time_series)
    subset_within_ensemble: DataArray = expanded_calendar[variable_name][0]
    subset_within_ensemble.to_netcdf(intermediate_files.intermediate_nc_path)
    simplified_netcdf: Dataset = open_dataset(
        intermediate_files.intermediate_nc_path, decode_coords="all"
    )
    simplified_netcdf[variable_name].to_netcdf(intermediate_files.simplified_nc_path)

    warped_to_22700_path = gdal_warp_wrapper(
        input_path=intermediate_files.simplified_nc_path,
        output_path=intermediate_files.intermediate_warp_path,
        copy_metadata=True,
        format=None,
    )

    assert warped_to_22700_path == intermediate_files.intermediate_warp_path
    warped_to_22700 = open_dataset(intermediate_files.intermediate_warp_path)
    assert warped_to_22700.rio.crs == BRITISH_NATIONAL_GRID_EPSG
    assert len(warped_to_22700.time) == len(expanded_calendar.time)

    # Commenting these out in prep for addressing
    # https://github.com/alan-turing-institute/clim-recal/issues/151
    # warped_to_22700_y_axis_inverted: Dataset = warped_to_22700.reindex(
    #     y=list(reversed(warped_to_22700.y))
    # )
    #
    # warped_to_22700_y_axis_inverted = warped_to_22700_y_axis_inverted.rename(
    #     {"x": final_x_coord_label, "y": final_y_coord_label}
    # )

    warped_to_22700_y_axis_inverted.to_netcdf(intermediate_files.final_nc_path)
    final_results = open_dataset(intermediate_files.final_nc_path, decode_coords="all")
    assert (final_results.time == expanded_calendar.time).all()
    return final_results


def interpolate_coords(
    xr_time_series: Dataset,
    variable_name: str,
    x_grid: NDArray,
    y_grid: NDArray,
    xr_time_series_x_column_name: str = FINAL_RESAMPLE_LON_COL,
    xr_time_series_y_column_name: str = FINAL_RESAMPLE_LAT_COL,
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
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG,
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
    # assert isinstance(crop_geom, GeoDataFrame)
    # crop_geom.set_crs(crs=final_crs, inplace=True)
    # if initial_clip_box:
    # return xr_time_series.rio.clip_box(
    #     minx=crop_geom.bounds.minx,
    #     miny=crop_geom.bounds.miny,
    #     maxx=crop_geom.bounds.maxx,
    #     maxy=crop_geom.bounds.maxy,
    # )
    # return xr_time_series.rio.clip(
    #     crop_geom.geometry.values, drop=True, invert=invert, **kwargs
    # )
    assert False
    gdal_warp_wrapper(
        input_path=xr_time_series,
        output_path=output_path,
    )


def ensure_xr_dataset(
    xr_time_series: Dataset | DataArray, default_name="to_convert"
) -> Dataset:
    """Return `xr_time_series` as a `xarray.Dataset` instance.

    Parameters
    ----------
    xr_time_series
        Instance to check and if necessary to convert to `Dataset`.
    default_name
        Name to give returned `Dataset` if `xr_time_series.name` is empty.

    Returns
    -------
    :
        Converted (or original) `Dataset`.

    Examples
    --------
    >>> ensure_xr_dataset(xarray_spatial_4_days)
    <xarray.Dataset>...
    Dimensions:      (time: 5, space: 3)
    Coordinates:
      * time         (time) datetime64[ns] ...1980-11-30 1980-12-01 ... 1980-12-04
      * space        (space) <U10 ...'Glasgow' 'Manchester' 'London'
    Data variables:
        xa_template  (time, space) float64 ...0.5488 0.7152 ... 0.9256 0.07104
    """
    if isinstance(xr_time_series, DataArray):
        array_name = xr_time_series.name or default_name
        return xr_time_series.to_dataset(name=array_name)
    else:
        return xr_time_series


def convert_xr_calendar(
    xr_time_series: DataArray | Dataset | PathLike,
    align_on: ConvertCalendarAlignOptions = DEFAULT_CALENDAR_ALIGN,
    calendar: CFCalendar = CFCalendarSTANDARD,
    use_cftime: bool = False,
    missing_value: Any | None = np.nan,
    interpolate_na: bool = False,
    ensure_output_type_is_dataset: bool = False,
    interpolate_method: InterpOptions = DEFAULT_INTERPOLATION_METHOD,
    keep_crs: bool = True,
    keep_attrs: bool = True,
    limit: int = 1,
    engine: XArrayEngineType = NETCDF4_XARRAY_ENGINE,
    extrapolate_fill_value: bool = True,
    check_cftime_cols: tuple[str] | None = None,
    cftime_range_gen_kwargs: dict[str, Any] | None = None,
    **kwargs,
) -> Dataset | DataArray:
    """Convert cpm 360 day time series to a standard 365/366 day time series.

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
    keep_crs
        Reapply initial Coordinate Reference System (CRS) after time projection.
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
    check_cftime_cols
        Columns to check `cftime` format on
    cftime_range_gen_kwargs
        Any `kwargs` to pass to `cftime_range_gen`
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
        if keep_crs and xr_time_series.rio.crs:
            assert xr_time_series.rio.crs
            return calendar_converted_ts.rio.write_crs(xr_time_series.rio.crs)
        else:
            return calendar_converted_ts
    else:
        return interpolate_xr_ts_nans(
            xr_ts=calendar_converted_ts,
            original_xr_ts=xr_time_series,
            check_cftime_cols=check_cftime_cols,
            interpolate_method=interpolate_method,
            keep_crs=keep_crs,
            keep_attrs=keep_attrs,
            limit=limit,
            cftime_range_gen_kwargs=cftime_range_gen_kwargs,
        )


def interpolate_xr_ts_nans(
    xr_ts: Dataset,
    original_xr_ts: Dataset | None = None,
    check_cftime_cols: tuple[str] | None = None,
    interpolate_method: InterpOptions = DEFAULT_INTERPOLATION_METHOD,
    keep_crs: bool = True,
    keep_attrs: bool = True,
    limit: int = 1,
    cftime_range_gen_kwargs: dict[str, Any] | None = None,
    **kwargs,
) -> Dataset:
    """Interpolate `nan` values in a `Dataset` time series.

    Notes
    -----
    For details and details of `keep_attrs`, `limit` and `**kwargs` parameters see:
    https://docs.xarray.dev/en/stable/generated/xarray.DataArray.interpolate_na.html

    Parameters
    ----------
    xr_ts
        `Dataset` to interpolate via `interpolate_na`. Requires a `time` coordinate.
    original_xr_ts
        A `Dataset` to compare the conversion process with. If
        not provided, set to the original `xr_ts` as a reference.
    check_cftime_cols
        `tuple` of column names in a `cftime` format to check.
    interpolate_method
        Which of the `xarray` interpolation methods to use.
    keep_crs
        Whether to ensure the original `crs` is kept via `rio.write_crs`.
    keep_attrs
        Passed to `keep_attrs` in `interpolate_na`. See Notes.
    limit
        How many `nan` are allowed either side of data point to interpolate. See Notes.
    cftime_range_gen_kwargs
        Any `cftime_range_gen` arguments to use with `check_cftime_cols` calls.

    Returns
    -------
    `Dataset` where `xr_ts` `nan` values are iterpolated with respect to the `time` coordinate.
    """
    if check_cftime_cols is None:
        check_cftime_cols = tuple()
    if cftime_range_gen_kwargs is None:
        cftime_range_gen_kwargs = dict()
    original_xr_ts = original_xr_ts if original_xr_ts else xr_ts

    # Ensure `fill_value` is set to `extrapolate`
    # Without this the `nan` values don't get filled
    kwargs["fill_value"] = "extrapolate"

    interpolated_ts: Dataset = xr_ts.interpolate_na(
        dim="time",
        method=interpolate_method,
        keep_attrs=keep_attrs,
        limit=limit,
        **kwargs,
    )
    for cftime_col in check_cftime_cols:
        if cftime_col in interpolated_ts:
            cftime_fix: NDArray = cftime_range_gen(
                interpolated_ts[cftime_col], **cftime_range_gen_kwargs
            )
            interpolated_ts[cftime_col] = (
                interpolated_ts[cftime_col].dims,
                cftime_fix,
            )
    if keep_crs and original_xr_ts.rio.crs:
        return interpolated_ts.rio.write_crs(xr_ts.rio.crs)
    else:
        return interpolated_ts


def gdal_warp_wrapper(
    input_path: PathLike,
    output_path: PathLike,
    output_crs: str = BRITISH_NATIONAL_GRID_EPSG,
    output_x_resolution: int | None = None,
    output_y_resolution: int | None = None,
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
    to_raster: bool = False,
    export_path_as_output_path_kwarg: bool = False,
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
    if export_path_as_output_path_kwarg:
        kwargs["output_path"] = export_path
    results: Dataset | Path | GDALDataset = func(source_path, **kwargs)
    if to_netcdf or to_raster:
        if isinstance(results, Path):
            results = open_dataset(results)
        if isinstance(results, GDALDataset):
            raise TypeError(
                f"Restuls from 'gdal_warp_wrapper' can't directly export to NetCDF form, only return a Path or GDALDataset"
            )
        assert isinstance(results, Dataset)
        if export_path.exists():
            if export_path.is_dir():
                raise FileExistsError(
                    f"Dataset export path is a folder: '{export_path}'"
                )
            else:
                raise FileExistsError(f"Cannot overwrite: '{export_path}'")
        if to_netcdf:
            results.to_netcdf(export_path)
        if to_raster:
            results.rio.to_raster(export_path)
    if return_results:
        return results
    else:
        return export_path


# Note: `rioxarray` is imported to ensure GIS methods are included. See:
# https://corteva.github.io/rioxarray/stable/getting_started/getting_started.html#rio-accessor
def xarray_example(
    start_date: DateType = XARRAY_EXAMPLE_START_DATE_STR,
    end_date: DateType = XARRAY_EXAMPLE_END_DATE_4_YEARS,
    coordinates: dict[str, tuple[float, float]] = THREE_CITY_CENTRE_COORDS,
    skip_dates: Iterable[date] | None = None,
    random_seed_int: int | None = XARRAY_EXAMPLE_RANDOM_SEED,
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
    :
        A `DataArray` of `start_date` to `end_date` date
        range a random variable for coordinates regions
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
    date_range: list[DateType] = list(
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
    random_data: array = random.rand(len(date_range), len(coordinates))
    spaces: list[str] = list(coordinates.keys())
    # If useful, add lat/lon (currently not working)
    # lat: list[float] = [coord[0] for coord in coordinates.values()]
    # lon: list[float] = [coord[1] for coord in coordinates.values()]
    da: DataArray = DataArray(
        random_data,
        name=name,
        coords={
            "time": to_datetime(date_range),
            "space": spaces,
            # "lon": lon,# *len(date_range),
            # "lat": lat,
        },
    )
    if as_dataset:
        return da.to_dataset()
    else:
        return da


def file_name_to_start_end_dates(
    path: PathLike, date_format: str = CLI_DATE_FORMAT_STR
) -> tuple[datetime, datetime]:
    """Return dates of file name with `date_format`-`date_format` structure.

    Parameters
    ----------
    path
        Path to file
    date_format
        Format of date for `strptime`

    Examples
    --------
    The examples below are meant to demonstrate usage, and
    the significance of when the last date is included or
    not by default.

    >>> from .core import date_range_generator
    >>> tif_365_path: Path = (Path('some') /
    ...     'folder' /
    ...     'pr_rcp85_land-cpm_uk_2.2km_06_day_20761201-20771130.tif')
    >>> start_date, end_date = file_name_to_start_end_dates(tif_365_path)
    >>> start_date
    datetime.datetime(2076, 12, 1, 0, 0)
    >>> end_date
    datetime.datetime(2077, 11, 30, 0, 0)
    >>> dates: tuple[date, ...] =  tuple(
    ...     date_range_generator(start_date=start_date,
    ...                          end_date=end_date,
    ...                          inclusive=True))
    >>> dates[:3]
    (datetime.datetime(2076, 12, 1, 0, 0),
     datetime.datetime(2076, 12, 2, 0, 0),
     datetime.datetime(2076, 12, 3, 0, 0))
    >>> len(dates)
    365
    >>> tif_366_path: Path = (Path('some') /
    ...     'folder' /
    ...     'pr_rcp85_land-cpm_uk_2.2km_06_day_20791201-20801130.tif')
    >>> dates = date_range(*file_name_to_start_end_dates(tif_366_path))
    >>> len(dates)
    366
    """
    date_range_path: Path = Path(Path(path).name.split("_")[-1])
    date_strs: list[str] = date_range_path.stem.split("-")
    try:
        assert len(date_strs) == 2
    except AssertionError:
        raise ValueError(
            f"Maximum of 2 date strs in YYYMMDD form allowed from: '{date_range_path}'"
        )
    start_date: date = datetime.strptime(date_strs[0], date_format)
    end_date: date = datetime.strptime(date_strs[1], date_format)
    return start_date, end_date


def generate_360_to_standard(array_to_expand: DataArray) -> DataArray:
    """Return `array_to_expand` 360 days expanded to 365 or 366 days.

    This may be dropped if `cpm_reproject_with_standard_calendar` is successful.

    Examples
    --------
    """
    initial_days: int = len(array_to_expand)
    assert initial_days == 360
    extra_days: int = 5
    index_block_length: int = int(initial_days / extra_days)  # 72
    expanded_index: list[int] = []
    for i in range(extra_days):
        start_index: int = i * index_block_length
        stop_index: int = (i + 1) * index_block_length
        slice_to_append: list[int] = array_to_expand[start_index:stop_index]
        expanded_index.append(slice_to_append)
        if i < extra_days - 1:
            expanded_index.append(np.nan)
    return DataArray(expanded_index)


def correct_int_time_datafile(
    xr_dataset_path: Path,
    new_index_name: str = "time",
    replace_index: str | None = "band",
    data_attribute_name: str = "band_data",
) -> Dataset:
    """Load a `Dataset` from path and generate `time` index.

    Notes
    -----
    This is not finished and may be removed in future.

    Examples
    --------
    >>> pytest.skip(reason="Not finished implementing")
    >>> rainfall_dataset = correct_int_time_datafile(
    ...     glasgow_example_cropped_cpm_rainfall_path)
    >>> assert False
    """
    xr_dataset: Dataset = open_dataset(xr_dataset_path)
    metric_name: str = str(xr_dataset_path).split("_")[0]
    start_date, end_date = file_name_to_start_end_dates(xr_dataset_path)
    dates_index: DatetimeIndex = date_range(start_date, end_date)
    intermediate_new_index: str = new_index_name + "_standard"
    # xr_intermediate_date = xr_dataset.assign_coords({intermediate_new_index: dates_index})
    xr_dataset[intermediate_new_index]: Dataset = dates_index
    xr_360_datetime = xr_dataset[intermediate_new_index].convert_calendar(
        "360_day", align_on="year", dim=intermediate_new_index
    )
    if len(xr_360_datetime[intermediate_new_index]) == 361:
        # If the range overlaps a leap and non leap year,
        # it is possible to have 361 days
        # See https://docs.xarray.dev/en/stable/generated/xarray.Dataset.convert_calendar.html
        # Assuming first date is a December 1
        xr_360_datetime = xr_360_datetime[intermediate_new_index][1:]
    assert len(xr_360_datetime[intermediate_new_index]) == 360
    # xr_with_datetime['time'] = xr_360_datetime
    assert False
    xr_bands_time_indexed: DataArray = xr_intermediate_date[
        data_attribute_name
    ].expand_dims(dim={new_index_name: xr_intermediate_date[new_index_name]})
    # xr_365_data_array: DataArray = convert_xr_calendar(xr_bands_time_indexed)
    xr_365_dataset: Dataset = Dataset({metric_name: xr_bands_time_indexed})
    partial_fix_365_dataset: Dataset = convert_xr_calendar(xr_365_dataset.time)
    assert False


def cftime_range_gen(time_data_array: DataArray, **kwargs) -> NDArray:
    """Convert a banded time index a banded standard (Gregorian)."""
    assert hasattr(time_data_array, "time")
    time_bnds_fix_range_start: CFTimeIndex = cftime_range(
        time_data_array.time.dt.strftime(ISO_DATE_FORMAT_STR).values[0],
        time_data_array.time.dt.strftime(ISO_DATE_FORMAT_STR).values[-1],
        **kwargs,
    )
    time_bnds_fix_range_end: CFTimeIndex = time_bnds_fix_range_start + timedelta(days=1)
    return np.array((time_bnds_fix_range_start, time_bnds_fix_range_end)).T
