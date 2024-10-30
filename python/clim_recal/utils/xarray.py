import warnings
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from itertools import islice
from logging import getLogger
from os import PathLike
from pathlib import Path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any, Callable, Final, Iterable, Iterator, Literal, Sequence, overload

import numpy as np
import rioxarray  # nopycln: import
import seaborn
from cftime._cftime import Datetime360Day
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from numpy.typing import NDArray
from osgeo.gdal import Dataset as GDALDataset
from osgeo.gdal import (
    GDALTranslateOptions,
    GDALWarpAppOptions,
    Translate,
    TranslateOptions,
    Warp,
    WarpOptions,
)
from osgeo.gdal import config_option as config_GDAL_option
from rasterio.enums import Resampling
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from tqdm import tqdm
from xarray import CFTimeIndex, DataArray, Dataset, cftime_range, open_dataset
from xarray.coding.calendar_ops import convert_calendar
from xarray.core.types import (
    CFCalendar,
    InterpOptions,
    T_DataArray,
    T_DataArrayOrSet,
    T_Dataset,
)

from .core import (
    CLI_DATE_FORMAT_STR,
    ISO_DATE_FORMAT_STR,
    climate_data_mount_path,
    console,
    multiprocess_execute,
    range_len,
    results_path,
)
from .data import (
    BRITISH_NATIONAL_GRID_EPSG,
    CPM_NAME,
    CPM_RAW_X_COLUMN_NAME,
    CPM_RAW_Y_COLUMN_NAME,
    DEFAULT_CALENDAR_ALIGN,
    DEFAULT_INTERPOLATION_METHOD,
    DEFAULT_RESAMPLING_METHOD,
    GLASGOW_GEOM_LOCAL_PATH,
    HADS_NAME,
    HADS_RAW_X_COLUMN_NAME,
    HADS_RAW_Y_COLUMN_NAME,
    NETCDF4_XARRAY_ENGINE,
    TIME_COLUMN_NAME,
    BoundingBoxCoords,
    CFCalendarSTANDARD,
    ConvertCalendarAlignOptions,
    RegionOptions,
    RunOptions,
    VariableOptions,
    XArrayEngineType,
)
from .gdal_formats import (
    NETCDF_EXTENSION_STR,
    TIF_EXTENSION_STR,
    GDALFormatsType,
    GDALGeoTiffFormatStr,
    GDALNetCDFFormatStr,
)

logger = getLogger(__name__)

seaborn.set()  # Use seaborn style for all `matplotlib` plots

ReprojectFuncType = Callable[[T_Dataset], T_Dataset]

GLASGOW_GEOM_ABSOLUTE_PATH: Final[Path] = (
    climate_data_mount_path() / GLASGOW_GEOM_LOCAL_PATH
)
CPM_REGEX: Final[str] = "**/[!.]*cpm*.nc"
HADS_MIN_NULL: float = -1000000

FINAL_CONVERTED_HADS_WIDTH: Final[int] = 493
FINAL_CONVERTED_HADS_HEIGHT: Final[int] = 607
FINAL_CONVERTED_CPM_WIDTH: Final[int] = 514
FINAL_CONVERTED_CPM_HEIGHT: Final[int] = 625

HADS_DROP_VARS_AFTER_PROJECTION: Final[tuple[str, ...]] = ("longitude", "latitude")

FINAL_RESAMPLE_LON_COL: Final[str] = "x"
FINAL_RESAMPLE_LAT_COL: Final[str] = "y"

DEFAULT_WARP_DICT_OPTIONS: dict[str, str | float] = {
    "VARIABLES_AS_BANDS": "YES",
    "GDAL_NETCDF_VERIFY_DIMS": "STRICT",
}

TQDM_FILE_NAME_PRINT_CHARS_INDEX: Final[int] = -7


def cpm_xarray_to_standard_calendar(
    cpm_xr_time_series: T_Dataset | PathLike, include_bnds_index: bool = False
) -> T_Dataset:
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
    cpm_xr_time_series, _ = check_xarray_path_and_var_name(cpm_xr_time_series, None)
    cpm_to_std_calendar: T_Dataset = convert_xr_calendar(
        cpm_xr_time_series, interpolate_na=True, check_cftime_cols=("time_bnds",)
    )
    cpm_to_std_calendar["month_number"] = (
        cpm_to_std_calendar.month_number.interpolate_na(
            "time", fill_value="extrapolate"
        )
    )
    cpm_to_std_calendar["year"] = cpm_to_std_calendar.year.interpolate_na(
        "time", fill_value="extrapolate"
    )
    yyyymmdd_fix: T_DataArray = cpm_to_std_calendar.time.dt.strftime(
        CLI_DATE_FORMAT_STR
    )
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


def check_xarray_path_and_var_name(
    xr_time_series: T_Dataset | PathLike,
    variable_name: str | None,
    ignore_warnings: bool = True,
) -> tuple[Dataset, str]:
    """Check and return a `T_Dataset` instances and included variable name."""
    if isinstance(xr_time_series, PathLike):
        with warnings.catch_warnings():
            # Filter repeating warning such as:
            # UserWarning: Variable(s) referenced in bounds not in variables: ['time_bnds']
            if ignore_warnings:
                warnings.simplefilter(action="ignore", category=UserWarning)
            xr_time_series = open_dataset(xr_time_series, decode_coords="all")
    try:
        assert isinstance(xr_time_series, Dataset)
    except AssertionError:
        raise TypeError(
            f"'xr_time_series' should be a 'Dataset' or 'NetCDF', recieved: '{type(xr_time_series)}'"
        )

    if not variable_name:
        data_vars_count: int = len(xr_time_series.data_vars)
        try:
            assert data_vars_count == 1
        except:
            ValueError(
                f"'variable_name' must be specified or 'data_vars' count must be 1, not {data_vars_count}."
            )
        variable_name = tuple(xr_time_series.data_vars)[0]
    if not isinstance(variable_name, str):
        raise ValueError(
            "'variable_name' must be a 'str' or inferred from 'xr_time_series.data_vars'. Got: '{variable_name}'"
        )
    return xr_time_series, variable_name


def cpm_check_converted(cpm_xr_time_series: T_Dataset | PathLike) -> bool:
    """Check if `cpm_xr_time_series` is likely already reprojected.

    Parameters
    ----------
    cpm_xr_time_series
        `Dataset` instance or `Path` to check.

    Returns
    -------
    `True` if all of the methics are `True`, else `False`
    """
    cpm_xr_time_series, _ = check_xarray_path_and_var_name(
        xr_time_series=cpm_xr_time_series, variable_name=None
    )
    checks_dict: dict[str, bool] = {}
    if "time" in cpm_xr_time_series.sizes:
        checks_dict["time-365-or-366"] = cpm_xr_time_series.sizes["time"] in (365, 366)
    if "x" in cpm_xr_time_series.sizes:
        checks_dict["x-final-coords"] = (
            cpm_xr_time_series.sizes["x"] == FINAL_CONVERTED_CPM_WIDTH
        )
    if "y" in cpm_xr_time_series.sizes:
        checks_dict["y-final-coords"] = (
            cpm_xr_time_series.sizes["y"] == FINAL_CONVERTED_CPM_HEIGHT
        )
    if all(checks_dict.values()):
        return True
    else:
        return False


def cpm_reproject_with_standard_calendar(
    cpm_xr_time_series: T_Dataset | PathLike,
    variable_name: str | None = None,
    close_temp_paths: bool = True,
    force: bool = False,
) -> T_Dataset:
    """Convert raw `cpm_xr_time_series` to an 365/366 days and 27700 coords.

    Notes
    -----
    Currently makes UTM coordinate structure

    Parameters
    ----------
    cpm_xr_time_series
        `Dataset` (or path to load as `Dataset`) expected to be in raw UKCPM
        format, with 360 day years and a rotated coordinate system.
    variable_name
        Name of variable used, usually a measure of climate change like
        `tasmax` and `tasmin`.

    Returns
    -------
    Final `xarray` `Dataset` after spatial and temporal changes.

    Examples
    --------
    >>> tasmax_cpm_1980_raw = getfixture('tasmax_cpm_1980_raw')
    >>> if not tasmax_cpm_1980_raw:
    ...     pytest.skip(mount_or_cache_doctest_skip_message)
    >>> tasmax_cpm_1980_365_day: T_Dataset = cpm_reproject_with_standard_calendar(
    ...     cpm_xr_time_series=tasmax_cpm_1980_raw,
    ...     variable_name="tasmax")
    Warp:      ...nc ...100%...
    Translate: ...tif ...100%...
    >>> tasmax_cpm_1980_365_day
    <xarray.Dataset>
    Dimensions:              (time: 365, x: 493, y: 607)
    Coordinates:
      * time                 (time) datetime64[ns]...
      * x                    (x) float64...
      * y                    (y) float64...
        transverse_mercator  |S1...
        spatial_ref          int64...
    Data variables:
        tasmax               (time, y, x) float32...
    Attributes: (12/18)
        ...
    >>> tasmax_cpm_1980_365_day.dims
    Frozen...({'time': 365, 'x': 493, 'y': 607})
    """
    if not force:
        logger.info("Checking if already converted...")
        if cpm_check_converted(cpm_xr_time_series):
            logger.info("Similar to already converted. Returning unmodified")
            xr_dataset, _ = check_xarray_path_and_var_name(
                cpm_xr_time_series, variable_name=variable_name
            )
            return xr_dataset
    else:
        logger.info("Force skip checking if already converted...")
    temp_cpm: _TemporaryFileWrapper = NamedTemporaryFile(
        suffix="." + NETCDF_EXTENSION_STR
    )
    temp_tif: _TemporaryFileWrapper = NamedTemporaryFile(suffix="." + TIF_EXTENSION_STR)
    temp_translated_ncf: _TemporaryFileWrapper = NamedTemporaryFile(
        suffix="." + NETCDF_EXTENSION_STR
    )
    if isinstance(cpm_xr_time_series, Dataset):
        xr_time_series_instance: T_Dataset = cpm_xr_time_series
        cpm_xr_time_series = temp_cpm.name
        xr_time_series_instance.to_netcdf(cpm_xr_time_series)
    assert isinstance(cpm_xr_time_series, PathLike | str)
    gdal_warp_wrapper(
        cpm_xr_time_series,
        output_path=Path(temp_tif.name),
        format=GDALGeoTiffFormatStr,
        use_tqdm_progress_bar=False,
        # Leaving this if further projection is needed
        # resampling_method=VariableOptions.resampling_method(variable=variable_name).name,
    )
    gdal_translate_wrapper(
        input_path=Path(temp_tif.name),
        output_path=Path(temp_translated_ncf.name),
        use_tqdm_progress_bar=False,
        # Leaving this if further projection is needed
        # resampling_method=VariableOptions.resampling_method(variable=variable_name).name,
    )
    reprojected_cpm_xr_time_series, _ = check_xarray_path_and_var_name(
        Path(temp_translated_ncf.name), variable_name
    )
    reprojected_dropped_ensemble_member = reprojected_cpm_xr_time_series.squeeze(
        "ensemble_member"
    ).drop("ensemble_member")

    standard_calendar_ts: T_Dataset = convert_xr_calendar(
        reprojected_dropped_ensemble_member, interpolate_na=True
    )
    if close_temp_paths:
        temp_cpm.close()
        temp_tif.close()
        temp_translated_ncf.close()
    return standard_calendar_ts


def xr_reproject_crs(
    xr_time_series: T_Dataset | PathLike,
    x_dim_name: str = CPM_RAW_X_COLUMN_NAME,
    y_dim_name: str = CPM_RAW_Y_COLUMN_NAME,
    time_dim_name: str = TIME_COLUMN_NAME,
    variable_name: str | None = None,
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG,
    match_xr_time_series: T_Dataset | PathLike | None = None,
    match_xr_time_series_load_func: Callable | None = None,
    match_xr_time_series_load_kwargs: dict[str, Any] | None = None,
    resampling_method: Resampling = DEFAULT_RESAMPLING_METHOD,
    nodata: float = np.nan,
    **kwargs,
) -> T_Dataset:
    """Reproject `source_xr` to `target_xr` coordinate structure.

    Parameters
    ----------
    xr_time_series
        `Dataset` or `PathLike` to load and reproject.
    x_dim_name
        `str` name of `x` spatial dimension in `xr_time_series`. Default matches CPM UK projections.
    y_dim_name
        `str` name of `y` spatial dimension in `xr_time_series`. Default matches CPM UK projections.
    time_dim_name
        `str` name of `time` dimension in `xr_time_series`.
    variable_name
        Name of datset to apply projection to within `xr_time_series`.
        Inferred if `None` assuming only one `data_var` attribute.
    final_crs
        Coordinate system `str` to project `xr_time_series` to.
    resampling_method
        `rasterio` resampling method to apply.

    Examples
    --------
    >>> tasmax_hads_1980_raw = getfixture('tasmax_hads_1980_raw')
    >>> if not tasmax_hads_1980_raw:
    ...     pytest.skip(mount_or_cache_doctest_skip_message)
    >>> tasmax_hads_1980_raw.dims
    FrozenMappingWarningOnValuesAccess({'time': 31,
                                        'projection_y_coordinate': 1450,
                                        'projection_x_coordinate': 900,
                                        'bnds': 2})
    >>> tasmax_hads_2_2km: T_Dataset = xr_reproject_crs(
    ...     tasmax_hads_1980_raw,
    ...     variable_name="tasmax",
    ...     x_dim_name=HADS_RAW_X_COLUMN_NAME,
    ...     y_dim_name=HADS_RAW_Y_COLUMN_NAME,
    ...     resolution=(CPM_RESOLUTION_METERS,
    ...                 CPM_RESOLUTION_METERS),)
    >>> tasmax_hads_2_2km.dims
    FrozenMappingWarningOnValuesAccess({'x': 410,
                                        'y': 660,
                                        'time': 31})
    """
    xr_time_series, variable_name = check_xarray_path_and_var_name(
        xr_time_series, variable_name
    )
    xr_time_series = xr_time_series.rio.set_spatial_dims(
        x_dim=x_dim_name, y_dim=y_dim_name, inplace=True
    )
    # info requires a df parameter, not straightforward for logging
    # logger.info(xr_time_series.info())
    data_array: T_DataArray = xr_time_series[variable_name]
    index_names: tuple[str, str, str] = (time_dim_name, x_dim_name, y_dim_name)
    extra_dims: set[str] = set(data_array.indexes.dims) - set(index_names)
    if extra_dims:
        raise ValueError(
            f"Can only reindex using dims: {index_names}, extra dim(s): {extra_dims}"
        )
    coords: dict[str, DataArray] = {
        time_dim_name: xr_time_series[time_dim_name],
        y_dim_name: xr_time_series[y_dim_name],
        x_dim_name: xr_time_series[x_dim_name],
    }
    without_attributes: T_DataArray = DataArray(
        data=data_array.to_numpy(), coords=coords, name=variable_name
    )
    without_attributes = without_attributes.rio.write_crs(xr_time_series.rio.crs)
    without_attributes_reprojected: T_DataArray
    if match_xr_time_series:
        if match_xr_time_series_load_func:
            match_xr_time_series_load_kwargs = match_xr_time_series_load_kwargs or {}
            match_xr_time_series = match_xr_time_series_load_func(
                match_xr_time_series, **match_xr_time_series_load_kwargs
            )
        if not {x_dim_name, y_dim_name} < match_xr_time_series.sizes.keys():
            # If dim name
            # likely x, y indexes from a projection like cpm, need to match
            logger.debug(
                f"'x_dim_name': {x_dim_name} and "
                f"'y_dim_name': {y_dim_name} not in "
                f"'match_xr_time_series' dims: "
                f"{match_xr_time_series.sizes.keys()}."
            )
            if {"x", "y"} < match_xr_time_series.sizes.keys():
                logger.debug(
                    f"Renaming dims: '{x_dim_name}' -> 'x', '{y_dim_name}' -> 'y'"
                )
                without_attributes = without_attributes.rename(
                    {x_dim_name: "x", y_dim_name: "y"}
                )
            else:
                raise ValueError("Can't match dim names.")
        without_attributes_reprojected = without_attributes.rio.reproject_match(
            match_xr_time_series, resampling=resampling_method, nodata=nodata, **kwargs
        )
    else:
        without_attributes_reprojected: T_DataArray = without_attributes.rio.reproject(
            final_crs, resampling=resampling_method, nodata=nodata, **kwargs
        )
    final_dataset: T_Dataset = Dataset({variable_name: without_attributes_reprojected})
    return final_dataset.rio.write_crs(BRITISH_NATIONAL_GRID_EPSG)


def _ensure_resample_method_name(
    method: str | Resampling, allow_none: bool = True
) -> str | None:
    """Ensure the correct method name `str` is returned."""

    def error_message(method: str) -> str:
        return f"Method '{method}' not a valid GDAL 'Resampling' method."

    if isinstance(method, str):
        try:
            assert method in Resampling.__members__
        except KeyError:
            raise KeyError(error_message(method))
        return method
    elif isinstance(method, Resampling):
        return method.name
    elif method is None:
        return None
    else:
        raise ValueError(error_message(method))


def hads_resample_and_reproject(
    hads_xr_time_series: T_Dataset | PathLike,
    variable_name: str,
    cpm_to_match: T_Dataset | PathLike,
    cpm_to_match_func: Callable | None = cpm_reproject_with_standard_calendar,
    x_dim_name: str = HADS_RAW_X_COLUMN_NAME,
    y_dim_name: str = HADS_RAW_Y_COLUMN_NAME,
) -> T_Dataset:
    """Resample `HADs` `xarray` time series to 2.2km."""
    if isinstance(cpm_to_match, Dataset) and {"x", "y"} < cpm_to_match.sizes.keys():
        cpm_to_match_func = None
    epsg_277000_2_2km: T_Dataset = xr_reproject_crs(
        hads_xr_time_series,
        variable_name=variable_name,
        x_dim_name=x_dim_name,
        y_dim_name=y_dim_name,
        match_xr_time_series=cpm_to_match,
        match_xr_time_series_load_func=cpm_to_match_func,
        resampling_method=VariableOptions.resampling_method(variable_name),
        # Check if the following line is needed
        # match_xr_time_series_load_kwargs=dict(variable_name=variable_name),
    )

    final_epsg_277000_2_2km: T_Dataset

    # Check if the minimum values should be NULL
    min_value: float = epsg_277000_2_2km[variable_name].min()

    if min_value < HADS_MIN_NULL:
        logger.info(f"Setting '{variable_name}' values less than {min_value} as `nan`")
        final_epsg_277000_2_2km = epsg_277000_2_2km.where(
            epsg_277000_2_2km[variable_name] > min_value
        )
    else:
        logger.debug(
            f"Keeping '{variable_name}' values less than {min_value}. 'HADS_MIN_NULL': {HADS_MIN_NULL}"
        )
        final_epsg_277000_2_2km = epsg_277000_2_2km
    return final_epsg_277000_2_2km.rio.write_crs(BRITISH_NATIONAL_GRID_EPSG)


def plot_xarray(
    da: T_DataArrayOrSet,
    path: PathLike | None = None,
    time_stamp: bool = False,
    return_path: bool = True,
    **kwargs,
) -> Path | Figure | None:
    """Plot `da` with `**kwargs` to `path`.

    Parameters
    ----------
    da
        `xarray` objec to plot.
    path
        File to write plot to.
    time_stamp
        Whather to add a `datetime` `str` of time of writing in file name.
    kwargs
        Additional parameters to pass to `plot`.

    Examples
    --------
    >>> example_path: Path = (
    ...     getfixture('tmp_path') / 'test-path/example.png')
    >>> image_path: Path = plot_xarray(
    ...     xarray_spatial_4_days, example_path)
    >>> example_path == image_path
    True
    >>> example_time_stamped: Path = (
    ...      example_path.parent / 'example-stamped.png')
    >>> timed_image_path: Path = plot_xarray(
    ...     xarray_spatial_4_days, example_time_stamped,
    ...     time_stamp=True)
    >>> example_time_stamped != timed_image_path
    True
    >>> print(timed_image_path)
    /.../test-path/example-stamped_...-...-..._...png
    """
    fig: Figure = da.plot(**kwargs)
    if path:
        path = Path(path)
        path.parent.mkdir(exist_ok=True, parents=True)
        if time_stamp:
            path = results_path(
                name=path.stem,
                path=path.parent,
                mkdir=True,
                extension=path.suffix,
                dot_pre_extension=False,
            )
        plt.savefig(path)
        plt.close()
    if return_path:
        return path
    else:
        return fig


def join_xr_time_series_var(
    path: PathLike,
    variable_name: str | None = None,
    method_name: str = "median",
    time_dim_name: str = "time",
    regex: str = CPM_REGEX,
    start: int = 0,
    stop: int | None = None,
    step: int = 1,
) -> T_Dataset:
    """Join a set of xr_time_series files chronologically.

    Parameters
    ----------
    path
        Path to collect files to process from, filtered via `regex`.
    variable_name
        A variable name to specify for data expected. If none that
        will be extracted and checked from the files directly.
    method_name
        What method to use to summarise each time point results.
    time_dim_name
        Name of time dimension in passed files.
    regex
        A str to filter files within `path`
    start
        What point to start indexing `path` results from.
    stop
        What point to stop indexing `path` restuls from.
    step
        How many paths to jump between when iterating between `stop` and `start`.

    Examples
    --------
    >>> tasmax_cpm_1980_raw = getfixture('tasmax_cpm_1980_raw')
    >>> if not tasmax_cpm_1980_raw:
    ...     pytest.skip(mount_or_cache_doctest_skip_message)
    >>> tasmax_cpm_1980_raw_path = getfixture('tasmax_cpm_1980_raw_path').parents[1]
    >>> results: T_Dataset = join_xr_time_series_var(
    ...     tasmax_cpm_1980_raw_path,
    ...     'tasmax', stop=3)
    >>> results
    <xarray.Dataset> ...
    Dimensions:  (time: 1080)
    Coordinates:
      * time     (time) object ... 1980-12-01 12:00:00 ... 1983-11-30 12:00:00
    Data variables:
        tasmax   (time) float64 ... 8.221 6.716 6.499 7.194 ... 8.456 8.153 5.501
    """
    results: list[tuple[Any, float]] = []
    for nc_path in islice(Path(path).glob(regex), start, stop, step):
        xr_time_series, nc_var_name = check_xarray_path_and_var_name(
            nc_path, variable_name=variable_name
        )
        if not variable_name:
            variable_name = nc_var_name
        try:
            assert variable_name == nc_var_name
        except AssertionError:
            raise ValueError(f"'{nc_var_name}' should match '{variable_name}'")
        results += [
            (date_obj, getattr(val, method_name)().values.item())
            for date_obj, val in xr_time_series[variable_name].groupby(time_dim_name)
        ]
    data_vars = {variable_name: ([time_dim_name], [data[1] for data in results])}
    coords = {time_dim_name: (time_dim_name, [data[0] for data in results])}
    return Dataset(data_vars=data_vars, coords=coords).sortby(time_dim_name)


def annual_group_xr_time_series(
    joined_xr_time_series: T_Dataset | PathLike,
    variable_name: str,
    groupby_method: str = "time.dayofyear",
    method_name: str = "median",
    time_dim_name: str = "time",
    regex: str = CPM_REGEX,
    start: int = 0,
    stop: int | None = None,
    step: int = 1,
    plot_path: PathLike = "annual-aggregated.png",
    time_stamp: bool = True,
    **kwargs,
) -> Path:
    """
    Return and plot a `Dataset` of time series temporally overlayed.

    Parameters
    ----------
    joined_xr_time_series
        Provide existing `Dataset` to aggregate and plot (otherwise check `path`).
    variable_name
        A variable name to specify for data expected. If none that
        will be extracted and checked from the files directly.
    groupby_method
        `xarray` method to aggregate time of `xr_time_series`.
    method_name
        `xarray` method to calculate for plot.
    time_dim_name
        Name of time dimension in passed files.
    regex
        A str to filter files within `path`
    start
        What point to start indexing `path` results from.
    stop
        What point to stop indexing `path` restuls from.
    step
        How many paths to jump between when iterating between `stop` and `start`.
    plot_path
        `Path` to save plot to.
    time_stamp
        Whether to include a time stamp in the `plot_path` name.
    **kwargs
        Additional parameters to pass to `plot_xarray`.

    Examples
    --------
    >>> tasmax_cpm_1980_raw = getfixture('tasmax_cpm_1980_raw')
    >>> if not tasmax_cpm_1980_raw:
    ...     pytest.skip(mount_or_cache_doctest_skip_message)
    >>> tasmax_cpm_1980_raw_path = getfixture('tasmax_cpm_1980_raw_path').parents[1]
    >>> results: T_Dataset = annual_group_xr_time_series(
    ...     tasmax_cpm_1980_raw_path, 'tasmax', stop=3)
    >>> results
    <xarray.Dataset> ...
    Dimensions:    (dayofyear: 360)
    Coordinates:
      * dayofyear  (dayofyear) int64 ... 1 2 3 4 5 6 7 ... 355 356 357 358 359 360
    Data variables:
        tasmax     (dayofyear) float64 ... 9.2 8.95 8.408 8.747 ... 6.387 8.15 9.132
    """
    if isinstance(joined_xr_time_series, PathLike):
        joined_xr_time_series = join_xr_time_series_var(
            path=joined_xr_time_series,
            variable_name=variable_name,
            method_name=method_name,
            time_dim_name=time_dim_name,
            regex=regex,
            start=start,
            stop=stop,
            step=step,
        )
    summarised_year_groups: T_Dataset = joined_xr_time_series.groupby(groupby_method)
    summarised_year: T_Dataset = getattr(summarised_year_groups, method_name)()
    try:
        assert 360 <= summarised_year.sizes["time"] <= 366
    except:
        ValueError(f"Dimensions are not annual in {summarised_year}.")
    if plot_path:
        plot_xarray(
            getattr(summarised_year, variable_name),
            path=plot_path,
            time_stamp=time_stamp,
            **kwargs,
        )
    return summarised_year


def crop_xarray(
    xr_time_series: T_Dataset | PathLike,
    crop_box: BoundingBoxCoords,
    **kwargs,
) -> T_Dataset:
    """Crop `xr_time_series` with `crop_path` `shapefile`.

    Parameters
    ----------
    xr_time_series
        `Dataset` or path to `netcdf` file to load and crop.
    crop_box
        Instance of `BoundingBoxCoords` with coords.
    kwargs
        Any additional parameters to pass to `clip`

    Returns
    -------
    :
        Spatially cropped `xr_time_series` `Dataset` with `final_crs` spatial coords.

    Examples
    --------
    >>> from clim_recal.utils.data import GlasgowCoordsEPSG27700
    >>> from numpy.testing import assert_allclose
    >>> tasmax_cpm_1980_raw = getfixture('tasmax_cpm_1980_raw')
    >>> if not tasmax_cpm_1980_raw:
    ...     pytest.skip(mount_or_cache_doctest_skip_message)
    >>> tasmax_cpm_1980_365_day: T_Dataset = cpm_reproject_with_standard_calendar(
    ...     cpm_xr_time_series=tasmax_cpm_1980_raw,
    ...     variable_name="tasmax")
    >>> cropped = crop_xarray(
    ...     tasmax_cpm_1980_365_day,
    ...     crop_box=GlasgowCoordsEPSG27700)
    >>> assert_allclose(cropped.rio.bounds(),
    ...                 GlasgowCoordsEPSG27700.as_rioxarray_tuple(),
    ...                 rtol=.01)
    >>> tasmax_cpm_1980_365_day.sizes
    Frozen({'x': 529, 'y': 653, 'time': 365})
    >>> cropped.sizes
    Frozen({'x': 10, 'y': 8, 'time': 365})
    """
    xr_time_series, _ = check_xarray_path_and_var_name(xr_time_series, None)
    try:
        assert str(xr_time_series.rio.crs) == crop_box.rioxarry_epsg
    except AssertionError:
        raise ValueError(
            f"'xr_time_series.rio.crs': '{xr_time_series.rio.epsg}' must equal 'crop_box.crs': '{crop_box.crs}'"
        )
    return xr_time_series.rio.clip_box(
        **crop_box.as_rioxarray_dict(), crs=crop_box.rioxarry_epsg, **kwargs
    )


def ensure_xr_dataset(
    xr_time_series: T_DataArrayOrSet, default_name="to_convert"
) -> T_Dataset:
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
    xr_time_series: T_DataArray | T_Dataset | PathLike,
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
    check_cftime_cols: tuple[str] | None = None,
    cftime_range_gen_kwargs: dict[str, Any] | None = None,
    # This may need to be removed
    # **kwargs,
) -> T_DataArrayOrSet:
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
    check_cftime_cols
        Columns to check `cftime` format on
    cftime_range_gen_kwargs
        Any `kwargs` to pass to `cftime_range_gen`

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
    >>> xr_360_to_365_datetime64: T_Dataset = convert_xr_calendar(
    ...     xarray_spatial_4_years_360_day, align_on="date")
    >>> xr_360_to_365_datetime64.sel(
    ...     time=slice("1981-01-30", "1981-02-01"),
    ...     space="Glasgow").day_360
    <xarray.DataArray 'day_360' (time: 3)>...
    Coordinates:
      * time     (time) datetime64[ns] ...1981-01-30 1981-01-31 1981-02-01
        space    <U10 ...'Glasgow'
    >>> xr_360_to_365_datetime64_interp: T_Dataset = convert_xr_calendar(
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
    calendar_converted_ts: T_DataArrayOrSet = convert_calendar(
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
    xr_ts: T_Dataset,
    original_xr_ts: T_Dataset | None = None,
    check_cftime_cols: tuple[str] | None = None,
    interpolate_method: InterpOptions = DEFAULT_INTERPOLATION_METHOD,
    keep_crs: bool = True,
    keep_attrs: bool = True,
    limit: int = 1,
    cftime_range_gen_kwargs: dict[str, Any] | None = None,
    **kwargs,
) -> T_Dataset:
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

    interpolated_ts: T_Dataset = xr_ts.interpolate_na(
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


def _progress_bar_file_description(
    input_path: PathLike,
    prefix: str = "",
    tqdm_file_name_chars: int = TQDM_FILE_NAME_PRINT_CHARS_INDEX,
    suffix: str = "",
) -> str:
    return f"{prefix}{Path(input_path).name[tqdm_file_name_chars:]}{suffix}"


def _gen_progress_bar(description: str = "") -> tuple[tqdm, Callable[..., None]]:
    progress_bar: tqdm = tqdm(total=100, desc=description)

    def _tqdm_progress_callback_func(progress: float, *args) -> None:
        progress_bar.update(progress * 100 - progress_bar.n)

    return progress_bar, _tqdm_progress_callback_func


def gdal_translate_wrapper(
    input_path: PathLike,
    output_path: PathLike,
    return_path: bool = True,
    translate_format: GDALFormatsType | str = GDALNetCDFFormatStr,
    use_tqdm_progress_bar: bool = True,
    tqdm_file_name_chars: int = TQDM_FILE_NAME_PRINT_CHARS_INDEX,
    resampling_method: Resampling | None = None,
    supress_warnings: bool = True,
    **kwargs,
) -> Path | GDALDataset:
    if use_tqdm_progress_bar:
        description: str = _progress_bar_file_description(
            input_path=input_path,
            prefix="Translate: ",
            tqdm_file_name_chars=tqdm_file_name_chars,
        )
        progress_bar, progress_callback = _gen_progress_bar(description=description)
        kwargs["callback"] = progress_callback
    translate_options: GDALTranslateOptions = TranslateOptions(
        format=translate_format,
        resampleAlg=_ensure_resample_method_name(resampling_method),
        **kwargs,
    )
    translation: GDALDataset
    if supress_warnings:
        with config_GDAL_option("CPL_LOG", "gdal_warnings.log"):
            translation = Translate(
                destName=output_path,
                srcDS=input_path,
                options=translate_options,
            )
    else:
        translation = Translate(
            destName=output_path,
            srcDS=input_path,
            options=translate_options,
        )
    if use_tqdm_progress_bar:
        translation.FlushCache()
        progress_bar.close()
    assert translation is not None
    return Path(output_path) if return_path else translation


def gdal_warp_wrapper(
    input_path: PathLike,
    output_path: PathLike,
    output_crs: str = BRITISH_NATIONAL_GRID_EPSG,
    output_x_resolution: int | None = None,
    output_y_resolution: int | None = None,
    copy_metadata: bool = True,
    return_path: bool = True,
    format: GDALFormatsType | str | None = GDALNetCDFFormatStr,
    multithread: bool = True,
    warp_dict_options: dict[str, str | float] | None = DEFAULT_WARP_DICT_OPTIONS,
    use_tqdm_progress_bar: bool = True,
    tqdm_file_name_chars: int = TQDM_FILE_NAME_PRINT_CHARS_INDEX,
    resampling_method: Resampling | None = None,
    supress_warnings: bool = True,
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
        Path to save resampled `input_path` file(s) to `destNameOrDestDS`
        in `Warp`.
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
    format
        Format to write new file to.
    multithread
        Whether to use `multithread` to speed up calculations.
    kwargs
        Any additional parameters to pass to `WarpOption`.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if use_tqdm_progress_bar:
        description: str = _progress_bar_file_description(
            input_path=input_path,
            prefix="Warp:      ",
            tqdm_file_name_chars=tqdm_file_name_chars,
        )
        progress_bar, progress_callback = _gen_progress_bar(description=description)
        kwargs["callback"] = progress_callback

    try:
        assert not Path(output_path).is_dir()
    except AssertionError:
        raise FileExistsError(f"Path exists as a directory: {output_path}")
    warp_config: GDALWarpAppOptions = WarpOptions(
        dstSRS=output_crs,
        format=format,
        xRes=output_x_resolution,
        yRes=output_y_resolution,
        copyMetadata=copy_metadata,
        multithread=multithread,
        warpOptions=warp_dict_options,
        resampleAlg=_ensure_resample_method_name(resampling_method),
        **kwargs,
    )
    projection: GDALDataset
    if supress_warnings:
        with config_GDAL_option("CPL_LOG", "gdal_warnings.log"):
            projection = Warp(
                destNameOrDestDS=output_path,
                srcDSOrSrcDSTab=input_path,
                options=warp_config,
            )
    else:
        projection = Warp(
            destNameOrDestDS=output_path,
            srcDSOrSrcDSTab=input_path,
            options=warp_config,
        )
    projection.FlushCache()
    if use_tqdm_progress_bar:
        progress_bar.close()

    assert projection is not None
    return output_path if return_path else projection


def converted_output_path(
    source_path: PathLike | None,
    export_folder: PathLike,
    new_path_name_func: Callable[..., Path] | None = None,
    **kwargs,
) -> Path:
    """`source_path` in `export_folder` via `new_path_name_func`.

    Parameters
    ----------
    source_path
        Original path to extract file name via `Path(source_path).name`
    export_folder
        Folder to save new path in.
    new_path_name_func
        Function to convert old file name to new file name.
    **kwargs
        Additional parameters passed to `new_path_name_func`

    Returns
    -------
    Converted path of `export_folder` / then either
    `source_path` or results of `new_path_name_func(source_path)`.
    """
    if not source_path:
        raise ValueError(
            f"Source path must be a folder, currently '{source_path}'. "
            f"May need to mount drive."
        )
    # Generate export_path following source_path name
    source_as_path: Path = Path(source_path)
    if new_path_name_func:
        source_as_path = Path(new_path_name_func(source_as_path, **kwargs))
    return Path(export_folder) / source_as_path.name


def apply_geo_func(
    source_path: PathLike,
    func: ReprojectFuncType,
    export_folder: PathLike,
    new_path_name_func: Callable[[Path], Path] | None = None,
    to_netcdf: bool = True,
    to_raster: bool = False,
    export_path_as_output_path_kwarg: bool = False,
    return_results: bool = False,
    **kwargs,
) -> Path | T_Dataset | GDALDataset:
    """Apply a `Callable` to `netcdf_source` file and export via `to_netcdf`.

    Parameters
    ----------
    source_path
        `netcdf` file to apply `func` to.
    func
        `Callable` to modify `netcdf`.
    export_folder
        Where to save results.
    new_path_name_func
        `Callabe` to generate new path to save to.
    to_netcdf
        Whether to call `to_netcdf()` method on `results` `Dataset`.
    to_raster
        Whether to call `rio.to_raster()` on `results` `Dataset`.
    export_path_as_output_path_kwarg
        Whether to add `output_path = export_path` to `kwargs` passed to
        `func`. Meant for cases calling `gdal_warp_wrapper`.
    return_results
        Whether to return results, which would be a `Dataset` or
        `GDALDataset` (the latter if `gdal_warp_wrapper` is used).
    **kwargs
        Other parameters passed to `func` call.

    Returns
    -------
       Either a `Path` to generated file or converted `xarray` object.
    """
    if not source_path:
        raise ValueError(
            f"Source path must be a folder, currently '{source_path}'. "
            f"May need to mount drive."
        )
    # Generate export_path following source_path name
    export_path: Path = Path(source_path)
    if new_path_name_func:
        export_path = new_path_name_func(export_path)
    export_path = Path(export_folder) / export_path.name
    if export_path_as_output_path_kwarg:
        kwargs["output_path"] = export_path
    results: T_Dataset | Path | GDALDataset = func(source_path, **kwargs)
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
    >>> from pandas import date_range
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


def date_seq_to_str(datetime_seq: Sequence[datetime], join_str: str = " ") -> str:
    """Return a `str` joining `str` of `dates` from `datetime_seq`.
    Parameters
    ----------
    datetime_seq
        Iterable of `datetimes` to convert to `strs` to join via `join_str`.
    join_str
        `str` to join `datetime_seq` elements with.

    Examples
    --------
    >>> date_seq_to_str((datetime(1980, 12, 1), datetime(1981, 11, 30)))
    '1980-12-01 1981-11-30'
    >>> date_seq_to_str((date(1980, 12, 1), datetime(1981, 11, 30)))
    '1980-12-01 1981-11-30'
    """
    return join_str.join(
        str(d.date()) if isinstance(d, datetime) else str(d) for d in datetime_seq
    )


def data_path_to_date_range(
    path: PathLike, return_type: Literal["raw", "string"] = "string"
) -> tuple[date, date] | str:
    """Extract date range as `tuple` or `str` from `path`.

    Examples
    --------
    >>> data_path_to_date_range('cpm/tasmax_19801201-19811130.nc')
    '1980-12-01 1981-11-30'
    >>> data_path_to_date_range('cpm/tasmax_19801201-19811130.nc', return_type="raw")
    (datetime.date(1980, 12, 1), datetime.date(1981, 11, 30))
    """
    date_range_tuple: tuple[datetime, datetime] = file_name_to_start_end_dates(path)
    if return_type == "raw":
        return date_range_tuple[0].date(), date_range_tuple[1].date()
    elif return_type == "string":
        return date_seq_to_str(date_range_tuple)
    else:
        raise ValueError(f"'return_type' must be 'raw' or 'string'")


def path_parent_types(
    path_or_instance: PathLike | Iterable,
    data_type: Literal[HADS_NAME, CPM_NAME],
    trim_tail: int | None = None,
    nc_file: bool = False,  # crop: bool = False
) -> str:
    """Extract relevant path info to print progress.

    Examples
    --------
    >>> path_parent_types(
    ...     'UKCP2.2/tasmax/05/latest/',
    ...     data_type=CPM_NAME)
    'tasmax-05-latest'
    >>> path_parent_types(
    ...     'UKCP2.2/tasmax/05/latest/', trim_tail=-1,
    ...     data_type=CPM_NAME)
    'tasmax-05'
    >>> path_parent_types(
    ...     'crop/Glasgow/tasmax/05/',
    ...     data_type=CPM_NAME)
    'Glasgow-tasmax-05'
    >>> path_parent_types(
    ...     'HadsUKgrid/tasmax/day',
    ...     data_type=HADS_NAME, trim_tail=-1)
    'tasmax'
    >>> path_parent_types(
    ...     'HadsUKgrid/tasmax/day/hads-1980-1982.nc',
    ...     data_type=HADS_NAME, trim_tail=-1, nc_file=True)
    'tasmax'
    """
    if hasattr(path_or_instance, "input_path"):
        path_or_instance = path_or_instance.input_path
    assert isinstance(path_or_instance, PathLike | str)
    path_or_instance = Path(path_or_instance)
    path_parents_count: int = -3 if data_type == CPM_NAME else -2
    if nc_file:
        path_parents_count -= 1
        trim_tail = trim_tail - 1 if trim_tail else -1
    return "-".join(path_or_instance.parts[path_parents_count:trim_tail])


def generate_360_to_standard(array_to_expand: T_DataArray) -> T_DataArray:
    """Return `array_to_expand` 360 days expanded to 365 or 366 days.

    This may be dropped if `cpm_reproject_with_standard_calendar` is successful.
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


def cftime_range_gen(time_data_array: T_DataArray, **kwargs) -> NDArray:
    """Convert a banded time index a banded standard (Gregorian)."""
    assert hasattr(time_data_array, "time")
    time_bnds_fix_range_start: CFTimeIndex = cftime_range(
        time_data_array.time.dt.strftime(ISO_DATE_FORMAT_STR).values[0],
        time_data_array.time.dt.strftime(ISO_DATE_FORMAT_STR).values[-1],
        **kwargs,
    )
    time_bnds_fix_range_end: CFTimeIndex = time_bnds_fix_range_start + timedelta(days=1)
    return np.array((time_bnds_fix_range_start, time_bnds_fix_range_end)).T


def get_cpm_for_coord_alignment(
    cpm_for_coord_alignment: PathLike | T_Dataset | None,
    skip_reproject: bool = False,
    cpm_regex: str = CPM_REGEX,
) -> T_Dataset:
    """Check if `cpm_for_coord_alignment` is a `Dataset`, process if a `Path`.

    Parameters
    ----------
    cpm_for_coord_alignment
        Either a `Path` or a file or folder with a `cpm` file to align to
        or a `xarray.Dataset`. If a folder, the first file matching
        `cpm_regex` will be used. It will then be processed via
        `cpm_reproject_with_standard_calendar` for comparability and use
        alongside `cpm` files.
    skip_reproject
        Whether to skip calling `cpm_reproject_with_standard_calendar`.
    cpm_regex
        A regular expression to filter suitable files if
        `cpm_for_coord_alignment` is a folder `Path`.

    Returns
    -------
    An `xarray.Dataset` coordinate structure to align `HADs` coordinates.
    """
    if not cpm_for_coord_alignment:
        raise ValueError("'cpm_for_coord_alignment' must be a Path or xarray Dataset.")
    elif isinstance(cpm_for_coord_alignment, PathLike):
        path: Path = Path(cpm_for_coord_alignment)
        try:
            assert path.exists()
        except:
            raise FileExistsError(f"No 'cpm_for_coord_alignment' at '{path}'")
        if path.is_dir():
            path = next(path.glob(cpm_regex))
        if skip_reproject:
            logger.info(f"Skipping reprojection and loading '{path}'...")
            cpm_for_coord_alignment, variable = check_xarray_path_and_var_name(
                cpm_for_coord_alignment, None
            )
            logger.info(
                f"Variable '{variable}' loaded for coord alignment from '{path}'."
            )
        else:
            logger.info(f"Converting coordinates of '{path}'...")
            cpm_for_coord_alignment = cpm_reproject_with_standard_calendar(path)
            logger.info(f"Coordinates converted from '{path}''")
    elif not skip_reproject:
        logger.info(
            f"Converting coordinates of type {type(cpm_for_coord_alignment)} ..."
        )
        cpm_for_coord_alignment = cpm_reproject_with_standard_calendar(
            cpm_for_coord_alignment
        )
        logger.info(f"Coordinates converted to type {type(cpm_for_coord_alignment)}")
    else:
        logger.info(
            f"Coordinate converter of type {type(cpm_for_coord_alignment)} "
            f"loaded without processing."
        )
    try:
        assert isinstance(cpm_for_coord_alignment, Dataset)
    except AssertionError:
        raise AttributeError(
            f"'cpm_for_coord_alignment' must be a 'Dataset'. "
            f"Currently a {type(cpm_for_coord_alignment)}."
        )
    return cpm_for_coord_alignment


def region_crop_file_name(
    file_name: PathLike, crop_region: str | RegionOptions | None
) -> str:
    """Generate a file name for a regional crop.

    Parameters
    ----------
    file_name
        File name to add `crop_region` name to.
    crop_region
        Region name to include in cropped file name.

    Examples
    --------
    >>> region_crop_file_name(
    ...    'tasmax.nc',
    ...    'Glasgow')
    'crop_Glasgow_tasmax.nc'
    >>> region_crop_file_name(
    ...    'tasmax_hadukgrid_uk_2_2km_day_19800601-19800630.nc',
    ...    'Glasgow')
    'crop_Glasgow_tasmax_hads_19800601-19800630.nc'
    >>> region_crop_file_name(
    ...     'tasmax_rcp85_land-cpm_uk_2.2km_05_day_std_year_19861201-19871130.nc',
    ...     'Glasgow')
    'crop_Glasgow_tasmax_cpm_05_19861201-19871130.nc'
    """
    file_name_sections = Path(file_name).name.split("_")
    final_suffix: str
    crop_region = crop_region or ""
    if "_rcp85_land-cpm_uk_2" in str(file_name):
        final_suffix = "_".join(
            (
                file_name_sections[0],
                "cpm",
                file_name_sections[5],
                file_name_sections[-1],
            )
        )
    elif "_hadukgrid_uk_2_2km_day" in str(file_name):
        final_suffix = "_".join(
            (
                file_name_sections[0],
                "hads",
                file_name_sections[-1],
            )
        )
    else:
        final_suffix = str(file_name)
    return "_".join(("crop", str(crop_region), final_suffix))


def _write_and_or_return_results(
    instance,
    result: T_Dataset,
    output_path_func: Callable,
    source_path: Path,
    write_results: bool,
    return_path: bool,
    override_export_path: Path | None = None,
    **kwargs,
) -> Path | T_Dataset:
    """Write and or return `resample` or `crop` results.

    Parameters
    ----------
    instance
        Instance of `ConvertBase`.
    result
        Instance of resambled or croped dataset.
    output_path_func
        Callable to return new result file name to write to.
    source_path
        `Path` original data used to calculate `result`.
    write_results
        Whether to write `ConvertBase` results to a file.
    return_path
        Whether to return the `write_results` `Path` or `T_Dataset` results instance.
    override_export_path
        Path to override default calculated output path.
    **kwargs
        Addional paths to pass to `converted_output_path`
        to generate default new path.
    """
    instance._result_paths[source_path] = None
    if write_results or return_path:
        export_path: Path = override_export_path or converted_output_path(
            source_path=source_path,
            export_folder=instance.output_path,
            new_path_name_func=output_path_func,
            **kwargs,
        )
        if write_results:
            result.to_netcdf(export_path)
            instance._result_paths[source_path] = export_path
        if return_path:
            return export_path
    return result


def progress_wrapper(
    instance: Sequence,
    method_name: str,
    start: int = 0,
    stop: int | None = None,
    step: int = 1,
    description: str = "",
    override_export_path: Path | None = None,
    source_to_index: Sequence | None = None,
    return_path: bool = True,
    write_results: bool = True,
    use_progress_bar: bool = True,
    progress_bar_refresh_per_sec: int = 1,
    description_func: Callable[..., str] | None = None,
    description_kwargs: dict[str, Any] | None = None,
    progress_instance: Progress | None = None,
    skip_progress_kwargs_method_name: str = "to_reprojection",
    **kwargs,
) -> Iterator[Path | T_Dataset]:
    """Iterate over `instance` with or without a progress bar.

    Parameters
    ----------
    instance
        An instance of a class with `method_name` for iterating calls.
    method_name
        Method name to call on `instance` to iterate calculations.
    start
        Index to start iterating from.
    stop
        Index to end interating at.
    step
        Hops of iterating between `start` and `stop` of `instance`.
    description
        What to print in front of progress bar if `progress_bar` is `True`.
    override_export_path
        Export `Path` to write to instead of `self.output_path`.
    source_to_index
        `Sequence` of paths to iterate over instaed of `self`
    return_path
        Whether to return `Path` of export. If not, result objects are returned.
    write_results
        Whether to write results to disk. Required if `return_path` is `True`.
    use_progress_bar
        Whether to use progress bar.
    progress_bar_refresh_per_sec
        How many `progress_bar` refreshes per second if `progress_bar` is used.
    description_func
        Function to return description.
    description_kwargs
        Parameters to pass to description_func.
    skip_progress_kwargs_method_name
        Method name when progress instance should not be pased.
    **kwargs
        Additional parameters to pass to `method_name`.
    """
    # progress_bar = True
    start = start or 0
    stop = stop or len(instance)
    total_tasks: int = range_len(len(instance), start=start, stop=stop, step=step)
    if not progress_instance:
        progress_instance = Progress(
            SpinnerColumn(),
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
            console=console,
            refresh_per_second=progress_bar_refresh_per_sec,
        )
    assert progress_instance
    task_id: float = progress_instance.add_task(
        description=description, total=total_tasks, visible=use_progress_bar
    )
    for index in range(start, stop, step):
        if description_func:
            # print(description_kwargs)
            # print(instance[index])
            description = description_func(instance[index], **description_kwargs)
            print(description)
        else:
            description = f"task {index}"
        # progress_instance.update(task_id, description=description, refresh=True)
        progress_instance.update(task_id, description=description)
        if "index" in kwargs:
            popped_index = kwargs.pop("index")
            console.log(f"popping index {popped_index} vs actual index {index}")
        if not method_name == skip_progress_kwargs_method_name:
            kwargs["progress_instance"] = progress_instance
            if "end" in kwargs:
                popped_end = kwargs.pop("end")
                console.log(f"popping end {popped_end} vs actual end {stop}")
            else:
                print("end not in kwargs")
        yield getattr(instance, method_name)(
            index=index,
            override_export_path=override_export_path,
            source_to_index=source_to_index,
            return_path=return_path,
            write_results=write_results,
            **kwargs,
        )
        progress_instance.update(task_id=task_id, advance=1)


def execute_configs(
    instance: Any,
    data_type: Literal[CPM_NAME, HADS_NAME],
    configs_method: str = "yield_configs",
    multiprocess: bool = False,
    cpus: int | None = None,
    return_instances: bool = False,
    return_path: bool = True,
    # start: int = 0,
    # stop: int | None = None,
    # step: int = 1,
    # # start_calc_index: int =0,
    # # stop_calc_index: int | None =None,
    # # step_calc_index: int =1,
    use_progress_bar: bool = True,
    description_iter_func: Callable[..., str] | None = path_parent_types,
    description_iter_kwargs: dict[str, Any] | None = None,
    **kwargs,
) -> tuple | list[T_Dataset | Path]:
    """Run all converter configurations.

    Parameters
    ----------
    multiprocess
        If `True` run parameters in `resample_configs` with `multiprocess_execute`.
    configs_method
        Method name to yield model parameters.
    cpus
        Number of `cpus` to pass to `multiprocess_execute`.
    return_instances
        Return instances of generated `class` (e.g. `HADsConvert`
        or `CPMConvert`), or return the `results` of each
        `execute` call.
    return_path
        Return `Path` to results object if True, else resampled `Dataset`.
    **kwargs
        Parameters to path to sampler `execute` calls.
    """
    # config: tuple = getattr(instance, configs_method)()
    configs: tuple = tuple(getattr(instance, configs_method)())
    # configs: tuple = tuple(instance) if isinstance(instance, Generator) else tuple(getattr(instance, configs_method)())
    # console.print(f"Processing {len(configs)} config(s)...")
    results: list[tuple[Path, ...]] = []
    multiprocess = False
    if multiprocess:
        cpus = cpus or instance.cpus
        if instance.total_cpus and cpus:
            cpus = min(cpus, instance.total_cpus - 1)
        results = multiprocess_execute(
            # tuple(config),
            # config,
            configs,
            cpus=cpus,
            include_sub_process_config=True,
            sub_process_progress_bar=False,
            return_path=return_path,
            **kwargs,
        )
    else:
        config_progress = Progress(
            "{task.description}",
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
        )
        configs_count: int = len(configs)
        progress_task = config_progress.add_task(
            f"{configs_count} {data_type} configs...",
            total=configs_count,
            visible=use_progress_bar,
        )
        with Live(config_progress, console=console, refresh_per_second=1):
            for i, config in enumerate(configs):
                progress_results: tuple = tuple(
                    progress_wrapper(
                        config,
                        method_name="execute",
                        return_path=True,
                        start=config.start_index,
                        end=config.stop_index,
                        description=f"{i}/{len(config)} configs...",
                        description_func=description_iter_func,
                        description_kwargs=description_iter_kwargs,
                        use_progress_bar=use_progress_bar,
                        progress_instance=config_progress,
                        **kwargs,
                    )
                )
                results.append(progress_results)
                config_progress.update(
                    progress_task, advance=1, description=config.input_path.name
                )
    if return_instances:
        return configs
    else:
        return results


@dataclass
class XarrayTimeSeriesCalcManager(Sequence):
    """
    Manage cacluations over time for `.nc` files.

    Attributes
    ----------
    path
        `Path` to aggreate raw files from, following standard `cpm` hierarchy.
    sub_path
        A subpath to parse, like 'latest' for `UKCPM2.2`.
    save_folder
        Where to save resulting summary `nc` files.
    variables
        Which variables to include
    runs
        Which `RunOptions` to include.
    method_name
        Which method to use to aggreate. Must be a standard `xarray` `Dataset` method.
    time_dim_name
        Name of the temporal `dim` on the joined `.nc` files.
    regex
        Check `.nc` paths to match and then aggregate.
    source_folders
        `List` of folders to iterate over, filled via `path` if None.

    Examples
    --------
    >>> tasmax_hads_1980_raw = getfixture('tasmax_hads_1980_raw')
    >>> if not tasmax_hads_1980_raw:
    ...     pytest.skip(mount_or_cache_doctest_skip_message)
    >>> tmp_save: Path = getfixture('tmp_path') / 'xarray-time-series-summary-manager'
    >>> xr_var_managers = XarrayTimeSeriesCalcManager(save_folder=tmp_save)
    >>> save_paths: tuple[Path, ...] = xr_var_managers.save_joined_xr_time_series(stop=2, ts_stop=2)
    Aggregating '05' 'tasmax' (1/2)...
    Aggregating '06' 'tasmax' (2/2)...
    >>> pprint(save_paths)
    (...Path('.../median-tasmax-05.nc'),
     ...Path('.../median-tasmax-06.nc'))
    >>> pprint(sorted(tmp_save.iterdir()))
    [...Path('.../median-tasmax-05.nc'),
     ...Path('.../median-tasmax-06.nc')]
    """

    path: PathLike = climate_data_mount_path() / "Raw/UKCP2.2/"
    save_folder: PathLike = Path("../docs/assets/cpm-raw-medians")
    sub_path: PathLike | None = Path("latest")
    variables: Sequence[str | VariableOptions] = VariableOptions.cpm_values()
    runs: Sequence[str | RunOptions] = RunOptions.preferred_and_first()
    method_name: str = "median"
    time_dim_name: str = "time"
    regex: str = CPM_REGEX
    source_folders: list = field(default_factory=list)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.save_folder = Path(self.save_folder)
        self.sub_path = Path(self.sub_path) if self.sub_path else Path()
        if not self.source_folders:
            self._set_source_folders()

    @overload
    def __getitem__(self, idx: int) -> Path: ...

    @overload
    def __getitem__(self, s: slice) -> Sequence[Path]: ...

    def __getitem__(self, item):
        return self.source_folders[item]

    def __len__(self) -> int:
        return len(self.source_folders)

    def _set_source_folders(self, path: PathLike | None = None):
        if path:
            self.path = Path(path)
        self.source_folders = [
            self.path / variable_name / run_name / self.sub_path
            for variable_name in self.variables
            for run_name in self.runs
        ]

    def _get_var_run(self, var_path: Path) -> tuple[str, str]:
        var_run: Sequence[Path]
        if self.sub_path and self.sub_path.name:
            var_run = var_path.parents[:2]
        else:
            var_run = Path(var_path.name), Path(var_path.parent.name)
        return var_run[1].name, var_run[0].name

    def _source_path_to_file_name(self, path: PathLike) -> str:
        return f"{self.method_name}-{'-'.join(self._get_var_run(Path(path)))}.nc"

    def join_xr_time_series_vars_iter(
        self,
        start: int = 0,
        stop: int | None = None,
        step: int = 1,
        ts_start: int = 0,
        ts_stop: int | None = None,
        ts_step: int = 1,
    ) -> Iterable[tuple[T_Dataset, Path]]:
        if not self.source_folders:
            self._set_source_folders()
        aggregate_count: int = range_len(
            maximum=len(self), start=start, stop=stop, step=step
        )
        for i, var_path in enumerate(islice(self, start, stop, step)):
            var_path = Path(var_path)
            run, variable = self._get_var_run(var_path)
            log_str: str = f"Aggregating '{variable}' '{run}'"
            console.print(f"{log_str} ({i + 1}/{aggregate_count})...")
            yield join_xr_time_series_var(
                var_path, start=ts_start, stop=ts_stop, step=ts_step
            ), var_path

    def save_joined_xr_time_series(
        self, save_folder: PathLike | None = None, **kwargs
    ) -> tuple[Path, ...]:
        save_folder = save_folder or self.save_folder
        Path(save_folder).mkdir(parents=True, exist_ok=True)
        self.results: dict[Path, Path] = {}
        for ds, var_path in self.join_xr_time_series_vars_iter(**kwargs):
            write_path: Path = Path(save_folder) / self._source_path_to_file_name(
                var_path
            )
            ds.to_netcdf(write_path)
            self.results[var_path] = write_path
        return tuple(self.results.values())
