import warnings
from datetime import date, datetime, timedelta
from logging import getLogger
from os import PathLike
from pathlib import Path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any, Callable, Final

import numpy as np
import rioxarray  # nopycln: import
import seaborn
from cftime._cftime import Datetime360Day
from matplotlib import pyplot as plt
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
    results_path,
)
from .data import (
    BRITISH_NATIONAL_GRID_EPSG,
    CPM_RAW_X_COLUMN_NAME,
    CPM_RAW_Y_COLUMN_NAME,
    DEFAULT_CALENDAR_ALIGN,
    DEFAULT_INTERPOLATION_METHOD,
    DEFAULT_RESAMPLING_METHOD,
    GLASGOW_GEOM_LOCAL_PATH,
    HADS_RAW_X_COLUMN_NAME,
    HADS_RAW_Y_COLUMN_NAME,
    NETCDF4_XARRAY_ENGINE,
    TIME_COLUMN_NAME,
    BoundingBoxCoords,
    CFCalendarSTANDARD,
    ConvertCalendarAlignOptions,
    RegionOptions,
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

FINAL_CONVERTED_CPM_WIDTH: Final[int] = 493
FINAL_CONVERTED_CPM_HEIGHT: Final[int] = 607

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
    da: T_DataArrayOrSet, path: PathLike, time_stamp: bool = False, **kwargs
) -> Path:
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
    da.plot(**kwargs)
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
    return Path(path)


def join_xr_time_series(
    path: PathLike,
    variable_name: str | None = None,
    method_name: str = "median",
    regex: str = CPM_REGEX,
) -> T_Dataset:
    """Join a set of xr_time_series files chronologically.

    Examples
    --------
    >>> tasmax_cpm_1980_raw_path = getfixture('tasmax_cpm_1980_raw_path').parents[1]
    >>> if not tasmax_cpm_1980_raw_path:
    ...     pytest.skip(mount_or_cache_doctest_skip_message)
    >>> results = join_xr_time_series(tasmax_cpm_1980_raw_path,
    ...                               'tasmax')
    >>> assert False

    """
    results: list[dict] = []
    for nc_path in Path(path).glob(regex):
        xr_time_series, nc_var_name = check_xarray_path_and_var_name(
            nc_path, variable_name=variable_name
        )
        if not variable_name:
            variable_name = nc_var_name
        try:
            assert variable_name == nc_var_name
        except AssertionError:
            raise ValueError(f"'{nc_var_name}' should match '{variable_name}'")
        # trimmed = xr_time_series.isel(time=slice(0, 10))
        results.append(
            {
                date_obj: getattr(val, method_name)().values.item()
                for date_obj, val in xr_time_series[variable_name].groupby("time")
            }
        )
    return results


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
    # This may need removing, including in docs
    # extrapolate_fill_value: bool = True,
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
        if Path(path).is_dir():
            path = next(Path(path).glob(cpm_regex))
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
    crop_region: str | RegionOptions | None, file_name: PathLike
) -> str:
    """Generate a file name for a regional crop.

    Parameters
    ----------
    crop_region
        Region name to include in cropped file name.
    file_name
        File name to add `crop_region` name to.

    Examples
    --------
    >>> region_crop_file_name(
    ...    'Glasgow',
    ...    'tasmax.nc')
    'crop_Glasgow_tasmax.nc'
    >>> region_crop_file_name(
    ...    'Glasgow',
    ...    'tasmax_hadukgrid_uk_2_2km_day_19800601-19800630.nc')
    'crop_Glasgow_tasmax_hads_19800601-19800630.nc'
    >>> region_crop_file_name(
    ...     'Glasgow',
    ...     'tasmax_rcp85_land-cpm_uk_2.2km_05_day_std_year_19861201-19871130.nc')
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
