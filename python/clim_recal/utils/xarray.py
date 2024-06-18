from datetime import date, datetime, timedelta
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Final

import numpy as np
import rioxarray  # nopycln: import
import seaborn
from cftime._cftime import Datetime360Day
from matplotlib import pyplot as plt
from numpy import ndarray
from numpy.typing import NDArray
from osgeo.gdal import Dataset as GDALDataset
from osgeo.gdal import GDALWarpAppOptions, Warp, WarpOptions
from pandas import DatetimeIndex, date_range
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
    DEFAULT_CALENDAR_ALIGN,
    DEFAULT_INTERPOLATION_METHOD,
    DEFAULT_RELATIVE_GRID_DATA_PATH,
    GLASGOW_GEOM_LOCAL_PATH,
    NETCDF4_XARRAY_ENGINE,
    TIME_COLUMN_NAME,
    BoundingBoxCoords,
    CFCalendarSTANDARD,
    ConvertCalendarAlignOptions,
    XArrayEngineType,
)
from .gdal_formats import NETCDF_EXTENSION_STR, GDALFormatsType, GDALGeoTiffFormatStr

logger = getLogger(__name__)

seaborn.set()  # Use seaborn style for all `matplotlib` plots

ReprojectFuncType = Callable[[T_Dataset], T_Dataset]

GLASGOW_GEOM_ABSOLUTE_PATH: Final[Path] = (
    climate_data_mount_path() / GLASGOW_GEOM_LOCAL_PATH
)

# MONTH_DAY_DROP: DropDayType = {(1, 31), (4, 1), (6, 1), (8, 1), (10, 1), (12, 1)}
# """A `set` of tuples of month and day numbers for `enforce_date_changes`."""


HADS_RAW_X_COLUMN_NAME: Final[str] = "projection_x_coordinate"
HADS_RAW_Y_COLUMN_NAME: Final[str] = "projection_y_coordinate"
HADS_DROP_VARS_AFTER_PROJECTION: Final[tuple[str, ...]] = ("longitude", "latitude")

CPM_RAW_X_COLUMN_NAME: Final[str] = "grid_longitude"
CPM_RAW_Y_COLUMN_NAME: Final[str] = "grid_latitude"

# TODO: CHECK IF THESE ARE BACKWARDS
FINAL_RESAMPLE_LON_COL: Final[str] = "x"
FINAL_RESAMPLE_LAT_COL: Final[str] = "y"


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
    cpm_to_std_calendar[
        "month_number"
    ] = cpm_to_std_calendar.month_number.interpolate_na(
        "time", fill_value="extrapolate"
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
    xr_time_series: T_Dataset | PathLike, variable_name: str | None
) -> tuple[Dataset, str]:
    """Check and return a `T_Dataset` instances and included variable name."""
    if isinstance(xr_time_series, PathLike):
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


def cpm_reproject_with_standard_calendar(
    cpm_xr_time_series: T_Dataset | PathLike,
    variable_name: str | None = None,
    x_dim_name: str = CPM_RAW_X_COLUMN_NAME,
    y_dim_name: str = CPM_RAW_Y_COLUMN_NAME,
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
    >>> tasmax_cpm_1980_365_day
    <xarray.Dataset> Size: 504MB
    Dimensions:      (x: 529, y: 653, time: 365)
    Coordinates:
      * x            (x) float64 4kB -3.129e+05 -3.107e+05 ... 8.465e+05 8.487e+05
      * y            (y) float64 5kB 1.197e+06 1.195e+06 ... -2.353e+05 -2.375e+05
      * time         (time) datetime64[ns] 3kB 1980-12-01T12:00:00 ... 1981-11-30...
        spatial_ref  int64 8B 0
    Data variables:
        tasmax       (time, y, x) float32 504MB 3.403e+38 3.403e+38 ... 3.403e+38
    >>> tasmax_cpm_1980_raw.dims
    FrozenMappingWarningOnValuesAccess({'ensemble_member': 1,
                                        'time': 360,
                                        'grid_latitude': 606,
                                        'grid_longitude': 484,
                                        'bnds': 2})
    >>> tasmax_cpm_1980_365_day.dims
    FrozenMappingWarningOnValuesAccess({'x': 529, 'y': 653, 'time': 365})
    """
    cpm_xr_time_series, variable_name = check_xarray_path_and_var_name(
        cpm_xr_time_series, variable_name
    )

    standar_calendar_ts: T_Dataset = cpm_xarray_to_standard_calendar(cpm_xr_time_series)
    subset_within_ensemble: T_Dataset = Dataset(
        {variable_name: standar_calendar_ts[variable_name][0]}
    )

    subset_in_epsg_27700: T_DataArray = xr_reproject_crs(
        subset_within_ensemble,
        variable_name=variable_name,
        x_dim_name=x_dim_name,
        y_dim_name=y_dim_name,
    )
    try:
        assert (subset_in_epsg_27700.time == standar_calendar_ts.time).all()
    except:
        raise ValueError(
            f"Time series of 'standar_calendar_ts' does not match time series of projection to {BRITISH_NATIONAL_GRID_EPSG}."
        )
    return subset_in_epsg_27700


def xr_reproject_crs(
    xr_time_series: T_Dataset | PathLike,
    x_dim_name: str = CPM_RAW_X_COLUMN_NAME,
    y_dim_name: str = CPM_RAW_Y_COLUMN_NAME,
    time_dim_name: str = TIME_COLUMN_NAME,
    variable_name: str | None = None,
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG,
    final_resolution: tuple[int, int] | None = (2200, 2200),
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
    final_resolution
        Resolution to project `xr_time_series` raster data to.

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
    ...     x_dim_name=HADS_RAW_X_COLUMN_NAME,
    ...     y_dim_name=HADS_RAW_Y_COLUMN_NAME,)
    >>> tasmax_hads_2_2km.dims
    FrozenMappingWarningOnValuesAccess({'x': 410, 'y': 660, 'time': 31})
    """
    xr_time_series, variable_name = check_xarray_path_and_var_name(
        xr_time_series, variable_name
    )
    xr_time_series = xr_time_series.rio.set_spatial_dims(
        x_dim=x_dim_name, y_dim=y_dim_name, inplace=True
    )
    # info requires a bf parameter, not straightforward for logging
    # logger.info(xr_time_series.info())
    data_array: T_DataArray = xr_time_series[variable_name]
    final_index_names: tuple[str, str, str] = (time_dim_name, x_dim_name, y_dim_name)
    extra_dims: set[str] = set(data_array.indexes.dims) - set(final_index_names)
    if extra_dims:
        raise ValueError(
            f"Can only reindex using dims: {final_index_names}, extra dim(s): {extra_dims}"
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
    without_attributes_reprojected: DataArray = without_attributes.rio.reproject(
        final_crs, resolution=final_resolution
    )
    return Dataset({variable_name: without_attributes_reprojected})


def interpolate_coords(
    xr_time_series: T_Dataset,
    variable_name: str,
    x_grid: NDArray | None = None,
    y_grid: NDArray | None = None,
    x_coord_column_name: str = HADS_RAW_X_COLUMN_NAME,
    y_coord_column_name: str = HADS_RAW_Y_COLUMN_NAME,
    reference_coords: T_Dataset | PathLike = DEFAULT_RELATIVE_GRID_DATA_PATH,
    reference_coord_x_column_name: str = HADS_RAW_X_COLUMN_NAME,
    reference_coord_y_column_name: str = HADS_RAW_Y_COLUMN_NAME,
    method: str = "linear",
    engine: XArrayEngineType = NETCDF4_XARRAY_ENGINE,
    use_reference_grid: bool = True,
    **kwargs,
) -> T_Dataset:
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

    try:
        assert isinstance(xr_time_series, Dataset)
    except:
        ValueError(f"'xr_time_series' must be an 'xr.Dataset' instance.")

    if use_reference_grid or (x_grid is None or y_grid is None):
        if isinstance(reference_coords, PathLike | str):
            reference_coords = open_dataset(
                reference_coords, decode_coords="all", engine=engine
            )
        try:
            assert isinstance(reference_coords, Dataset)
        except:
            ValueError(f"'reference_coords' must be an 'xr.Dataset' instance.")
        try:
            assert reference_coord_x_column_name in reference_coords.coords
            assert reference_coord_y_column_name in reference_coords.coords
            assert x_coord_column_name in xr_time_series.coords
            assert y_coord_column_name in xr_time_series.coords
        except AssertionError:
            raise ValueError(
                f"At least one of\n"
                f"'reference_coord_x_column_name': '{reference_coord_x_column_name}'\n"
                f"'reference_coord_y_column_name': '{reference_coord_y_column_name}'\n"
                f"'x_coord_column_name': '{x_coord_column_name}'\n"
                f"'y_coord_column_name': '{y_coord_column_name}'\n"
                f"not in 'reference_coords' and/or 'xr_time_series'."
            )

        x_grid = (
            reference_coords[reference_coord_x_column_name].values
            if x_grid is None
            else x_grid
        )
        y_grid = (
            reference_coords[reference_coord_y_column_name].values
            if y_grid is None
            else y_grid
        )
        use_reference_grid = True

    try:
        assert isinstance(x_grid, ndarray)
        assert isinstance(y_grid, ndarray)
    except:
        raise ValueError(
            f"Both must be 'ndarray' instances.\n"
            f"'x_grid': {x_grid}\n'y_grid': {y_grid}"
        )
    kwargs[x_coord_column_name] = x_grid
    kwargs[y_coord_column_name] = y_grid
    reprojected_data_array: T_DataArray = xr_time_series[variable_name].interp(
        method=method, **kwargs
    )

    # Ensure original `rio.crs` is kept in returned `Dataset`
    if use_reference_grid:
        reprojected_data_array.rio.write_crs(reference_coords.rio.crs, inplace=True)
    else:
        reprojected_data_array.rio.write_crs(xr_time_series.rio.crs, inplace=True)
    reprojected: T_Dataset = Dataset({variable_name: reprojected_data_array})
    return reprojected


def hads_resample_and_reproject(
    hads_xr_time_series: T_Dataset | PathLike,
    variable_name: str,
    x_dim_name: str = HADS_RAW_X_COLUMN_NAME,
    y_dim_name: str = HADS_RAW_Y_COLUMN_NAME,
    # x_grid: NDArray | None = None,
    # y_grid: NDArray | None = None,
    # method: str = "linear",
    # source_x_coord_column_name: str = HADS_RAW_X_COLUMN_NAME,
    # source_y_coord_column_name: str = HADS_RAW_Y_COLUMN_NAME,
    # final_x_coord_column_name: str = FINAL_RESAMPLE_LON_COL,
    # final_y_coord_column_name: str = FINAL_RESAMPLE_LAT_COL,
    # final_crs: str | None = BRITISH_NATIONAL_GRID_EPSG,
    # vars_to_drop: Sequence[str] | None = HADS_DROP_VARS_AFTER_PROJECTION,
    # use_reference_grid: bool = False,
) -> T_Dataset:
    """Resample `HADs` `xarray` time series to 2.2km."""
    hads_xr_time_series, variable_name = check_xarray_path_and_var_name(
        hads_xr_time_series, variable_name
    )
    # if isinstance(hads_xr_time_series, PathLike):
    #     hads_xr_time_series = open_dataset(hads_xr_time_series, decode_coords="all")
    epsg_277000_2_2km: T_Dataset = xr_reproject_crs(
        hads_xr_time_series,
        variable_name=variable_name,
        x_dim_name=x_dim_name,
        y_dim_name=y_dim_name,
    )

    # interpolated_hads: T_Dataset = interpolate_coords(
    #     hads_xr_time_series,
    #     variable_name=variable_name,
    #     x_grid=x_grid,
    #     y_grid=y_grid,
    #     x_coord_column_name=source_x_coord_column_name,
    #     y_coord_column_name=source_y_coord_column_name,
    #     method=method,
    #     use_reference_grid=use_reference_grid,
    # )
    # if vars_to_drop:
    #     interpolated_hads = interpolated_hads.drop_vars(vars_to_drop)
    #
    # interpolated_hads = interpolated_hads.rename(
    #     {
    #         source_x_coord_column_name: final_x_coord_column_name,
    #         source_y_coord_column_name: final_y_coord_column_name,
    #     }
    # )
    # if final_crs:
    #     interpolated_hads.rio.write_crs(final_crs, inplace=True)
    # return interpolated_hads
    return epsg_277000_2_2km


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
    ...                 GlasgowCoordsEPSG27700.as_tuple(),
    ...                 rtol=.01)
    >>> tasmax_cpm_1980_365_day.sizes
    Frozen({'x': 529, 'y': 653, 'time': 365})
    >>> cropped.sizes
    Frozen({'x': 186, 'y': 185, 'time': 365})
    """
    xr_time_series, _ = check_xarray_path_and_var_name(xr_time_series, None)
    try:
        assert str(xr_time_series.rio.crs) == crop_box.rioxarry_epsg
    except AssertionError:
        raise ValueError(
            f"'xr_time_series.rio.crs': '{xr_time_series.rio.epsg}' must equal 'crop_box.crs': '{crop_box.crs}'"
        )
    return xr_time_series.rio.clip_box(*crop_box.as_tuple(), **kwargs)


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
    extrapolate_fill_value: bool = True,
    check_cftime_cols: tuple[str] | None = None,
    cftime_range_gen_kwargs: dict[str, Any] | None = None,
    **kwargs,
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


def gdal_warp_wrapper(
    input_path: PathLike,
    output_path: PathLike,
    output_crs: str = BRITISH_NATIONAL_GRID_EPSG,
    output_x_resolution: int | None = None,
    output_y_resolution: int | None = None,
    copy_metadata: bool = True,
    return_path: bool = True,
    format: GDALFormatsType | str | None = GDALGeoTiffFormatStr,
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
    format
        Format to write new file to.
    multithread
        Whether to use `multithread` to speed up calculations.
    kwargs
        Any additional parameters to pass to `WarpOption`.
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
    """
    if not source_path:
        raise ValueError(
            f"Source path must be a folder, currently '{source_path}'. "
            f"May need to mount drive."
        )
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
) -> T_Dataset:
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
    xr_dataset: T_Dataset = open_dataset(xr_dataset_path)
    metric_name: str = str(xr_dataset_path).split("_")[0]
    start_date, end_date = file_name_to_start_end_dates(xr_dataset_path)
    dates_index: DatetimeIndex = date_range(start_date, end_date)
    intermediate_new_index: str = new_index_name + "_standard"
    # xr_intermediate_date = xr_dataset.assign_coords({intermediate_new_index: dates_index})
    xr_dataset[intermediate_new_index]: T_Dataset = dates_index
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
    xr_bands_time_indexed: T_DataArray = xr_intermediate_date[
        data_attribute_name
    ].expand_dims(dim={new_index_name: xr_intermediate_date[new_index_name]})
    # xr_365_data_array: T_DataArray = convert_xr_calendar(xr_bands_time_indexed)
    xr_365_dataset: T_Dataset = Dataset({metric_name: xr_bands_time_indexed})
    partial_fix_365_dataset: T_Dataset = convert_xr_calendar(xr_365_dataset.time)
    assert False


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


# def tansform_lat_lng_bounding_box(bbox: tuple[float, float] | BoundsTupleType):
#     # clip_box = box(*bbox)
#     wgs84 = pyproj.CRS('EPSG:4326')
#     brit_grid = pyproj.CRS('EPSG:27700')
#     project = pyproj.Transformer.from_crs(wgs84, brit_grid, always_xy=True).transform
#     clip_box = transform(project, clip_box)
#     clipped = grid.rio.clip([clip_box])
#     return clipped
