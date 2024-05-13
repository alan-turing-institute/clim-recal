from datetime import date, datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import Any, Final, Iterable, Literal

import numpy as np
import rioxarray  # nopycln: import
from numpy import array, random
from numpy.typing import NDArray
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
)

DropDayType = set[tuple[int, int]]
ChangeDayType = set[tuple[int, int]]

# MONTH_DAY_DROP: DropDayType = {(1, 31), (4, 1), (6, 1), (8, 1), (10, 1), (12, 1)}
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

GLASGOW_COORDS: Final[tuple[float, float]] = (55.86279, -4.25424)
MANCHESTER_COORDS: Final[tuple[float, float]] = (53.48095, -2.23743)
LONDON_COORDS: Final[tuple[float, float]] = (51.509865, -0.118092)
THREE_CITY_COORDS: Final[dict[str, tuple[float, float]]] = {
    "Glasgow": GLASGOW_COORDS,
    "Manchester": MANCHESTER_COORDS,
    "London": LONDON_COORDS,
}
"""Coordinates of Glasgow, Manchester and London as `(lon, lat)` `tuples`."""

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
        return interpolate_xr_ts(
            xr_ts=calendar_converted_ts,
            original_xr_ts=xr_time_series,
            check_cftime_cols=check_cftime_cols,
            interpolate_method=interpolate_method,
            keep_crs=keep_crs,
            keep_attrs=keep_attrs,
            limit=limit,
            cftime_range_gen_kwargs=cftime_range_gen_kwargs,
        )
        if extrapolate_fill_value:
            kwargs["fill_value"] = "extrapolate"
        interpolated_ts: Dataset | DataArray = calendar_converted_ts.interpolate_na(
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
        if keep_crs and xr_time_series.rio.crs:
            return interpolated_ts.rio.write_crs(xr_time_series.rio.crs)
        else:
            return interpolated_ts


def interpolate_xr_ts(
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
    if check_cftime_cols is None:
        check_cftime_cols = tuple()
    if cftime_range_gen_kwargs is None:
        cftime_range_gen_kwargs = dict()
    original_xr_ts = original_xr_ts if original_xr_ts else xr_ts
    # Preveent a kwargs overwrite conflict
    kwargs["fill_value"] = "extrapolate"

    # if extrapolate_fill_value:
    #     kwargs["fill_value"] = "extrapolate"
    interpolated_ts: Dataset = xr_ts.interpolate_na(
        dim="time",
        method=interpolate_method,
        keep_attrs=keep_attrs,
        limit=limit,
        # fill_value="extrapolate",
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


# Below requires packages outside python standard library
# Note: `rioxarray` is imported to ensure GIS methods are included. See:
# https://corteva.github.io/rioxarray/stable/getting_started/getting_started.html#rio-accessor
def xarray_example(
    start_date: DateType = XARRAY_EXAMPLE_START_DATE_STR,
    end_date: DateType = XARRAY_EXAMPLE_END_DATE_4_YEARS,
    coordinates: dict[str, tuple[float, float]] = THREE_CITY_COORDS,
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

    Examples
    --------
    >>>
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

    Examples
    --------
    >>> pytest.xfail(reason="Not finished implementing")
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


GDALFormatsType = Literal[
    "VRT",
    "DERIVED",
    "GTiff",
    "COG",
    "NITF",
    "RPFTOC",
    "ECRGTOC",
    "HFA",
    "SAR_CEOS",
    "CEOS",
    "JAXAPALSAR",
    "GFF",
    "ELAS",
    "ESRIC",
    "AIG",
    "AAIGrid",
    "GRASSASCIIGrid",
    "ISG",
    "SDTS",
    "DTED",
    "PNG",
    "JPEG",
    "MEM",
    "JDEM",
    "GIF",
    "BIGGIF",
    "ESAT",
    "FITS",
    "BSB",
    "XPM",
    "BMP",
    "DIMAP",
    "AirSAR",
    "RS2",
    "SAFE",
    "PCIDSK",
    "PCRaster",
    "ILWIS",
    "SGI",
    "SRTMHGT",
    "Leveller",
    "Terragen",
    "netCDF",
    "ISIS3",
    "ISIS2",
    "PDS",
    "PDS4",
    "VICAR",
    "TIL",
    "ERS",
    "JP2OpenJPEG",
    "L1B",
    "FIT",
    "GRIB",
    "RMF",
    "WCS",
    "WMS",
    "MSGN",
    "RST",
    "GSAG",
    "GSBG",
    "GS7BG",
    "COSAR",
    "TSX",
    "COASP",
    "R",
    "MAP",
    "KMLSUPEROVERLAY",
    "WEBP",
    "PDF",
    "Rasterlite",
    "MBTiles",
    "PLMOSAIC",
    "CALS",
    "WMTS",
    "SENTINEL2",
    "MRF",
    "PNM",
    "DOQ1",
    "DOQ2",
    "PAux",
    "MFF",
    "MFF2",
    "GSC",
    "FAST",
    "BT",
    "LAN",
    "CPG",
    "NDF",
    "EIR",
    "DIPEx",
    "LCP",
    "GTX",
    "LOSLAS",
    "NTv2",
    "CTable2",
    "ACE2",
    "SNODAS",
    "KRO",
    "ROI_PAC",
    "RRASTER",
    "BYN",
    "NOAA_B",
    "NSIDCbin",
    "ARG",
    "RIK",
    "USGSDEM",
    "GXF",
    "BAG",
    "S102",
    "HDF5",
    "HDF5Image",
    "NWT_GRD",
    "NWT_GRC",
    "ADRG",
    "SRP",
    "BLX",
    "PostGISRaster",
    "SAGA",
    "XYZ",
    "HF2",
    "OZI",
    "CTG",
    "ZMap",
    "NGSGEOID",
    "IRIS",
    "PRF",
    "EEDAI",
    "DAAS",
    "SIGDEM",
    "EXR",
    "HEIF",
    "TGA",
    "OGCAPI",
    "STACTA",
    "STACIT",
    "JPEGXL",
    "GPKG",
    "OpenFileGDB",
    "CAD",
    "PLSCENES",
    "NGW",
    "GenBin",
    "ENVI",
    "EHdr",
    "ISCE",
    "Zarr",
    "HTTP",
]
GDALGeoTiffFormatStr: Final[str] = "GTiff"
GDALNetCDFFormatStr: Final[str] = "netCDF"

TIF_EXTENSION_STR: Final[str] = "tif"
NETCDF_EXTENSION_STR: Final[str] = "nc"

GDALFormatExtensions: Final[dict[str, str]] = {
    GDALGeoTiffFormatStr: TIF_EXTENSION_STR,
    GDALNetCDFFormatStr: NETCDF_EXTENSION_STR,
}
