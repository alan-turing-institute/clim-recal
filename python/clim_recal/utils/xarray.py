from datetime import date
from typing import Final, Iterable

import rioxarray  # nopycln: import
from numpy import array, random
from pandas import to_datetime
from xarray import DataArray, Dataset

from .core import ISO_DATE_FORMAT_STR, DateType, date_range_generator

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
