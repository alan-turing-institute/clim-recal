"""Utility functions."""
import sys
from copy import deepcopy
from csv import DictReader
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from logging import getLogger
from os import PathLike, chdir
from pathlib import Path
from typing import Any, Callable, Final, Generator, Iterable, Iterator, Optional, Union

logger = getLogger(__name__)

DateType = Union[date, str]
DATE_FORMAT_STR: Final[str] = "%Y%m%d"
ISO_DATE_FORMAT_STR: Final[str] = "%Y-%m-%d"
DATE_FORMAT_SPLIT_STR: Final[str] = "-"

NORMAL_YEAR_DAYS: Final[int] = 365
LEAP_YEAR_DAYS: Final[int] = NORMAL_YEAR_DAYS + 1
CPM_YEAR_DAYS: Final[int] = 360

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

CPM_FILE_NAME_MIDDLE_STR: Final[str] = "_rcp85_land-cpm_uk_2.2km_01_day_"
NETCDF_EXTENSION: Final[str] = "nc"
MAX_TEMP_STR: Final[str] = "tasmax"

MODULE_NAMES: Final[tuple[PathLike, ...]] = ("clim_recal",)
CURRENT_PATH = Path().absolute()
PYTHON_PACKAGE_DIR_NAME: Final[Path] = Path("python")


@dataclass
class MonthDay:
    """A class to ease generating annual time series.

    Attributes
    ----------
    month
        What Month as an integer from 1 to 12.
    day
        What day in the month, an integer from 1 to 31.
    format_str
        Format to use if generating a `str`.

    Examples
    --------
    >>> jan_1: MonthDay = MonthDay()
    >>> jan_1.from_year(1980)
    datetime.date(1980, 1, 1)
    >>> jan_1.from_year('1980')
    datetime.date(1980, 1, 1)
    >>> jan_1.from_year('1980', as_str=True)
    '1980-01-01'
    """

    month: int = 1
    day: int = 1
    format_str: str = ISO_DATE_FORMAT_STR

    def from_year(self, year: int | str, as_str: bool = False) -> date | str:
        """Return a"""
        year_date: date = date(int(year), self.month, self.day)
        return year_date.strftime(self.format_str) if as_str else year_date

    def from_year_range_to_str(
        self,
        start_year: int | str,
        end_year: int | str,
        split_str: str = DATE_FORMAT_SPLIT_STR,
        in_format_str: str = DATE_FORMAT_STR,
        out_format_str: str = DATE_FORMAT_STR,
        include_end_date: bool = True,
    ) -> str:
        """Return an annual range str.

        Parameters
        ----------
        start_year
            Starting year to combine with `self.month` and `self.day`.
        end_year
            Starting year to combine with `self.month` and `self.day`.
        split_str
            `str` between `start_date` and `end_date` generated.
        include_end_date
            Whether to include the end_date in the final `str`. If `False`
            follow `python` convention to return the day prior.

        Examples
        --------
        >>> jan_1: MonthDay = MonthDay()
        >>> jan_1.from_year_range_to_str(1980, 1982, include_end_date=False)
        '19800101-19811231'
        >>> jan_1.from_year_range_to_str('1980', 2020)
        '19800101-20200101'
        """
        start_date: date = date(int(start_year), self.month, self.day)
        end_date: date = date(int(end_year), self.month, self.day)
        return date_range_to_str(
            start_date=start_date,
            end_date=end_date,
            split_str=split_str,
            in_format_str=in_format_str,
            out_format_str=out_format_str,
            include_end_date=include_end_date,
        )


DEFAULT_START_MONTH_DAY: Final[MonthDay] = MonthDay(month=12, day=1)


def check_package_path(strict: bool = True, try_chdir: bool = False) -> bool:
    """Return path for test running.

    Parameters
    ----------
    strict
        Whether to raise a `ValueError` if check fails.
    try_chdir
        Whether to attempt changing directory if initial check fails

    Raises
    ------
    ValueError
        If `strict` and checks fail.

    Returns
    -------
    Whether to check if call was successful.

    Examples
    --------
    >>> check_package_path()
    True
    >>> chdir('..')
    >>> check_package_path(strict=False)
    False
    >>> check_package_path()
    Traceback (most recent call last):
        ...
    ValueError: 'clim-recal' pipeline must be run in...
    >>> check_package_path(try_chdir=True)
    True
    """
    current_path: Path = Path()
    if not set(MODULE_NAMES) <= set(path.name for path in current_path.iterdir()):
        if try_chdir:
            chdir(PYTHON_PACKAGE_DIR_NAME)
            return check_package_path(strict=strict, try_chdir=False)
        elif strict:
            raise ValueError(
                f"'clim-recal' pipeline must be "
                f"run in 'clim-recal/{PYTHON_PACKAGE_DIR_NAME}', "
                f"not '{current_path.absolute()}'"
            )
        else:
            return False
    else:
        return True


def is_platform_darwin() -> bool:
    """Check if `sys.platform` is `Darwin` (macOS)."""
    return sys.platform.startswith("darwin")


def ensure_date(date_to_check: DateType, format_str: str = DATE_FORMAT_STR) -> date:
    """Ensure passed `date_to_check` is a `date`.

    Parameters
    ----------
    date_to_check
        Date or `str` to ensure is a `date`.
    format_str
        `strptime` `str` to convert `date_to_check` if necessary.

    Returns
    -------
    `date` instance from `date_to_check`.

    Examples
    --------
    >>> ensure_date('19801130')
    datetime.date(1980, 11, 30)
    >>> ensure_date(date(1980, 11, 30))
    datetime.date(1980, 11, 30)
    """
    if isinstance(date_to_check, date):
        return date_to_check
    else:
        return datetime.strptime(date_to_check, format_str).date()


def date_range_generator(
    start_date: DateType,
    end_date: DateType,
    inclusive: bool = False,
    skip_dates: Iterable[DateType] | DateType | None = None,
    start_format_str: str = DATE_FORMAT_STR,
    end_format_str: str = DATE_FORMAT_STR,
    output_format_str: str = DATE_FORMAT_STR,
    skip_dates_format_str: str = DATE_FORMAT_STR,
    yield_type: type[date] | type[str] = date,
) -> Iterator[DateType]:
    """Return a tuple of `DateType` objects.

    Parameters
    ----------
    start_date
        `DateType` at start of time series.
    end_date
        `DateType` at end of time series.
    inclusive
        Whether to include the `end_date` in the returned time series.
    skip_dates
        Dates to skip between `start_date` and `end_date`.
    start_format_str
        A `strftime` format to apply if `start_date` `type` is `str`.
    end_format_str
        A `strftime` format to apply if `end_date` `type` is `str`.
    output_format_str
        A `strftime` format to apply if `yield_type` is `str`.
    skip_dates_format_str
        A `strftime` format to apply if any `skip_dates` are `str`.
    yield_type
        Whether which date type to return in `tuple` (`date` or `str`).

    Returns
    -------
    :
        A `tuple` of `date` or `str` objects (only one type throughout).

    Examples
    --------
    >>> four_years: tuple[date] = tuple(date_range_generator('19801130', '19841130'))
    >>> len(four_years)
    1461
    >>> four_years_inclusive: tuple[date] = tuple(
    ...     date_range_generator('1980-11-30', '19841130',
    ...                          inclusive=True,
    ...                          start_format_str=ISO_DATE_FORMAT_STR))
    >>> len(four_years_inclusive)
    1462
    >>> four_years_inclusive_skip: tuple[date] = tuple(
    ...     date_range_generator('19801130', '19841130',
    ...                          inclusive=True,
    ...                          skip_dates='19840229'))
    >>> len(four_years_inclusive_skip)
    1461
    >>> skip_dates: tuple[date] = (date(1981, 12, 1), date(1982, 12, 1))
    >>> four_years_inclusive_skip: tuple[date] = list(
    ...     date_range_generator('19801130', '19841130',
    ...                          inclusive=True,
    ...                          skip_dates=skip_dates))
    >>> len(four_years_inclusive_skip)
    1460
    >>> skip_dates in four_years_inclusive_skip
    False
    """
    start_date = ensure_date(start_date, start_format_str)
    end_date = ensure_date(end_date, end_format_str)
    if inclusive:
        end_date += timedelta(days=1)
    try:
        assert start_date < end_date
    except AssertionError:
        raise ValueError(
            f"start_date: {start_date} must be before end_date: {end_date}"
        )
    if skip_dates:
        if isinstance(skip_dates, str | date):
            skip_dates = [skip_dates]
        skip_dates = set(
            ensure_date(skip_date, skip_dates_format_str) for skip_date in skip_dates
        )
    for day_number in range(int((end_date - start_date).days)):
        date_obj: date = start_date + timedelta(day_number)
        if skip_dates:
            if date_obj in skip_dates:
                continue
        yield (date_obj if yield_type == date else date_obj.strftime(output_format_str))


def date_to_str(
    date_obj: DateType,
    in_format_str: str = DATE_FORMAT_STR,
    out_format_str: str = DATE_FORMAT_STR,
) -> str:
    """Return a `str` in `date_format_str` of `date_obj`.

    Parameters
    ----------
    date_obj
        A `datetime.date` or `str` object to convert.
    in_format_str
        A `strftime` format `str` to convert `date_obj` from if `date_obj` is a `str`.
    out_format_str
        A `strftime` format `str` to convert `date_obj` to.

    Returns
    -------
    :
        A `str` version of `date_obj` in `out_format_str` format.

    Examples
    --------
    >>> date_to_str('20100101')
    '20100101'
    >>> date_to_str(date(2010, 1, 1))
    '20100101'

    """
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, in_format_str).date()
    return date_obj.strftime(out_format_str)


def date_range_to_str(
    start_date: DateType,
    end_date: DateType,
    split_str: str = DATE_FORMAT_SPLIT_STR,
    in_format_str: str = DATE_FORMAT_STR,
    out_format_str: str = DATE_FORMAT_STR,
    include_end_date: bool = True,
) -> str:
    """Take `start_date` and `end_date` and return a date range `str`.

    Parameters
    ----------
    start_date
        First date in range.
    end_date
        Last date in range
    split_str
        `char` to split returned date range `str`.
    in_format_str
        A `strftime` format `str` to convert `start_date` from.
    out_format_str
        A `strftime` format `str` to convert `end_date` from.

    Returns
    -------
    :
        A `str` of date range from `start_date` to `end_date` in the
        `out_format_str` format.

    Examples
    --------
    >>> date_range_to_str('20100101', '20100330')
    '20100101-20100330'
    >>> date_range_to_str(date(2010, 1, 1), '20100330')
    '20100101-20100330'
    >>> date_range_to_str(date(2010, 1, 1), '20100330', include_end_date=False)
    '20100101-20100329'
    """
    start_date = date_to_str(
        start_date, in_format_str=in_format_str, out_format_str=out_format_str
    )
    if not include_end_date:
        end_date = ensure_date(end_date) - timedelta(days=1)
    end_date = date_to_str(
        end_date, in_format_str=in_format_str, out_format_str=out_format_str
    )
    return f"{start_date}{split_str}{end_date}"


def iter_to_tuple_strs(
    iter_var: Iterable[Any], func: Callable[[Any], str] = str
) -> tuple[str, ...]:
    """Return a `tuple` with all components converted to `strs`.

    Parameters
    ----------
    iter_var
        Iterable of objects that can be converted into `strs`.
    func
        Callable to convert each `obj` in `iter_val` to a `str`.

    Returns
    -------
    :
        A `tuple` of all `iter_var` elements in `str` format.

    Examples
    --------
    >>> iter_to_tuple_strs(['cat', 1, Path('a/path')])
    ('cat', '1', 'a/path')
    >>> iter_to_tuple_strs(
    ...     ['cat', 1, Path('a/path')],
    ...     lambda x: f'{x:02}' if type(x) == int else str(x))
    ('cat', '01', 'a/path')

    """
    return tuple(func(obj) for obj in iter_var)


def path_iterdir(
    path: PathLike, strict: bool = False
) -> Generator[Optional[Path], None, None]:
    """Return an `Generator` after ensuring `path` exists.

    Parameters
    ----------
    path
        `Path` to folder to iterate through.
    strict
        Whether to raise `FileNotFoundError` if `path` not found.

    #Yields
    #------
    #A `Path` for each folder  in `path`.

    Raises
    ------
    FileNotFoundError
        Raised if `strict = True` and `path` does not exist.

    Returns
    -------
    :
        `None` if `FileNotFoundError` error and `strict` is `False`.

    Examples
    --------
    >>> tmp_path = getfixture('tmp_path')
    >>> from os import chdir
    >>> chdir(tmp_path)
    >>> example_path: Path = Path('a/test/path')
    >>> example_path.exists()
    False
    >>> tuple(path_iterdir(example_path.parent))
    ()
    >>> tuple(path_iterdir(example_path.parent, strict=True))
    Traceback (most recent call last):
        ...
    FileNotFoundError: [Errno 2] No such file or directory: 'a/test'
    >>> example_path.parent.mkdir(parents=True)
    >>> example_path.touch()
    >>> tuple(path_iterdir(example_path.parent))
    (PosixPath('a/test/path'),)
    >>> example_path.unlink()
    >>> tuple(path_iterdir(example_path.parent))
    ()
    """
    try:
        yield from Path(path).iterdir()
    except FileNotFoundError as error:
        if strict:
            raise error
        else:
            return


def kwargs_to_cli_str(space_prefix: bool = True, **kwargs) -> str:
    """Convert `kwargs` into a `cli` `str`.

    Parameters
    ----------
    kwargs
        `key=val` parameters to concatenate as `str`.

    Returns
    -------
    :
        A final `str` of concatenated `**kwargs` in
        command line form.

    Examples
    --------
    >>> kwargs_to_cli_str(cat=4, in_a="hat", fun=False)
    ' --cat 4 --in-a hat --not-fun'
    >>> kwargs_to_cli_str(space_prefix=False, cat=4, fun=True)
    '--cat 4 --fun'
    >>> kwargs_to_cli_str()
    ''
    """
    if kwargs:
        cmd_str: str = " ".join(
            f"{'--' + key.replace('_', '-')} {val}"
            if type(val) != bool
            else f"{'--' + key if val else '--not-' + key}"
            for key, val in kwargs.items()
        )
        return cmd_str if not space_prefix else " " + cmd_str
    else:
        return ""


def set_and_pop_attr_kwargs(instance: Any, **kwargs) -> dict[str, Any]:
    """Extract any `key: val` pairs from `kwargs` to modify `instance`.

    Parameters
    ----------
    instance
        An object to modify.
    kwargs
        `key`: `val` parameters to potentially modify `instance` attributes.

    Returns
    -------
    :
        Any remaining `kwargs` not used to modify `instance`.

    Examples
    --------
    >>> kwrgs = set_and_pop_attr_kwargs(
    ...    conda_lock_file_manager, env_paths=['pyproject.toml'], cat=4)
    >>> conda_lock_file_manager.env_paths
    ['pyproject.toml']
    >>> kwrgs
    {'cat': 4}
    """
    kwargs_copy = deepcopy(kwargs)
    for key, val in kwargs.items():
        if hasattr(instance, key):
            logger.debug(f"Changing '{key}' to '{val}'")
            setattr(instance, key, val)
            kwargs_copy.pop(key)  # This should eliminate all `kwargs` for `instance`
    return kwargs_copy


def annual_data_paths(
    start_year: int = 1980,
    end_year: int = 1986,
    month_day: MonthDay | tuple[int, int] | None = DEFAULT_START_MONTH_DAY,
    include_end_date: bool = False,
    parent_path: Path | None = None,
    file_name_middle_str: str = CPM_FILE_NAME_MIDDLE_STR,
    file_name_extension: str = NETCDF_EXTENSION,
    data_type: str = MAX_TEMP_STR,
    make_paths: bool = False,
) -> Iterator[Path]:
    """Yield `Path` of annual data files.

    Examples
    --------
    >>> pprint(tuple(annual_data_paths()))
    (PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19811201-19821130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19821201-19831130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19831201-19841130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19841201-19851130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19851201-19861130.nc'))
    >>> parent_path_example: Iterator[Path] = annual_data_paths(
    ...     parent_path=Path('test/path'))
    >>> str(next(parent_path_example))
    'test/path/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc'
    """
    if not month_day:
        month_day = MonthDay()
    elif not isinstance(month_day, MonthDay):
        month_day = MonthDay(*month_day)
    for year in range(start_year, end_year):
        date_range_str: str = month_day.from_year_range_to_str(
            year, year + 1, include_end_date=include_end_date
        )
        file_name: str = (
            f"{data_type}{file_name_middle_str}{date_range_str}.{file_name_extension}"
        )
        if parent_path:
            if make_paths:
                parent_path.mkdir(exist_ok=True, parents=True)
            yield parent_path / file_name
        else:
            yield Path(file_name)


def csv_reader(path: PathLike, **kwargs) -> Iterator[dict[str, str]]:
    """Yield a `dict` per row from a `CSV` file at `path`.

    Parameters
    ----------
    path
        `CSV` file `Path`.
    **kwargs
        Additional parameters for `csv.DictReader`.

    #Yields
    #------
    #A `dict` per row from `CSV` file at `path`.

    Examples
    --------
    >>> import csv
    >>> csv_path: Path = TEST_AUTH_CSV_PATH
    >>> auth_dict: dict[str, str] = {
    ...    'sally': 'fig*new£kid',
    ...    'george': 'tee&iguana*sky',
    ...    'susan': 'history!bill-walk',}
    >>> field_names: tuple[str, str] = ('user_name', 'password')
    >>> with open(csv_path, 'w') as csv_file:
    ...     writer = csv.writer(csv_file)
    ...     line_num: int = writer.writerow(('user_name', 'password'))
    ...     for user_name, password in auth_dict.items():
    ...         line_num = writer.writerow((user_name, password))
    >>> tuple(csv_reader(csv_path))
    ({'user_name': 'sally', 'password': 'fig*new£kid'},
     {'user_name': 'george', 'password': 'tee&iguana*sky'},
     {'user_name': 'susan', 'password': 'history!bill-walk'})
    """
    with open(path) as csv_file:
        for row in DictReader(csv_file, **kwargs):
            yield row


# Below requires packages outside python standard library
# Note: `rioxarray` is imported to ensure GIS methods are included. See:
# https://corteva.github.io/rioxarray/stable/getting_started/getting_started.html#rio-accessor
try:
    import rioxarray  # nopycln: import
    from numpy import array, random
    from pandas import to_datetime
    from xarray import DataArray, Dataset

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

except ImportError:
    # This allows the file to be imported without any packages installed
    pass
