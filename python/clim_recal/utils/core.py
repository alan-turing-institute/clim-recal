"""Utility functions."""

import json
import sys
import warnings
from calendar import monthrange
from collections.abc import KeysView
from csv import DictReader
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from itertools import product, repeat
from logging import getLogger
from multiprocessing import Pool
from os import PathLike, chdir, cpu_count
from pathlib import Path
from typing import (
    Any,
    Callable,
    Final,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Union,
)

from rich.console import Console
from rich.progress import track
from tqdm import TqdmExperimentalWarning

logger = getLogger(__name__)

console = Console()

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)

DateType = Union[date, str]
DateRange = tuple[DateType, DateType]
CLI_DATE_FORMAT_STR: Final[str] = "%Y%m%d"
ISO_DATE_FORMAT_STR: Final[str] = "%Y-%m-%d"
YEAR_DATE_FORMAT_STR: Final[str] = "%Y"
YEAR_MONTH_DATE_FORMAT_STR: Final[str] = "%Y-%m"
CLI_DATE_FORMATS: Final[tuple[str, ...]] = (
    YEAR_DATE_FORMAT_STR,
    YEAR_MONTH_DATE_FORMAT_STR,
    ISO_DATE_FORMAT_STR,
    CLI_DATE_FORMAT_STR,
)
DATE_FORMAT_SPLIT_STR: Final[str] = "-"

NORMAL_YEAR_DAYS: Final[int] = 365
LEAP_YEAR_DAYS: Final[int] = NORMAL_YEAR_DAYS + 1
CPM_YEAR_DAYS: Final[int] = 360

CPM_FILE_NAME_MIDDLE_STR: Final[str] = "_rcp85_land-cpm_uk_2.2km_01_day_"
NETCDF_EXTENSION: Final[str] = "nc"
MAX_TEMP_STR: Final[str] = "tasmax"

MODULE_NAMES: Final[tuple[PathLike, ...]] = ("clim_recal",)
CURRENT_PATH = Path().absolute()
PYTHON_PACKAGE_DIR_NAME: Final[Path] = Path("python")
RUN_TIME_STAMP_FORMAT: Final[str] = "%y-%m-%d_%H-%M"

DEBIAN_MOUNT_PATH: Final[Path] = Path("/mnt/vmfileshare")
DARWIN_MOUNT_PATH: Final[Path] = Path("/Volumes/vmfileshare")
CLIMATE_DATA_PATH: Final[Path] = Path("ClimateData")

DEFAULT_CALLABLE_ATTR_NAME: Final[str] = "execute"


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
        """Return a `date` or `str` of date given `year`."""
        year_date: date = date(int(year), self.month, self.day)
        return year_date.strftime(self.format_str) if as_str else year_date

    def from_year_range_to_str(
        self,
        start_year: int | str,
        end_year: int | str,
        split_str: str = DATE_FORMAT_SPLIT_STR,
        in_format_str: str = CLI_DATE_FORMAT_STR,
        out_format_str: str = CLI_DATE_FORMAT_STR,
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


DEFAULT_CPM_START_MONTH_DAY: Final[MonthDay] = MonthDay(month=12, day=1)


def range_len(
    maximum: int, start: int = 0, stop: int | None = None, step: int = 1
) -> int:
    """Cacluate the total length of range with indexing.

    Parameters
    ----------
    maximum
        Maximum range length.
    start
        Index to start from.
    stop
        Index to stop at.
    step
        Steps between `start` and `stop` indexes

    Examples
    --------
    >>> range_len(100)
    100
    >>> range_len(100, 90)
    10
    >>> range_len(100, 20, 30)
    10
    >>> range_len(100, 20, 30, 2)
    5
    """
    stop = stop or maximum
    return (stop - start - 1) // step + 1


def run_callable_attr(
    instance: object, *args, method_name: str = DEFAULT_CALLABLE_ATTR_NAME, **kwargs
) -> Any:
    """Extract `method_name` from `instance` to call.

    Parameters
    ----------
    instance
        `object` to call `method_name` from.
    *args
        Parameters passed to `method_name` from `instance`.
    method_name
        Name of method on `object` to call.
    **kwargs
        Parameters passed to `method_name` from `instance`.

    Notes
    -----
    This is primarily meant to address issues with `pickle`, particularly
    when using `multiprocessing` and `lambda` functions yield `pickle` errors.

    Examples
    --------
    >>> jan_1: MonthDay = MonthDay()
    >>> run_callable_attr(jan_1, 1984, method_name='from_year')
    datetime.date(1984, 1, 1)
    """
    if isinstance(args[-1], dict) and "method_name" in args[-1]:
        logger.debug(
            f"Replacing 'method_name' param '{method_name}' "
            f"with '{args[-1]["method_name"]}'."
        )
        kwargs |= args[-1]
        args = args[:-1]
        method_name = kwargs.pop("method_name")
    method: Callable = getattr(instance, method_name)
    return method(*args, **kwargs)


def multiprocess_execute(
    iter: Sequence,
    *args: Any,
    method_name: str = DEFAULT_CALLABLE_ATTR_NAME,
    progress_bar: bool = False,
    bar_name: str = "",
    cpus: int | None = None,
    include_sub_process_config: bool = False,
    sub_process_progress_bar: bool = False,
    **kwargs: Any,
) -> list:
    """Run `method_name` as from `iter` via `multiprocessing`.

    Parameters
    ----------
    iter
        `Sequence` of instances to iterate over calling `method_name` from.
    *args
        Args to pass to `method_name`
    method_name
        What to call from objects in `inter`.
    progress_bar
        Whether to render a progress bar.
    bar_name
        Passed to `description` parameter for progress bar.
    cpus
        Number of cpus to pass to `Pool` for multiprocessing.
    include_sub_process_config
        Include configurations specific to multiprocessess
        (eg. `progress_bar` config)
    sub_process_progress_bar
        Whether to include `progress_bar` within each `multiprocess` run.
    **kwargs
        Args to pass to `method_name`

    Examples
    --------
    >>> test_strs: tuple[tuple[str], ...] = (('the third'), ('man'))
    >>> multiprocess_execute(test_strs, method_name="split")
    [['the', 'third'], ['man']]
    >>> test_strs = (('lumbar'), ('puncture'))
    >>> multiprocess_execute(test_strs, 'u', method_name="split")
    [['l', 'mbar'], ['p', 'nct', 're']]
    >>> multiprocess_execute(test_strs, 'u', method_name="split",
    ...                      progress_bar=True)
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100%...
    [['l', 'mbar'], ['p', 'nct', 're']]

    Notes
    -----
    Failed asserting cpus <= total - 1.
    """
    results: list
    total_cpus: int | None = cpu_count()
    cpus = cpus or 1
    if isinstance(total_cpus, int):
        cpus = min(cpus, total_cpus - 1)
    else:
        logger.warning(f"'total_cpus' not checkable, running with 'cpus': {cpus}")
    kwargs["method_name"] = method_name
    if include_sub_process_config:
        kwargs["progress_bar"] = sub_process_progress_bar
    param_tuples: tuple[tuple[Any, ...], ...] = (
        tuple(zip(iter, repeat(*args), repeat(kwargs)))
        if args
        else tuple(zip(iter, repeat(kwargs)))
    )

    # Had build errors when generating a wheel,
    # Followed solution here:
    # https://stackoverflow.com/questions/45720153/python-multiprocessing-error-attributeerror-module-main-has-no-attribute
    __spec__ = None
    with Pool(processes=cpus) as pool:
        if progress_bar:
            results = pool.starmap(
                run_callable_attr,
                track(param_tuples, description=bar_name),
            )
        else:
            results = pool.starmap(run_callable_attr, param_tuples)
    return results


def product_dict(**kwargs) -> Iterator[dict[Hashable, Any]]:
    """Return product combinatorics of `kwargs`.

    Examples
    --------
    >>> pprint(tuple(
    ...     product_dict(animal=['cat', 'fish'], activity=('swim', 'eat'))))
    ({'activity': 'swim', 'animal': 'cat'},
     {'activity': 'eat', 'animal': 'cat'},
     {'activity': 'swim', 'animal': 'fish'},
     {'activity': 'eat', 'animal': 'fish'})
    """
    keys: KeysView = kwargs.keys()
    for instance in product(*kwargs.values()):
        yield dict(zip(keys, instance))


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


def ensure_date(date_to_check: DateType, format_str: str = CLI_DATE_FORMAT_STR) -> date:
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


def date_strs_from_met_office_file_name(path: PathLike) -> tuple[str, ...]:
    """Extract date range from MetOffice file name.

    Examples
    --------
    >>> date_strs_from_met_office_file_name(
    ...     'test/path/'
    ...     'tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc')
    ('19801201', '19811130')
    """
    return tuple(Path(path).stem.split("_")[-1].split("-"))


def dates_path_to_date_tuple(
    path: PathLike,
    date_str_extractor: Callable[[PathLike], str] = date_strs_from_met_office_file_name,
    date_str_format: str = CLI_DATE_FORMAT_STR,
) -> tuple[date, ...]:
    """Extract date range from MetOffice file name.

    Examples
    --------
    >>> dates_path_to_date_tuple(
    ...     'test/path/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc')
    (datetime.date(1980, 12, 1), datetime.date(1981, 11, 30))

    """
    return tuple(
        ensure_date(date_str, format_str=date_str_format)
        for date_str in date_str_extractor(path)
    )


def date_range_generator(
    start_date: DateType,
    end_date: DateType,
    inclusive: bool = False,
    skip_dates: Iterable[DateType] | DateType | None = None,
    start_format_str: str = CLI_DATE_FORMAT_STR,
    end_format_str: str = CLI_DATE_FORMAT_STR,
    output_format_str: str = CLI_DATE_FORMAT_STR,
    skip_dates_format_str: str = CLI_DATE_FORMAT_STR,
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
    in_format_str: str = CLI_DATE_FORMAT_STR,
    out_format_str: str = CLI_DATE_FORMAT_STR,
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
    in_format_str: str = CLI_DATE_FORMAT_STR,
    out_format_str: str = CLI_DATE_FORMAT_STR,
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


def annual_data_paths_generator(
    start_year: int = 1980, end_year: int = 1986, **kwargs
) -> Iterator[Path]:
    """Yield `Path` of annual data files.

    Examples
    --------
    >>> pprint(tuple(annual_data_paths_generator()))
    (PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19811201-19821130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19821201-19831130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19831201-19841130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19841201-19851130.nc'),
     PosixPath('tasmax_rcp85_land-cpm_uk_2.2km_01_day_19851201-19861130.nc'))
    >>> parent_path_example: Iterator[Path] = annual_data_paths_generator(
    ...     parent_path=Path('test/path'))
    >>> str(next(parent_path_example))
    'test/path/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc'
    """
    for year in range(start_year, end_year):
        yield annual_data_path(start_year=year, end_year=year + 1, **kwargs)


def annual_data_path(
    start_year: int = 1980,
    end_year: int = 1981,
    month_day: MonthDay | tuple[int, int] | None = DEFAULT_CPM_START_MONTH_DAY,
    include_end_date: bool = False,
    parent_path: Path | None = None,
    file_name_middle_str: str = CPM_FILE_NAME_MIDDLE_STR,
    file_name_extension: str = NETCDF_EXTENSION,
    data_type: str = MAX_TEMP_STR,
    make_paths: bool = False,
) -> Path:
    """Return `Path` of annual data files.

    Examples
    --------
    >>> str(annual_data_path(parent_path=Path('test/path')))
    'test/path/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc'
    """
    if not month_day:
        month_day = MonthDay()
    elif not isinstance(month_day, MonthDay):
        month_day = MonthDay(*month_day)
    date_range_str: str = month_day.from_year_range_to_str(
        start_year=start_year, end_year=end_year, include_end_date=include_end_date
    )
    file_name: str = (
        f"{data_type}{file_name_middle_str}{date_range_str}.{file_name_extension}"
    )
    if parent_path:
        if make_paths:
            parent_path.mkdir(exist_ok=True, parents=True)
        return parent_path / file_name
    else:
        return Path(file_name)


def time_str(
    time: date | datetime | None = None,
    format: str = RUN_TIME_STAMP_FORMAT,
    replace_char_tuple: tuple[str, str] | None = None,
) -> str:
    """Return a `str` of passed or generated time suitable for a file name.

    Parameters
    ----------
    time
        Time to format. Will be set to current time if `None` is passed, and add current time if a `date` is passed to convert to a `datetime`.
    format
        `strftime` `str` format to return `time` as. If `replace_chars` is passed, these will be used to replace those in `strftime`.
    replace_char_tuple
        `tuple` of (char_to_match, char_to_replace) order.

    Examples
    --------
    >>> time_str(datetime(2024, 10, 10, 10, 10, 10))
    '24-10-10_10-10'
    """
    time = time if time else datetime.now()
    # `isinstance` would fail because `date` is a
    # subclass of `datetime`
    if type(time) is date:
        time = datetime.combine(time, datetime.now().time())
    if replace_char_tuple:
        format = format.replace(*replace_char_tuple)
    return time.strftime(format)


def results_path(
    name: str,
    path: PathLike | None = None,
    time: datetime | None = None,
    extension: str | None = None,
    mkdir: bool = False,
    dot_pre_extension: bool = True,
) -> Path:
    """Return `Path`: `path`/`name`_`time`.`extension`.

    Examples
    --------
    >>> temp_path = getfixture('tmpdir')
    >>> path: Path = results_path('hads', path='temp_path/folder',
    ...                           extension='cat')
    >>> str(path)
    'temp_path/folder/hads_..._...-....cat'
    >>> path.exists()
    False
    >>> path = results_path('run', path='temp_path/other-folder', extension='log', mkdir=True)
    >>> str(path)
    'temp_path/other-folder/run_..._...-....log'
    >>> path.parent.is_dir()
    True
    >>> path.is_file()
    False
    >>> path.touch()
    >>> path.is_file()
    True
    """
    path = Path() if path is None else Path(path)
    if mkdir:
        path.mkdir(parents=True, exist_ok=True)
    if not time:
        time = datetime.now()
    file_name: str = f"{name}_{time_str(time)}"
    if extension:
        if dot_pre_extension:
            file_name += f".{extension}"
        else:
            file_name += extension
    return path / file_name


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


def climate_data_mount_path(
    is_darwin: bool | None = None, full_path: bool = True
) -> Path:
    """Return likely climate data mount path based on operating system.

    Parameters
    ----------
    is_darwin
        Whether to use `CLIMATE_DATA_MOUNT_PATH_DARWIN` or
        call `is_platform_darwin` if None. fixture `is_platform_darwin`.

    Returns
    -------
    The `Path` climate data would likely be mounted to.
    """
    path: Path
    if is_darwin is None:
        is_darwin = is_platform_darwin()
    if is_darwin:
        path = DARWIN_MOUNT_PATH
    else:
        path = DEBIAN_MOUNT_PATH
    if full_path:
        return path / CLIMATE_DATA_PATH
    else:
        return path


def _get_source_path(
    instance, index: int, source_to_index: Sequence | None = None
) -> Path:
    """Return a path indexed from `source_to_index` or `self`."""
    if source_to_index is None:
        return instance[index]
    elif isinstance(source_to_index, str):
        return getattr(instance, source_to_index)[index]
    else:
        return source_to_index[index]


def is_climate_data_mounted(mount_path: PathLike | None = None) -> bool:
    """Check if `CLIMATE_DATA_MOUNT_PATH` is mounted.

    Notes
    -----
    Consider refactoring to `climate_data_mount_path` returns `None`
    when conditions here return `False`.
    """
    if not mount_path:
        mount_path = climate_data_mount_path()
    assert isinstance(mount_path, Path)
    return mount_path.exists()


class StrEnumReprName(StrEnum):
    def __repr__(self) -> str:
        """Return value as `str`."""
        return f"'{self.value}'"


def ensure_all_attr_types(
    instance, types: Sequence[type], converter: Callable[[Any], Any], **kwargs
) -> Any:
    """Check all `instance` attributes of `types` are converted.

    Parameters
    ----------
    instance
        `object` to check `date` attributes of.
    types
        Attribute types to check and apply `converter` to.

    Examples
    --------
    >>> from ..config import ClimRecalConfig
    >>> clim_runner: ClimRecalConfig = getfixture('clim_runner')
    >>> clim_runner.hads_start_date = str(clim_runner.hads_start_date)
    >>> clim_runner.hads_start_date
    '1980-01-01'
    >>> clim_runner = ensure_all_attr_types(
    ...     clim_runner, types=(date | None,),
    ...     converter=ensure_date, format_str=ISO_DATE_FORMAT_STR)
    >>> clim_runner.hads_start_date
    datetime.date(1980, 1, 1)
    """
    for instance_field in fields(instance):
        if instance_field.type in types:
            value: Any = getattr(instance, instance_field.name)
            if value:
                setattr(instance, instance_field.name, converter(value, **kwargs))
    return instance


def ensure_all_attr_dates(
    instance,
    date_types: tuple[type, ...] = (date, date | None, date | str),
    format_str: str = ISO_DATE_FORMAT_STR,
) -> Any:
    """Check all `date` attributes are converted to `date`.

    Parameters
    ----------
    instance
        `object` to check `date` attributes of.
    date_types
        Attribute types to check are `date` instances.
    format_str
        `str` format to convert dates.

    Examples
    --------
    >>> from ..config import ClimRecalConfig
    >>> clim_runner: ClimRecalConfig = getfixture('clim_runner')
    >>> clim_runner.hads_start_date = str(clim_runner.hads_start_date)
    >>> clim_runner.hads_start_date
    '1980-01-01'
    >>> clim_runner = ensure_all_attr_dates(clim_runner)
    >>> clim_runner.hads_start_date
    datetime.date(1980, 1, 1)
    """
    return ensure_all_attr_types(
        instance=instance,
        types=date_types,
        converter=ensure_date,
        format_str=format_str,
    )


def ensure_all_attr_paths(
    instance, path_types: tuple[type, ...] = (PathLike, Path, PathLike | None)
) -> Any:
    """Check all `PathLike` attributes are converted to `Path`.

    Parameters
    ----------
    instance
        `object` to check `Path` attributes of.
    path_types
        What attributes on `object` to ensure are `Path` instances if not
        `None`.

    Examples
    --------
    >>> from ..config import ClimRecalConfig
    >>> clim_runner: ClimRecalConfig = getfixture('clim_runner')
    >>> clim_runner.output_path = str(clim_runner.output_path)
    >>> clim_runner.output_path
    <BLANKLINE>
    '.../test-run-results...'
    >>> clim_runner = ensure_all_attr_paths(clim_runner)
    >>> clim_runner.output_path
    <BLANKLINE>
    ...Path('.../test-run-results...')
    """
    return ensure_all_attr_types(instance=instance, types=path_types, converter=Path)


def ensure_attr_tuples(
    instance,
    sequence_type_strs: tuple[str, ...] = ("Sequence", "tuple"),
) -> Any:
    """Ensure attributes of `instance` set to `tuples`.

    Parameters
    ----------
    instance
        `object` to check `tuple` attributes of.
    sequence_type_strs
        `str` of types to check `attrs` of `instance` of. Using `str` of
        `type` rather than `type` instances to be more flexible.

    Examples
    --------
    >>> from ..config import ClimRecalConfig
    >>> clim_runner: ClimRecalConfig = getfixture('clim_runner')
    >>> clim_runner.regions = list(clim_runner.regions)
    >>> clim_runner.regions
    ['Glasgow', 'Manchester']
    >>> clim_runner = ensure_attr_tuples(clim_runner)
    >>> clim_runner.regions
    ('Glasgow', 'Manchester')
    """
    for var_field in fields(instance):
        if any(class_str in str(var_field.type) for class_str in sequence_type_strs):
            setattr(instance, var_field.name, tuple(getattr(instance, var_field.name)))
    return instance


def read_json_config(
    path: PathLike,
    constructor: Any,
    seq_to_tuples: bool = True,
    ensure_all_dates: bool = True,
    date_format_str: str = ISO_DATE_FORMAT_STR,
    ensure_all_paths: bool = True,
) -> Any:
    """Create an instance of `constructor` from `path`.

    Parameters
    ----------
    path
        `Path` to read `json` format from.
    constructor
        `Class` to pass parameters from `path` `json` as `**kwargs`.
    seq_to_tuples
        Whether to pass new instance to `ensure_attr_tuples` to convert
        `Sequence` typed attributes to `tuples`.
    ensure_all_dates
        Whether to check and convert `date` typed attributes from `str` to
        `date` objects using `date_format_str`.
    date_format_str
        `date` `strftime` format for `ensure_all_dates`.
    ensure_all_paths
        Whether to check and convert all `Path` typed attributes to `Path`.

    Examples
    --------
    >>> from ..config import ClimRecalConfig
    >>> clim_runner: ClimRecalConfig = getfixture('clim_runner')
    >>> save_path: Path = getfixture('tmp_path') / 'config.json'
    >>> json_path: Path = clim_runner.write_config(path=save_path)
    >>> clim_config: ClimRecalConfig = read_json_config(
    ...     json_path, constructor=ClimRecalConfig)
    >>> assert clim_config == clim_runner
    """
    with open(path) as json_config_path:
        config: Any = constructor(**json.load(json_config_path))
    if seq_to_tuples:
        ensure_attr_tuples(config)
    if ensure_all_paths:
        ensure_all_attr_paths(config)
    if ensure_all_dates:
        ensure_all_attr_dates(config, format_str=date_format_str)
    return config


def save_config(
    instance: Any,
    path: PathLike | None = None,
    indent: int = 2,
    save_path_attr_name: str = "exec_path",
    **kwargs,
) -> Path:
    """Save `dataclass` instance config to `path` and return saved path.

    Parameters
    ----------
    instance
        Instance to save in `json` format.
    path
        `Path` to save instance `json` to. If `None` a `path` is
        generated from calling the `save_path_attr_name` on `instance`.
    indent
        How many spaces to indent generated `json`.
    **kwargs
        Any additional parameters to pass to `json.dump`.

    Examples
    --------
    >>> clim_runner = getfixture('clim_runner')
    >>> save_path: Path = getfixture('tmp_path') / 'save_config_test.json'
    >>> json_path: Path = save_config(clim_runner, path=save_path)
    >>> save_path == json_path
    True
    >>> with open(save_path) as source:
    ...     loaded_config = json.load(source)
    >>> loaded_config['regions']
    ['Glasgow', 'Manchester']
    """
    try:
        assert is_dataclass(instance)
    except AssertionError:
        raise ValueError(f"'{instance.__class__.__name__}' is not a dataclass.")
    path = path or results_path(
        name="config",
        path=getattr(instance, save_path_attr_name),
        extension="json",
        mkdir=True,
    )
    with open(path, "w") as write_path:
        json.dump(instance.to_dict(), write_path, indent=indent, **kwargs)
    return path


def dataclass_to_dict(instance, force_serialise: bool = True) -> dict[str, Any]:
    """Convert `dataclass` instance to a `dict`.

    Examples
    --------
    >>> from clim_recal.config import ClimRecalConfig, VariableOptions
    >>> from clim_recal.cli import cli
    >>> clim_runner: ClimRecalConfig = getfixture('clim_runner')
    >>> config_from_dict: ClimRecalConfig  = ClimRecalConfig(
    ...     **dataclass_to_dict(clim_runner, force_serialise=False))
    >>> clim_runner == config_from_dict
    True
    >>> config_from_dict.variables = VariableOptions.all()
    >>> clim_runner == config_from_dict
    False
    >>> from tests.utils import compare_dataclass_instances
    >>> compare_dataclass_instances(clim_runner, config_from_dict)
    {'variables': (('tasmax',), ('tasmax', 'rainfall', 'tasmin'))}
    """
    try:
        assert is_dataclass(instance)
    except AssertionError:
        raise ValueError(f"'{instance.__class__.__name__}' is not a dataclass.")
    fields_to_set_to_none: Sequence[str] = []
    if hasattr(instance, "_skip_json_serialise"):
        fields_to_set_to_none = instance._skip_json_serialise
    if force_serialise:
        serialiser: dict[str, Any] = asdict(instance)
        for name, value in serialiser.items():
            if isinstance(value, Path | date):
                serialiser[name] = str(value)
            if name in fields_to_set_to_none:
                serialiser[name] = None
        return serialiser
    else:
        return asdict(instance)


def infer_end_date(
    date_obj: date, check_month: bool = False, check_day: bool = False
) -> date:
    """Infer the last of a date range.

    Parameters
    ----------
    date_obj
        Date object to infer latest date from.
    check_month
        Whether to check latest month within year.
    check_day
        Whether to check latest day within month.

    Examples
    --------
    >>> infer_end_date(date(2024, 11, 5), check_month=True, check_day=True)
    datetime.date(2024, 11, 5)
    >>> infer_end_date(date(2024, 11, 1), check_month=True)
    datetime.date(2024, 11, 1)
    >>> infer_end_date(date(2024, 11, 1), check_day=True)
    datetime.date(2024, 11, 30)
    >>> infer_end_date(date(2024, 1, 1), check_month=True)
    datetime.date(2024, 12, 1)
    >>> infer_end_date(date(2024, 1, 1), check_month=True, check_day=True)
    datetime.date(2024, 12, 31)
    """
    if check_month and date_obj.month == 1 and date_obj.day == 1:
        date_obj = date(year=date_obj.year, month=12, day=date_obj.day)
    if check_day and date_obj.day == 1:
        day: int = monthrange(date_obj.year, date_obj.month)[1]
        date_obj = date(year=date_obj.year, month=date_obj.month, day=day)
    return date_obj


def date_str_infer_end(
    date_or_str: str | datetime | date, formats: Sequence[str] = CLI_DATE_FORMATS
) -> date | None:
    """Infer latest date from `date_str`.

    Parameters
    ----------
    date_or_str
        Instance to infer potential latest implied date from.
    formats
        `strptime` formats to check for date parsing.

    Examples
    --------
    >>> date_str_infer_end(None)
    >>> date_str_infer_end("")
    >>> date_str_infer_end(datetime(2021, 12, 31, 12, 0))
    datetime.date(2021, 12, 31)
    >>> date_str_infer_end("2024-11-5")
    datetime.date(2024, 11, 5)
    >>> date_str_infer_end("2024-11-1")
    datetime.date(2024, 11, 1)
    >>> date_str_infer_end("2024-1-1")
    datetime.date(2024, 1, 1)
    >>> date_str_infer_end("2024")
    datetime.date(2024, 12, 31)
    >>> date_str_infer_end("2024-1")
    datetime.date(2024, 1, 31)
    >>> date_str_infer_end("2024-11")
    datetime.date(2024, 11, 30)
    >>> date_str_infer_end("2024-11-")
    Traceback (most recent call last):
        ...
    ValueError: 'date_or_str': '2024-11-' doesn't match
    'formats': ('%Y', '%Y-%m', '%Y-%m-%d', '%Y%m%d')
    """
    if not date_or_str:
        return None
    elif isinstance(date_or_str, datetime):
        return date_or_str.date()
    elif isinstance(date_or_str, date):
        return date_or_str
    elif YEAR_DATE_FORMAT_STR in formats and len(date_or_str) == 4:
        return infer_end_date(
            datetime.strptime(date_or_str, YEAR_DATE_FORMAT_STR).date(),
            check_month=True,
            check_day=True,
        )
    elif YEAR_MONTH_DATE_FORMAT_STR in formats and 6 <= len(date_or_str) <= 7:
        return infer_end_date(
            datetime.strptime(date_or_str, YEAR_MONTH_DATE_FORMAT_STR).date(),
            check_day=True,
        )
    else:
        for format in set(formats) - {YEAR_DATE_FORMAT_STR, YEAR_MONTH_DATE_FORMAT_STR}:
            try:
                return datetime.strptime(date_or_str, format).date()
            except ValueError:
                pass
        raise ValueError(
            f"'date_or_str': '{date_or_str}' doesn't match 'formats': {formats}"
        )


def check_parent_sub_paths(
    parent_path: PathLike | None = None,
    sub_path: PathLike | None = None,
    data_name: str | None = None,
) -> Path:
    """Check combination of `input_path` and `sub_path`.

    Parameters
    ----------
    parent_path
        Expected to be a parent path for `sub_path`.
    sub_path
        Expected to a path within `input_path`.
    data_name
        Name of data provided via `parent_path` and `sub_path`. Only used in logs.

    Examples
    --------
    >>> caplob = getfixture('caplog')
    >>> str(check_parent_sub_paths('mount/path', 'HadsUKgrid'))
    'mount/path/HadsUKgrid'
    >>> str(check_parent_sub_paths('mount/path', '/HadsUKgrid'))
    '/HadsUKgrid'
    >>> str(check_parent_sub_paths('mount/path'))
    'mount/path'
    >>> str(check_parent_sub_paths(None, 'HadsUKgrid'))
    'HadsUKgrid'
    >>> str(check_parent_sub_paths())
    '.'
    """
    final_path: Path
    parent_path = Path(parent_path) if parent_path else parent_path
    sub_path = Path(sub_path) if sub_path else sub_path
    data_name_info_text: str = " for " + data_name if data_name else ""
    if parent_path:
        if sub_path:
            if sub_path.is_absolute():
                logger.info(
                    f"Overriding 'parent_path': '{parent_path}' with absolute: '{sub_path}'"
                )
                final_path = sub_path
            else:
                final_path = parent_path / sub_path
        else:
            logger.debug(
                f"No 'sub_path'{data_name_info_text}. Using 'parent_path': {parent_path}"
            )
            final_path = parent_path
    else:
        if sub_path:
            logger.info(f"No 'parent_path', using '{sub_path}'{data_name_info_text}.")
            final_path = sub_path
        else:
            logger.info(f"Neither 'parent_path' nor 'sub_path'{data_name_info_text}.")
            final_path = Path()
    return final_path


def resolve_parent_sub_paths(
    parent_path: PathLike, paths_dict: dict[str, PathLike]
) -> dict[str, Path]:
    """
    Return `check_parent_sub_paths` applied to `paths_dict` with `parent_path`.

    Parameters
    ----------
    parent_path
        `Path` meant to serve as default parent to `paths_dict` values.
    paths_dict
        Label to `Path` intended to by default be subpaths of `parent_path`.

    Examples
    --------
    >>> caplob = getfixture('caplog')
    >>> pprint(resolve_parent_sub_paths(
    ...     'mount/path', {'hads': 'HadsUKgrid', 'cpm': 'UKCP2.2'}))
    {'cpm': ...Path('mount/path/UKCP2.2'),
     'hads': ...Path('mount/path/HadsUKgrid')}
    >>> pprint(resolve_parent_sub_paths(
    ...     'mount/path', {'hads': '/HadsUKgrid', 'cpm': ''}))
    {'cpm': ...Path('mount/path'),
     'hads': ...Path('/HadsUKgrid')}
    """
    return {
        label: check_parent_sub_paths(parent_path, path, label)
        for label, path in paths_dict.items()
    }
