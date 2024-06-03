import asyncio
from collections import UserDict
from dataclasses import dataclass, field
from datetime import date, datetime
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import Awaitable, Final, Iterable, Sequence, TypedDict

import sysrsync
from numpy import array, random
from pandas import date_range, to_datetime
from xarray import DataArray
from xarray.core.types import T_DataArray, T_DataArrayOrSet

from clim_recal.debiasing.debias_wrapper import (
    CALIB_DATES_STR_DEFAULT,
    CMETHODS_FILE_NAME,
    CMETHODS_OUT_FOLDER_DEFAULT,
    DATA_PATH_DEFAULT,
    MOD_FOLDER_DEFAULT,
    OBS_FOLDER_DEFAULT,
    PREPROCESS_FILE_NAME,
    PREPROCESS_OUT_FOLDER_DEFAULT,
    PROCESSORS_DEFAULT,
    VALID_DATES_STR_DEFAULT,
    CityOptions,
    MethodOptions,
    RunOptions,
    VariableOptions,
)
from clim_recal.utils.core import (
    CPM_YEAR_DAYS,
    ISO_DATE_FORMAT_STR,
    LEAP_YEAR_DAYS,
    NORMAL_YEAR_DAYS,
    DateType,
    date_range_generator,
    iter_to_tuple_strs,
)
from clim_recal.utils.xarray import THREE_CITY_CENTRE_COORDS

logger = getLogger(__name__)

XARRAY_EXAMPLE_RANDOM_SEED: Final[int] = 0
# Default 4 year start and end date covering leap year
XARRAY_EXAMPLE_START_DATE_STR: Final[str] = "1980-11-30"
XARRAY_EXAMPLE_END_DATE_4_YEARS: Final[str] = "1984-11-30"

XARRAY_END_DATE_4_DAYS: Final[str] = "1980-12-5"
XARRAY_END_DATE_8_DAYS: Final[str] = "1980-12-10"
XARRAY_SKIP_2_FROM_8_DAYS: Final[tuple[str, str]] = (
    "1980-12-7",
    "1980-12-8",
)
TEST_AUTH_CSV_FILE_NAME: Final[Path] = Path("test_auth.csv")

CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT: Final[tuple[str | Path, ...]] = (
    "python",
    PREPROCESS_FILE_NAME,
    "--mod",
    DATA_PATH_DEFAULT / MOD_FOLDER_DEFAULT / CityOptions.default(),
    "--obs",
    DATA_PATH_DEFAULT / OBS_FOLDER_DEFAULT / CityOptions.default(),
    "-v",
    VariableOptions.default(),
    "-r",
    RunOptions.default(),
    "--out",
    (
        DATA_PATH_DEFAULT
        / PREPROCESS_OUT_FOLDER_DEFAULT
        / CityOptions.default()
        / RunOptions.default()
        / VariableOptions.default()
    ),
    "--calib_dates",
    CALIB_DATES_STR_DEFAULT,
    "--valid_dates",
    VALID_DATES_STR_DEFAULT,
)

CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT: Final[
    tuple[str, ...]
] = iter_to_tuple_strs(CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT)

CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT: Final[str] = " ".join(
    CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
)

CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT: Final[tuple[str | Path | int, ...]] = (
    "python",
    CMETHODS_FILE_NAME,
    "--input_data_folder",
    CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT[11],
    "--out",
    (
        DATA_PATH_DEFAULT
        / CMETHODS_OUT_FOLDER_DEFAULT
        / CityOptions.default()
        / RunOptions.default()
    ).resolve(),
    "--method",
    MethodOptions.default(),
    "-v",
    VariableOptions.default(),
    "-p",
    PROCESSORS_DEFAULT,
)

CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT: Final[
    tuple[str, ...]
] = iter_to_tuple_strs(CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT)

CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT: Final[str] = " ".join(
    CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
)

MOD_FOLDER_FILES_COUNT_CORRECT: Final[int] = 1478
OBS_FOLDER_FILES_COUNT_CORRECT: Final[int] = MOD_FOLDER_FILES_COUNT_CORRECT
PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT: Final[int] = 4


class StandardWith360DayError(Exception):
    ...


def year_days_count(
    standard_years: int = 0,
    leap_years: int = 0,
    xarray_360_day_years: int = 0,
    strict: bool = True,
) -> int:
    """Return the number of days for the combination of year lengths.

    Parameters
    ----------
    standard_years
        Count of 365 day years.
    leap_years
        Count of 366 day years.
    xarray_360_day_years
        Count of 360 day years following xarray's specification.
    strict
        Whether to prevent combining `standard_years` or `leap_years`
        with `xarray_360_day_years`.

    Returns
    -------
    Sum of all year type counts

    Examples
    --------
    >>> year_days_count(standard_years=4) == NORMAL_YEAR_DAYS*4 == 365*4
    True
    >>> year_days_count(xarray_360_day_years=4) == CPM_YEAR_DAYS*4 == 360*4
    True
    >>> (year_days_count(standard_years=3, leap_years=1)
    ...  == NORMAL_YEAR_DAYS*3 + LEAP_YEAR_DAYS
    ...  == 365*3 + 366)
    True
    """
    if strict and (standard_years or leap_years) and xarray_360_day_years:
        raise StandardWith360DayError(
            f"With 'strict == True', "
            f"{standard_years} standard (365 day) years and/or "
            f"{leap_years} leap (366 day) years "
            f"cannot be combined with "
            f"xarray_360_day_years ({xarray_360_day_years})."
        )
    return (
        standard_years * NORMAL_YEAR_DAYS
        + leap_years * LEAP_YEAR_DAYS
        + xarray_360_day_years * CPM_YEAR_DAYS
    )


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
) -> T_DataArrayOrSet:
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
    da: T_DataArray = DataArray(
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


CacheLogType = tuple[str, datetime | None, Path]
SyncedLog = TypedDict("SyncedLog", {"time": datetime | None, "path": Path})


@dataclass
class LocalCache:

    """Manager for caching files locally.

    Attributes
    ----------
    source_path
        Path to file to cache
    local_cache_folder
        Optional path to save `cache_path` relative to.
    local_cache_path
        Path to copy `source_path` to within `local_cache_folder`.
    reader
        Optional function to call to read cached file with.
    reader_kwargs
        Any parameters to pass to `reader`.

    Examples
    --------
    >>> tmp_path = getfixture('tmp_path')
    >>> cacher: LocalCache = LocalCache(
    ...     name="example-user-config",
    ...     source_path=(data_fixtures_path /
    ...                  'test_user_accounts.xlsx'),
    ...     local_cache_path=tmp_path,)
    >>> cacher
    <LocalCache(name='example-user-config', is_cached=False)>
    >>> pprint(cacher.sync())
    ('example-user-config',
     datetime.datetime(...),
     ...Path('.../test_user_accounts.xlsx'))
    >>> cacher.cache_path.is_file()
    True
    >>> cacher
    <LocalCache(name='example-user-config', is_cached=True)>
    """

    name: str
    source_path: PathLike
    local_cache_path: PathLike
    created: datetime | None = None
    synced: datetime | None = None
    _make_local_cache_folders: bool = True

    # reader: Callable | None
    # reader_kwargs: dict[str, Any] = field(default_factory=dict)
    def __repr__(self) -> str:
        """Summary of caching config."""
        return f"<LocalCache(name='{self.name}', is_cached={self.is_cached})>"

    def __post_init__(self) -> None:
        """Account `self.cache_path` consistency."""
        self.source_path = Path(self.source_path)
        self.local_cache_path = Path(self.local_cache_path)
        try:
            assert self.source_path.is_file()
        except:
            raise ValueError(f"Only files can be cached, not: '{self.source_path}'")
        if self._make_local_cache_folders:
            self.local_cache_path.mkdir(parents=True, exist_ok=True)

    @property
    def is_cached(self) -> bool:
        """Whether `self.source_path` has been cached."""
        return self.cache_path.is_file()

    @property
    def cache_path(self) -> Path:
        """Path to copy `source_path` file to."""
        assert isinstance(self.source_path, Path)
        assert isinstance(self.local_cache_path, Path)
        if not self.local_cache_path.is_file():
            logger.debug(
                f"'local_cache_path' is a folder: "
                f"'{self.local_cache_path}'. Inferring file name "
                f"from 'source_path': '{self.source_path}'."
            )
            return self.local_cache_path / self.source_path.name
        else:
            return self.local_cache_path

    @property
    def source_path_exists(self) -> bool:
        """Whether `self.source_path` is a file."""
        return Path(self.source_path).is_file()

    def _check_source_path(self, fail_if_no_source: bool = True) -> bool:
        """Check access to `self.source_path`."""
        if not self.source_path_exists:
            message: str = (
                f"No access, pehaps because server not mounted, to: "
                f"'{self.source_path}'"
            )
            if fail_if_no_source:
                raise FileNotFoundError(message)
            else:
                logger.error(message)
                return False
        else:
            return True

    def _update_synced(self) -> None:
        """Update `self.synced` and `self.created`."""
        self.synced = datetime.now()
        if not self.created:
            self.created = self.synced

    async def async_sync(
        self, fail_if_no_source: bool = True, **kwargs
    ) -> CacheLogType:
        """Asyncronously sync `self.source_path` to `self.cache_path`."""
        if self._check_source_path(fail_if_no_source=fail_if_no_source):
            await asyncio.create_subprocess_exec(
                "rsync", str(self.source_path), str(self.cache_path), **kwargs
            )
            self._update_synced()
        return self.name, self.synced, self.cache_path

    def sync(self, fail_if_no_source: bool = True, **kwargs) -> CacheLogType:
        """Sync `self.source_path` to `self.cache_path`."""
        if self._check_source_path(fail_if_no_source=fail_if_no_source):
            sysrsync.run(
                source=str(self.source_path), destination=str(self.cache_path), **kwargs
            )
            self._update_synced()
        return self.name, self.synced, self.cache_path


@dataclass
class LocalCachesManager(UserDict):

    """Manager for a set of local caches.

    Attributes
    ----------
    sources
        A `dict` of `name`: `configs` to construct `LocalCache` instances

    Examples
    --------
    >>> glasow_tif_cache = getfixture('glasgow_tif_cache')
    >>> cache_configs = LocalCachesManager([glasow_tif_cache])
    >>> cache_configs
    <LocalCachesManager(count=1)>
    >>> asyncio.run(cache_configs.async_sync_all())
    (...Path('.../test-local-cache/test_user_accounts.xlsx'),)
    >>> 'test-users' in cache_configs._synced
    True
    >>> cache_configs.sync_all()
    (...Path('.../test-local-cache/test_user_accounts.xlsx'),)
    >>> cache_configs['test-users'].cache_path.is_file()
    True
    """

    caches: Sequence[LocalCache] | None
    _synced: dict[str, SyncedLog] = field(default_factory=dict)

    def __repr__(self) -> str:
        """Summary of config."""
        return f"<LocalCachesManager(count={len(self)})>"

    def _sync_caches_attr(self) -> None:
        """Sync `self.caches` and `self.data`."""
        if self.caches:
            self.data = {cache.name: cache for cache in self.caches}

    def __post_init__(self) -> None:
        """Populate the `data` attribute."""
        self._sync_caches_attr()

    @property
    def cached_paths(self) -> tuple[Path, ...]:
        """Return paths currently logged as cached."""
        return tuple(synced_path["path"] for synced_path in self._synced.values())

    def sync_all(self, fail_if_no_source: bool = True, **kwargs) -> tuple[Path, ...]:
        """Run `sync` on all `self.caches` instances."""
        sync_results: CacheLogType
        for local_cacher in self.values():
            sync_results = local_cacher.sync(
                fail_if_no_source=fail_if_no_source, **kwargs
            )
            self._synced[local_cacher.name] = {
                "time": sync_results[1],
                "path": sync_results[2],
            }
        return self.cached_paths

    async def async_sync_all(
        self, fail_if_no_source: bool = True, **kwargs
    ) -> tuple[Path, ...]:
        """Run `sync` on all `self.caches` instances."""
        async_save_calls: list[Awaitable] = [
            local_cacher.async_sync(fail_if_no_source=fail_if_no_source, **kwargs)
            for local_cacher in self.values()
        ]
        async_results: list[CacheLogType] = await asyncio.gather(*async_save_calls)
        self._synced = {
            result[0]: {"time": result[1], "path": result[2]}
            for result in async_results
        }
        return self.cached_paths
