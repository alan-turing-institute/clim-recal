import asyncio
from collections import UserDict
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import date, datetime
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Final,
    Iterable,
    Literal,
    Sequence,
    TypedDict,
)

import sysrsync
from numpy import array
from numpy import array as np_array
from numpy import nan, random
from numpy.typing import NDArray
from pandas import to_datetime
from xarray import DataArray, open_dataset
from xarray.core.types import T_DataArray, T_DataArrayOrSet, T_Dataset

from clim_recal.convert import (
    RAW_CPM_PATH,
    RAW_CPM_TASMAX_PATH,
    RAW_HADS_TASMAX_PATH,
    IterCalcBase,
)
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
from clim_recal.utils.data import (
    THREE_CITY_CENTRE_COORDS,
    HadUKGrid,
    MethodOptions,
    RegionOptions,
    RunOptions,
    UKCPLocalProjections,
    VariableOptions,
)

logger = getLogger(__name__)

# FINAL_CONVERTED_HADS_WIDTH: Final[int] = 410
# FINAL_CONVERTED_HADS_HEIGHT: Final[int] = 660

FINAL_CPM_DEC_10_X_2_Y_200_210: Final[NDArray] = array(
    (
        nan,
        nan,
        9.637598,
        9.646631,
        9.636621,
        9.622217,
        9.625147,
        9.640039,
        9.6349125,
        9.509668,
    )
)

HADS_UK_TASMAX_DAY_SERVER_PATH: Final[Path] = Path("Raw/HadsUKgrid/tasmax/day")

CPM_RAW_TASMAX_1980_FILE: Final[Path] = Path(
    "tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc"
)
CPM_CONVERTED_TASMAX_1980_FILE: Final[Path] = Path(
    "tasmax_rcp85_land-cpm_uk_2.2km_05_day_std_year_19801201-19811130.nc"
)
HADS_RAW_TASMAX_1980_FILE: Final[Path] = Path(
    "tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc"
)
HADS_CONVERTED_TASMAX_1980_FILE: Final[Path] = Path(
    "tasmax_hadukgrid_uk_2_2km_day_19800101-19800131.nc"
)

HADS_UK_TASMAX_LOCAL_TEST_PATH: Final[Path] = (
    Path(HadUKGrid.slug) / HADS_RAW_TASMAX_1980_FILE
)

CPM_TASMAX_DAY_SERVER_PATH: Final[Path] = Path("Raw/UKCP2.2/tasmax/01/latest")
# Todo: Change "tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc"
# to "tasmax_cpm_example.nc"
CPM_TASMAX_LOCAL_TEST_PATH: Final[Path] = (
    Path(UKCPLocalProjections.slug) / CPM_RAW_TASMAX_1980_FILE
)

HADS_UK_RAINFALL_DAY_SERVER_PATH: Final[Path] = Path("Raw/HadsUKgrid/rainfall/day")
HADS_RAW_RAINFALL_1980_FILE: Final[Path] = Path(
    "rainfall_hadukgrid_uk_1km_day_19800101-19800131.nc"
)


RAW_CPM_RAINFALL_PATH: Final[Path] = Path(RAW_CPM_PATH) / "Raw/UKCP2.2/pr/01/latest/"
RAW_HADS_RAINFALL_PATH: Final[Path] = Path(RAW_CPM_PATH) / "rainfall/day"
CPM_RAW_RAINFALL_1980_FILE: Final[Path] = Path(
    "pr_rcp85_land-cpm_uk_2.2km_01_day_20331201-20341130.nc"
)

CPM_RAW_TASMAX_EXAMPLE_PATH: Final[Path] = (
    RAW_CPM_TASMAX_PATH / CPM_RAW_TASMAX_1980_FILE
)
CPM_RAW_RAINFALL_EXAMPLE_PATH: Final[Path] = (
    RAW_CPM_RAINFALL_PATH / CPM_RAW_RAINFALL_1980_FILE
)

HADS_RAW_TASMAX_EXAMPLE_PATH: Final[Path] = (
    RAW_HADS_TASMAX_PATH / HADS_RAW_TASMAX_1980_FILE
)

HADS_RAW_RAINFALL_EXAMPLE_PATH: Final[Path] = (
    RAW_HADS_RAINFALL_PATH / HADS_RAW_RAINFALL_1980_FILE
)


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
    DATA_PATH_DEFAULT / MOD_FOLDER_DEFAULT / RegionOptions.default(),
    "--obs",
    DATA_PATH_DEFAULT / OBS_FOLDER_DEFAULT / RegionOptions.default(),
    "-v",
    VariableOptions.default(),
    "-r",
    RunOptions.default(),
    "--out",
    (
        DATA_PATH_DEFAULT
        / PREPROCESS_OUT_FOLDER_DEFAULT
        / RegionOptions.default()
        / RunOptions.default()
        / VariableOptions.default()
    ),
    "--calib_dates",
    CALIB_DATES_STR_DEFAULT,
    "--valid_dates",
    VALID_DATES_STR_DEFAULT,
)

CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT: Final[tuple[str, ...]] = (
    iter_to_tuple_strs(CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT)
)

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
        / RegionOptions.default()
        / RunOptions.default()
    ).resolve(),
    "--method",
    MethodOptions.default(),
    "-v",
    VariableOptions.default(),
    "-p",
    PROCESSORS_DEFAULT,
)

CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT: Final[tuple[str, ...]] = (
    iter_to_tuple_strs(CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT)
)

CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT: Final[str] = " ".join(
    CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
)

MOD_FOLDER_FILES_COUNT_CORRECT: Final[int] = 1478
OBS_FOLDER_FILES_COUNT_CORRECT: Final[int] = MOD_FOLDER_FILES_COUNT_CORRECT
PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT: Final[int] = 4

CPM_FIRST_DATES: NDArray = np_array(
    ["19801201", "19801202", "19801203", "19801204", "19801205"]
)
CPM_CONVERTED_DEC_10_XY_300: NDArray = np_array(
    [
        0.39833984,
        0.50039065,
        0.28383788,
        0.43569335,
        1.122461,
        1.1024414,
        1.042627,
        0.7235352,
        0.6480957,
        0.88491213,
    ]
)
CPM_GLASGOW_CONVERTED_FIRST_VALUES: NDArray = np_array(
    [
        6.471338,
        6.238672,
        5.943994,
        5.6705565,
        5.4742675,
        5.126611,
        4.7020507,
        4.2784667,
        4.1996093,
    ]
)
HADS_FIRST_DATES: NDArray = np_array(
    ["19800101", "19800102", "19800103", "19800104", "19800105"]
)
HADS_CONVERTED_DEC_10_XY_300: NDArray = np_array(
    [
        2.29335493,
        2.26689276,
        1.12071333,
        1.30746908,
        1.50594347,
        1.84169973,
        1.93766855,
        1.72104809,
        1.21958198,
        1.47863651,
    ]
)
HADS_GLASGOW_CONVERTED_FIRST_VALUES: NDArray = np_array(
    [
        6.06706882,
        5.93332477,
        5.69923384,
        5.55714165,
        4.85292518,
        4.23700658,
        4.53450007,
        4.58407563,
        4.55542638,
    ]
)

IterCheckTestType = Literal["direct", "range", "direct_provided", "range_provided"]
CalcType = Literal["convert", "crop"]


def match_convert_or_crop(
    instance: IterCalcBase,
    calc_type: CalcType,
    check_type: IterCheckTestType,
) -> T_Dataset:
    """Check configuration tests and return export."""
    direct_method_name: str = (
        "to_reprojection" if calc_type == "convert" else "crop_projection"
    )
    range_method_name: str = (
        "range_to_reprojection" if calc_type == "convert" else "range_crop_projection"
    )
    paths: list[Path]
    match check_type:
        case "direct":
            paths = [getattr(instance, direct_method_name)(return_path=True)]
        case "range":
            paths = list(getattr(instance, range_method_name)(stop=1))
        case "direct_provided":
            paths = [
                getattr(instance, direct_method_name)(
                    index=0, source_to_index=tuple(instance), return_path=True
                )
            ]
        case "range_provided":
            paths = list(
                getattr(instance, range_method_name)(
                    stop=1,
                    source_to_index=tuple(instance),
                )
            )
    return open_dataset(paths[0])


class StandardWith360DayError(Exception): ...


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
    skip_dates: Iterable[date | str] | None = None,
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

    reader: Callable | None = None
    reader_kwargs: dict[str, Any] = field(default_factory=dict)

    parser: Callable | None = None
    parser_kwargs: dict[str, Any] = field(default_factory=dict)

    _make_parent_folders: bool = True

    def __repr__(self) -> str:
        """Summary of caching config."""
        return f"<LocalCache(name='{self.name}', is_cached={self.is_cached})>"

    def __post_init__(self) -> None:
        """Account `self.cache_path` consistency."""
        self.source_path = Path(self.source_path)
        self.local_cache_path = Path(self.local_cache_path)
        if self.source_path.exists():
            if not self.source_path.is_file():
                raise ValueError(f"Only files can be cached, not: '{self.source_path}'")
        if self._make_parent_folders:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def is_cached(self) -> bool:
        """Whether `self.source_path` has been cached."""
        return self.cache_path.is_file()

    @property
    def cache_path(self) -> Path:
        """Path to copy `source_path` file to."""
        assert isinstance(self.source_path, Path)
        assert isinstance(self.local_cache_path, Path)
        if not self.local_cache_path.is_file() and not self.local_cache_path.suffix:
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

    def parse(self) -> T_DataArrayOrSet | None:
        """If `self.parser` is set, process file prior to syncing.

        Examples
        --------
        >>> test_users_cache = getfixture('test_users_cache')
        >>> from pandas import read_excel
        >>> from xarray import Dataset, open_dataset
        >>>
        >>> def excel_to_netcdf(path: PathLike):
        ...     df = read_excel(Path(path))
        ...     return df.to_xarray()
        >>> test_users_cache.parser = excel_to_netcdf
        >>> test_users_cache.reader = open_dataset
        >>> users_df: Dataset = test_users_cache.parse()
        >>> test_users_cache.read(run_sync=True)
        <xarray.Dataset>...
        Dimensions:    (index: 5)
        Coordinates:
          * index      (index) int64...0 1 2 3 4
        Data variables:
            A Column   (index)...
            User Name  (index)...
            Password   (index)...
        >>> test_users_cache
        <LocalCache(name='test-users', is_cached=True)>
        """
        if self.parser:
            return self.parser(self.source_path, **self.parser_kwargs)
        else:
            return None

    async def async_sync(
        self, fail_if_no_source: bool = True, **kwargs
    ) -> CacheLogType:
        """Asyncronously sync `self.source_path` to `self.cache_path`."""
        if self._check_source_path(fail_if_no_source=fail_if_no_source):
            if self.parser:
                logger.info(f"Asyncing 'parser' for {self}...")
                intermediate: T_DataArrayOrSet = self.parse()
                intermediate.to_netcdf(self.cache_path)
            else:
                logger.info(f"Asyncing 'rsync' for {self}...")
                await asyncio.create_subprocess_exec(
                    "rsync", str(self.source_path), str(self.cache_path), **kwargs
                )
            self._update_synced()
        return self.name, self.synced, self.cache_path

    def sync(self, fail_if_no_source: bool = True, **kwargs) -> CacheLogType:
        """Sync `self.source_path` to `self.cache_path`."""
        if self._check_source_path(fail_if_no_source=fail_if_no_source):
            if self.parser:
                logger.info(f"Syncing 'parser' for {self}...")
                intermediate: T_DataArrayOrSet = self.parse()
                intermediate.to_netcdf(self.cache_path)
            else:
                logger.info(f"Syncing 'rsync' for {self}...")
                sysrsync.run(
                    source=str(self.source_path),
                    destination=str(self.cache_path),
                    **kwargs,
                )
                self._update_synced()
        return self.name, self.synced, self.cache_path

    def read(self, cache_path: bool = True, run_sync: bool = False, **kwargs) -> Any:
        """Read file using `self.reader` and `self.kwargs`.

        Parameters
        ----------
        cache_path
            Whether to read `self.cache_path` (`True`) or `self.source_path` (`False`).
        run_sync
            Whether to run `run_sync` if `self.cache_path` is not set.
        kwargs
            Any additional parameters to pass to `self.reader`

        Returns
        -------
        A processed version of either `self.cache_path` or `self.source_path`.

        Examples
        --------
        >>> test_users_cache = getfixture('test_users_cache')
        >>> test_users_cache.read()
        Traceback (most recent call last):
            ...
        ValueError: `reader` attribute must be set for
        <LocalCache(name='test-users', is_cached=False)>
        >>> from pandas import read_excel
        >>> test_users_cache.reader = read_excel
        >>> test_users_cache.read()
        Traceback (most recent call last):
            ...
        ValueError: Can't use `local_path` prior to
        cache run. Run with `run_sync` to override for
        <LocalCache(name='test-users', is_cached=False)>.
        >>> test_users_cache.read(run_sync=True)
           A Column User Name      Password
        0         1     sally        a pass
        1         2    george  another pass
        2        34      jean       passing
        3         4  felicity      pastoral
        4         2     frank        plough
        >>> test_users_cache
        <LocalCache(name='test-users', is_cached=True)>
        """
        if not self.reader:
            raise ValueError(f"`reader` attribute must be set for {self}")
        path: Path
        if cache_path:
            if self.is_cached:
                path = self.cache_path
            elif run_sync:
                _, _, path = self.sync()
            else:
                raise ValueError(
                    f"Can't use `local_path` prior to cache run. "
                    f"Run with `run_sync` to override for {self}."
                )
        else:
            self._check_source_path()
            path = Path(self.source_path)
        return self.reader(path, **(kwargs | self.reader_kwargs))


@dataclass
class LocalCachesManager(UserDict):
    """Manager for a set of local caches.

    Attributes
    ----------
    caches
        A `Sequence` of `LocalCache` instances to manage
    fail_if_no_source
        Whether to raise an exception if `souce` is not available
    default_local_cache_path
        `Path` to default to if none specified

    Examples
    --------
    >>> test_users_cache = getfixture('test_users_cache')
    >>> cache_configs = LocalCachesManager([test_users_cache])
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
    fail_if_no_source: bool = False
    default_local_cache_path: PathLike | None = None
    _synced: dict[str, SyncedLog] = field(default_factory=dict)

    def __repr__(self) -> str:
        """Summary of config."""
        return f"<LocalCachesManager(count={len(self)})>"

    def _sync_caches_attr(self) -> None:
        """Sync `self.caches` and `self.data`."""
        if self.caches:
            self.data = {cache.name: cache for cache in self.caches}

    def check_default_cache_path(self) -> bool:
        """Check `self.cached_paths` in `self.default_local_cache_path`."""
        if len(self):
            return all(
                self.default_local_cache_path in cache_config.local_cache_path.parents
                for cache_config in self.values()
            )
        else:
            return False

    def __post_init__(self) -> None:
        """Populate the `data` attribute."""
        self._sync_caches_attr()
        # Removing this to avoid test delays from auto_syncing
        # if not self._synced:
        #     self.sync_all(fail_if_no_source=self.fail_if_no_source)

    @property
    def cached_paths(self) -> tuple[Path, ...]:
        """Return paths currently logged as cached."""
        return tuple(synced_path["path"] for synced_path in self._synced.values())

    def sync_all(self, fail_if_no_source: bool = True, **kwargs) -> tuple[Path, ...]:
        """Run `sync` on all `self.caches` instances."""
        sync_results: CacheLogType
        fail_if_no_source = self.fail_if_no_source or self.fail_if_no_source
        logger.info(
            f"'fail_if_no_source' is {self.fail_if_no_source}. "
            f"Syncing {len(self)} cached files..."
        )
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


def compare_dataclass_instances(a: Any, b: Any, print_all: bool = False) -> dict:
    """Test compare and print attributes shared by `a` and `b`."""
    try:
        for instance in a, b:
            assert is_dataclass(instance)
    except AssertionError:
        raise ValueError(
            f"'{instance}' class '{instance.__class__.__name__}' is not a dataclass."
        )
    try:
        assert type(b) is type(a)
    except AssertionError:
        raise ValueError(f"Classes of 'a': {a} and 'b': {b} are not the same.")
    diffs: dict[str, tuple[Any, Any]] = {}
    for var_field in fields(a):
        a_attr = getattr(a, var_field.name)
        b_attr = getattr(b, var_field.name)
        if print_all or a_attr != b_attr:
            diffs[var_field.name] = a_attr, b_attr
    return diffs
