import asyncio
from argparse import BooleanOptionalAction
from os import PathLike
from pathlib import Path
from pprint import pprint
from shutil import copytree, rmtree
from typing import Final, Iterator

import pytest
from coverage_badge.__main__ import main as gen_cov_badge
from xarray import DataArray, Dataset, open_dataset
from xarray.core.types import T_Dataset

from clim_recal.config import ClimRecalConfig
from clim_recal.debiasing.debias_wrapper import RegionOptions
from clim_recal.utils.core import (
    ISO_DATE_FORMAT_STR,
    check_package_path,
    climate_data_mount_path,
    is_climate_data_mounted,
    is_platform_darwin,
    results_path,
)
from clim_recal.utils.data import (
    CPM_OUTPUT_PATH,
    HADS_OUTPUT_PATH,
    HADS_SUB_PATH,
    BoundsTupleType,
    RunOptions,
)
from clim_recal.utils.server import CondaLockFileManager
from clim_recal.utils.xarray import (
    GLASGOW_GEOM_LOCAL_PATH,
    cpm_reproject_with_standard_calendar,
)
from tests.utils import (
    CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT,
    CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT,
    CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT,
    CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT,
    CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT,
    CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT,
    CPM_CONVERTED_TASMAX_1980_FILE,
    CPM_RAW_TASMAX_1980_FILE,
    CPM_RAW_TASMAX_EXAMPLE_PATH,
    HADS_RAW_TASMAX_1980_FILE,
    HADS_RAW_TASMAX_EXAMPLE_PATH,
    MOD_FOLDER_FILES_COUNT_CORRECT,
    OBS_FOLDER_FILES_COUNT_CORRECT,
    PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT,
    TEST_AUTH_CSV_FILE_NAME,
    XARRAY_END_DATE_4_DAYS,
    XARRAY_END_DATE_8_DAYS,
    XARRAY_EXAMPLE_END_DATE_4_YEARS,
    XARRAY_SKIP_2_FROM_8_DAYS,
    LocalCache,
    LocalCachesManager,
    xarray_example,
)

MOUNT_DOCTEST_SKIP_MESSAGE: Final[str] = "requires external data mounted"
MOUNT_OR_CACHE_DOCTEST_SKIP_MESSAGE: Final[str] = (
    "requires external data mounted or cached"
)

BADGE_PATH: Final[Path] = Path("docs") / "assets" / "coverage.svg"

TEST_RUN_PATH: Final[Path] = Path().absolute()
TEST_FILE_PATH: Final[Path] = TEST_RUN_PATH / "tests"
TEST_RESULTS_PATH: Final[Path] = results_path(
    name="test-run-results", path=TEST_FILE_PATH, mkdir=True
)
PYTHON_DIR_NAME: Final[Path] = Path("python")
TEST_DATA_PATH: Final[Path] = TEST_FILE_PATH / "data"
LOCAL_FIXTURE_PATH_NAME: Final[Path] = Path("local-cache")

collect_ignore_glob: list[str] = ["*run*", "*.png", "*.xlsx"]


@pytest.fixture
def mod_folder_files_count_correct() -> int:
    return MOD_FOLDER_FILES_COUNT_CORRECT


@pytest.fixture
def obs_folder_files_count_correct() -> int:
    return OBS_FOLDER_FILES_COUNT_CORRECT


@pytest.fixture
def preprocess_out_folder_files_count_correct() -> int:
    """Return `PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT`."""
    return PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT


@pytest.fixture
def cli_preprocess_default_command_str_correct() -> str:
    return CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT


@pytest.fixture
def data_mount_path() -> Path:
    """Return likely climate data mount path based on operating system.

    Returns
    -------
    The `Path` climate data would likely be mounted to.
    """
    return climate_data_mount_path()


@pytest.fixture
def is_data_mounted(data_mount_path) -> bool:
    """Check if CLIMATE_DATA_MOUNT_PATH is mounted.

    Todo:
        Remove if `climate_data_mount_path` can manage
        condition by returning `None`.
    """
    return is_climate_data_mounted(mount_path=data_mount_path)


@pytest.fixture(scope="session")
def local_cache_fixtures(
    local_cache_path: Path,
    local_cpm_cache_path: Path,
    local_hads_cache_path: Path,
    sync_all: bool,
    use_async: bool,
) -> LocalCachesManager:
    cache_manager: LocalCachesManager = LocalCachesManager(
        default_local_cache_path=local_cache_path,
        caches=(
            LocalCache(
                name="tasmax_cpm_1980_raw",
                source_path=CPM_RAW_TASMAX_EXAMPLE_PATH,
                # local_cache_path=local_cpm_cache_path / 'tasmax/01/latest' / CPM_RAW_TASMAX_1980_FILE,
                local_cache_path=local_cpm_cache_path / CPM_RAW_TASMAX_1980_FILE,
                reader=open_dataset,
                reader_kwargs={"decode_coords": "all"},
            ),
            LocalCache(
                name="tasmax_cpm_1980_converted",
                source_path=CPM_RAW_TASMAX_EXAMPLE_PATH,
                local_cache_path=local_cache_path
                / "converted"
                / CPM_OUTPUT_PATH
                / CPM_CONVERTED_TASMAX_1980_FILE,
                reader=open_dataset,
                reader_kwargs={"decode_coords": "all"},
                parser=cpm_reproject_with_standard_calendar,
            ),
            LocalCache(
                name="tasmax_hads_1980_raw",
                source_path=HADS_RAW_TASMAX_EXAMPLE_PATH,
                local_cache_path=local_hads_cache_path / HADS_RAW_TASMAX_1980_FILE,
                reader=open_dataset,
                reader_kwargs={"decode_coords": "all"},
            ),
            # LocalCache(
            #     name="railfall_hads_1980_raw",
            #     source_path=HADS_RAW_RAINFALL_EXAMPLE_PATH,
            #     local_cache_path=local_hads_cache_path / HADS_RAW_RAINFALL_1980_FILE,
            #     reader=open_dataset,
            #     reader_kwargs={"decode_coords": "all"},
            # ),
            # LocalCache(
            #     name="railfall_cpm_1980_raw",
            #     source_path=CPM_RAW_RAINFALL_EXAMPLE_PATH,
            #     local_cache_path=local_hads_cache_path / CPM_RAW_RAINFALL_1980_FILE,
            #     reader=open_dataset,
            #     reader_kwargs={"decode_coords": "all"},
            # ),
        ),
    )
    if sync_all:
        if use_async:
            _ = asyncio.run(cache_manager.async_sync_all())
        else:
            _ = cache_manager.sync_all()
    return cache_manager


@pytest.fixture(scope="session")
def tasmax_cpm_1980_raw(
    local_cache: bool,
    local_cpm_cache_path: Path,
    local_cache_fixtures: LocalCachesManager,
) -> T_Dataset | None:
    if local_cache:
        return local_cache_fixtures["tasmax_cpm_1980_raw"].read(
            cache_path=local_cpm_cache_path
        )
    else:
        return None


@pytest.fixture(scope="session")
def tasmax_cpm_1980_raw_path(
    local_cache: bool,
    local_cache_fixtures: LocalCachesManager,
) -> T_Dataset:
    if local_cache:
        return local_cache_fixtures["tasmax_cpm_1980_raw"].local_cache_path
    else:
        return local_cache_fixtures["tasmax_cpm_1980_raw"].source_path


@pytest.fixture(scope="session")
def tasmax_hads_1980_raw(
    local_cache: bool,
    local_hads_cache_path: Path,
    local_cache_fixtures: LocalCachesManager,
) -> T_Dataset | None:
    if local_cache:
        return local_cache_fixtures["tasmax_hads_1980_raw"].read(
            cache_path=local_hads_cache_path
        )
    else:
        return None


@pytest.fixture(scope="session")
def tasmax_hads_1980_raw_path(
    local_cache: bool,
    local_cache_fixtures: LocalCachesManager,
) -> T_Dataset:
    if local_cache:
        return local_cache_fixtures["tasmax_hads_1980_raw"].local_cache_path
    else:
        return local_cache_fixtures["tasmax_hads_1980_raw"].source_path


@pytest.fixture(scope="session")
def hads_data_path(
    local_cache: bool,
    local_cache_fixtures: LocalCachesManager,
    local_hads_cache_path: Path,
    # local_hads_tasmax_cache_path: Path,
    # local_hads_tasmax_cache_path: Path,
) -> T_Dataset:
    if local_cache:
        # return local_cache_fixtures["tasmax_hads_1980_raw"].local_cache_path.parents[2]
        return local_hads_cache_path
    else:
        return local_cache_fixtures["tasmax_hads_1980_raw"].source_path.parents[2]


@pytest.fixture(scope="session")
def cpm_data_path(
    local_cache: bool,
    local_cache_fixtures: LocalCachesManager,
    local_cpm_cache_path: Path,
) -> T_Dataset:
    if local_cache:
        # return local_cache_fixtures["tasmax_cpm_1980_raw"].local_cache_path.parents[1]
        return local_cpm_cache_path
    else:
        return local_cache_fixtures["tasmax_cpm_1980_raw"].source_path.parents[2]


@pytest.fixture(scope="session")
def tasmax_cpm_1980_converted(
    local_cache: bool,
    local_cpm_cache_path: Path,
    local_cache_fixtures: LocalCachesManager,
) -> T_Dataset | None:
    if local_cache:
        return local_cache_fixtures["tasmax_cpm_1980_converted"].read(
            cache_path=local_cpm_cache_path
        )
    else:
        return None


@pytest.fixture(scope="session")
def tasmax_cpm_1980_converted_path(
    local_cache: bool,
    local_cache_fixtures: LocalCachesManager,
) -> T_Dataset:
    if local_cache:
        return local_cache_fixtures["tasmax_cpm_1980_converted"].local_cache_path
    else:
        return local_cache_fixtures["tasmax_cpm_1980_converted"].source_path


# This may be removed in future
# @pytest.fixture(autouse=True)
def ensure_python_path() -> None:
    """Return path for test running."""
    check_package_path(try_chdir=True)


@pytest.fixture
def xarray_spatial_4_days() -> DataArray:
    """Generate a `xarray` spatial time series 1980-11-30 to 1980-12-05."""
    return xarray_example(end_date=XARRAY_END_DATE_4_DAYS)


@pytest.fixture
def xarray_spatial_8_days() -> DataArray:
    """Generate a `xarray` spatial time series 1980-11-30 to 1980-12-10."""
    return xarray_example(end_date=XARRAY_END_DATE_8_DAYS)


@pytest.fixture
def xarray_spatial_6_days_2_skipped() -> DataArray:
    """Generate a `xarray` spatial time series 1980-11-30 to 1980-12-05."""
    return xarray_example(
        end_date=XARRAY_END_DATE_8_DAYS,
        skip_dates=XARRAY_SKIP_2_FROM_8_DAYS,
        skip_dates_format_str=ISO_DATE_FORMAT_STR,
    )


@pytest.fixture
def xarray_spatial_4_years() -> DataArray:
    """Generate a `xarray` spatial time series 1980-11-30 to 1984-11-30."""
    return xarray_example(end_date=XARRAY_EXAMPLE_END_DATE_4_YEARS)


@pytest.fixture
def xarray_spatial_4_years_360_day() -> Dataset:
    """Generate a `xarray` spatial time series 1980-11-30 to 1984-11-30."""
    four_normal_years: Dataset = xarray_example(
        end_date=XARRAY_EXAMPLE_END_DATE_4_YEARS, as_dataset=True, name="day_360"
    )
    return four_normal_years.convert_calendar("360_day", align_on="day")


@pytest.fixture
def conda_lock_file_manager() -> CondaLockFileManager:
    return CondaLockFileManager()


@pytest.fixture
def data_fixtures_path(tmp_path: Path) -> Iterator[Path]:
    yield copytree(TEST_DATA_PATH, tmp_path / TEST_FILE_PATH.name)
    rmtree(tmp_path / TEST_FILE_PATH.name)


@pytest.fixture
def glasgow_shape_file_path(data_fixtures_path: Path) -> Path:
    return data_fixtures_path / Path(*GLASGOW_GEOM_LOCAL_PATH.parts[-2:])


@pytest.fixture(scope="session")
def test_runs_output_path(
    keep_test_results: bool, path=TEST_RESULTS_PATH
) -> Iterator[Path]:
    path.mkdir(exist_ok=True, parents=True)
    yield path
    if not keep_test_results:
        rmtree(path, ignore_errors=True)


def pytest_addoption(parser):
    """Add cli config to use used cached test fixture files."""
    parser.addoption(
        "--local-cache",
        action=BooleanOptionalAction,
        default=False,
        help="use 'local_cache' data fixtures",
    )
    parser.addoption(
        "--sync-all",
        action=BooleanOptionalAction,
        default=False,
        help="sync all local cached data fixtures",
    )
    parser.addoption(
        "--use-async",
        action=BooleanOptionalAction,
        default=False,
        help="If --sync-all, use async calls",
    )
    parser.addoption(
        "--keep-results",
        action=BooleanOptionalAction,
        default=False,
        help="Keep test result files (else deleted after each test)",
    )


@pytest.fixture(scope="session")
def local_cache(request) -> bool:
    return request.config.getoption("--local-cache")


@pytest.fixture(scope="session")
def sync_all(request) -> bool:
    return request.config.getoption("--sync-all")


@pytest.fixture(scope="session")
def use_async(request) -> bool:
    return request.config.getoption("--use-async")


@pytest.fixture(scope="session")
def keep_test_results(request) -> bool:
    return request.config.getoption("--keep-results")


@pytest.fixture(scope="session")
def local_test_data_path() -> Path:
    return TEST_DATA_PATH


@pytest.fixture(scope="session")
def local_cache_path(local_test_data_path) -> Path:
    return local_test_data_path / LOCAL_FIXTURE_PATH_NAME


@pytest.fixture(scope="session")
def local_hads_cache_path(local_cache_path: Path) -> Path:
    return local_cache_path / "hadsuk"


@pytest.fixture(scope="session")
def local_hads_tasmax_cache_path(local_hads_cache_path: Path) -> Path:
    return local_hads_cache_path / "tasmax" / HADS_SUB_PATH


@pytest.fixture(scope="session")
def local_cpm_cache_path(local_cache_path: Path) -> Path:
    return local_cache_path / "ukcp"


@pytest.fixture(scope="session")
def local_cpm_tasmax_01_cache_path(local_hads_cache_path: Path) -> Path:
    return local_hads_cache_path / "tasmax" / RunOptions.ONE / HADS_SUB_PATH


@pytest.fixture
def glasgow_epsg_27700_bounds() -> BoundsTupleType:
    """Boundaries of Glasgow in EPSG:27700 coords.

    The structure is (`minx`, `miny`, `maxx`, `maxy`)
    """
    return (
        249799.9996000016,
        657761.4720000029,
        269234.99959999975,
        672330.6968000066,
    )


@pytest.fixture
def uk_rotated_grid_bounds() -> BoundsTupleType:
    """Boundaries of the UK in EPSG:27700 coords.

    The structure is (`minx`, `miny`, `maxx`, `maxy`)
    """
    return (
        353.92520902961434,
        -4.693282346489016,
        364.3162765660888,
        8.073382596733156,
    )


# Note: it may be worth setting this to cache for session runs
# This requires a different tmp_path configuration
@pytest.fixture
def clim_runner(
    tmp_path: Path,
    local_cache: bool,
    local_cache_fixtures: LocalCachesManager,
    test_runs_output_path: PathLike,
    local_hads_cache_path: PathLike,
    local_cpm_cache_path: PathLike,
    tasmax_cpm_1980_converted_path: PathLike,
) -> ClimRecalConfig:
    """Return default `ClimRecalConfig`."""
    assert local_cache_fixtures.default_local_cache_path
    assert local_cache_fixtures.check_default_cache_path()
    regions: tuple[RegionOptions, RegionOptions] = (
        RegionOptions.GLASGOW,
        RegionOptions.MANCHESTER,
    )
    try:
        # Todo: refactor to more easily specify `local_cache`
        assert not local_cache
        return ClimRecalConfig(
            preprocess_out_folder=tmp_path,
            regions=regions,
            output_path=test_runs_output_path,
            cpm_for_coord_alignment=CPM_RAW_TASMAX_EXAMPLE_PATH,
        )
    except (FileExistsError, AssertionError):
        return ClimRecalConfig(
            preprocess_out_folder=tmp_path,
            regions=regions,
            output_path=test_runs_output_path,
            hads_input_path=local_hads_cache_path,
            cpm_input_path=local_cpm_cache_path,
            # Todo: refactor to use caching to speed up runs
            cpm_kwargs=dict(_allow_check_fail=True),
            hads_kwargs=dict(_allow_check_fail=True),
            cpm_for_coord_alignment=tasmax_cpm_1980_converted_path,
        )


@pytest.fixture
def resample_test_cpm_output_path(
    test_runs_output_path: Path,
) -> Path:
    return test_runs_output_path / CPM_OUTPUT_PATH


@pytest.fixture
def resample_test_hads_output_path(
    test_runs_output_path: Path,
) -> Path:
    return test_runs_output_path / HADS_OUTPUT_PATH


@pytest.fixture
def glasgow_example_cropped_cpm_rainfall_path(data_fixtures_path: Path) -> Path:
    return (
        data_fixtures_path
        / "Glasgow/cropped-tif/cpr_rcp85_land-cpm_uk_2.2km_06_day_20791201-20801130.tif"
    )


@pytest.fixture
def test_users_cache(data_fixtures_path: Path, tmp_path: Path) -> LocalCache:
    return LocalCache(
        name="test-users",
        source_path=data_fixtures_path / "test_user_accounts.xlsx",
        local_cache_path=tmp_path / "test-local-cache",
    )


@pytest.fixture(autouse=True)
def doctest_auto_fixtures(
    doctest_namespace: dict,
    is_data_mounted: bool,
    preprocess_out_folder_files_count_correct: int,
    xarray_spatial_4_days: DataArray,
    xarray_spatial_6_days_2_skipped: DataArray,
    xarray_spatial_8_days: DataArray,
    xarray_spatial_4_years: DataArray,
    xarray_spatial_4_years_360_day: Dataset,
    conda_lock_file_manager: CondaLockFileManager,
    data_fixtures_path: Path,
    data_mount_path: Path,
    uk_rotated_grid_bounds: BoundsTupleType,
    glasgow_epsg_27700_bounds: BoundsTupleType,
    glasgow_shape_file_path: Path,
    test_runs_output_path: Path,
    glasgow_example_cropped_cpm_rainfall_path: Path,
) -> None:
    """Elements to add to default `doctest` namespace."""
    doctest_namespace["CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT"] = (
        CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT
    )
    doctest_namespace["CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT"] = (
        CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT
    )
    doctest_namespace["CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT"] = (
        CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
    )
    doctest_namespace["MOD_FOLDER_FILES_COUNT_CORRECT"] = MOD_FOLDER_FILES_COUNT_CORRECT
    doctest_namespace["OBS_FOLDER_FILES_COUNT_CORRECT"] = OBS_FOLDER_FILES_COUNT_CORRECT
    doctest_namespace["PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT"] = (
        preprocess_out_folder_files_count_correct
    )
    doctest_namespace["CLI_CMEHTODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT"] = (
        CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
    )
    doctest_namespace["CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT"] = (
        CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT
    )
    doctest_namespace["CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT"] = (
        CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT
    )
    doctest_namespace["TEST_AUTH_CSV_PATH"] = TEST_AUTH_CSV_FILE_NAME
    doctest_namespace["is_platform_darwin"] = is_platform_darwin()
    doctest_namespace["is_data_mounted"] = is_data_mounted
    doctest_namespace["mount_path"] = data_mount_path
    doctest_namespace["pprint"] = pprint
    doctest_namespace["pytest"] = pytest
    doctest_namespace["xarray_spatial_4_days"] = xarray_spatial_4_days
    doctest_namespace["xarray_spatial_6_days_2_skipped"] = (
        xarray_spatial_6_days_2_skipped
    )
    doctest_namespace["xarray_spatial_8_days"] = xarray_spatial_8_days
    doctest_namespace["xarray_spatial_4_years"] = xarray_spatial_4_years
    doctest_namespace["xarray_spatial_4_years_360_day"] = xarray_spatial_4_years_360_day
    doctest_namespace["conda_lock_file_manager"] = conda_lock_file_manager
    doctest_namespace["data_fixtures_path"] = data_fixtures_path
    doctest_namespace["uk_rotated_grid_bounds"] = uk_rotated_grid_bounds
    doctest_namespace["glasgow_epsg_27700_bounds"] = glasgow_epsg_27700_bounds
    doctest_namespace["glasgow_shape_file_path"] = glasgow_shape_file_path
    doctest_namespace["test_runs_output_path"] = test_runs_output_path
    doctest_namespace["mount_doctest_skip_message"] = MOUNT_DOCTEST_SKIP_MESSAGE
    doctest_namespace["mount_or_cache_doctest_skip_message"] = (
        MOUNT_OR_CACHE_DOCTEST_SKIP_MESSAGE
    )
    doctest_namespace["glasgow_example_cropped_cpm_rainfall_path"] = (
        glasgow_example_cropped_cpm_rainfall_path
    )


def pytest_sessionfinish(session, exitstatus):
    """Generate badges for docs after tests finish.

    Note
    ----
    This example assumes the `doctest` for `utils.csv_reader` is written in
    the `tests/` folder.
    """
    test_auth_csv_paths: tuple[Path, ...] = (
        Path(TEST_AUTH_CSV_FILE_NAME),
        Path("tests") / TEST_AUTH_CSV_FILE_NAME,
    )
    for test_auth_path in test_auth_csv_paths:
        if test_auth_path.exists():
            test_auth_path.unlink()
    if exitstatus == 0:
        BADGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        gen_cov_badge(["-o", f"{BADGE_PATH}", "-f"])
