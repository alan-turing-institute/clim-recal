from pathlib import Path
from pprint import pprint
from shutil import copytree, rmtree
from typing import Final, Iterator

import pytest
from coverage_badge.__main__ import main as gen_cov_badge
from xarray import DataArray, Dataset

from clim_recal.config import (
    ClimRecalConfig,
    climate_data_mount_path,
    is_climate_data_mounted,
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
    PROCESSESORS_DEFAULT,
    VALID_DATES_STR_DEFAULT,
    CityOptions,
    MethodOptions,
    RunOptions,
    VariableOptions,
)
from clim_recal.utils.core import (
    ISO_DATE_FORMAT_STR,
    check_package_path,
    is_platform_darwin,
    iter_to_tuple_strs,
)
from clim_recal.utils.server import CondaLockFileManager
from clim_recal.utils.xarray import (
    GLASGOW_GEOM_LOCAL_PATH,
    XARRAY_EXAMPLE_END_DATE_4_YEARS,
    BoundsTupleType,
    xarray_example,
)

# Date Range covering leap year
XARRAY_END_DATE_4_DAYS: Final[str] = "1980-12-5"
XARRAY_END_DATE_8_DAYS: Final[str] = "1980-12-10"
XARRAY_SKIP_2_FROM_8_DAYS: Final[tuple[str, str]] = (
    "1980-12-7",
    "1980-12-8",
)
TEST_AUTH_CSV_FILE_NAME: Final[Path] = Path("test_auth.csv")

BADGE_PATH: Final[Path] = Path("docs") / "assets" / "coverage.svg"
CLIMATE_DATA_MOUNT_PATH_LINUX: Final[Path] = Path("/mnt/vmfileshare/ClimateData")
CLIMATE_DATA_MOUNT_PATH_MACOS: Final[Path] = Path("/Volumes/vmfileshare/ClimateData")

TEST_PATH: Final[Path] = Path().absolute()
TEST_DATA_PATH: Final[Path] = TEST_PATH / "tests/data"
PYTHON_DIR_NAME: Final[Path] = Path("python")

CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT: Final[tuple[str, ...]] = (
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

CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT: Final[tuple[str, ...]] = (
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
    PROCESSESORS_DEFAULT,
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

    Parameters
    ----------
    is_platform_darwin
        Calls fixture `is_platform_darwin`.

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
def data_fixtures_path(tmp_path) -> Iterator[Path]:
    yield copytree(TEST_DATA_PATH, tmp_path / TEST_DATA_PATH.name)
    rmtree(tmp_path / TEST_DATA_PATH.name)


@pytest.mark.mount
@pytest.fixture
def glasgow_shape_file_path(data_fixtures_path) -> Path:
    return data_fixtures_path / Path(*GLASGOW_GEOM_LOCAL_PATH.parts[-2:])


@pytest.fixture
def resample_test_runs_output_path(
    path=TEST_DATA_PATH / "resample" / "runs",
) -> Iterator[Path]:
    path.mkdir(exist_ok=True, parents=True)
    yield path
    # rmtree(path, ignore_errors=True)


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
def uk_epsg_27700_bounds() -> BoundsTupleType:
    """Boundaries of the UK in EPSG:27700 coords.

    The structure is (`minx`, `miny`, `maxx`, `maxy`)
    """
    return (
        353.92520902961434,
        -4.693282346489016,
        364.3162765660888,
        8.073382596733156,
    )


@pytest.fixture
def clim_runner(tmp_path) -> ClimRecalConfig:
    """Return default `ClimRecalConfig`."""
    return ClimRecalConfig(
        preprocess_out_folder=tmp_path,
        cities=(CityOptions.GLASGOW, CityOptions.MANCHESTER),
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
    uk_epsg_27700_bounds: BoundsTupleType,
    glasgow_epsg_27700_bounds: BoundsTupleType,
    glasgow_shape_file_path: Path,
    resample_test_runs_output_path: Path,
    clim_runner: ClimRecalConfig,
) -> None:
    """Elements to add to default `doctest` namespace."""
    doctest_namespace[
        "CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT"
    ] = CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT
    doctest_namespace[
        "CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT"
    ] = CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT
    doctest_namespace[
        "CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT"
    ] = CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
    doctest_namespace["MOD_FOLDER_FILES_COUNT_CORRECT"] = MOD_FOLDER_FILES_COUNT_CORRECT
    doctest_namespace["OBS_FOLDER_FILES_COUNT_CORRECT"] = OBS_FOLDER_FILES_COUNT_CORRECT
    doctest_namespace[
        "PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT"
    ] = preprocess_out_folder_files_count_correct
    doctest_namespace[
        "CLI_CMEHTODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT"
    ] = CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
    doctest_namespace[
        "CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT"
    ] = CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT
    doctest_namespace[
        "CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT"
    ] = CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT
    doctest_namespace["TEST_AUTH_CSV_PATH"] = TEST_AUTH_CSV_FILE_NAME
    doctest_namespace["is_platform_darwin"] = is_platform_darwin()
    doctest_namespace["is_data_mounted"] = is_data_mounted
    doctest_namespace["mount_path"] = data_mount_path
    doctest_namespace["pprint"] = pprint
    doctest_namespace["pytest"] = pytest
    doctest_namespace["xarray_spatial_4_days"] = xarray_spatial_4_days
    doctest_namespace[
        "xarray_spatial_6_days_2_skipped"
    ] = xarray_spatial_6_days_2_skipped
    doctest_namespace["xarray_spatial_8_days"] = xarray_spatial_8_days
    doctest_namespace["xarray_spatial_4_years"] = xarray_spatial_4_years
    doctest_namespace["xarray_spatial_4_years_360_day"] = xarray_spatial_4_years_360_day
    doctest_namespace["conda_lock_file_manager"] = conda_lock_file_manager
    doctest_namespace["data_fixtures_path"] = data_fixtures_path
    doctest_namespace["uk_epsg_27700_bounds"] = uk_epsg_27700_bounds
    doctest_namespace["glasgow_epsg_27700_bounds"] = glasgow_epsg_27700_bounds
    doctest_namespace["glasgow_shape_file_path"] = glasgow_shape_file_path
    doctest_namespace["resample_test_output_path"] = resample_test_runs_output_path
    doctest_namespace["clim_runner"] = clim_runner


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
