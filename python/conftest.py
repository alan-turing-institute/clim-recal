import pprint
import sys
from os import PathLike
from pathlib import Path
from typing import Final

import pytest
from coverage_badge.__main__ import main as gen_cov_badge
from debiasing.debias_wrapper import (
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
from utils import iter_to_tuple_strs

BADGE_PATH: Final[Path] = Path("docs") / "assets" / "coverage.svg"
CLIMATE_DATA_MOUNT_PATH = Path("/mnt/vmfileshare/ClimateData")
TEST_PATH = Path().absolute()
PYTHON_DIR_NAME: Final[Path] = Path("python")
MODULE_NAMES: Final[tuple[PathLike, ...]] = (
    "debiasing",
    "resampling",
    "data_download",
    "load_data",
)

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


@pytest.fixture()
def is_platform_darwin() -> bool:
    """Check if `sys.platform` is `Darwin` (macOS)."""
    return sys.platform.startswith("darwin")


@pytest.fixture()
def is_climate_data_mounted() -> bool:
    """Check if `sys.platform` is `Darwin` (macOS)."""
    return CLIMATE_DATA_MOUNT_PATH.exists()


@pytest.fixture(autouse=True)
def ensure_python_path() -> None:
    """Return path for test running."""
    if not set(MODULE_NAMES) <= set(path.name for path in TEST_PATH.iterdir()):
        raise ValueError(
            f"'clim-recal' python tests must be "
            f"run in 'clim-recal/{PYTHON_DIR_NAME}', "
            f"not '{TEST_PATH.absolute()}'"
        )


@pytest.fixture
def preprocess_out_folder_files_count_correct() -> int:
    """Return `PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT`."""
    return PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT


@pytest.fixture(autouse=True)
def doctest_auto_fixtures(
    doctest_namespace: dict,
    is_platform_darwin: bool,
    is_climate_data_mounted: bool,
    preprocess_out_folder_files_count_correct: int,
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
    doctest_namespace["is_platform_darwin"] = is_platform_darwin
    doctest_namespace["is_climate_data_mounted"] = is_climate_data_mounted
    doctest_namespace["pprint"] = pprint
    doctest_namespace["pytest"] = pytest


def pytest_sessionfinish(session, exitstatus):
    """Generate badges for docs after tests finish."""
    if exitstatus == 0:
        BADGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        gen_cov_badge(["-o", f"{BADGE_PATH}", "-f"])
