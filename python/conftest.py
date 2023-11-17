import pprint
import sys
from os import PathLike
from pathlib import Path
from typing import Final

import pytest
from coverage_badge.__main__ import main as gen_cov_badge

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
            f"not '{path.absolute()}'"
        )


@pytest.fixture(autouse=True)
def doctest_auto_fixtures(
    doctest_namespace: dict, is_platform_darwin: bool, is_climate_data_mounted: bool
) -> None:
    """Elements to add to default `doctest` namespace."""
    doctest_namespace["is_platform_darwin"] = is_platform_darwin
    doctest_namespace["is_climate_data_mounted"] = is_climate_data_mounted
    doctest_namespace["pprint"] = pprint
    doctest_namespace["pytest"] = pytest


def pytest_sessionfinish(session, exitstatus):
    """Generate badges for docs after tests finish."""
    if exitstatus == 0:
        BADGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        gen_cov_badge(["-o", f"{BADGE_PATH}", "-f"])
