from enum import StrEnum, auto
from os import PathLike
from pathlib import Path
from typing import Final

from .utils.core import is_platform_darwin

DEBIAN_MOUNT_PATH: Final[Path] = Path("/mnt/vmfileshare")
DARWIN_MOUNT_PATH: Final[Path] = Path("/Volumes/vmfileshare")
CLIMATE_DATA_PATH: Final[Path] = Path("ClimateData")


class VariableOptions(StrEnum):
    """Supported options for variables"""

    TASMAX = auto()
    RAINFALL = auto()
    TASMIN = auto()

    @classmethod
    def default(cls) -> str:
        """Default option."""
        return cls.TASMAX.value


class RunOptions(StrEnum):
    """Supported options for variables.

    Notes
    -----
    Options `TWO` and `THREE` are not available for `UKCP2.2`.
    """

    ONE = "01"
    # TWO = "02"
    # THREE = "03"
    FOUR = "04"
    FIVE = "05"
    SIX = "06"
    SEVEN = "07"
    EIGHT = "08"
    NINE = "09"
    TEN = "10"
    ELEVEN = "11"
    TWELVE = "12"
    THIRTEEN = "13"
    FOURTEEN = "14"
    FIFTEEN = "15"

    @classmethod
    def default(cls) -> str:
        """Default option."""
        return cls.FIVE.value


class CityOptions(StrEnum):
    """Supported options for variables."""

    GLASGOW = "Glasgow"
    MANCHESTER = "Manchester"
    LONDON = "London"

    @classmethod
    def default(cls) -> str:
        """Default option."""
        return cls.MANCHESTER.value


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
