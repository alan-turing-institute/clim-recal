from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Final, Sequence
from urllib.error import URLError
from urllib.request import urlopen

from xarray import open_dataset
from xarray.core.types import T_Dataset

from .data import RunOptions, VariableOptions
from .gdal_formats import NETCDF_EXTENSION_STR

logger = getLogger(__name__)

RUN_TYPES: tuple[str, ...] = RunOptions.preferred_and_first()
VARIABLE_TYPES: tuple[str, ...] = VariableOptions.cpm_values()
METHODS_PATHS: dict[str, str] = {
    "raw": "cpm-raw-medians",
    "linear": "cpm-converted-linear-medians",
    "nearest": "cpm-converted-nearest-medians",
}

BYTES_MODE: Final[str] = "#mode=bytes"
REMOTE_EXT_LEN: Final[int] = len("." + NETCDF_EXTENSION_STR + BYTES_MODE)

cal_360_day_4_years: Final[int] = 360 * 4
cal_standard_4_years: Final[int] = 365 * 3 + 366


HOSTING_URL: Final[str] = (
    "https://climrecal.blob.core.windows.net/analysis/cpm-median-time-series"
)
LOCAL_ASSETS_FOLDER: Final[Path] = Path("./assets")


def gap_360_days(is_leap_year: bool) -> tuple[int, ...]:
    if not is_leap_year:
        # https://docs.xarray.dev/en/stable/generated/xarray.Dataset.convert_calendar.html
        #  February 6th (36), April 19th (109), July 2nd (183), September 12th (255), November 25th (329).
        # First missing day should be 37, not 36 since February 6th is 37
        return tuple([37, 109, 183, 255, 329])
    else:
        # January 31st (31), March 31st (91), June 1st (153), July 31st (213), September 31st (275) and November 30th (335).
        return tuple([31, 91, 153, 213, 275, 335])


def plot_axvlines(
    plot_obj,
    coords: Sequence,
    zorder: int = 1,
    linewidth: float = 1,
    color: str = "k",
    ls: str = ":",
    **kwargs,
) -> None:
    """Add `coords` as full vertical lines to `plot_obj`."""
    for x_value in coords:
        plot_obj.axvline(
            x=x_value, zorder=zorder, linewidth=linewidth, color=color, ls=ls, **kwargs
        )


@dataclass
class CPMSummaryTimeSeries:
    """
    Manage time series files for documentation.

    Attributes
    ----------
    remote_folders
        A `dict` of `kind` to remote folder `url`.

    Examples
    --------
    >>> ts_temp_path = getfixture('tmp_path') / 'ts_doc_tests'
    >>> cpm_ts = CPMSummaryTimeSeries(local_save_folder=ts_temp_path)
    >>> results = cpm_ts.get_local_xarrays_dict()
    >>> tuple(results.keys())
    ('raw', 'linear', 'nearest')
    >>> pprint(tuple(results['raw'].keys()))
    (('tasmax', '01'),
     ('tasmax', '05'),
     ('tasmax', '06'),
     ('tasmax', '07'),
     ('tasmax', '08'),
     ('pr', '01'),
     ('pr', '05'),
     ('pr', '06'),
     ('pr', '07'),
     ('pr', '08'),
     ('tasmin', '01'),
     ('tasmin', '05'),
     ('tasmin', '06'),
     ('tasmin', '07'),
     ('tasmin', '08'))
    """

    remote_folders: dict[str, str] = field(default_factory=dict)
    local_folders: dict[str, Path] = field(default_factory=dict)

    hosting_url: str = HOSTING_URL
    local_save_folder: Path = LOCAL_ASSETS_FOLDER

    variables: tuple[VariableOptions | str, ...] = VARIABLE_TYPES
    runs: tuple[RunOptions | str, ...] = RUN_TYPES
    kinds: dict[str, str] = field(default_factory=lambda: METHODS_PATHS)
    method: str = "median"
    remote_files_tail_str: str = BYTES_MODE

    def __post_init__(self) -> None:
        if not self.remote_folders:
            self.set_remote_folders()
        if not self.local_folders:
            self.set_local_folders()

    def get_local_path(self, kind: str, variable: str, run: str) -> Path:
        """Return the relevant path for passed parameters."""
        return self.local_folders[kind] / f"{self.method}-{variable}-{run}.nc"

    def get_remote_path(self, kind: str, variable: str, run: str) -> str:
        """Return the relevant path for passed parameters."""
        return (
            self.remote_folders[kind]
            + f"/{self.method}-{variable}-{run}.nc{self.remote_files_tail_str}"
        )

    def set_remote_folders(self) -> dict[str, str]:
        """Set `self.remote_folders` using `self.hosting_url`"""
        if self.hosting_url and not self.remote_folders:
            for name, path in self.kinds.items():
                self.remote_folders[name] = self.hosting_url + "/" + path
        return self.remote_folders

    def set_local_folders(self) -> dict[str, Path]:
        """Set `local_folders` using `self.local_save_folder`."""
        if self.local_save_folder and not self.local_folders:
            for name, path in self.kinds.items():
                self.local_folders[name] = self.local_save_folder / path
        return self.local_folders

    def set_remote_paths(self, force: bool = False) -> dict[str, list[str]]:
        """Set and return a `dict` of `kind` -> `list` of file names."""
        if not hasattr(self, "remote_paths") or force:
            self.remote_paths: dict[str, list[str]] = {
                kind: [
                    self.get_remote_path(kind=kind, variable=variable, run=run)
                    for variable in self.variables
                    for run in self.runs
                ]
                for kind in self.kinds
            }
        return self.remote_paths

    def set_local_paths(self, force: bool = False) -> dict[str, list[Path]]:
        """Set and return a `dict` of `kind` -> `list` of file names."""
        if not hasattr(self, "local_paths") or force:
            self.local_paths: dict[str, list[Path]] = {
                kind: [
                    self.get_local_path(kind=kind, variable=variable, run=run)
                    for variable in self.variables
                    for run in self.runs
                ]
                for kind in self.kinds
            }
        return self.local_paths

    def set_local_remote_dict(self) -> dict[str, Path]:
        # self.set_local_paths()
        # self.set_remote_paths()
        self.remote_to_local: dict[str, Path] = {}
        for kind in self.kinds:
            for run in self.runs:
                for variable in self.variables:
                    remote_path: str = self.get_remote_path(
                        kind=kind, variable=variable, run=run
                    )
                    local_path: Path = self.get_local_path(
                        kind=kind, variable=variable, run=run
                    )
                    self.remote_to_local[remote_path] = local_path
        return self.remote_to_local

    def cache(self) -> dict[str, Path]:
        self.set_local_remote_dict()
        failed_caches: dict[str, Path] = {}
        for remote, local in self.remote_to_local.items():
            try:
                if not local.is_file():
                    if local.exists() and not local.is_file():
                        raise FileNotFoundError(
                            f"'local' path: '{local}' should be a file."
                        )
                    local.parent.mkdir(exist_ok=True, parents=True)
                    with urlopen(remote) as response, open(local, "wb") as out_file:
                        logger.info(f"Downloading '{remote}' to '{local}'")
                        data = response.read()  # a `bytes` object
                        out_file.write(data)
            except URLError:
                failed_caches[remote] = local
                logger.warning(f"Failed to download and cache '{remote}' to '{local}'")
        return failed_caches

    def get_local_xarrays_dict(self) -> dict[str, dict[tuple[str, str], T_Dataset]]:
        """Return a `dict` of data `kind` and related `Dataset` objects."""
        self.set_remote_paths()
        self.set_local_paths()
        self.cache()
        return {
            kind: {
                tuple(path.stem.split("-")[1:3]): open_dataset(path)
                for path_list in self.local_paths.values()
                for path in path_list
            }
            for kind in self.kinds
        }
