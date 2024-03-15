"""Wrapper for running `preprocess_data.py` and `run_cmethods.py`"""
from dataclasses import dataclass
from datetime import date
from enum import StrEnum, auto
from os import PathLike
from pathlib import Path
from typing import Final, Generator, Optional, Union

from ..pipeline import climate_data_mount_path
from ..utils import (
    DATE_FORMAT_SPLIT_STR,
    DATE_FORMAT_STR,
    DateType,
    date_range_to_str,
    iter_to_tuple_strs,
    path_iterdir,
)

DATA_PATH_DEFAULT: Final[Path] = climate_data_mount_path()

COMMAND_DIR_DEFAULT: Final[Path] = Path("clim_recal/debiasing").resolve()
PREPROCESS_FILE_NAME: Final[Path] = Path("preprocess_data.py")
CMETHODS_FILE_NAME: Final[Path] = Path("run_cmethods.py")


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
    """Supported options for variables"""

    ONE = "01"
    TWO = "02"
    THREE = "03"
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


class MethodOptions(StrEnum):
    """Supported options for methods."""

    QUANTILE_DELTA_MAPPING = auto()
    QUANTILE_MAPPING = auto()
    VARIANCE_SCALING = auto()
    DELTA_METHOD = auto()

    @classmethod
    def default(cls) -> str:
        """Default method option."""
        return cls.QUANTILE_DELTA_MAPPING.value


PROCESSESORS_DEFAULT: Final[int] = 2
RUN_PREFIX_DEFAULT: Final[str] = "python"

MOD_FOLDER_DEFAULT: Final[Path] = Path("CPM")
OBS_FOLDER_DEFAULT: Final[Path] = Path("Hads.updated360")
PREPROCESS_OUT_FOLDER_DEFAULT: Final[Path] = Path("Preprocessed")
CMETHODS_OUT_FOLDER_DEFAULT: Final[Path] = Path("Debiased/three.cities.cropped")

CALIB_DATE_START_DEFAULT: DateType = date(1981, 1, 1)
CALIB_DATE_END_DEFAULT: DateType = date(1981, 12, 30)

VALID_DATE_START_DEFAULT: DateType = date(2010, 1, 1)
VALID_DATE_END_DEFAULT: DateType = date(2010, 12, 30)

CALIB_DATES_STR_DEFAULT: Final[str] = date_range_to_str(
    CALIB_DATE_START_DEFAULT, CALIB_DATE_END_DEFAULT
)
VALID_DATES_STR_DEFAULT: Final[str] = date_range_to_str(
    VALID_DATE_START_DEFAULT, VALID_DATE_END_DEFAULT
)


@dataclass
class RunConfig:

    """Manage creating command line scripts to run `debiasing` `cli`."""

    command_dir: Path = COMMAND_DIR_DEFAULT
    variable: str = VariableOptions.default()
    run: str = RunOptions.default()
    city: str = CityOptions.default()
    method: str = MethodOptions.default()
    run_prefix: str = RUN_PREFIX_DEFAULT
    preprocess_data_file: PathLike = PREPROCESS_FILE_NAME
    run_cmethods_file: PathLike = CMETHODS_FILE_NAME

    data_path: Path = DATA_PATH_DEFAULT
    mod_folder: PathLike = MOD_FOLDER_DEFAULT
    obs_folder: PathLike = OBS_FOLDER_DEFAULT
    preprocess_out_folder: PathLike = PREPROCESS_OUT_FOLDER_DEFAULT
    cmethods_out_folder: PathLike = CMETHODS_OUT_FOLDER_DEFAULT

    calib_date_start: DateType = CALIB_DATE_START_DEFAULT
    calib_date_end: DateType = CALIB_DATE_END_DEFAULT

    valid_date_start: DateType = VALID_DATE_START_DEFAULT
    valid_date_end: DateType = VALID_DATE_END_DEFAULT

    processors: int = PROCESSESORS_DEFAULT

    date_format_str: str = DATE_FORMAT_STR
    date_split_str: str = DATE_FORMAT_SPLIT_STR

    def calib_dates_to_str(
        self,
        start_date: DateType,
        end_date: DateType,
        in_format_str: Optional[str] = None,
        out_format_str: Optional[str] = None,
        split_str: Optional[str] = None,
    ) -> str:
        """Return date range as `str` from `calib_date_start` to `calib_date_end`.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> config.calib_dates_to_str('20100101', '20100330')
        '20100101-20100330'
        >>> config.calib_dates_to_str(date(2010, 1, 1), '20100330')
        '20100101-20100330'
        >>> config.calib_dates_to_str(date(2010, 1, 1), '20100330', split_str="_")
        '20100101_20100330'
        """
        start_date = start_date if start_date else self.calib_date_start
        end_date = end_date if end_date else self.calib_date_end
        return self._date_range_to_str(
            start_date, end_date, in_format_str, out_format_str, split_str
        )

    def valid_dates_to_str(
        self,
        start_date: DateType,
        end_date: DateType,
        in_format_str: Optional[str] = None,
        out_format_str: Optional[str] = None,
        split_str: Optional[str] = None,
    ) -> str:
        """Return date range as `str` from `valid_date_start` to `valid_date_end`.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> config.valid_dates_to_str('20100101', '20100330')
        '20100101-20100330'
        >>> config.valid_dates_to_str(date(2010, 1, 1), '20100330')
        '20100101-20100330'
        >>> config.valid_dates_to_str(date(2010, 1, 1), '20100330', split_str="_")
        '20100101_20100330'
        """
        start_date = start_date if start_date else self.valid_date_start
        end_date = end_date if end_date else self.valid_date_end
        return self._date_range_to_str(
            start_date, end_date, in_format_str, out_format_str, split_str
        )

    def _date_range_to_str(
        self,
        start_date: DateType,
        end_date: DateType,
        in_format_str: Optional[str] = None,
        out_format_str: Optional[str] = None,
        split_str: Optional[str] = None,
    ) -> str:
        """Return date range as `str` from `calib_date_start` to `calib_date_end`.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> config._date_range_to_str('20100101', '20100330')
        '20100101-20100330'
        >>> config._date_range_to_str(date(2010, 1, 1), '20100330')
        '20100101-20100330'
        >>> config._date_range_to_str(date(2010, 1, 1), '20100330', split_str="_")
        '20100101_20100330'
        """
        in_format_str = in_format_str if in_format_str else self.date_format_str
        out_format_str = out_format_str if out_format_str else self.date_format_str
        split_str = split_str if split_str else self.date_split_str
        return date_range_to_str(
            start_date=start_date,
            end_date=end_date,
            in_format_str=in_format_str,
            out_format_str=out_format_str,
            split_str=split_str,
        )

    def mod_path(self, city: Optional[str] = None) -> Path:
        """Return city estimates path.

        Examples
        --------
        >>> if not is_data_mounted:
        ...     pytest.skip('requires linux server mount paths')
        >>> config: RunConfig = RunConfig()
        >>> assert False
        >>> config.mod_path()
        PosixPath('/.../ClimateData/Cropped/three.cities/CPM/Manchester')
        >>> config.mod_path('Glasgow')
        PosixPath('/.../ClimateData/Cropped/three.cities/CPM/Glasgow')
        """
        city = city if city else self.city
        return self.data_path / self.mod_folder / city

    def obs_path(self, city: Optional[str] = None) -> Path:
        """Return city observations path.

        Examples
        --------
        >>> if not is_data_mounted:
        ...     pytest.skip('requires linux server mount paths')
        >>> config: RunConfig = RunConfig()
        >>> config.obs_path()
        PosixPath('/.../ClimateData/Cropped/three.cities/Hads.updated360/Manchester')
        >>> config.obs_path('Glasgow')
        PosixPath('/.../ClimateData/Cropped/three.cities/Hads.updated360/Glasgow')
        """
        city = city if city else self.city
        return self.data_path / self.obs_folder / city

    def preprocess_out_path(
        self,
        city: Optional[str] = None,
        run: Optional[str] = None,
        variable: Optional[str] = None,
    ) -> Path:
        """Return path to save results.

        Examples
        --------
        >>> if not is_data_mounted:
        ...     pytest.skip('requires linux server mount paths')
        >>> config: RunConfig = RunConfig()
        >>> config.preprocess_out_path()
        PosixPath('.../ClimateData/Cropped/three.cities/Preprocessed/Manchester/05/tasmax')
        >>> config.preprocess_out_path(city='Glasgow', run='07')
        PosixPath('.../ClimateData/Cropped/three.cities/Preprocessed/Glasgow/07/tasmax')
        """
        city = city if city else self.city
        run = run if run else self.run
        variable = variable if variable else self.variable
        return (
            self.data_path / self.preprocess_out_folder / city / run / variable
        ).resolve()

    def cmethods_out_path(
        self,
        city: Optional[str] = None,
        run: Optional[str] = None,
    ) -> Path:
        """Return path to save cmethods results.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> config.cmethods_out_path()
        PosixPath('/.../ClimateData/Debiased/three.cities.cropped/Manchester/05')
        >>> config.cmethods_out_path(city='Glasgow', run='07')
        PosixPath('/.../ClimateData/Debiased/three.cities.cropped/Glasgow/07')
        """
        city = city if city else self.city
        run = run if run else self.run
        return (self.data_path / self.cmethods_out_folder / city / run).resolve()

    @property
    def run_prefix_tuple(self) -> tuple[str, ...]:
        """Split `self.run_prefix` by ' ' to a `tuple`.

        Examples
        --------
        >>> config: RunConfig = RunConfig(run_prefix='python -m')
        >>> config.run_prefix_tuple
        ('python', '-m')
        """
        return tuple(self.run_prefix.split(" "))

    def to_cli_preprocess_tuple(
        self,
        variable: Optional[str] = None,
        run: Optional[str] = None,
        city: Optional[str] = None,
        calib_start: Optional[DateType] = None,
        calib_end: Optional[DateType] = None,
        valid_start: Optional[DateType] = None,
        valid_end: Optional[DateType] = None,
    ) -> tuple[Union[str, PathLike], ...]:
        """Generate a `tuple` of `str` for a command line command.

        This will leave `Path` objects uncoverted. See
        `self.to_cli_preprocess_tuple_strs` for passing to a terminal.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> command_str_tuple: tuple[str, ...] = config.to_cli_preprocess_tuple()
        >>> assert command_str_tuple == CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_CORRECT
        """
        city = city if city else self.city
        variable = variable if variable else self.variable
        run = run if run else self.run

        mod_path: Path = self.mod_path(city=city)
        obs_path: Path = self.obs_path(city=city)
        preprocess_out_path: Path = self.preprocess_out_path(
            city=city, run=run, variable=variable
        )
        calib_dates_str: str = self.calib_dates_to_str(
            start_date=calib_start, end_date=calib_end
        )
        valid_dates_str: str = self.valid_dates_to_str(
            start_date=valid_start, end_date=valid_end
        )

        return (
            *self.run_prefix_tuple,
            self.preprocess_data_file,
            "--mod",
            mod_path,
            "--obs",
            obs_path,
            "-v",
            variable,
            "-r",
            run,
            "--out",
            preprocess_out_path,
            "--calib_dates",
            calib_dates_str,
            "--valid_dates",
            valid_dates_str,
        )

    def to_cli_preprocess_tuple_strs(
        self,
        variable: Optional[str] = None,
        run: Optional[str] = None,
        city: Optional[str] = None,
        calib_start: Optional[DateType] = None,
        calib_end: Optional[DateType] = None,
        valid_start: Optional[DateType] = None,
        valid_end: Optional[DateType] = None,
    ) -> tuple[str, ...]:
        """Generate a command line interface `str` `tuple` a test example.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> command_str_tuple: tuple[str, ...] = config.to_cli_preprocess_tuple_strs()
        >>> assert command_str_tuple == CLI_PREPROCESS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
        """
        return iter_to_tuple_strs(
            self.to_cli_preprocess_tuple(
                variable=variable,
                run=run,
                city=city,
                calib_start=calib_start,
                calib_end=calib_end,
                valid_start=valid_start,
                valid_end=valid_end,
            )
        )

    def to_cli_preprocess_str(
        self,
        variable: Optional[str] = None,
        run: Optional[str] = None,
        city: Optional[str] = None,
        calib_start: Optional[DateType] = None,
        calib_end: Optional[DateType] = None,
        valid_start: Optional[DateType] = None,
        valid_end: Optional[DateType] = None,
    ) -> str:
        """Generate a command line interface str as a test example.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> config.to_cli_preprocess_str() == CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT
        True
        >>> CLI_PREPROCESS_DEFAULT_COMMAND_STR_CORRECT
        'python preprocess_data.py --mod /.../CPM/Manchester...'
        """
        return " ".join(
            self.to_cli_preprocess_tuple_strs(
                variable=variable,
                run=run,
                city=city,
                calib_start=calib_start,
                calib_end=calib_end,
                valid_start=valid_start,
                valid_end=valid_end,
            )
        )

    def yield_mod_folder(
        self, city: Optional[str] = None
    ) -> Generator[Path, None, None]:
        """`Iterable` of all `Path`s in `self.mod_folder`.

        Examples
        --------
        >>> if not is_data_mounted:
        ...     pytest.skip('requires linux server mount paths')
        >>> config: RunConfig = RunConfig()
        >>> len(tuple(config.yield_mod_folder())) == MOD_FOLDER_FILES_COUNT_CORRECT
        True
        """
        city = city if city else self.city
        return path_iterdir(self.obs_path(city=city))

    def yield_obs_folder(
        self, city: Optional[str] = None
    ) -> Generator[Path, None, None]:
        """`Iterable` of all `Path`s in `self.obs_folder`.

        Examples
        --------
        >>> if not is_data_mounted:
        ...     pytest.skip('requires linux server mount paths')
        >>> config: RunConfig = RunConfig()
        >>> len(tuple(config.yield_obs_folder())) == OBS_FOLDER_FILES_COUNT_CORRECT
        True
        """
        city = city if city else self.city
        return path_iterdir(self.obs_path(city=city))

    def yield_preprocess_out_folder(
        self,
        city: Optional[str] = None,
        run: Optional[str] = None,
        variable: Optional[str] = None,
    ) -> Generator[Path, None, None]:
        """`Iterable` of all `Path`s in `self.preprocess_out_folder`.

        Examples
        --------
        >>> if not is_data_mounted:
        ...     pytest.skip('requires linux server mount paths')
        >>> config: RunConfig = RunConfig()
        >>> (len(tuple(config.yield_preprocess_out_folder())) ==
        ...  PREPROCESS_OUT_FOLDER_FILES_COUNT_CORRECT)
        True
        """
        city = city if city else self.city
        run = run if run else self.run
        variable = variable if variable else self.variable
        return path_iterdir(
            self.preprocess_out_path(city=city, run=run, variable=variable)
        )

    @property
    def command_path(self) -> Path:
        """Return command path relative to running tests."""
        return (Path() / self.command_dir).absolute()

    def to_cli_run_cmethods_tuple(
        self,
        city: Optional[str] = None,
        run: Optional[str] = None,
        variable: Optional[str] = None,
        method: Optional[str] = None,
        input_data_path: Optional[PathLike] = None,
        cmethods_out_path: Optional[PathLike] = None,
        processors: Optional[int] = None,
    ) -> tuple[Union[str, PathLike], ...]:
        """Generate a `tuple` of `str` for a command line command.

        This will leave `Path` objects uncoverted. See
        `self.to_cli_run_cmethods_tuple_strs` for passing to a terminal.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> command_str_tuple: tuple[str, ...] = config.to_cli_run_cmethods_tuple()
        >>> assert command_str_tuple == CLI_CMETHODS_DEFAULT_COMMAND_TUPLE_CORRECT
        """
        city = city if city else self.city
        variable = variable if variable else self.variable
        run = run if run else self.run
        method = method if method else self.method
        processors = processors if processors else self.processors

        input_data_path = (
            input_data_path
            if input_data_path
            else self.preprocess_out_path(city=city, run=run, variable=variable)
        )

        cmethods_out_path = (
            cmethods_out_path
            if cmethods_out_path
            else self.cmethods_out_path(city=city, run=run)
        )

        return (
            *self.run_prefix_tuple,
            self.run_cmethods_file,
            "--input_data_folder",
            input_data_path,
            "--out",
            cmethods_out_path,
            "--method",
            method,
            "-v",
            variable,
            "-p",
            processors,
        )

    def to_cli_run_cmethods_tuple_strs(
        self,
        city: Optional[str] = None,
        run: Optional[str] = None,
        variable: Optional[str] = None,
        method: Optional[str] = None,
        input_data_path: Optional[PathLike] = None,
        cmethods_out_path: Optional[PathLike] = None,
        processors: Optional[int] = None,
    ) -> tuple[str, ...]:
        """Generate a command line interface `str` `tuple` a test example.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> command_str_tuple: tuple[str, ...] = config.to_cli_run_cmethods_tuple_strs()
        >>> assert command_str_tuple == CLI_CMEHTODS_DEFAULT_COMMAND_TUPLE_STR_CORRECT
        """
        return iter_to_tuple_strs(
            self.to_cli_run_cmethods_tuple(
                city=city,
                run=run,
                variable=variable,
                method=method,
                input_data_path=input_data_path,
                cmethods_out_path=cmethods_out_path,
                processors=processors,
            )
        )

    def to_cli_run_cmethods_str(
        self,
        city: Optional[str] = None,
        run: Optional[str] = None,
        variable: Optional[str] = None,
        method: Optional[str] = None,
        input_data_path: Optional[PathLike] = None,
        cmethods_out_path: Optional[PathLike] = None,
        processors: Optional[int] = None,
    ) -> str:
        """Generate a command line interface str as a test example.

        Examples
        --------
        >>> config: RunConfig = RunConfig()
        >>> config.to_cli_run_cmethods_str() == CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT
        True
        >>> CLI_CMETHODS_DEFAULT_COMMAND_STR_CORRECT
        'python run_cmethods.py...--method quantile_delta_mapping...'
        """
        return " ".join(
            self.to_cli_run_cmethods_tuple_strs(
                city=city,
                run=run,
                variable=variable,
                method=method,
                input_data_path=input_data_path,
                cmethods_out_path=cmethods_out_path,
                processors=processors,
            )
        )
