"""
Test generating and running `debiasing` scripts

"""
import pytest
from pathlib import Path
from os import system, PathLike
from dataclasses import dataclass
from typing import Final, Generator
from datetime import date, datetime
import subprocess

from utils import (
    DATE_FORMAT_SPLIT_STR, DATE_FORMAT_STR, DateType, date_to_str,
    iter_to_tuple_strs, date_range_to_str, path_iterdir
)


DATA_PATH_DEFAULT: Final[Path] = Path('/mnt/vmfileshare/ClimateData/Cropped/three.cities/')

RUN_NAME_DEFAULT: Final[str] = '05'
VARIABLE_NAME_DEFAULT: Final[str] = "tasmax"

CITY_NAME_DEFAULT: Final[str] = "Manchester"

MOD_FOLDER_DEFUALT: Final[Path] = Path('CPM')
OBS_FOLDER_DEFUALT: Final[Path] = Path('Hads.updated360')
OUT_FOLDER_DEFUALT: Final[Path] = Path('Preprocessed')

CALIB_DATE_START_DEFAULT: DateType = date(1981, 1, 1)
CALIB_DATE_END_DEFAULT: DateType = date(1981, 12, 30)

VALID_DATE_START_DEFAULT: DateType = date(2010, 1, 1)
VALID_DATE_END_DEFAULT: DateType = date(2010, 3, 30)

CALIB_DATES_STR_DEFAULT: Final[str] = date_range_to_str(
    CALIB_DATE_START_DEFAULT, CALIB_DATE_END_DEFAULT
)
VALID_DATES_STR_DEFAULT: Final[str] = date_range_to_str(
    VALID_DATE_START_DEFAULT, VALID_DATE_END_DEFAULT
)


CLI_DEBIASING_DEFAULT_COMMAND_TUPLE_CORRECT: Final[tuple[str]] = (
    "python", "preprocess_data.py",
    "--mod", DATA_PATH_DEFAULT / MOD_FOLDER_DEFUALT / CITY_NAME_DEFAULT,
    "--obs", DATA_PATH_DEFAULT / OBS_FOLDER_DEFUALT / CITY_NAME_DEFAULT,
    "-v", VARIABLE_NAME_DEFAULT,
    "-r", RUN_NAME_DEFAULT,
    "--out", (DATA_PATH_DEFAULT / OUT_FOLDER_DEFUALT / CITY_NAME_DEFAULT /
              RUN_NAME_DEFAULT  / VARIABLE_NAME_DEFAULT),
    "--calib_dates", CALIB_DATES_STR_DEFAULT,
    "--valid_dates", VALID_DATES_STR_DEFAULT,
)
CLI_DEBIASING_DEFAULT_COMMAND_STR_CORRECT: Final[str] = ' '.join(iter_to_tuple_strs(CLI_DEBIASING_DEFAULT_COMMAND_TUPLE_CORRECT))

MOD_FOLDER_FILES_COUNT_CORRECT: Final[int] = 1478
OBS_FOLDER_FILES_COUNT_CORRECT: Final[int] = MOD_FOLDER_FILES_COUNT_CORRECT
OUT_FOLDER_FILES_COUNT_CORRECT: Final[int] = 4


@dataclass
class RunConfig:
    variable: str = VARIABLE_NAME_DEFAULT
    run: str = RUN_NAME_DEFAULT
    city: str = CITY_NAME_DEFAULT
    method_1: str = "quantile_delta_mapping"
    method_2: str = "variance_scaling"
    run_prefix: str = 'python preprocess_data.py'

    data_path: Path = DATA_PATH_DEFAULT
    mod_folder: PathLike = MOD_FOLDER_DEFUALT
    obs_folder: PathLike = OBS_FOLDER_DEFUALT
    out_folder: PathLike = OUT_FOLDER_DEFUALT

    calib_date_start: DateType = CALIB_DATE_START_DEFAULT
    calib_date_end: DateType = CALIB_DATE_END_DEFAULT

    valid_date_start: DateType = VALID_DATE_START_DEFAULT
    valid_date_end: DateType =  VALID_DATE_END_DEFAULT

    processes: int = 32

    date_format_str: str = DATE_FORMAT_STR
    date_split_str: str = DATE_FORMAT_SPLIT_STR

    def calib_dates_to_str(self, 
            start_date: DateType,
            end_date: DateType,
            in_format_str: str | None = None,
            out_format_str: str | None = None,
            split_str: str | None = None) -> str:
        """Return date range as `str` from `calib_date_start` to `calib_date_end`.

        Example
        -------

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
        return self._date_range_to_str(start_date, end_date, in_format_str, out_format_str, split_str)

    def valid_dates_to_str(self, 
            start_date: DateType,
            end_date: DateType,
            in_format_str: str | None = None,
            out_format_str: str | None = None,
            split_str: str | None = None) -> str:
        """Return date range as `str` from `valid_date_start` to `valid_date_end`.

        Example
        -------

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
        return self._date_range_to_str(start_date, end_date, in_format_str, out_format_str, split_str)

    def _date_range_to_str(self,
            start_date: DateType,
            end_date: DateType,
            in_format_str: str | None = None,
            out_format_str: str | None = None,
            split_str: str | None = None) -> str:
        """Return date range as `str` from `calib_date_start` to `calib_date_end`.

        Example
        -------

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
                split_str=split_str)

    def mod_path(self, city: str | None = None) -> Path:
        """Return city estimates path.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> config.mod_path()
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/CPM/Manchester')
        >>> config.mod_path('Glasgow')
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/CPM/Glasgow')

        """
        city = city if city else self.city
        return self.data_path / self.mod_folder / city
    
    def obs_path(self, city: str | None = None) -> Path:
        """Return city observations path.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> config.obs_path()
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/Hads.updated360/Manchester')
        >>> config.obs_path('Glasgow')
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/Hads.updated360/Glasgow')

        """
        city = city if city else self.city
        return self.data_path / self.obs_folder / city
    
    def out_path(self, city: str | None = None, run: str | None = None, variable: str | None = None) -> Path:
        """Return path to save results.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> config.out_path()
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/Preprocessed/Manchester/05/tasmax')
        >>> config.out_path(city='Glasgow', run='07')
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/Preprocessed/Glasgow/07/tasmax')

        """
        city = city if city else self.city
        run = run if run else self.run
        variable = variable if variable else self.variable
        return self.data_path / self.out_folder / city / run / variable

    @property
    def run_prefix_tuple(self) -> tuple[str, ...]:
        """Split `self.run_prefix` by ' ' to a `tuple`.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> config.run_prefix_tuple
        ('python', 'preprocess_data.py')

        """
        return tuple(self.run_prefix.split(' '))

    def to_cli_preprocess_tuple(self,
            variable: str | None = None,
            run: str | None = None,
            city: str | None = None,
            calib_start: DateType | None = None,
            calib_end: DateType | None = None,
            valid_start: DateType | None = None,
            valid_end: DateType | None = None,
        ) -> tuple[str | PathLike, ...]:
        """Generate a `tuple` of `str` for a command line command.

        Note
        ----

        This will leave `Path` objects uncoverted. See
        `self.to_cli_preprocess_tuple_strs` for passing to a terminal.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> command_str_tuple: tuple[str, ...] = config.to_cli_preprocess_tuple()
        >>> assert command_str_tuple == CLI_DEBIASING_DEFAULT_COMMAND_TUPLE_CORRECT

        """
        city = city if city else self.city
        variable = variable if variable else self.variable
        run = run if run else self.run

        mod_path: Path = self.mod_path(city=city)
        obs_path: Path = self.obs_path(city=city)
        out_path: Path = self.out_path(city=city, run=run, variable=variable)
        calib_dates_str: str = self.calib_dates_to_str(start_date=calib_start, end_date=calib_end)
        valid_dates_str: str = self.valid_dates_to_str(start_date=valid_start, end_date=valid_end)

        return (
            *self.run_prefix_tuple, 
            '--mod', mod_path,
            '--obs', obs_path, 
            '-v', variable,
            '-r', run,
            '--out', out_path,
            '--calib_dates', calib_dates_str,
            '--valid_dates', valid_dates_str,
        )

    def to_cli_preprocess_tuple_strs(self,
            variable: str | None = None,
            run: str | None = None,
            city: str | None = None,
            calib_start: DateType | None = None,
            calib_end: DateType | None = None,
            valid_start: DateType | None = None,
            valid_end: DateType | None = None,
        ) -> tuple[str, ...]:
        """Generate a command line interface `str` `tuple` a test example.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> command_str_tuple: tuple[str, ...] = config.to_cli_preprocess_tuple()
        >>> assert command_str_tuple == CLI_DEBIASING_DEFAULT_COMMAND_TUPLE_CORRECT

        """
        return iter_to_tuple_strs(self.to_cli_preprocess_tuple(
            variable=variable,
            run=run,
            city=city,
            calib_start=calib_start,
            calib_end=calib_end,
            valid_start=valid_start,
            valid_end=valid_end,
        ))


    def to_cli_preprocess_str(self,
            variable: str | None = None,
            run: str | None = None,
            city: str | None = None,
            calib_start: DateType | None = None,
            calib_end: DateType | None = None,
            valid_start: DateType | None = None,
            valid_end: DateType | None = None,
        ) -> str:
        """Generate a command line interface str as a test example.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> config.to_cli_preprocess_str() == CLI_DEBIASING_DEFAULT_COMMAND_STR_CORRECT
        True
        >>> CLI_DEBIASING_DEFAULT_COMMAND_STR_CORRECT[:96]  #doctest: +ELLIPSIS
        'python preprocess_data.py --mod /.../CPM/Manchester'

        """
        return ' '.join(self.to_cli_preprocess_tuple_strs(
            variable=variable,
            run=run,
            city=city,
            calib_start=calib_start,
            calib_end=calib_end,
            valid_start=valid_start,
            valid_end=valid_end,
        ))

    def list_mod_folder(self, city: str | None = None) -> Generator[Path, None, None]:
        """`Iterable` of all `Path`s in `self.mod_folder`.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> len(tuple(config.list_mod_folder())) == MOD_FOLDER_FILES_COUNT_CORRECT
        True
        """
        return path_iterdir(self.obs_path(city=city))

    def list_obs_folder(self, city: str | None = None) -> Generator[Path, None, None]:
        """`Iterable` of all `Path`s in `self.obs_folder`.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> len(tuple(config.list_obs_folder())) == OBS_FOLDER_FILES_COUNT_CORRECT
        True
        """
        return path_iterdir(self.obs_path(city=city))

    def list_out_folder(self, city: str | None = None, run: str | None = None, variable: str | None = None) -> Generator[Path, None, None]:
        """`Iterable` of all `Path`s in `self.out_folder`.

        Example
        -------

        >>> config: RunConfig = RunConfig()
        >>> len(tuple(config.list_out_folder())) == OUT_FOLDER_FILES_COUNT_CORRECT
        True
        """
        return path_iterdir(self.out_path(city=city, run=run, variable=variable))


@pytest.fixture
def run_config(tmp_path: Path) -> RunConfig:
    """Generate a `RunConfig` instance to ease paramaterizing tests."""
    return RunConfig(out_folder=tmp_path)


def test_command_line_default() -> None:
    """Test default generated cli `str`."""
    run_config: RunConfig = RunConfig()
    assert run_config.to_cli_preprocess_str() == CLI_DEBIASING_DEFAULT_COMMAND_STR_CORRECT


@pytest.mark.parametrize(
    'run_kwargs, out_count', (
        ({}, 0),
        ({'city': 'Glasgow'}, 0),
    )
)
def test_run(run_config, run_kwargs, out_count, capsys) -> None:
    """Test running generated command script via a subprocess."""
    process_complete: subprocess.CompletedProcess = (
        subprocess.run(run_config.to_cli_preprocess_tuple_strs(**run_kwargs), shell=True, check=True)
    )
    assert process_complete.returncode == 0
    assert len(tuple(run_config.list_mod_folder())) == MOD_FOLDER_FILES_COUNT_CORRECT
    assert len(tuple(run_config.list_obs_folder())) == OBS_FOLDER_FILES_COUNT_CORRECT
    assert len(tuple(run_config.list_out_folder())) == out_count
