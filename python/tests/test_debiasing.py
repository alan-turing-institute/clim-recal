"""
Test generating and running `debiasing` scripts

"""
import pytest
from pathlib import Path
from os import system, PathLike
from dataclasses import dataclass
from typing import Final
from datetime import date, datetime


DATA_PATH: Final[Path] = Path('/mnt/vmfileshare/ClimateData/Cropped/three.cities/')
DateType = date | str
DATE_FORMAT_STR: Final[str] = '%Y%m%d'
DATE_FORMAT_SPLIT_STR: Final[str] = '-'

CORRECT_CLI_DEBIASING_DEFAULT_COMMAND: Final[str] = (
    "python preprocess_data.py "
    "--mod /mnt/vmfileshare/ClimateData/Cropped/three.cities/CPM/Manchester "
    "--obs /mnt/vmfileshare/ClimateData/Cropped/three.cities/Hads.original360/Manchester "
    "-v tasmax "
    "-r 05 "
    "--out /mnt/vmfileshare/ClimateData/Cropped/three.cities/Preprocessed/Manchester/05/tasmax "
    "--calib_dates 19810101-19811230 "
    "--valid_dates 20100101-20100330"
)

    
def date_to_str(date_obj: DateType, in_format_str: str = DATE_FORMAT_STR, out_format_str: str = DATE_FORMAT_STR) -> str:
    """Return a `str` in `date_format_str` of `date_obj`.

    Example
    -------

    >>> date_to_str('20100101')
    '20100101'
    >>> date_to_str(date(2010, 1, 1))
    '20100101'

    """
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, in_format_str).date()
    return date_obj.strftime(out_format_str)


def date_range_to_str(
    start_date: DateType,
    end_date: DateType, 
    split_str: str = DATE_FORMAT_SPLIT_STR,
    in_format_str: str = DATE_FORMAT_STR,
    out_format_str: str = DATE_FORMAT_STR,
) -> str:
    """Take `start_date` and `end_date` `str` or `date` instances and return a range `str`.

    Example
    -------

    >>> date_range_to_str('20100101', '20100330')
    '20100101-20100330'
    >>> date_range_to_str(date(2010, 1, 1), '20100330')
    '20100101-20100330'

    """
    start_date = date_to_str(start_date,
        in_format_str=in_format_str,
        out_format_str=out_format_str)
    end_date = date_to_str(end_date,
        in_format_str=in_format_str,
        out_format_str=out_format_str)
    return f'{start_date}{split_str}{end_date}'


@dataclass
class RunConfig:
    variable: str = 'tasmax'
    run: str = '05'
    city: str = 'Manchester'
    method_1: str = "quantile_delta_mapping"
    method_2: str = "variance_scaling"
    run_prefix: str = 'python preprocess_data.py'

    data_path: Path = DATA_PATH
    mod_folder: PathLike = 'CPM'
    obs_folder: PathLike = 'Hads.original360'
    out_folder: PathLike = 'Preprocessed'

    calib_date_start: DateType = date(1981, 1, 1)
    calib_date_end: DateType = date(1981, 12, 30)

    valid_date_start: DateType = date(2010, 1, 1)
    valid_date_end: DateType = date(2010, 3, 30)

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
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/Hads.original360/Manchester')
        >>> config.obs_path('Glasgow')
        PosixPath('/mnt/vmfileshare/ClimateData/Cropped/three.cities/Hads.original360/Glasgow')

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
        >>> config.to_cli_preprocess_str() == CORRECT_CLI_DEBIASING_DEFAULT_COMMAND
        True
        >>> CORRECT_CLI_DEBIASING_DEFAULT_COMMAND[:96]  #doctest: +ELLIPSIS
        'python preprocess_data.py --mod /.../CPM/Manchester'

        """
        city = city if city else self.city
        variable = variable if variable else self.variable
        run = run if run else self.run

        mod_path: Path = self.mod_path(city=city)
        obs_path: Path = self.obs_path(city=city)
        out_path: Path = self.out_path(city=city, run=run, variable=variable)
        calib_dates_str: str = self.calib_dates_to_str(start_date=calib_start, end_date=calib_end)
        valid_dates_str: str = self.valid_dates_to_str(start_date=valid_start, end_date=valid_end)

        return ' '.join((
                    self.run_prefix, 
                    f'--mod {mod_path}',
                    f'--obs {obs_path}', 
                    f'-v {variable}',
                    f'-r {run}',
                    f'--out {out_path}',
                    f'--calib_dates {calib_dates_str}',
                    f'--valid_dates {valid_dates_str}',
                )
            )


def test_command_line_default() -> None:
    """Test default generated cli `str`."""
    config: RunConfig = RunConfig()
    assert config.to_cli_preprocess_str() == CORRECT_CLI_DEBIASING_DEFAULT_COMMAND
