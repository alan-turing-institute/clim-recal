"""Utility functions."""
import subprocess
import sys
from copy import deepcopy
from csv import DictReader
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from logging import getLogger
from os import PathLike, chdir
from pathlib import Path
from shutil import rmtree
from typing import (
    Any,
    Callable,
    Final,
    Generator,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Union,
)

logger = getLogger(__name__)

DateType = Union[date, str]
DATE_FORMAT_STR: Final[str] = "%Y%m%d"
ISO_DATE_FORMAT_STR: Final[str] = "%Y-%m-%d"
DATE_FORMAT_SPLIT_STR: Final[str] = "-"
RSTUDIO_DOCKER_USER_PATH: Path = Path("/home/rstudio")
JUPYTER_DOCKER_USER_PATH: Path = Path("/home/jovyan")
DEBIAN_HOME_PATH: Path = Path("/home/")

DEFAULT_CONDA_LOCK_PATH: Final[PathLike] = Path("../conda-lock.yml")
DEFAULT_ENV_PATHS: Final[tuple[PathLike, ...]] = (
    Path("../environment.yml"),
    Path("pyproject.toml"),
)
DEFAULT_CONDA_LOCK_KWARGS: Final[dict[str, str | float | bool]] = {
    "check-input-hash": True,
}
CONDA_LEGACY_PREFIX: Final[PathLike] = "."

GITHUB_ACTIONS_ARCHITECTURE: Final[str] = "linux-64"

NORMAL_YEAR_DAYS: Final[int] = 365
LEAP_YEAR_DAYS: Final[int] = NORMAL_YEAR_DAYS + 1
CPM_YEAR_DAYS: Final[int] = 360

MODULE_NAMES: Final[tuple[PathLike, ...]] = ("clim_recal",)
CURRENT_PATH = Path().absolute()
PYTHON_PACKAGE_DIR_NAME: Final[Path] = Path("python")

GLASGOW_COORDS: Final[tuple[float, float]] = (55.86279, -4.25424)
MANCHESTER_COORDS: Final[tuple[float, float]] = (53.48095, -2.23743)
LONDON_COORDS: Final[tuple[float, float]] = (51.509865, -0.118092)
THREE_CITY_COORDS: Final[dict[str, tuple[float, float]]] = {
    "Glasgow": GLASGOW_COORDS,
    "Manchester": MANCHESTER_COORDS,
    "London": LONDON_COORDS,
}
"""Coordinates of Glasgow, Manchester and London as `(lon, lat)` `tuples`."""


def ensure_date(date_to_check: DateType, format_str: str = DATE_FORMAT_STR) -> date:
    """Ensure passed `date_to_check` is a `date`.

    Parameters
    ----------
    date_to_check
        Date or `str` to ensure is a `date`.
    format_str
        `strptime` `str` to convert `date_to_check` if necessary.

    Returns
    -------
    `date` instance from `date_to_check`.

    Examples
    --------
    >>> ensure_date('19801130')
    datetime.date(1980, 11, 30)
    >>> ensure_date(date(1980, 11, 30))
    datetime.date(1980, 11, 30)
    """
    if isinstance(date_to_check, date):
        return date_to_check
    else:
        return datetime.strptime(date_to_check, format_str).date()


def date_range_generator(
    start_date: DateType,
    end_date: DateType,
    inclusive: bool = False,
    skip_dates: Iterable[DateType] | DateType | None = None,
    start_format_str: str = DATE_FORMAT_STR,
    end_format_str: str = DATE_FORMAT_STR,
    output_format_str: str = DATE_FORMAT_STR,
    skip_dates_format_str: str = DATE_FORMAT_STR,
    yield_type: type[date] | type[str] = date,
) -> Iterator[DateType]:
    """Return a tuple of `DateType` objects.

    Parameters
    ----------
    start_date
        `DateType` at start of time series.
    end_date
        `DateType` at end of time series.
    inclusive
        Whether to include the `end_date` in the returned time series.
    skip_dates
        Dates to skip between `start_date` and `end_date`.
    start_format_str
        A `strftime` format to apply if `start_date` `type` is `str`.
    end_format_str
        A `strftime` format to apply if `end_date` `type` is `str`.
    output_format_str
        A `strftime` format to apply if `yield_type` is `str`.
    skip_dates_format_str
        A `strftime` format to apply if any `skip_dates` are `str`.
    yield_type
        Whether which date type to return in `tuple` (`date` or `str`).

    Returns
    -------
    :
        A `tuple` of `date` or `str` objects (only one type throughout).

    Examples
    --------
    >>> four_years: tuple[date] = tuple(date_range_generator('19801130', '19841130'))
    >>> len(four_years)
    1461
    >>> four_years_inclusive: tuple[date] = tuple(
    ...     date_range_generator('1980-11-30', '19841130',
    ...                          inclusive=True,
    ...                          start_format_str=ISO_DATE_FORMAT_STR))
    >>> len(four_years_inclusive)
    1462
    >>> four_years_inclusive_skip: tuple[date] = tuple(
    ...     date_range_generator('19801130', '19841130',
    ...                          inclusive=True,
    ...                          skip_dates='19840229'))
    >>> len(four_years_inclusive_skip)
    1461
    >>> skip_dates: tuple[date] = (date(1981, 12, 1), date(1982, 12, 1))
    >>> four_years_inclusive_skip: tuple[date] = list(
    ...     date_range_generator('19801130', '19841130',
    ...                          inclusive=True,
    ...                          skip_dates=skip_dates))
    >>> len(four_years_inclusive_skip)
    1460
    >>> skip_dates in four_years_inclusive_skip
    False
    """
    start_date = ensure_date(start_date, start_format_str)
    end_date = ensure_date(end_date, end_format_str)
    if inclusive:
        end_date += timedelta(days=1)
    try:
        assert start_date < end_date
    except AssertionError:
        raise ValueError(
            f"start_date: {start_date} must be before end_date: {end_date}"
        )
    if skip_dates:
        if isinstance(skip_dates, str | date):
            skip_dates = [skip_dates]
        skip_dates = set(
            ensure_date(skip_date, skip_dates_format_str) for skip_date in skip_dates
        )
    for day_number in range(int((end_date - start_date).days)):
        date_obj: date = start_date + timedelta(day_number)
        if skip_dates:
            if date_obj in skip_dates:
                continue
        yield (date_obj if yield_type == date else date_obj.strftime(output_format_str))


def check_package_path(strict: bool = True, try_chdir: bool = False) -> bool:
    """Return path for test running.

    Parameters
    ----------
    strict
        Whether to raise a `ValueError` if check fails.
    try_chdir
        Whether to attempt changing directory if initial check fails

    Raises
    ------
    ValueError
        If `strict` and checks fail.

    Returns
    -------
    Whether to check if call was successful.

    Examples
    --------
    >>> check_package_path()
    True
    >>> chdir('..')
    >>> check_package_path(strict=False)
    False
    >>> check_package_path()
    Traceback (most recent call last):
        ...
    ValueError: 'clim-recal' pipeline must be run in...
    >>> check_package_path(try_chdir=True)
    True
    """
    current_path: Path = Path()
    if not set(MODULE_NAMES) <= set(path.name for path in current_path.iterdir()):
        if try_chdir:
            chdir(PYTHON_PACKAGE_DIR_NAME)
            return check_package_path(strict=strict, try_chdir=False)
        elif strict:
            raise ValueError(
                f"'clim-recal' pipeline must be "
                f"run in 'clim-recal/{PYTHON_PACKAGE_DIR_NAME}', "
                f"not '{current_path.absolute()}'"
            )
        else:
            return False
    else:
        return True


def is_platform_darwin() -> bool:
    """Check if `sys.platform` is `Darwin` (macOS)."""
    return sys.platform.startswith("darwin")


def date_to_str(
    date_obj: DateType,
    in_format_str: str = DATE_FORMAT_STR,
    out_format_str: str = DATE_FORMAT_STR,
) -> str:
    """Return a `str` in `date_format_str` of `date_obj`.

    Parameters
    ----------
    date_obj
        A `datetime.date` or `str` object to convert.
    in_format_str
        A `strftime` format `str` to convert `date_obj` from if `date_obj` is a `str`.
    out_format_str
        A `strftime` format `str` to convert `date_obj` to.

    Returns
    -------
    :
        A `str` version of `date_obj` in `out_format_str` format.

    Examples
    --------
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

    Parameters
    ----------
    start_date
        First date in range.
    end_date
        Last date in range
    split_str
        `char` to split returned date range `str`.
    in_format_str
        A `strftime` format `str` to convert `start_date` from.
    out_format_str
        A `strftime` format `str` to convert `end_date` from.

    Returns
    -------
    :
        A `str` of date range from `start_date` to `end_date` in the
        `out_format_str` format.

    Examples
    --------
    >>> date_range_to_str('20100101', '20100330')
    '20100101-20100330'
    >>> date_range_to_str(date(2010, 1, 1), '20100330')
    '20100101-20100330'

    """
    start_date = date_to_str(
        start_date, in_format_str=in_format_str, out_format_str=out_format_str
    )
    end_date = date_to_str(
        end_date, in_format_str=in_format_str, out_format_str=out_format_str
    )
    return f"{start_date}{split_str}{end_date}"


def iter_to_tuple_strs(
    iter_var: Iterable[Any], func: Callable[[Any], str] = str
) -> tuple[str, ...]:
    """Return a `tuple` with all components converted to `strs`.

    Parameters
    ----------
    iter_var
        Iterable of objects that can be converted into `strs`.
    func
        Callable to convert each `obj` in `iter_val` to a `str`.

    Returns
    -------
    :
        A `tuple` of all `iter_var` elements in `str` format.

    Examples
    --------
    >>> iter_to_tuple_strs(['cat', 1, Path('a/path')])
    ('cat', '1', 'a/path')
    >>> iter_to_tuple_strs(
    ...     ['cat', 1, Path('a/path')],
    ...     lambda x: f'{x:02}' if type(x) == int else str(x))
    ('cat', '01', 'a/path')

    """
    return tuple(func(obj) for obj in iter_var)


def path_iterdir(
    path: PathLike, strict: bool = False
) -> Generator[Optional[Path], None, None]:
    """Return an `Generator` after ensuring `path` exists.

    Parameters
    ----------
    path
        `Path` to folder to iterate through.
    strict
        Whether to raise `FileNotFoundError` if `path` not found.

    #Yields
    #------
    #A `Path` for each folder  in `path`.

    Raises
    ------
    FileNotFoundError
        Raised if `strict = True` and `path` does not exist.

    Returns
    -------
    :
        `None` if `FileNotFoundError` error and `strict` is `False`.

    Examples
    --------
    >>> tmp_path = getfixture('tmp_path')
    >>> from os import chdir
    >>> chdir(tmp_path)
    >>> example_path: Path = Path('a/test/path')
    >>> example_path.exists()
    False
    >>> tuple(path_iterdir(example_path.parent))
    ()
    >>> tuple(path_iterdir(example_path.parent, strict=True))
    Traceback (most recent call last):
        ...
    FileNotFoundError: [Errno 2] No such file or directory: 'a/test'
    >>> example_path.parent.mkdir(parents=True)
    >>> example_path.touch()
    >>> tuple(path_iterdir(example_path.parent))
    (PosixPath('a/test/path'),)
    >>> example_path.unlink()
    >>> tuple(path_iterdir(example_path.parent))
    ()
    """
    try:
        yield from Path(path).iterdir()
    except FileNotFoundError as error:
        if strict:
            raise error
        else:
            return


def kwargs_to_cli_str(space_prefix: bool = True, **kwargs) -> str:
    """Convert `kwargs` into a `cli` `str`.

    Parameters
    ----------
    kwargs
        `key=val` parameters to concatenate as `str`.

    Returns
    -------
    :
        A final `str` of concatenated `**kwargs` in
        command line form.

    Examples
    --------
    >>> kwargs_to_cli_str(cat=4, in_a="hat", fun=False)
    ' --cat 4 --in-a hat --not-fun'
    >>> kwargs_to_cli_str(space_prefix=False, cat=4, fun=True)
    '--cat 4 --fun'
    >>> kwargs_to_cli_str()
    ''
    """
    if kwargs:
        cmd_str: str = " ".join(
            f"{'--' + key.replace('_', '-')} {val}"
            if type(val) != bool
            else f"{'--' + key if val else '--not-' + key}"
            for key, val in kwargs.items()
        )
        return cmd_str if not space_prefix else " " + cmd_str
    else:
        return ""


def set_and_pop_attr_kwargs(instance: Any, **kwargs) -> dict[str, Any]:
    """Extract any `key: val` pairs from `kwargs` to modify `instance`.

    Parameters
    ----------
    instance
        An object to modify.
    kwargs
        `key`: `val` parameters to potentially modify `instance` attributes.

    Returns
    -------
    :
        Any remaining `kwargs` not used to modify `instance`.

    Examples
    --------
    >>> kwrgs = set_and_pop_attr_kwargs(
    ...    conda_lock_file_manager, env_paths=['pyproject.toml'], cat=4)
    >>> conda_lock_file_manager.env_paths
    ['pyproject.toml']
    >>> kwrgs
    {'cat': 4}
    """
    kwargs_copy = deepcopy(kwargs)
    for key, val in kwargs.items():
        if hasattr(instance, key):
            logger.debug(f"Changing '{key}' to '{val}'")
            setattr(instance, key, val)
            kwargs_copy.pop(key)  # This should eliminate all `kwargs` for `instance`
    return kwargs_copy


@dataclass
class CondaLockFileManager:

    """Run `conda_lock` `install` to generate `conda` `yml`.

    Attributes
    ----------
    conda_file_path
        Path to write `conda-lock` file to.
    env_paths
        Paths of configs to combine. For supported formats see:
        https://conda.github.io/conda-lock/
    replace_file_path
        Whether to replace `file_path` if it already exists.
    legacy_arch
        What archeticture to use for legacy export.
    legacy_name_prefix
        `str` to precede `legacy_arch` export file if
        `run_legacy_mv()` is run.
    default_kwargs
        `kwargs` to pass to `self.run_conda_lock()`.

    Notes
    -----
    This is derived from automating, with the `-p osx-64` etc. components now
    specified in `pyproject.toml` and `environment.yml`, the following command:
    ```bash
    conda-lock -f environment.yml -f python/pyproject.toml -p osx-64 -p linux-64 -p linux-aarch64
    ```
    A full exmaple with options matching saved defaults:
    ```bash
    conda-lock -f environment.yml -f python/pyproject.toml -p osx-64 -p linux-64 -p linux-aarch64 --check-input-hash
    ```

    Examples
    --------
    >>> conda_lock = CondaLockFileManager()
    >>> conda_lock
    <CondaLockFileManager(conda_file_path='../conda-lock.yml', env_paths=('../environment.yml', 'pyproject.toml'), legacy_arch='linux-64')>
    >>> conda_lock.run()
    ['conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml']
    >>> conda_lock.run(as_str=True, use_default_kwargs=True)
    'conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml --check-input-hash'
    """

    conda_file_path: PathLike = DEFAULT_CONDA_LOCK_PATH
    env_paths: Sequence[PathLike] = DEFAULT_ENV_PATHS
    replace_file_path: bool = False
    legacy_arch: str | None = GITHUB_ACTIONS_ARCHITECTURE
    legacy_name_prefix: PathLike | str = CONDA_LEGACY_PREFIX
    default_kwargs: dict[str, Any] = field(
        default_factory=lambda: DEFAULT_CONDA_LOCK_KWARGS
    )

    @property
    def _env_path_strs(self) -> tuple[str, ...]:
        """Return `self.env_paths` as `str`"""
        return tuple(str(path) for path in self.env_paths)

    def __repr__(self) -> str:
        """Summarise config as a `str`."""
        return (
            f"<CondaLockFileManager("
            f"conda_file_path='{self.conda_file_path}', "
            f"env_paths={self._env_path_strs}, "
            f"legacy_arch='{self.legacy_arch}')>"
        )

    def conda_lock_cmd_str(self, use_default_kwargs=False, **kwargs) -> str:
        """Return configured `conda-lock` command."""
        kwargs = set_and_pop_attr_kwargs(self, **kwargs)
        if use_default_kwargs:
            kwargs.update(self.default_kwargs)
        command_str: str = f"conda-lock lock --lockfile {self.conda_file_path} "
        command_str += " ".join(f"-f {name}" for name in self.env_paths)
        return command_str + kwargs_to_cli_str(**kwargs)

    @property
    def initial_legacy_export_name(self) -> Path:
        """Initially generated `Path` for legacy `conda_lock` export file."""
        return Path(f"conda-{self.legacy_arch}.lock")

    @property
    def final_legacy_export_path(self) -> Path:
        """Final `Path` for legacy `conda_lock` export file."""
        return Path(f"{self.legacy_name_prefix}{self.initial_legacy_export_name}")

    def legacy_export_cmd_str(self, **kwargs) -> str:
        """Command to export legacy `conda_lock` file from `self.conda_file_path`."""
        command: str = (
            f"conda-lock render --kind explicit --platform {self.legacy_arch}"
        )
        return command + kwargs_to_cli_str(**kwargs)

    @property
    def legacy_mv_cmd_str(self) -> str:
        """Command to mv legacy `conda_lock` file to `self.final_legacy_export_path`."""
        return f"mv {self.initial_legacy_export_name} {self.final_legacy_export_path}"

    def _lock_cmd(self, use_default_kwargs: bool = False, **kwargs) -> str:
        kwargs = set_and_pop_attr_kwargs(self, **kwargs)
        return self.conda_lock_cmd_str(
            use_default_kwargs=use_default_kwargs, **kwargs
        )  # Remaining `kwargs` should all fit `conda_lock_cmd_str`

    def _check_export_path(self) -> bool:
        """Return whether to write `self.conda_file_path`."""
        if Path(self.conda_file_path).exists():
            logger.debug(f"{self.conda_file_path} exists.")
            if not self.replace_file_path:
                logger.debug(f"Set 'replace_file_path' to True to overwrite.")
                return False
            else:
                logger.debug(f"Replacing... ('replace_file_path' set to True).")
        return True

    def run_conda_lock(
        self,
        execute: bool = False,
        use_default_kwargs: bool = False,
        parent_dir_after_lock: bool = False,
        **kwargs,
    ) -> str:
        """Check and optionally execute `self.conda_lock_cmd_str()`.

        Parameters
        ----------
        execute
            Whether to run `self.conda_lock_cmd_str()` as a `subprocess`.
        use_default_kwargs
            Whether to include the `self.default_kwargs` in run.
        kwargs
            Any additional parameters to pass to `self.conda_lock_cmd_str()`.

        Returns
        -------
        :
            Final generated command `str`, whether excuted or not.

        Examples
        --------
        >>> conda_lock_file_manager.run_conda_lock()
        'conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml'
        >>> conda_lock_file_manager.run_conda_lock(use_default_kwargs=True)
        'conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml --check-input-hash'
        >>> conda_lock_file_manager.run_conda_lock(pdb=True)
        'conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml --pdb'
        """
        command: str = self._lock_cmd(use_default_kwargs=use_default_kwargs, **kwargs)
        logger.debug(f"Create lock file with envs: {self._env_path_strs}")
        if execute:
            if self._check_export_path():
                subprocess.run(command, shell=True)
            if parent_dir_after_lock:
                chdir("..")
        return command

    def run_legacy_export(self, execute: bool = False, **kwargs) -> str:
        """Run `self.legacy_export_cmd_str()`.

        Parameters
        ----------
        execute
            Whether to run `self.conda_lock_cmd_str()`
            as a `subprocess`.
        kwargs
            Any additional parameters to pass to
            `self.legacy_export_cmd_str()`.

        Returns
        -------
        :
            Final generated command `str`, whether excuted or not.

        Examples
        --------
        >>> conda_lock_file_manager.run_legacy_export()
        'conda-lock render --kind explicit --platform linux-64'
        >>> conda_lock_file_manager.run_legacy_export(pdb=True)
        'conda-lock render --kind explicit --platform linux-64 --pdb'
        """
        logger.debug(f"Export to legacy '{self.legacy_arch}'")
        command: str = self.legacy_export_cmd_str(**kwargs)
        if execute:
            subprocess.run(command, shell=True)
        return command

    def run_legacy_mv(self, execute: bool = False) -> str:
        """Run `self.legacy_export_cmd_str`.

        Parameters
        ----------
        execute
            Whether to run `self.conda_lock_cmd_str()` as a `subprocess`.

        Returns
        -------
        :
            Final generated command `str`, whether excuted or not.

        Examples
        --------
        >>> conda_lock_file_manager.run_legacy_mv()
        'mv conda-linux-64.lock .conda-linux-64.lock'
        >>> conda_lock_file_manager.legacy_name_prefix = '../.'
        >>> conda_lock_file_manager.run_legacy_mv()
        'mv conda-linux-64.lock ../.conda-linux-64.lock'
        """
        logger.debug(
            f"Move '{self.initial_legacy_export_name}' "
            f"to legacy '{self.final_legacy_export_path}'"
        )
        if execute:
            subprocess.run(self.legacy_mv_cmd_str, shell=True)
        return self.legacy_mv_cmd_str

    def run(
        self,
        as_str: bool = False,
        include_all: bool = False,
        execute_all: bool = False,
        conda_lock: bool = True,
        execute_conda_lock: bool = False,
        use_default_kwargs: bool = False,
        legacy_export: bool = False,
        execute_legacy_export: bool = False,
        legacy_move: bool = False,
        execute_legacy_move: bool = False,
        cmds_list: list[str] | None = None,
        execute_priors: bool = False,
        cmds_post_list: list[str] | None = None,
        execute_cmds_post: bool = False,
        parent_dir_after_lock: bool = False,
        **kwargs,
    ) -> list[str] | str:
        """Return `self` configurations, optionally execute as `subprocess`.

        Parameters
        ----------
        as_str
            Whether to return as a `str`, if not as a `list[str]`.
        include_all
            Include all commands, overriding individual parameters like
            `conda_lock` etc. Combine with `execute_all` to also run.
        execute_all
            Run all included commands, overriding individual parameters
            like `execute_conda_lock` etc. Combine with `include_all` to
            run all commands.
        conda_lock
            Whether to include `self.run_conda_lock()`.
        execute_conda_lock
            Whether to run the generated commands via
            `subprocess.run()`.
        use_default_kwargs
            Whether to use `self.default_kwargs` params to run
            `self.run_conda_lock()`.
        legacy_export
            Whether to add the `self.legacy_export_cmd_str` command.
        execute_legacy_export
            Whether to run the `self.legacy_export_cmd_str()`.
        legacy_move
            Whether to add the `self.legacy_mv_cmd_str()` command.
        execute_legacy_move
            Whether to run the `self.legacy_mv_cmd_str()`.
        cmds_list
            A list of commands to execute. If passed, these are executed prior.
        execute_priors
            Execute commands passed in `cmds_list` prior to any others.
        cmds_post_list
            A list of commands to run after all others.
        execute_cmds_post
            Execute commands passed in `cmds_post_list` after all others.
        parent_dir_after_lock
            Whether to return to parent dir after lock command.

        Returns
        -------
        :
            A `list` of commands generated, or a `str` of each command
            separated by a newline character (`\\n`).

        Examples
        --------
        >>> conda_lock_file_manager.run()
        ['conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml']
        >>> print(conda_lock_file_manager.run(
        ...     as_str=True, legacy_export=True, legacy_move=True))
        conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml
        conda-lock render --kind explicit --platform linux-64
        mv conda-linux-64.lock .conda-linux-64.lock
        """
        if not cmds_list:
            cmds_list = []
        if execute_priors or execute_all:
            for command in cmds_list:
                subprocess.run(command, shell=True)
        if conda_lock or include_all:
            cmds_list.append(
                self.run_conda_lock(
                    execute=execute_conda_lock | execute_all,
                    use_default_kwargs=use_default_kwargs,
                    parent_dir_after_lock=parent_dir_after_lock,
                    **kwargs,
                )
            )
        if legacy_export or include_all:
            cmds_list.append(
                self.run_legacy_export(
                    execute=execute_legacy_export | execute_all, **kwargs
                )
            )
        if legacy_move or include_all:
            cmds_list.append(
                self.run_legacy_mv(execute=execute_legacy_move | execute_all)
            )
        if cmds_post_list:
            if execute_cmds_post or execute_all:
                for cmd in cmds_post_list:
                    subprocess.run(cmd, shell=True)
            cmds_list += cmds_post_list
        if as_str:
            return "\n".join(cmds_list)
        else:
            return cmds_list


def _pre_commit_conda_lock(
    include_all: bool = True,
    execute_all: bool = False,
    **kwargs,
) -> str:
    """A customised config for use in `.pre-commit.yml`.

    Parameters
    ----------
    include_all
        Ensure all commands are processed to generate a final
        command `str` (but not necessarily run).
    execute_all
        Run all enabled commands

    Returns
    -------
    :
        Command `str`, split by `\n` if multiple commands.

    Examples
    --------
    >>> print(_pre_commit_conda_lock())
    conda-lock lock --lockfile ../conda-lock.yml -f ../environment.yml -f pyproject.toml --check-input-hash
    conda-lock render --kind explicit --platform linux-64
    mv conda-linux-64.lock .conda-linux-64.lock
    """
    conda_lock: CondaLockFileManager = CondaLockFileManager(
        replace_file_path=True,
    )
    return conda_lock.run(
        execute_all=execute_all,
        as_str=True,
        use_default_kwargs=True,
        include_all=include_all,
        parent_dir_after_lock=True,
        **kwargs,
    )


def make_user(
    user: str,
    password: str,
    code_path: PathLike = RSTUDIO_DOCKER_USER_PATH,
    user_home_path: PathLike = DEBIAN_HOME_PATH,
) -> Path:
    """Make user account and copy code to that environment.

    Parameters
    ----------
    user
        Name for user and home folder name to append to `user_home_path`.
    password
        Login password.
    code_path
        `Path` to copy code from to `user` home directory.
    user_home_path
        Path that `user` folder will be in, often `Path('/home')` in `linux`.

    Returns
    -------
    :
        Full path to generated `user` home folder.

    Examples
    --------
    >>> import os
    >>> if os.geteuid() != 0:
    ...     pytest.skip('requires root permission to run')
    >>> user_name: str = 'an_unlinkely_test_user'
    >>> password: str = 'test_pass'
    >>> code_path: Path = Path('..')
    >>> make_user(user_name, password, code_path=code_path)
    PosixPath('/home/an_unlinkely_test_user')
    >>> Path(f'/home/{user_name}/python/conftest.py').is_file()
    True
    >>> rm_user(user_name)
    'an_unlinkely_test_user'
    """
    home_path: Path = Path(user_home_path) / Path(user)
    subprocess.run(f"useradd {user}", shell=True)
    subprocess.run(f"echo {user}:{password} | chpasswd", shell=True)
    subprocess.run(f"mkdir {home_path}", shell=True)
    subprocess.run(f"cp -r {code_path}/* {home_path}", shell=True)
    subprocess.run(f"chown -R {user}:{user} home_path", shell=True)
    return home_path


def rm_user(user: str, user_home_path: PathLike = DEBIAN_HOME_PATH) -> str:
    """Remove user and user home folder.

    Parameters
    ----------
    user
        User home folder name (usually the same as the user login name).
    user_home_path
        Parent path of `user` folder name.

    Returns
    -------
    :
        `user` name of account and home folder deleted.

    Examples
    --------
    >>> import os
    >>> if os.geteuid() != 0:
    ...     pytest.skip('requires root permission to run')
    >>> user_name: str = 'very_unlinkely_test_user'
    >>> password: str = 'test_pass'
    >>> make_user(user_name, password, code_path=JUPYTER_DOCKER_USER_PATH)
    PosixPath('/home/very_unlinkely_test_user')
    >>> rm_user(user_name)
    'very_unlinkely_test_user'
    """
    subprocess.run(f"userdel {user}", shell=True)
    rmtree(Path(user_home_path) / user)
    return user


def csv_reader(path: PathLike, **kwargs) -> Iterator[dict[str, str]]:
    """Yield a `dict` per row from a `CSV` file at `path`.

    Parameters
    ----------
    path
        `CSV` file `Path`.
    **kwargs
        Additional parameters for `csv.DictReader`.

    #Yields
    #------
    #A `dict` per row from `CSV` file at `path`.

    Examples
    --------
    >>> import csv
    >>> csv_path: Path = 'test_auth.csv'
    >>> auth_dict: dict[str, str] = {
    ...    'sally': 'fig*new£kid',
    ...    'george': 'tee&iguana*sky',
    ...    'susan': 'history!bill-walk',}
    >>> field_names: tuple[str, str] = ('user_name', 'password')
    >>> with open(csv_path, 'w') as csv_file:
    ...     writer = csv.writer(csv_file)
    ...     line_num: int = writer.writerow(('user_name', 'password'))
    ...     for user_name, password in auth_dict.items():
    ...         line_num = writer.writerow((user_name, password))
    >>> tuple(csv_reader(csv_path))
    ({'user_name': 'sally', 'password': 'fig*new£kid'},
     {'user_name': 'george', 'password': 'tee&iguana*sky'},
     {'user_name': 'susan', 'password': 'history!bill-walk'})
    """
    with open(path) as csv_file:
        for row in DictReader(csv_file, **kwargs):
            yield row


def make_users(
    file_path: PathLike,
    user_col: str,
    password_col: str,
    file_reader: Callable,
    **kwargs,
) -> Iterator[Path]:
    """Load a file of usernames and passwords and pass each line to `make_user`.

    Parameters
    ----------
    file_path
        `Path` to collumned file including user names and passwords per row.
    user_col
        `str` of column name for user names.
    password_col
        `str` of column name for passwords.
    file_reader
        Callable (function) to read `file_path`.
    **kwargs
        Additional parameters for to pass to `file_reader` function.

    #Yields
    #------
    #:
    #    The home `Path` for each generated user.

    Examples
    --------
    >>> import os
    >>> if os.geteuid() != 0:
    ...     pytest.skip('requires root permission to run')
    >>> from pandas import read_excel
    >>> code_path: Path = Path('/home/jovyan')
    >>> def excel_row_iter(path: Path, **kwargs) -> dict:
    ...     df: DataFrame = read_excel(path, **kwargs)
    ...     return df.to_dict(orient="records")
    >>> test_accounts_path: Path = Path('tests/test_user_accounts.xlsx')
    >>> user_paths: tuple[Path, ...] = tuple(make_users(
    ...     file_path=test_accounts_path,
    ...     user_col="User Name",
    ...     password_col="Password",
    ...     file_reader=excel_row_iter,
    ...     code_path=JUPYTER_DOCKER_USER_PATH,
    ... ))
    >>> [(path / 'python' / 'conftest.py').is_file() for path in user_paths]
    [True, True, True, True, True]
    >>> [rm_user(user_path.name) for user_path in user_paths]
    ['sally', 'george', 'jean', 'felicity', 'frank']
    """
    for record in file_reader(file_path):
        yield make_user(user=record[user_col], password=record[password_col], **kwargs)


# Below requires packages outside python standard library
# Note: `rioxarray` is imported to ensure GIS methods are included. See:
# https://corteva.github.io/rioxarray/stable/getting_started/getting_started.html#rio-accessor
try:
    import rioxarray  # nopycln: import
    from numpy import array, random
    from pandas import to_datetime
    from xarray import DataArray, Dataset

    def xarray_example(
        start_date: DateType,
        end_date: DateType,
        coordinates: dict[str, tuple[float, float]] = THREE_CITY_COORDS,
        skip_dates: Iterable[date] | None = None,
        random_seed_int: int | None = None,
        name: str | None = None,
        as_dataset: bool = False,
        **kwargs,
    ) -> DataArray | Dataset:
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
        da: DataArray = DataArray(
            random_data,
            name=name,
            coords=[
                to_datetime(date_range),
                spaces,
            ],
            dims=[
                "time",
                "space",
            ],
            # If useful, add lat/lon (currently not working)
            # coords=[dates, spaces, lon, lat],
            # dims=["time", "space", "lon", "lat"]
        )
        if as_dataset:
            return da.to_dataset()
        else:
            return da

except ImportError:
    # This allows the file to be imported without any packages installed
    pass
