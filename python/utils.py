"""Utility functions."""
import subprocess
from csv import DictReader
from datetime import date, datetime, timedelta
from os import PathLike
from pathlib import Path
from shutil import rmtree
from typing import Any, Callable, Final, Generator, Iterable, Iterator, Optional, Union

DateType = Union[date, str]
DATE_FORMAT_STR: Final[str] = "%Y%m%d"
ISO_DATE_FORMAT_STR: Final[str] = "%Y-%m-%d"
DATE_FORMAT_SPLIT_STR: Final[str] = "-"
RSTUDIO_DOCKER_USER_PATH: Path = Path("/home/rstudio")
JUPYTER_DOCKER_USER_PATH: Path = Path("/home/jovyan")
DEBIAN_HOME_PATH: Path = Path("/home/")

DEFAULT_CONDA_LOCK_PATH: Final[PathLike] = Path("conda-lock.yml")
DEFAULT_ENV_PATHS: Final[tuple[PathLike, ...]] = (
    Path("../environment.yml"),
    Path("pyproject.toml"),
)
DEFAULT_CONDA_LOCK_KWARGS: Final[dict[str, str | float | bool]] = {
    "--check-input-hash": True,
    "--update": True,
}

GITHUB_ACTIONS_ARCHITECTURE: Final[str] = "linux-64"
# NC_GLOB_STR: Final[str] = "**/*.nc"


# def globs_to_paths(path: PathLike, glob_str: str, recursive: bool = True) -> tuple[Path, ...]:
#     """Return a tuple of `Paths` matching `glob_str`.
#
#     Parameters
#     ----------
#     path
#         `str` or `Path` to search via `glob_str`.
#
#     glob_str
#         Glob `str` to search for file names within `path`.
#
#     recursive
#         Whether to recursively search within `path`.
#
#     Returns
#     -------
#     A `tuple` of matched `paths`.
#
#     """


def date_range_generator(
    start_date: DateType,
    end_date: DateType,
    inclusive: bool = False,
    start_format_str: str = DATE_FORMAT_STR,
    end_format_str: str = DATE_FORMAT_STR,
    output_format_str: str = DATE_FORMAT_STR,
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
    start_format_str
        A `strftime` format to apply if `start_date` `type` is `str`.
    end_format_str
        A `strftime` format to apply if `end_date` `type` is `str`.
    output_format_str
        A `strftime` format to apply if `yield_type` is `str`.
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
    """
    start_date = (
        start_date
        if isinstance(start_date, date)
        else datetime.strptime(start_date, start_format_str).date()
    )
    end_date = (
        end_date
        if isinstance(end_date, date)
        else datetime.strptime(end_date, end_format_str).date()
    )
    if inclusive:
        end_date += timedelta(days=1)
    try:
        assert start_date < end_date
    except AssertionError:
        raise ValueError(
            f"start_date: {start_date} must be before end_date: {end_date}"
        )
    for day_number in range(int((end_date - start_date).days)):
        date_obj: date = start_date + timedelta(day_number)
        yield (date_obj if yield_type == date else date_obj.strftime(output_format_str))


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


def run_conda_lock(
    file_path: PathLike = DEFAULT_CONDA_LOCK_PATH,
    env_paths: Iterable[PathLike] = DEFAULT_ENV_PATHS,
    run: bool = False,
    replace_file_path: bool = False,
    legacy_explicit: str | None = GITHUB_ACTIONS_ARCHITECTURE,
    legacy_explicit_file_prefix: str = ".",
    default_kwargs: bool = False,
    **kwargs: dict[str, str | float | bool],
) -> str:
    """Run `conda_lock` `install` to generate `conda` `yml`.

    Parameters
    ----------
    file_path
        Path to write `conda-lock` file to.
    env_paths
        `Iterable` of paths of configs to combine. See
        [docs](https://conda.github.io/conda-lock/) for supported formats.
    run
        Whether to execute the command as a `subprocess`.
    replace_file_path
        Whether to replace `file_path` if it already exists.
    default_kwargs
        Whether to use `DEFAULT_CONDA_LOCK_KWARGS`.

    Returns
    -------
    :
        Generated command `str`.

    Notes
    -----
    This is derived from automating, with the `-p osx-64` etc. components now
    specified in `pyproject.toml` and `environment.yml`, the following command:
    ```bash
    conda-lock -f environment.yml -f python/pyproject.toml -p osx-64 -p linux-64 -p linux-aarch64
    ```

    Examples
    --------
    >>> run_conda_lock()
    'conda-lock --lockfile conda-lock.yml -f ../environment.yml -f pyproject.toml'
    >>> run_conda_lock(default_kwargs=True)
    'conda-lock --lockfile conda-lock.yml -f ../environment.yml -f pyproject.toml --check-input-hash --update'
    """
    if default_kwargs:
        kwargs.update(DEFAULT_CONDA_LOCK_KWARGS)
    command_str: str = f"conda-lock --lockfile {file_path} "
    command_str += " ".join(f"-f {name}" for name in env_paths)
    if kwargs:
        command_str += " " + " ".join(
            f"{key} {val}" if type(val) != bool else f"{key}"
            for key, val in kwargs.items()
        )
    if run:
        if Path(file_path).exists():
            print(f"{file_path} exists.")
            if not replace_file_path:
                print(f"Set 'replace_file_path' to True to overwrite.")
                return command_str
            else:
                print(f"Replacing... ('replace_file_path' set to True).")
        subprocess.run(command_str)
        if legacy_explicit:
            subprocess.run(
                f"conda-lock render --kind explict " f"--platform {legacy_explicit}"
            )
            subprocess.run(
                f"mv conda-{legacy_explicit}.lock "
                f"{legacy_explicit_file_prefix}conda-{legacy_explicit}.lock"
            )

    return command_str


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
    >>> user_name: str = 'very_unlinkely_test_user'
    >>> password: str = 'test_pass'
    >>> code_path: Path = Path('/home/jovyan')
    >>> make_user(user_name, password, code_path=JUPYTER_DOCKER_USER_PATH)
    PosixPath('/home/very_unlinkely_test_user')
    >>> Path(f'/home/{user_name}/python/conftest.py').is_file()
    True
    >>> rm_user(user_name)
    'very_unlinkely_test_user'
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
    >>> tuple(csv_reader(csv_path))  # doctest: +NORMALIZE_WHITESPACE
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
