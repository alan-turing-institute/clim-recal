"""Utility functions."""
import subprocess
from csv import DictReader
from datetime import date, datetime
from pathlib import Path
from shutil import rmtree
from typing import Any, Callable, Final, Generator, Iterable, Optional, Union

DateType = Union[date, str]
DATE_FORMAT_STR: Final[str] = "%Y%m%d"
DATE_FORMAT_SPLIT_STR: Final[str] = "-"
RSTUDIO_DOCKER_USER_PATH: Path = Path("/home/rstudio")
JUPYTER_DOCKER_USER_PATH: Path = Path("/home/jovyan")
DEBIAN_HOME_PATH: Path = Path("/home/")


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

    Return
    ------
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

    Return
    ------
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


def iter_to_tuple_strs(iter_var: Iterable[Any]) -> tuple[str, ...]:
    """Return a `tuple` with all components converted to `strs`.

    Parameters
    ----------
    iter_var
        Iterable of objects that can be converted into `strs`.

    Return
    ------
    A `tuple` of all `iter_var` elements in `str` format.

    Examples
    --------
    >>> iter_to_tuple_strs(['cat', 1, Path('a/path')])
    ('cat', '1', 'a/path')

    """
    return tuple(str(obj) for obj in iter_var)


def path_iterdir(
    path: Path, strict: bool = False
) -> Generator[Optional[Path], None, None]:
    """Return an `Generator` after ensuring `path` exists.

    Parameters
    ----------
    path
        `Path` to folder to iterate through.
    strict
        Whether to raise `FileNotFoundError` if `path` not found.

    Yield
    -----
    A `Path` for each folder  in `path`.

    Raises
    ------
    FileNotFoundError
        Raised if `strict = True` and `path` does not exist.

    Return
    ------
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
        yield from path.iterdir()
    except FileNotFoundError as error:
        if strict:
            raise error
        else:
            return


def make_user(
    user: str,
    password: str,
    code_path: Path = RSTUDIO_DOCKER_USER_PATH,
    user_home_path: Path = DEBIAN_HOME_PATH,
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

    Return
    ------
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
    home_path: Path = user_home_path / user
    subprocess.run(f"useradd {user}", shell=True)
    subprocess.run(f"echo {user}:{password} | chpasswd", shell=True)
    subprocess.run(f"mkdir {home_path}", shell=True)
    subprocess.run(f"cp -r {code_path}/* {home_path}", shell=True)
    subprocess.run(f"chown -R {user}:{user} home_path", shell=True)
    return home_path


def rm_user(user: str, user_home_path: Path = DEBIAN_HOME_PATH) -> str:
    """Remove user and user home folder.

    Parameters
    ----------
    user
        User home folder name (usually the same as the user login name).
    user_home_path
        Parent path of `user` folder name.

    Return
    ------
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
    rmtree(user_home_path / user)
    return user


def csv_reader(path: Path, **kwargs) -> Generator[dict[str, str], None, None]:
    """Yield a `dict` per row from a `CSV` file at `path`.

    Parameters
    ----------
    path
        `CSV` file `Path`.
    **kwargs
        Additional parameters for `csv.DictReader`.

    Yield
    -----
    A `dict` per row from `CSV` file at `path`.

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
    file_path: Path, user_col: str, password_col: str, file_reader: Callable, **kwargs
) -> Generator[Path, None, None]:
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

    Yield
    -----
    The home `Path` for each generated user.

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
