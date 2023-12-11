"""Utility functions."""
import subprocess
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

    Args:
        user: user and home folder name
        password: login password
        code_path: path to copy code from to user path

    Example:
        ```pycon
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

        ```
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

    Args:
        user: user and home folder name
        password: login password

    Example:
        ```pycon
        >>> import os
        >>> if os.geteuid() != 0:
        ...     pytest.skip('requires root permission to run')
        >>> user_name: str = 'very_unlinkely_test_user'
        >>> password: str = 'test_pass'
        >>> make_user(user_name, password, code_path=JUPYTER_DOCKER_USER_PATH)
        PosixPath('/home/very_unlinkely_test_user')
        >>> rm_user(user_name)
        'very_unlinkely_test_user'

        ```
    """
    subprocess.run(f"userdel {user}", shell=True)
    rmtree(user_home_path / user)
    return user


def make_users(
    file_path: Path, user_col: str, password_col: str, file_reader: Callable, **kwargs
) -> Generator[Path, None, None]:
    """Load a file of usernames and passwords and to pass to make_user.

    Args:
        file_path: path to collumned file including user names and passwords per row
        user_col: str of column name for user names
        password_col: name of column name for passwords
        file_reader: function to read `file_path`
        **kwargs: additional parameters for to pass to `file_reader`

    Example:
        ```pycon
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

        ```
    """
    for record in file_reader(file_path):
        yield make_user(user=record[user_col], password=record[password_col], **kwargs)
