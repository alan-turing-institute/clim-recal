"""Utility functions."""
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final, Generator, Iterable, Optional, Union

DateType = Union[date, str]
DATE_FORMAT_STR: Final[str] = "%Y%m%d"
DATE_FORMAT_SPLIT_STR: Final[str] = "-"
RSTUDIO_CODE_COPY_PATH: Path = Path("/home/rstudio/*")
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
    name: str,
    password: str,
    code_path: Path = RSTUDIO_CODE_COPY_PATH,
    user_home_path: Path = DEBIAN_HOME_PATH,
) -> Path:
    """Make user account and copy code to that environment.

    Args:
        name: user and home folder name
        password: login password
        code_path: path to copy code from to user path

    Example:
        ```pycon
        >>> import os
        >>> from shutil import rmtree
        >>> if os.geteuid() != 0:
        ...     pytest.skip('requires root permission to run')
        >>> user_name: str = 'very_unlinkely_test_user'
        >>> password: str = 'test_pass'
        >>> code_path: Path = Path('/home/jovyan')
        >>> make_user(user_name, password, code_path=code_path)
        PosixPath('/home/very_unlinkely_test_user')
        >>> Path(f'/home/{user_name}/python/conftest.py').is_file()
        True
        >>> subprocess.run(f'userdel {user_name}', shell=True)
        CompletedProcess(args='userdel very_unlinkely_test_user', returncode=0)
        >>> rmtree(f'/home/{user_name}')

        ```
    """
    home_path: Path = user_home_path / name
    subprocess.run(f"useradd {name}", shell=True)
    subprocess.run(f"echo {name}:{password} | chpasswd", shell=True)
    subprocess.run(f"mkdir {home_path}", shell=True)
    subprocess.run(f"cp -r {code_path}/* {home_path}", shell=True)
    subprocess.run(f"chown -R {name}:{name} home_path", shell=True)
    return home_path
