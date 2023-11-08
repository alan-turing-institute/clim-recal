"""
Utility functions.

"""
from typing import Final, Any, Iterable, Generator
from datetime import date, datetime
from pathlib import Path


DateType = date | str
DATE_FORMAT_STR: Final[str] = '%Y%m%d'
DATE_FORMAT_SPLIT_STR: Final[str] = '-'

    
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


def iter_to_tuple_strs(iter_var: Iterable[Any]) -> tuple[str, ...]:
    """Return a `tuple` with all components converted to `strs`.

    Examples
    --------

    >>> iter_to_tuple_strs(['cat', 1, Path('a/path')])
    ('cat', '1', 'a/path')

    """
    return tuple(str(obj) for obj in iter_var)


def path_iterdir(path: Path, strict: bool = False) -> Generator[Path | None, None, None]:
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
