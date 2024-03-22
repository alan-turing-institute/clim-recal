"""Utility functions."""
import subprocess
from dataclasses import dataclass, field
from logging import getLogger
from os import PathLike, chdir
from pathlib import Path
from shutil import rmtree
from typing import Any, Callable, Final, Iterator, Sequence

from .core import kwargs_to_cli_str, set_and_pop_attr_kwargs

logger = getLogger(__name__)

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
        r"""Return `self` configurations, optionally execute as `subprocess`.

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
            separated by a newline character (`\n`).

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
    r"""A customised config for use in `.pre-commit.yml`.

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
    >>> code_path: Path = JUPYTER_DOCKER_USER_PATH
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
    >>> if is_platform_darwin:
    ...     pytest.skip('test designed for docker jupyter')
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
    >>> if is_platform_darwin:
    ...     pytest.skip('test designed for docker jupyter')
    >>> import os
    >>> if os.geteuid() != 0:
    ...     pytest.skip('requires root permission to run')
    >>> tmp_data_path = getfixture('data_fixtures_path')
    >>> from pandas import read_excel
    >>> def excel_row_iter(path: Path, **kwargs) -> dict:
    ...     df: DataFrame = read_excel(path, **kwargs)
    ...     return df.to_dict(orient="records")
    >>> test_accounts_path: Path = tmp_data_path / 'test_user_accounts.xlsx'
    >>> assert test_accounts_path.exists()
    >>> user_paths: tuple[Path, ...] = tuple(make_users(
    ...     file_path=test_accounts_path,
    ...     user_col="User Name",
    ...     password_col="Password",
    ...     file_reader=excel_row_iter,
    ...     code_path=JUPYTER_DOCKER_USER_PATH,
    ... ))
    >>> [(path / 'python' / 'conftest.py').is_file()
    ...  for path in user_paths]
    [True, True, True, True, True]
    >>> [rm_user(user_path.name) for user_path in user_paths]
    ['sally', 'george', 'jean', 'felicity', 'frank']
    """
    for record in file_reader(file_path):
        yield make_user(user=record[user_col], password=record[password_col], **kwargs)
