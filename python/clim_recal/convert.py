"""Convert UKHADS data and UKCP18 data.

- UKCP18 is converted temporally from a 360 day calendar to a standard (365/366 day) calendar and projected to British National Grid (BNG) EPSG:27700 from its original rotated polar grid.
- UKHADS is converted spatially from 1km to 2.2km in BNG aligned with the projected UKCP18
"""

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date
from glob import glob
from itertools import islice
from logging import getLogger
from os import PathLike, cpu_count
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Iterator, Sequence

import dill as pickle
import rioxarray  # nopycln: import
from rich import print
from rich.progress import Progress
from xarray.core.types import T_Dataset

from clim_recal.debiasing.debias_wrapper import VariableOptions

from .utils.core import _get_source_path, dates_path_to_date_tuple
from .utils.data import (
    CONVERT_OUTPUT_PATH,
    CPM_END_DATE,
    CPM_NAME,
    CPM_OUTPUT_PATH,
    CPM_START_DATE,
    CPM_SUB_PATH,
    HADS_END_DATE,
    HADS_NAME,
    HADS_OUTPUT_PATH,
    HADS_START_DATE,
    HADS_SUB_PATH,
    HADS_XDIM,
    HADS_YDIM,
    NETCDF_OR_TIF,
    RAW_CPM_PATH,
    RAW_CPM_TASMAX_PATH,
    RAW_HADS_PATH,
    RAW_HADS_TASMAX_PATH,
    ClimDataType,
    RunOptions,
    VariableOptions,
)
from .utils.gdal_formats import NETCDF_EXTENSION_STR
from .utils.xarray import (
    BRITISH_NATIONAL_GRID_EPSG,
    _write_and_or_return_results,
    cpm_reproject_with_standard_calendar,
    data_path_to_date_range,
    execute_configs,
    get_cpm_for_coord_alignment,
    hads_resample_and_reproject,
    path_parent_types,
    progress_wrapper,
)

logger = getLogger(__name__)

DropDayType = set[tuple[int, int]]
ChangeDayType = set[tuple[int, int]]

CFCalendarSTANDARD: Final[str] = "standard"


def reproject_standard_calendar_file_name(path: Path) -> Path:
    """Return tweaked `path` to indicate standard day projection."""
    return path.parent / path.name.replace("_day", "_day_std_year")


def reproject_2_2km_file_name(path: Path) -> Path:
    """Return tweaked `path` to indicate standard day projection."""
    return path.parent / path.name.replace("_1km", "_2_2km")


@dataclass(kw_only=True)
class IterCalcBase:
    """Base class to inherit for `HADs` and `CPM`."""

    input_path: PathLike | None = Path()
    output_path: PathLike = CONVERT_OUTPUT_PATH
    variable_name: VariableOptions | str = VariableOptions.default()
    input_files: Iterable[PathLike] | None = None
    cpus: int | None = None
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG
    input_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    export_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    start_index: int = 0
    stop_index: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    _result_paths: dict[PathLike, PathLike | None] = field(default_factory=dict)
    _calc_method_name: str = "to_reprojection"
    _calc_method_description: str = "Converting..."
    _iter_calc_method_name: str = "range_to_reprojection"

    def __post_init__(self) -> None:
        """Generate related attributes."""
        try:
            assert self.input_path or self.input_files
        except AssertionError:
            raise AttributeError(
                f"'input_path' or 'input_file' are None; at least one must be set."
            )
        self.set_input_files()
        self.set_file_dates()
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        self.total_cpus: int | None = cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus

    def __len__(self) -> int:
        """Return the length of `self.input_files`."""
        return (
            len(self.input_files[self.start_index : self.stop_index])
            if isinstance(self.input_files, Sequence)
            else 0
        )

    @property
    def max_count(self) -> int:
        """Maximum length of `self.input_files` ignoring `start_index` and `start_index`."""
        return len(self.input_files) if isinstance(self.input_files, Sequence) else 0

    def __iter__(self) -> Iterator[Path] | None:
        if self.file_dates and isinstance(self.file_dates, Iterable):
            for file_path in tuple(self.file_dates)[self.start_index : self.stop_index]:
                yield Path(file_path)
        else:
            return None

    def __getitem__(self, key: int | slice) -> Path | tuple[Path] | None:
        if not self.file_dates:
            return None
        elif isinstance(key, int):
            return Path(tuple(self.file_dates)[key])
        elif isinstance(key, slice):
            return tuple(Path(path) for path in tuple(self.file_dates)[key])
        else:
            raise IndexError(f"Can only index with 'int', not: '{key}'")

    @property
    def max_start_date(self) -> date:
        """Return the earlest start date in the maximum full time series."""
        return tuple(self.max_file_dates.values())[0][0]

    @property
    def max_end_date(self) -> date:
        """Return the latest end date in the maximum full time series."""
        return tuple(self.max_file_dates.values())[-1][1]

    def set_file_dates(self) -> None:
        if not hasattr(self, "max_input_files"):
            self.set_input_files()
        self.max_file_dates: dict[Path, tuple[date, date]] = OrderedDict(
            (path, dates_path_to_date_tuple(path)) for path in self.max_input_files
        )
        self.start_date = (
            max(self.start_date, self.max_start_date)
            if self.start_date
            else self.max_start_date
        )
        self.end_date = self.end_date = (
            min(self.end_date, self.max_end_date)
            if self.end_date
            else self.max_end_date
        )
        self.file_dates: dict[Path, tuple[date, date]] = OrderedDict(
            (path, (start_date, end_date))
            for path, (start_date, end_date) in self.max_file_dates.items()
            if self.start_date <= start_date or self.end_date <= end_date
        )

    def set_input_files(self, new_input_path: PathLike | None = None) -> None:
        """Replace `self.input` and process `self.input_files`."""
        if new_input_path:
            assert Path(new_input_path).exists()
            self.input = new_input_path
        if not self.input_files or new_input_path:
            self.input_files = tuple(
                sorted(
                    Path(path)
                    for path in glob(
                        f"{self.input_path}/*.{self.input_file_extension}",
                        recursive=True,
                    )
                )
            )
            self.max_input_files = self.input_files

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"count={len(self)}, "
            f"max_count={self.max_count}, "
            f"start_date={repr(self.start_date)[9:]}, "
            f"end_date={repr(self.end_date)[9:]}, "
            f"input_path='{self.input_path}', "
            f"output_path='{self.output_path}')>"
        )

    def _get_source_path(
        self, index: int, source_to_index: Sequence | None = None
    ) -> Path:
        """Return a path indexed from `source_to_index` or `self`."""
        if source_to_index is None:
            return self[index]
        elif isinstance(source_to_index, str):
            return getattr(self, source_to_index)[index]
        else:
            return source_to_index[index]

    def range_to_reprojection(
        self,
        start: int | None = None,
        stop: int | None = None,
        step: int = 1,
        override_export_path: Path | None = None,
        source_to_index: Sequence | None = None,
        return_path: bool = True,
        write_results: bool = True,
        progress_bar: bool = True,
        description_func: (
            Callable[[PathLike, Any], str] | None
        ) = data_path_to_date_range,
        progress_instance: Progress | None = None,
        **kwargs,
    ) -> Iterator[Path | T_Dataset]:
        return progress_wrapper(
            self,
            self._calc_method_name,
            start=start,
            stop=stop,
            step=step,
            description=self._calc_method_description,
            override_export_path=override_export_path,
            source_to_index=source_to_index,
            return_path=return_path,
            write_results=write_results,
            use_progress_bar=progress_bar,
            description_func=description_func,
            description_kwargs={"return_type": "string"},
            progress_instance=progress_instance,
            skip_progress_kwargs_method_name=self._calc_method_name,
            **kwargs,
        )

    def execute(self, **kwargs) -> tuple[Path, ...]:
        """Run all steps for processing"""
        return tuple(getattr(self, self._iter_calc_method_name)(**kwargs))


@dataclass(kw_only=True, repr=False)
class HADsConvert(IterCalcBase):
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    input_files
        `Path` or `Paths` of `NCF` files to convert.
    resampling_func
        Function to call on `self.input_files`.
    crop
        Path or file to spatially crop `input_files` with.
    final_crs
        Coordinate Reference System (CRS) to return final format in.
    input_file_x_column_name
        Column name in `input_files` or `input` for `x` coordinates.
    input_file_y_column_name
        Column name in `input_files` or `input` for `y` coordinates.
    input_file_extension
        File extensions to glob `input_files` with.
    start_index
        First index of file to iterate processing from.
    stop_index
        Last index of files to iterate processing from as a count from `start_index`.
        If `None`, this will simply iterate over all available files.
    cpm_for_coord_alignment
        `CPM` `Path` or `Dataset` to match alignment with.
    cpm_for_coord_alignment_path_converted
        Whether a `Path` passed to `cpm_for_coord_alignment` should be processed.

    Examples
    --------
    >>> if not is_data_mounted or local_cache:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_hads_output_path: Path = getfixture(
    ...         'resample_test_hads_output_path')
    >>> hads_converter: HADsConvert = HADsConvert(
    ...     output_path=resample_test_hads_output_path,
    ... )
    >>> hads_converter
    <HADsConvert(count=504,
                 max_count=504,
                 start_date=date(1980, 1, 1),
                 end_date=date(2021, 12, 31),
                 input_path='.../tasmax/day',
                 output_path='...run-results_..._.../hads')>
    >>> pprint(hads_converter.input_files)
    (...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc'),
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800201-19800229.nc'),
     ...,
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc'))
    """

    input_path: PathLike | None = RAW_HADS_TASMAX_PATH
    output_path: PathLike = CONVERT_OUTPUT_PATH / HADS_OUTPUT_PATH
    input_files: Iterable[PathLike] | None = None

    cpus: int | None = None
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG
    input_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    export_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    input_file_x_column_name: str = HADS_XDIM
    input_file_y_column_name: str = HADS_YDIM
    cpm_for_coord_alignment: T_Dataset | PathLike | None = RAW_CPM_TASMAX_PATH
    cpm_for_coord_alignment_path_converted: bool = False

    def set_cpm_for_coord_alignment(self) -> None:
        """Check if `cpm_for_coord_alignment` is a `Dataset`, process if a `Path`."""
        self.cpm_for_coord_alignment = get_cpm_for_coord_alignment(
            self.cpm_for_coord_alignment,
            skip_reproject=self.cpm_for_coord_alignment_path_converted,
        )

    def to_reprojection(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        return_path: bool = False,
        write_results: bool = True,
        source_to_index: Sequence | None = None,
        **kwargs,
    ) -> Path | T_Dataset:
        source_path: Path = _get_source_path(
            self, index=index, source_to_index=source_to_index
        )
        logger.debug(f"Setting 'cpm_for_coord_alignment' for {self}")
        self.set_cpm_for_coord_alignment()
        logger.debug(f"Set 'cpm_for_coord_alignment' for {self}")
        logger.debug(f"Starting HADs index {index}...")
        result: T_Dataset = hads_resample_and_reproject(
            source_path,
            variable_name=self.variable_name,
            x_dim_name=self.input_file_x_column_name,
            y_dim_name=self.input_file_y_column_name,
            cpm_to_match=self.cpm_for_coord_alignment,
            **kwargs,
        )
        logger.debug(f"Completed HADs index {index}...")
        return _write_and_or_return_results(
            self,
            result=result,
            output_path_func=reproject_2_2km_file_name,
            source_path=source_path,
            write_results=write_results,
            return_path=return_path,
            override_export_path=override_export_path,
        )


@dataclass(kw_only=True, repr=False)
class CPMConvert(IterCalcBase):
    """CPM specific changes to HADsConvert.

    Attributes
    ----------
    input_path
        `Path` to `CPM` files to process.
    output
        `Path` to save processed `CPM` files.
    input_files
        `Path` or `Paths` of `NCF` files to reproject.
    cpus
        Number of `cpu` cores to use during multiprocessing.
    resampling_func
        Function to call on `self.input_files`.
    crop
        Path or file to spatially crop `input_files` with.
    final_crs
        Coordinate Reference System (CRS) to return final format in.
    input_file_extension
        File extensions to glob `input_files` with.

    Examples
    --------
    >>> if not is_data_mounted or local_cache:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_cpm_output_path: Path = getfixture(
    ...         'resample_test_cpm_output_path')
    >>> cpm_converter: CPMConvert = CPMConvert(
    ...     input_path=RAW_CPM_TASMAX_PATH,
    ...     output_path=resample_test_cpm_output_path,
    ... )
    >>> cpm_converter
    <CPMConvert(count=..., max_count=...,...
        ...input_path='.../tasmax/01/latest',...
        ...output_path='.../test-run-results_..._.../cpm')>
    >>> pprint(cpm_converter.input_files)
    (...Path('.../tasmax/01/latest/tasmax_...-cpm_uk_2.2km_01_day_19801201-19811130.nc'),
     ...Path('.../tasmax/01/latest/tasmax_...-cpm_uk_2.2km_01_day_19811201-19821130.nc'),
     ...
     ...Path('.../tasmax/01/latest/tasmax_...-cpm_uk_2.2km_01_day_20791201-20801130.nc'))
    """

    input_path: PathLike | None = RAW_CPM_TASMAX_PATH
    output_path: PathLike = CONVERT_OUTPUT_PATH / CPM_OUTPUT_PATH

    @property
    def cpm_variable_name(self) -> str:
        return VariableOptions.cpm_value(self.variable_name)

    def to_reprojection(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        return_path: bool = False,
        write_results: bool = True,
        source_to_index: Sequence | None = None,
        **kwargs,
    ) -> Path | T_Dataset:
        source_path: Path = _get_source_path(
            self, index=index, source_to_index=source_to_index
        )
        logger.debug(f"Converting CPM index {index}...")
        result: T_Dataset = cpm_reproject_with_standard_calendar(
            source_path, variable_name=self.cpm_variable_name, **kwargs
        )
        logger.debug(f"Completed CPM index {index}...")
        return _write_and_or_return_results(
            self,
            result=result,
            output_path_func=reproject_standard_calendar_file_name,
            source_path=source_path,
            write_results=write_results,
            return_path=return_path,
            override_export_path=override_export_path,
        )

    def __getstate__(self):
        """Meanse of testing what aspects of instance have issues multiprocessing.

        Notes
        -----
        As of 2 May 2023, picking is not an error on macOS but *is*
        on the server configured Linux architecture. This may relate
        to differences between Linux using `fork` while `win` and `macOS`
        use `spawn`. This `method` helps test that on instances of this
        `class`. This can probably be removed now the `dill` package is used.
        """
        for variable_name, value in vars(self).items():
            try:
                pickle.dumps(value)
            except pickle.PicklingError:
                print(f"{variable_name} with value {value} is not pickable")


@dataclass(kw_only=True)
class IterCalcManagerBase:
    """Base class to inherit for `HADs` and `CPM` converter managers."""

    input_paths: PathLike | Sequence[PathLike] = Path()
    output_paths: PathLike | Sequence[PathLike] = Path()
    variables: Sequence[VariableOptions | str] = (VariableOptions.default(),)
    sub_path: Path = Path()
    start_index: int = 0
    stop_index: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    start_calc_index: int | None = None
    stop_calc_index: int | None = None
    configs: list[HADsConvert | CPMConvert] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    calc_class: type[HADsConvert | CPMConvert] | None = None
    cpus: int | None = None
    _configs_method_name: str = "yield_configs"
    _input_path_dict: dict[Path, str] = field(default_factory=dict)
    _output_path_dict: dict[PathLike, VariableOptions | str] = field(
        default_factory=dict
    )
    _strict_fail_if_var_in_input_path: bool = True
    _allow_check_fail: bool = False

    class VarirableInBaseImportPathError(Exception):
        """Checking import path validity for `self.variables`."""

        pass

    def __post_init__(self) -> None:
        """Populate config attributes."""
        self.check_paths()
        self.total_cpus: int | None = cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus
        if not self.start_index:
            self.start_index = 0
        if not self.start_calc_index:
            self.start_calc_index = self.start_index

    @property
    def input_folder(self) -> Path | None:
        """Return `self._input_path` set by `set_input_paths()`."""
        if hasattr(self, "_input_path"):
            return Path(self._input_path)
        else:
            return None

    @property
    def output_folder(self) -> Path | None:
        """Return `self._output_path` set by `set_output_paths()`."""
        if hasattr(self, "_output_path"):
            return Path(self._output_path)
        else:
            return None

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"start_date={repr(self.start_date)[9:]}, "
            f"end_date={repr(self.end_date)[9:]}, "
            f"variables_count={len(self.variables)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_output_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        append_output_path_dict: bool = False,
    ) -> Iterator[tuple[Path, Path]]:
        """Yield paths of converted `self.variables` and `self.runs`."""
        for var in self.variables:
            input_path: Path = Path(path) / var / self.sub_path
            output_path: Path = Path(path) / var
            if append_input_path_dict:
                self._input_path_dict[input_path] = var
            if append_output_path_dict:
                self._output_path_dict[output_path] = var
            yield input_path, output_path

    def check_paths(
        self,
        run_set_data_paths: bool = True,
    ):
        """Check and set `input`, `convert` and `crop` paths."""

        if run_set_data_paths:
            self.set_input_paths()
            self.set_output_paths()
        assert isinstance(self.input_paths, Iterable)
        assert isinstance(self.output_paths, Iterable)
        assert len(self.input_paths) == len(self.output_paths)
        for path in self.input_paths:
            try:
                assert Path(path).exists()
                assert Path(path).is_dir()
            except AssertionError:
                message: str = (
                    f"One of 'self.input_paths' in {self} not valid: '{path}'"
                )
                if self._allow_check_fail:
                    logger.error(message)
                else:
                    raise FileExistsError(message)
            try:
                assert path in self._input_path_dict
            except AssertionError:
                NotImplementedError(
                    f"Syncing `self._input_path_dict` with changes to `self.input_paths`."
                )

    def set_input_paths(self):
        """Propagate `self.input_paths` if needed."""
        if isinstance(self.input_paths, PathLike):
            self._input_path = self.input_paths
            self.input_paths = tuple(
                input_path
                for input_path, _ in self._gen_output_folder_paths(
                    self.input_paths, append_input_path_dict=True
                )
            )
            if self._strict_fail_if_var_in_input_path:
                for var in self.variables:
                    try:
                        assert var not in str(self._input_path)
                    except AssertionError:
                        raise self.VarirableInBaseImportPathError(
                            f"Folder named '{var}' in self._input_path: "
                            f"'{self._input_path}'. Try passing a parent path or "
                            f"set '_strict_fail_if_var_in_input_path' to 'False'."
                        )

    def set_output_paths(self):
        """Propagate `self.output_paths` if needed."""
        if isinstance(self.output_paths, PathLike):
            self._output_path = self.output_paths
            self.output_paths = tuple(
                output_path
                for _, output_path in self._gen_output_folder_paths(
                    self.output_paths, append_output_path_dict=True
                )
            )

    def yield_configs(self, **kwargs) -> Iterable[IterCalcBase]:
        """Generate a `CPMConvert` or `HADsConvert` for `self.input_paths`."""
        self.check_paths()
        assert isinstance(self.output_paths, Iterable)
        assert isinstance(self.calc_class, type(IterCalcBase))
        if self.calc_class == HADsConvert:
            kwargs["cpm_for_coord_alignment"] = self.cpm_for_coord_alignment
            kwargs["cpm_for_coord_alignment_path_converted"] = (
                self.cpm_for_coord_alignment_path_converted
            )
        for index, var_path in enumerate(
            islice(self._input_path_dict.items(), self.start_index, self.stop_index)
        ):
            yield self.calc_class(
                input_path=var_path[0],
                output_path=self.output_paths[index],
                variable_name=var_path[1],
                start_index=self.start_calc_index or 0,
                stop_index=self.stop_calc_index,
                **self.config_default_kwargs,
                **kwargs,
            )

    def __len__(self) -> int:
        """Return the length of `self.input_files`."""
        return (
            len(self.input_paths[self.start_index : self.stop_index])
            if isinstance(self.input_paths, Sequence)
            else 0
        )

    @property
    def max_count(self) -> int:
        """Maximum length of `self.input_files` ignoring `start_index` and `start_index`."""
        return len(self.input_paths) if isinstance(self.input_paths, Sequence) else 0

    def __iter__(self) -> Iterator[Path] | None:
        if isinstance(self.input_paths, Sequence):
            for file_path in self.input_paths[self.start_index : self.stop_index]:
                yield Path(file_path)
        else:
            return None

    def __getitem__(self, key: int | slice) -> Path | tuple[Path, ...] | None:
        if not self.input_paths or not isinstance(self.input_paths, Sequence):
            return None
        elif isinstance(key, int):
            return Path(self.input_paths[key])
        elif isinstance(key, slice):
            return tuple(Path(path) for path in self.input_paths[key])
        else:
            raise IndexError(f"Can only index with 'int', not: '{key}'")

    def execute_configs(
        self,
        multiprocess: bool = False,
        cpus: int | None = None,
        return_instances: bool = False,
        return_path: bool = True,
        **kwargs,
    ) -> tuple[IterCalcBase, ...] | list[T_Dataset | Path]:
        """Run all converter configurations

        Parameters
        ----------
        multiprocess
            If `True` run parameters in `convert_configs` with `multiprocess_execute`.
        cpus
            Number of `cpus` to pass to `multiprocess_execute`.
        return_instances
            Return instances of generated `HADsConvert` or
            `CPMConvert`, or return the `results` of each
            `execute` call.
        return_path
            Return `Path` to results object if True, else converted `Dataset`.
        kwargs
            Parameters to path to sampler `execute` calls.
        """
        data_type: ClimDataType = (
            HADS_NAME if HADS_NAME in type(self).__name__ else CPM_NAME
        )
        return execute_configs(
            self,
            configs_method=self._configs_method_name,
            multiprocess=multiprocess,
            cpus=cpus,
            return_instances=return_instances,
            return_path=return_path,
            data_type=data_type,
            description_iter_func=path_parent_types,
            description_iter_kwargs={
                "data_type": data_type,
                "trim_tail": -1,
                "nc_file": True,
            },
            **kwargs,
        )


@dataclass(kw_only=True, repr=False)
class HADsConvertManager(IterCalcManagerBase):
    """Class to manage processing HADs resampling.

    Attributes
    ----------
    input_paths
        `Path` or `Paths` to `CPM` files to process. If `Path`, will be propegated with files matching
    output_paths
        `Path` or `Paths` to to save processed `CPM` files to. If `Path` will be propagated to match `input_paths`.
    variables
        Which `VariableOptions` to include.
    crop_regions
        `RegionOptions` (like Manchester, Scotland etc.) to crop results to.
    crop_paths
        Where to save region crop files.
    sub_path
        `Path` to include at the stem of `input_paths`.
    start_index
        Index to begin iterating input files for `resampling` or `cropping`.
    stop_index
        Index to to run from `start_index` to when `resampling` or
        `cropping`. If `None`, iterate full list of paths.
    start_date
        Not yet implemented, but in future from what date to generate start index from.
    end_date
        Not yet implemented, but in future from what date to generate stop index from.
    configs
        List of `HADsConvert` instances to iterate `resampling` or `cropping`.
    config_default_kwargs
        Parameters passed to all running `self.configs`.
    calc_class
        `class` to construct all `self.configs` instances with.
    cpus
        Number of `cpu` cores to use during multiprocessing.
    cpm_for_coord_alignment
        `CPM` `Path` or `Dataset` to match alignment with.
    cpm_for_coord_alignment_path_converted
        Whether a `Path` passed to `cpm_for_coord_alignment` should be processed.

    Examples
    --------
    >>> if not is_data_mounted or local_cache:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_hads_output_path: Path = getfixture(
    ...     'resample_test_hads_output_path')
    >>> hads_converter_manager: HADsConvertManager = HADsConvertManager(
    ...     variables=VariableOptions.all(),
    ...     stop_index=2, stop_calc_index=10,
    ...     output_paths=resample_test_hads_output_path,
    ...     )
    >>> hads_converter_manager
    <HADsConvertManager(start_date=date(1980, 1, 1),
                        end_date=date(2021, 12, 31),
                        variables_count=3,
                        input_paths_count=3)>
    >>> configs: tuple[HADsConvert, ...] = tuple(
    ...     hads_converter_manager.yield_configs())
    >>> pprint(configs)
    (<HADsConvert(count=10, max_count=504,
                  start_date=date(1980, 1, 1),
                  end_date=date(2021, 12, 31),
                  input_path='.../tasmax/day',
                  output_path='.../hads/tasmax')>,
     <HADsConvert(count=10, max_count=504,
                  start_date=date(1980, 1, 1),
                  end_date=date(2021, 12, 31),
                  input_path='.../rainfall/day',
                  output_path='.../hads/rainfall')>)
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_HADS_PATH
    output_paths: PathLike | Sequence[PathLike] = CONVERT_OUTPUT_PATH / HADS_OUTPUT_PATH
    sub_path: Path = HADS_SUB_PATH
    start_date: date | None = HADS_START_DATE
    end_date: date | None = HADS_END_DATE
    configs: list[HADsConvert] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    calc_class: type[HADsConvert] = HADsConvert
    cpm_for_coord_alignment: T_Dataset | PathLike = RAW_CPM_TASMAX_PATH
    cpm_for_coord_alignment_path_converted: bool = False

    def set_cpm_for_coord_alignment(self) -> None:
        """Check if `cpm_for_coord_alignment` is a `Dataset`, process if a `Path`."""
        self.cpm_for_coord_alignment = get_cpm_for_coord_alignment(
            self.cpm_for_coord_alignment,
            skip_reproject=self.cpm_for_coord_alignment_path_converted,
        )


@dataclass(kw_only=True, repr=False)
class CPMConvertManager(IterCalcManagerBase):
    """Class to manage processing CPM resampling.

    Attributes
    ----------
    input_paths
        `Path` or `Paths` to `CPM` files to process. If `Path`, will be propegated with files matching
    output_paths
        `Path` or `Paths` to to save processed `CPM` files to. If `Path` will be propagated to match `input_paths`.
    variables
        Which `VariableOptions` to include.
    runs
        Which `RunOptions` to include.
    crop_regions
        `RegionOptions` (like Manchester, Scotland etc.) to crop results to.
    crop_paths
        Where to save region crop files.
    sub_path
        `Path` to include at the stem of `input_paths`.
    start_index
        Index to begin iterating input files for `resampling` or `cropping`.
    stop_index
        Index to to run from `start_index` to when `resampling` or
        `cropping`. If `None`, iterate full list of paths.
    start_date
        Not yet implemented, but in future from what date to generate start index from.
    end_date
        Not yet implemented, but in future from what date to generate stop index from.
    configs
        List of `HADsConvert` instances to iterate `resampling` or `cropping`.
    config_default_kwargs
        Parameters passed to all running `self.configs`.
    calc_class
        `class` to construct all `self.configs` instances with.
    cpus
        Number of `cpu` cores to use during multiprocessing.


    Examples
    --------
    >>> if not is_data_mounted or local_cache:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_cpm_output_path: Path = getfixture(
    ...         'resample_test_cpm_output_path')
    >>> cpm_converter_manager: CPMConvertManager = CPMConvertManager(
    ...     stop_index=3, stop_calc_index=10,
    ...     output_paths=resample_test_cpm_output_path,
    ...     )
    >>> cpm_converter_manager
    <CPMConvertManager(start_date=date(1980, 12, 1),
                       end_date=date(2080, 11, 30),
                       variables_count=1,
                       runs_count=4,
                       input_paths_count=4)>
    >>> configs: tuple[CPMConvert, ...] = tuple(
    ...     cpm_converter_manager.yield_configs())
    >>> pprint(configs)
    (<CPMConvert(count=10, max_count=100,
                 start_date=date(1980, 12, 1),
                 end_date=date(2080, 11, 30),
                 input_path='.../tasmax/05/latest',
                 output_path='.../cpm/tasmax/05')>,
     <CPMConvert(count=10, max_count=100,
                 start_date=date(1980, 12, 1),
                 end_date=date(2080, 11, 30),
                 input_path='.../tasmax/06/latest',
                 output_path='.../cpm/tasmax/06')>,
     <CPMConvert(count=10, max_count=100,
                 start_date=date(1980, 12, 1),
                 end_date=date(2080, 11, 30),
                 input_path='.../tasmax/07/latest',
                 output_path='.../cpm/tasmax/07')>)
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_CPM_PATH
    output_paths: PathLike | Sequence[PathLike] = CONVERT_OUTPUT_PATH / CPM_OUTPUT_PATH
    sub_path: Path = CPM_SUB_PATH
    start_date: date | None = CPM_START_DATE
    end_date: date | None = CPM_END_DATE
    configs: list[CPMConvert] = field(default_factory=list)
    calc_class: type[CPMConvert] = CPMConvert
    runs: Sequence[RunOptions | str] = RunOptions.preferred()

    # Uncomment if cpm specific paths like 'pr' for 'rainbow'
    # are needed at the manager level.
    # @property
    # def cpm_vars(self) -> tuple[str, ...]:
    #     """Ensure variable paths match `CPM` path names."""
    #     return VariableOptions.cpm_values(self.variables)

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"start_date={repr(self.start_date)[9:]}, "
            f"end_date={repr(self.end_date)[9:]}, "
            f"variables_count={len(self.variables)}, "
            f"runs_count={len(self.runs)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_output_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        append_output_path_dict: bool = False,
        cpm_paths: bool = True,
    ) -> Iterator[tuple[Path, Path]]:
        """Return a Generator of paths of `self.variables` and `self.runs`."""
        for var in self.variables:
            for run_type in self.runs:
                if cpm_paths:
                    input_path: Path = (
                        Path(path)
                        / VariableOptions.cpm_value(var)
                        / run_type
                        / self.sub_path
                    )
                    output_path: Path = (
                        Path(path) / VariableOptions.cpm_value(var) / run_type
                    )
                else:
                    input_path = Path(path) / var / run_type / self.sub_path
                    output_path = Path(path) / var / run_type
                if append_input_path_dict:
                    self._input_path_dict[input_path] = var
                if append_output_path_dict:
                    self._output_path_dict[output_path] = var
                yield input_path, output_path
