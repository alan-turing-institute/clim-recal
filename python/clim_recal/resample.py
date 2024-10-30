"""Resample UKHADS data and UKCP18 data.

- UKCP18 is resampled temporally from a 360 day calendar to a standard (365/366 day) calendar and projected to British National Grid (BNG) EPSG:27700 from its original rotated polar grid.
- UKHADS is resampled spatially from 1km to 2.2km in BNG aligned with the projected UKCP18
"""

from dataclasses import dataclass, field
from datetime import date
from glob import glob
from logging import getLogger
from os import PathLike, cpu_count
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Iterator, Literal, Sequence

import dill as pickle
import rioxarray  # nopycln: import
from osgeo.gdal import Dataset as GDALDataset
from rich import print
from tqdm.rich import trange
from xarray import Dataset
from xarray.core.types import T_Dataset

from clim_recal.debiasing.debias_wrapper import VariableOptions

from .utils.core import climate_data_mount_path, console, multiprocess_execute
from .utils.data import (
    CPM_END_DATE,
    CPM_RAW_X_COLUMN_NAME,
    CPM_RAW_Y_COLUMN_NAME,
    CPM_START_DATE,
    CPM_SUB_PATH,
    HADS_END_DATE,
    HADS_START_DATE,
    HADS_SUB_PATH,
    HADS_XDIM,
    HADS_YDIM,
    RegionOptions,
    RunOptions,
    VariableOptions,
)
from .utils.gdal_formats import TIF_EXTENSION_STR
from .utils.xarray import (
    BRITISH_NATIONAL_GRID_EPSG,
    NETCDF_EXTENSION_STR,
    ReprojectFuncType,
    apply_geo_func,
    cpm_reproject_with_standard_calendar,
    crop_xarray,
    get_cpm_for_coord_alignment,
    hads_resample_and_reproject,
    region_crop_file_name,
)

logger = getLogger(__name__)

CLIMATE_DATA_MOUNT_PATH: Path = climate_data_mount_path()

CFCalendarSTANDARD: Final[str] = "standard"

# RESAMPLING_OUTPUT_PATH: Final[PathLike] = (
#    CLIMATE_DATA_MOUNT_PATH / "CPM-365/andys-two-gdal-step-approach/resample"
# )
RESAMPLING_OUTPUT_PATH: Final[PathLike] = "/datadrive/clim-recal-results/cropped"
RAW_HADS_PATH: Final[PathLike] = CLIMATE_DATA_MOUNT_PATH / "Raw/HadsUKgrid"
RAW_CPM_PATH: Final[PathLike] = CLIMATE_DATA_MOUNT_PATH / "Raw/UKCP2.2"
RAW_HADS_TASMAX_PATH: Final[PathLike] = RAW_HADS_PATH / "tasmax/day"
RAW_CPM_TASMAX_PATH: Final[PathLike] = RAW_CPM_PATH / "tasmax/01/latest"

CPM_OUTPUT_LOCAL_PATH: Final[Path] = Path("cpm")
HADS_OUTPUT_LOCAL_PATH: Final[Path] = Path("hads")
CPM_CROP_OUTPUT_LOCAL_PATH: Final[Path] = Path("cpm-crop")
HADS_CROP_OUTPUT_LOCAL_PATH: Final[Path] = Path("hads-crop")


NETCDF_OR_TIF = Literal[TIF_EXTENSION_STR, NETCDF_EXTENSION_STR]


def reproject_standard_calendar_filename(path: Path) -> Path:
    """Return tweaked `path` to indicate standard day projection."""
    return path.parent / path.name.replace("_day", "_day_std_year")


def reproject_2_2km_filename(path: Path) -> Path:
    """Return tweaked `path` to indicate standard day projection."""
    return path.parent / path.name.replace("_1km", "_2_2km")


@dataclass(kw_only=True)
class ResamblerBase:
    """Base class to inherit for `HADs` and `CPM`."""

    input_path: PathLike | None = Path()
    output_path: PathLike = RESAMPLING_OUTPUT_PATH
    variable_name: VariableOptions | str = VariableOptions.default()
    input_files: Iterable[PathLike] | None = None
    cpus: int | None = None
    crop_region: RegionOptions | str | None = RegionOptions.GLASGOW
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG
    input_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    export_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    input_file_x_column_name: str = ""
    input_file_y_column_name: str = ""
    start_index: int = 0
    stop_index: int | None = None

    def __post_init__(self) -> None:
        """Generate related attributes."""
        try:
            assert self.input_path or self.input_files
        except AssertionError:
            raise AttributeError(
                f"'input_path' or 'input_file' are None; at least one must be set."
            )
        self.set_input_files()
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        if self.crop_region:
            Path(self.crop_path).mkdir(parents=True, exist_ok=True)
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
        if self.input_files and isinstance(self.input_files, Sequence):
            for file_path in self.input_files[self.start_index : self.stop_index]:
                yield Path(file_path)
        else:
            return None

    def __getitem__(self, key: int | slice) -> Path | tuple[Path] | None:
        if not self.input_files:
            return None
        elif isinstance(key, int):
            return Path(self.input_files[key])
        elif isinstance(key, slice):
            return tuple(Path(path) for path in self.input_files[key])
        else:
            raise IndexError(f"Can only index with 'int', not: '{key}'")

    def set_input_files(self, new_input_path: PathLike | None = None) -> None:
        """Replace `self.input` and process `self.input_files`."""
        if new_input_path:
            assert Path(new_input_path).exists()
            self.input = new_input_path
        if not self.input_files or new_input_path:
            self.input_files = tuple(
                Path(path)
                for path in glob(
                    f"{self.input_path}/*.{self.input_file_extension}", recursive=True
                )
            )

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"count={len(self)}, "
            f"max_count={self.max_count}, "
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

    def _output_path(
        self, relative_output_path: Path, override_export_path: Path | None
    ) -> Path:
        path: PathLike = (
            override_export_path or Path(self.output_path) / relative_output_path
        )
        path.mkdir(exist_ok=True, parents=True)
        return path

    def _range_call(
        self,
        method: Callable,
        start: int,
        stop: int | None,
        step: int,
        override_export_path: Path | None = None,
        source_to_index: Iterable | None = None,
    ) -> list[Path | T_Dataset]:
        export_paths: list[Path | T_Dataset] = []
        if stop is None:
            stop = len(self)
        for index in trange(start, stop, step):
            export_paths.append(
                method(
                    index=index,
                    override_export_path=override_export_path,
                    source_to_index=source_to_index,
                )
            )
        return export_paths

    def range_to_reprojection(
        self,
        start: int | None = None,
        stop: int | None = None,
        step: int = 1,
        override_export_path: Path | None = None,
        source_to_index: Sequence | None = None,
    ) -> list[Path]:
        start = start or self.start_index
        stop = stop or self.stop_index
        return self._range_call(
            method=self.to_reprojection,
            start=start,
            stop=stop,
            step=step,
            override_export_path=override_export_path,
            source_to_index=source_to_index,
        )

    def execute(self, skip_spatial: bool = False, **kwargs) -> list[Path] | None:
        """Run all steps for processing"""
        return self.range_to_reprojection(**kwargs) if not skip_spatial else None

    def execute_crops(self, skip_crop: bool = False, **kwargs) -> list[Path] | None:
        """Run all specified crops."""
        return self.range_crop_projection(**kwargs) if not skip_crop else None

    def _sync_reprojected_paths(
        self, overwrite_output_path: PathLike | None = None
    ) -> None:
        """Sync `self._reprojected_paths` with files in `self.export_path`."""
        if not hasattr(self, "_reprojected_paths"):
            if overwrite_output_path:
                self.output_path = overwrite_output_path
            path: PathLike = self.output_path
        self._reprojected_paths: list[Path] = [
            local_path
            for local_path in Path(path).iterdir()
            if local_path.is_file() and local_path.suffix == f".{NETCDF_EXTENSION_STR}"
        ]

    def range_crop_projection(
        self,
        start: int | None = None,
        stop: int | None = None,
        step: int = 1,
        override_export_path: Path | None = None,
        return_results: bool = False,
        # possible meanse of reducing memory issues by removing
        # xarray instance while keeping paths for logging purposes
        # delete_xarray_after_save: bool = True,
        **kwargs,
    ) -> list[Path]:
        start = start or self.start_index
        stop = stop or self.stop_index
        export_paths: list[Path | T_Dataset] = []
        if stop is None:
            stop = len(self)
        console.print(f"Cropping to '{self.crop_path}'")
        for index in trange(start, stop, step):
            export_paths.append(
                self.crop_projection(
                    # region=region,
                    index=index,
                    override_export_path=override_export_path,
                    return_results=return_results,
                    **kwargs,
                )
            )
        return export_paths

    def crop_projection(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        return_results: bool = False,
        sync_reprojection_paths: bool = True,
        **kwargs,
    ) -> Path | T_Dataset:
        """Crop a projection to `region` geometry."""
        console.log(f"Preparing to crop `_reprojected_paths` index {index} from {self}")
        try:
            assert hasattr(self, "_reprojected_paths")
        except AssertionError:
            if sync_reprojection_paths:
                self._sync_reprojected_paths()
            else:
                raise AttributeError(
                    f"'_reprojected_paths' must be set. "
                    "Run after 'self.to_reprojection()' or set as a "
                    "list directly."
                )
        try:
            assert self.crop_region in RegionOptions
        except AttributeError:
            raise ValueError(
                f"'{self.crop_path}' not in 'RegionOptions': {RegionOptions.all()}"
            )
        path: PathLike = override_export_path or Path(self.crop_path)  # / (region)
        path.mkdir(exist_ok=True, parents=True)
        resampled_xr: Dataset = self._reprojected_paths[index]

        console.log(f"From {self} crop {resampled_xr}")
        cropped: Dataset = crop_xarray(
            xr_time_series=resampled_xr,
            crop_box=RegionOptions.bounding_box(self.crop_region),
            **kwargs,
        )
        cropped_file_name: str = region_crop_file_name(
            self.crop_region, resampled_xr.name
        )
        export_path: Path = path / cropped_file_name
        cropped.to_netcdf(export_path)
        if not hasattr(self, "_cropped_paths"):
            self._cropped_paths: list[PathLike] = []
        self._cropped_paths.append(export_path)
        if return_results:
            return cropped
        else:
            return export_path


@dataclass(kw_only=True, repr=False)
class HADsResampler(ResamblerBase):
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    input_files
        `Path` or `Paths` of `NCF` files to resample.
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

    Notes
    -----
    - [x] Try time projection first
    - [x] Combine with space (this worked)
    - [x] Add crop step

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_hads_output_path: Path = getfixture(
    ...         'resample_test_hads_output_path')
    >>> hads_resampler: HADsResampler = HADsResampler(  # doctest: +SKIP
    ...     output_path=resample_test_hads_output_path,
    ... )
    >>> hads_resampler  # doctest: +SKIP
    <HADsResampler(...count=504,...
        ...input_path='.../tasmax/day',...
        ...output_path='...run-results_..._.../hads')>
    >>> pprint(hads_resampler.input_files)   # doctest: +SKIP
    (...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc'),
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800201-19800229.nc'),
     ...,
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc'))
    """

    input_path: PathLike | None = RAW_HADS_TASMAX_PATH
    output_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_LOCAL_PATH
    input_files: Iterable[PathLike] | None = None

    cpus: int | None = None
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_CROP_OUTPUT_LOCAL_PATH
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG
    input_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    export_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    input_file_x_column_name: str = HADS_XDIM
    input_file_y_column_name: str = HADS_YDIM
    cpm_for_coord_alignment: T_Dataset | PathLike | None = RAW_CPM_TASMAX_PATH
    cpm_for_coord_alignment_path_converted: bool = False
    _resample_func: ReprojectFuncType = hads_resample_and_reproject

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
        return_results: bool = False,
        source_to_index: Sequence | None = None,
    ) -> Path | T_Dataset:
        source_path: Path = self._get_source_path(
            index=index, source_to_index=source_to_index
        )
        path: PathLike = self.output_path
        console.log(f"Setting 'cpm_for_coord_alignment' for {self}")
        self.set_cpm_for_coord_alignment()
        console.log(f"Set 'cpm_for_coord_alignment' for {self}")
        return apply_geo_func(
            source_path=source_path,
            func=self._resample_func,
            export_folder=path,
            # Leaving in case we return to using warp
            # export_path_as_output_path_kwarg=True,
            # to_netcdf=False,
            to_netcdf=True,
            variable_name=self.variable_name,
            x_dim_name=self.input_file_x_column_name,
            y_dim_name=self.input_file_y_column_name,
            cpm_to_match=self.cpm_for_coord_alignment,
            new_path_name_func=reproject_2_2km_filename,
            return_results=return_results,
        )


@dataclass(kw_only=True, repr=False)
class CPMResampler(ResamblerBase):
    """CPM specific changes to HADsResampler.

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
    input_file_x_column_name
        Column name in `input_files` or `input` for `x` coordinates.
    input_file_y_column_name
        Column name in `input_files` or `input` for `y` coordinates.
    input_file_extension
        File extensions to glob `input_files` with.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_cpm_output_path: Path = getfixture(
    ...         'resample_test_cpm_output_path')
    >>> cpm_resampler: CPMResampler = CPMResampler(
    ...     input_path=RAW_CPM_TASMAX_PATH,
    ...     output_path=resample_test_cpm_output_path,
    ...     crop_path=resample_test_cpm_output_path,
    ...     input_file_extension=TIF_EXTENSION_STR,
    ... )
    >>> cpm_resampler
    <CPMResampler(...count=2,...
        ...input_path='.../tasmax/01/latest',...
        ...output_path='.../test-run-results_..._.../cpm')>
    >>> pprint(cpm_resampler.input_files)
    (...Path('.../tasmax/01/latest/tasmax_..._19821201-19831130geotif.tif'),
     ...Path('.../tasmax/01/latest/tasmax_..._19901201-19911130geotif.tif'))
    """

    input_path: PathLike | None = RAW_CPM_TASMAX_PATH
    output_path: PathLike = RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_LOCAL_PATH
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH / CPM_CROP_OUTPUT_LOCAL_PATH
    input_file_x_column_name: str = CPM_RAW_X_COLUMN_NAME
    input_file_y_column_name: str = CPM_RAW_Y_COLUMN_NAME
    prior_time_series: PathLike | Dataset | None = None
    _resample_func: ReprojectFuncType = cpm_reproject_with_standard_calendar

    @property
    def cpm_variable_name(self) -> str:
        return VariableOptions.cpm_value(self.variable_name)

    def to_reprojection(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        return_results: bool = False,
        source_to_index: Sequence | None = None,
    ) -> Path | T_Dataset:
        source_path: Path = self._get_source_path(
            index=index, source_to_index=source_to_index
        )
        path: PathLike = self.output_path
        console.log(f"Reprojecting index CPM {index}...")
        result: Path | T_Dataset | GDALDataset = apply_geo_func(
            source_path=source_path,
            func=self._resample_func,
            new_path_name_func=reproject_standard_calendar_filename,
            export_folder=path,
            to_netcdf=True,
            variable_name=self.cpm_variable_name,
            return_results=return_results,
        )
        console.log(f"Completed index CPM {index}...")
        if isinstance(result, PathLike):
            if not hasattr(self, "_reprojected_paths"):
                self._reprojected_paths: list[Path] = []
            self._reprojected_paths.append(Path(result))
        return result

    def __getstate__(self):
        """Meanse of testing what aspects of instance have issues multiprocessing.

        Notes
        -----
        As of 2 May 2023, picking is not an error on macOS but *is*
        on the server configured Linux architecture. This may relate
        to differences between Linux using `fork` while `win` and `macOS`
        use `spawn`. This `method` helps test that on instances of this
        `class`.
        """
        for variable_name, value in vars(self).items():
            try:
                pickle.dumps(value)
            except pickle.PicklingError:
                print(f"{variable_name} with value {value} is not pickable")


@dataclass(kw_only=True)
class ResamblerManagerBase:
    """Base class to inherit for `HADs` and `CPM` resampler managers."""

    input_paths: PathLike | Sequence[PathLike] = Path()
    resample_paths: PathLike | Sequence[PathLike] = Path()
    variables: Sequence[VariableOptions | str] = (VariableOptions.default(),)
    crop_regions: tuple[RegionOptions | str, ...] | None = RegionOptions.all()
    crop_paths: Sequence[PathLike] | PathLike = Path()
    sub_path: Path = Path()
    start_index: int = 0
    stop_index: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    configs: list[HADsResampler | CPMResampler] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    resampler_class: type[HADsResampler | CPMResampler] | None = None
    cpus: int | None = None
    _input_path_dict: dict[Path, str] = field(default_factory=dict)
    _resampled_path_dict: dict[PathLike, VariableOptions | str] = field(
        default_factory=dict
    )
    _cropped_path_dict: dict[PathLike, VariableOptions | str] = field(
        default_factory=dict
    )
    _strict_fail_if_var_in_input_path: bool = True
    _allow_check_fail: bool = False

    class VarirableInBaseImportPathError(Exception):
        """Checking import path validity for `self.variables`."""

        pass

    def __post_init__(self) -> None:
        """Populate config attributes."""
        if not self.crop_regions:
            self.crop_regions = ()
        self.check_paths()
        self.total_cpus: int | None = cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus
        # self.cpm_for_coord_alignment: T_Dataset | PathLike = RAW_CPM_TASMAX_PATH

    @property
    def input_folder(self) -> Path | None:
        """Return `self._input_path` set by `set_input_paths()`."""
        if hasattr(self, "_input_path"):
            return Path(self._input_path)
        else:
            return None

    @property
    def resample_folder(self) -> Path | None:
        """Return `self._output_path` set by `set_resample_paths()`."""
        if hasattr(self, "_output_path"):
            return Path(self._output_path)
        else:
            return None

    @property
    def crop_folder(self) -> Path | None:
        """Return `self._output_path` set by `set_resample_paths()`."""
        if hasattr(self, "_crop_path"):
            return Path(self._crop_path)
        else:
            return None

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"variables_count={len(self.variables)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_resample_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        append_resampled_path_dict: bool = False,
    ) -> Iterator[tuple[Path, Path]]:
        """Yield paths of resampled `self.variables` and `self.runs`."""
        for var in self.variables:
            input_path: Path = Path(path) / var / self.sub_path
            resample_path: Path = Path(path) / var
            if append_input_path_dict:
                self._input_path_dict[input_path] = var
            if append_resampled_path_dict:
                self._resampled_path_dict[resample_path] = var
            yield input_path, resample_path

    def check_paths(
        self, run_set_data_paths: bool = True, run_set_crop_paths: bool = True
    ):
        """Check and set `input`, `resample` and `crop` paths."""

        if run_set_data_paths:
            self.set_resample_paths()
        if run_set_crop_paths:
            self.set_crop_paths()
        assert isinstance(self.input_paths, Iterable)
        assert isinstance(self.resample_paths, Iterable)
        if self.crop_paths:
            try:
                assert isinstance(self.crop_paths, Iterable)
            except AssertionError:
                raise ValueError(
                    f"'crop_paths' not iterable for {self}. Hint: try setting 'run_set_crop_paths' to 'True'."
                )
        assert len(self.input_paths) == len(self.resample_paths)
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

    def _set_input_paths(self):
        """Propagate `self.input_paths` if needed."""
        if isinstance(self.input_paths, PathLike):
            self._input_path = self.input_paths
            self.input_paths = tuple(
                input_path
                for input_path, _ in self._gen_resample_folder_paths(
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

    def set_resample_paths(self):
        """Propagate `self.resample_paths` if needed."""
        self._set_input_paths()
        if isinstance(self.resample_paths, PathLike):
            self._output_path = self.resample_paths
            self.resample_paths = tuple(
                resample_path
                for _, resample_path in self._gen_resample_folder_paths(
                    self.resample_paths, append_resampled_path_dict=True
                )
            )

    def set_crop_paths(self) -> None:
        """Propagate `self.resample_paths` if needed."""
        if isinstance(self.crop_paths, PathLike):
            self._crop_path = self.crop_paths
            self.crop_paths = tuple(
                self._gen_crop_folder_paths(
                    self.crop_paths, append_cropped_path_dict=True
                )
            )

    def yield_configs(self) -> Iterable[ResamblerBase]:
        """Generate a `CPMResampler` or `HADsResampler` for `self.input_paths`."""
        self.check_paths()
        assert isinstance(self.resample_paths, Iterable)
        for index, var_path in enumerate(self._input_path_dict.items()):
            yield self.resampler_class(
                input_path=var_path[0],
                output_path=self.resample_paths[index],
                # Setting to avoid test failure,
                # but crop_path should not be used
                crop_path=self.crop_folder or "",
                variable_name=var_path[1],
                start_index=self.start_index,
                stop_index=self.stop_index,
                **self.config_default_kwargs,
            )

    def yield_crop_configs(self) -> Iterable[ResamblerBase]:
        """Generate a `CPMResampler` or `HADsResampler` for `self.input_paths`."""
        self.check_paths()
        assert isinstance(self.input_paths, Iterable)
        assert isinstance(self.resample_paths, Iterable)
        assert isinstance(self.crop_paths, Iterable)
        for index, input_resample_paths in enumerate(self._resampled_path_dict.items()):
            for crop_path, region in self._cropped_path_dict.items():
                yield self.resampler_class(
                    input_path=input_resample_paths[0],
                    output_path=self.resample_paths[index],
                    variable_name=input_resample_paths[1],
                    start_index=self.start_index,
                    stop_index=self.stop_index,
                    crop_path=crop_path,
                    # Todo: remove below if single crop configs iterate over all
                    # crop_regions=self.crop_regions,
                    # crop_regions=(region,),
                    crop_region=region,
                    **self.config_default_kwargs,
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

    def execute_resample_configs(
        self, multiprocess: bool = False, cpus: int | None = None
    ) -> tuple[ResamblerBase, ...]:
        """Run all resampler configurations

        Parameters
        ----------
        multiprocess
            If `True` run parameters in `resample_configs` with `multiprocess_execute`.
        cpus
            Number of `cpus` to pass to `multiprocess_execute`.
        """
        resamplers: tuple[ResamblerBase, ...] = tuple(self.yield_configs())
        results: list[list[Path] | None] = []
        if multiprocess:
            cpus = cpus or self.cpus
            if self.total_cpus and cpus:
                cpus = min(cpus, self.total_cpus - 1)
            results = multiprocess_execute(resamplers, method_name="execute", cpus=cpus)
        else:
            for resampler in resamplers:
                print(resampler)
                results.append(resampler.execute())
        return resamplers

    def execute_crop_configs(
        self, multiprocess: bool = False, cpus: int | None = None
    ) -> tuple[ResamblerBase, ...]:
        """Run all resampler configurations

        Parameters
        ----------
        multiprocess
            If `True` run parameters in `resample_configs` with `multiprocess_execute`.
        cpus
            Number of `cpus` to pass to `multiprocess_execute`.
        """
        croppers: tuple[ResamblerBase, ...] = tuple(self.yield_crop_configs())
        results: list[list[Path] | None] = []
        if multiprocess:
            cpus = cpus or self.cpus
            if self.total_cpus and cpus:
                cpus = min(cpus, self.total_cpus - 1)
            results = multiprocess_execute(
                croppers, method_name="execute_crops", cpus=cpus
            )
        else:
            for cropper in croppers:
                print(cropper)
                results.append(cropper.execute_crops())
        return croppers


@dataclass(kw_only=True, repr=False)
class HADsResamplerManager(ResamblerManagerBase):
    """Class to manage processing HADs resampling.

    Attributes
    ----------
    input_paths
        `Path` or `Paths` to `CPM` files to process. If `Path`, will be propegated with files matching
    resample_paths
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
        List of `HADsResampler` instances to iterate `resampling` or `cropping`.
    config_default_kwargs
        Parameters passed to all running `self.configs`.
    resampler_class
        `class` to construct all `self.configs` instances with.
    cpus
        Number of `cpu` cores to use during multiprocessing.
    cpm_for_coord_alignment
        `CPM` `Path` or `Dataset` to match alignment with.
    cpm_for_coord_alignment_path_converted
        Whether a `Path` passed to `cpm_for_coord_alignment` should be processed.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_hads_output_path: Path = getfixture(
    ...         'resample_test_hads_output_path')
    >>> hads_resampler_manager: HADsResamplerManager = HADsResamplerManager(
    ...     variables=VariableOptions.all(),
    ...     resample_paths=resample_test_hads_output_path,
    ...     )
    >>> hads_resampler_manager
    <HADsResamplerManager(variables_count=3, input_paths_count=3)>
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_HADS_PATH
    resample_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_LOCAL_PATH
    )
    crop_paths: Sequence[PathLike] | PathLike = (
        RESAMPLING_OUTPUT_PATH / HADS_CROP_OUTPUT_LOCAL_PATH
    )
    sub_path: Path = HADS_SUB_PATH
    start_date: date = HADS_START_DATE
    end_date: date = HADS_END_DATE
    configs: list[HADsResampler] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    resampler_class: type[HADsResampler] = HADsResampler
    cpm_for_coord_alignment: T_Dataset | PathLike = RAW_CPM_TASMAX_PATH
    cpm_for_coord_alignment_path_converted: bool = False

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"variables_count={len(self.variables)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_crop_folder_paths(
        self, path: PathLike, append_cropped_path_dict: bool = False
    ) -> Iterator[Path | None]:
        """Return a Generator of paths of `self.variables` and `self.crops`."""
        if not self.crop_regions:
            return None
        if not self._resampled_path_dict:
            self._gen_resample_folder_paths(
                self.input_paths,
                append_input_path_dict=True,
                append_resampled_path_dict=True,
            )
        for var in self.variables:
            for region in self.crop_regions:
                crop_path = Path(path) / "hads" / region / var
                if append_cropped_path_dict:
                    self._cropped_path_dict[crop_path] = region
                yield crop_path

    def set_cpm_for_coord_alignment(self) -> None:
        """Check if `cpm_for_coord_alignment` is a `Dataset`, process if a `Path`."""
        self.cpm_for_coord_alignment = get_cpm_for_coord_alignment(
            self.cpm_for_coord_alignment,
            skip_reproject=self.cpm_for_coord_alignment_path_converted,
        )

    def yield_configs(self) -> Iterable[HADsResampler]:
        """Generate a `CPMResampler` or `HADsResampler` for `self.input_paths`."""
        self.check_paths()
        assert isinstance(self.resample_paths, Iterable)
        # assert isinstance(self.crop_paths, Iterable)
        for index, var_path in enumerate(self._input_path_dict.items()):
            yield self.resampler_class(
                input_path=var_path[0],
                output_path=self.resample_paths[index],
                variable_name=var_path[1],
                start_index=self.start_index,
                stop_index=self.stop_index,
                cpm_for_coord_alignment=self.cpm_for_coord_alignment,
                cpm_for_coord_alignment_path_converted=self.cpm_for_coord_alignment_path_converted,
                **self.config_default_kwargs,
            )


@dataclass(kw_only=True, repr=False)
class CPMResamplerManager(ResamblerManagerBase):
    """Class to manage processing CPM resampling.

    Attributes
    ----------
    input_paths
        `Path` or `Paths` to `CPM` files to process. If `Path`, will be propegated with files matching
    resample_paths
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
        List of `HADsResampler` instances to iterate `resampling` or `cropping`.
    config_default_kwargs
        Parameters passed to all running `self.configs`.
    resampler_class
        `class` to construct all `self.configs` instances with.
    cpus
        Number of `cpu` cores to use during multiprocessing.


    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_cpm_output_path: Path = getfixture(
    ...         'resample_test_cpm_output_path')
    >>> cpm_resampler_manager: CPMResamplerManager = CPMResamplerManager(
    ...     stop_index=9,
    ...     resample_paths=resample_test_cpm_output_path,
    ...     crop_paths=resample_test_cpm_output_path,
    ...     )
    >>> cpm_resampler_manager
    <CPMResamplerManager(variables_count=1, runs_count=4,
                         input_paths_count=4)>
    >>> configs: tuple[CPMResampler, ...] = tuple(
    ...     cpm_resampler_manager.yield_configs())
    >>> pprint(configs)
    (<CPMResampler(count=9, max_count=100,
                   input_path='.../tasmax/05/latest',
                   output_path='.../cpm/tasmax/05')>,
     <CPMResampler(count=9, max_count=100,
                   input_path='.../tasmax/06/latest',
                   output_path='.../cpm/tasmax/06')>,
     <CPMResampler(count=9, max_count=100,
                   input_path='.../tasmax/07/latest',
                   output_path='.../cpm/tasmax/07')>,
     <CPMResampler(count=9, max_count=100,
                   input_path='.../tasmax/08/latest',
                   output_path='.../cpm/tasmax/08')>)
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_CPM_PATH
    resample_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_LOCAL_PATH
    )
    sub_path: Path = CPM_SUB_PATH
    start_date: date = CPM_START_DATE
    end_date: date = CPM_END_DATE
    configs: list[CPMResampler] = field(default_factory=list)
    resampler_class: type[CPMResampler] = CPMResampler
    # Runs are CPM simulations, not applicalbe to HADs
    runs: Sequence[RunOptions | str] = RunOptions.preferred()
    crop_paths = RESAMPLING_OUTPUT_PATH / CPM_CROP_OUTPUT_LOCAL_PATH

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
            f"variables_count={len(self.variables)}, "
            f"runs_count={len(self.runs)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_resample_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        append_resampled_path_dict: bool = False,
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
                    resample_path: Path = (
                        Path(path) / VariableOptions.cpm_value(var) / run_type
                    )
                else:
                    input_path = Path(path) / var / run_type / self.sub_path
                    resample_path = Path(path) / var / run_type
                if append_input_path_dict:
                    self._input_path_dict[input_path] = var
                if append_resampled_path_dict:
                    self._resampled_path_dict[resample_path] = var
                yield input_path, resample_path

    def _gen_crop_folder_paths(
        self,
        path: PathLike,
        append_cropped_path_dict: bool = False,
        cpm_paths: bool = True,
    ) -> Iterator[Path]:
        """Return a Generator of paths of `self.variables` and `self.crops`."""
        for var in self.variables:
            for region in self.crop_regions:
                for run_type in self.runs:
                    if cpm_paths:
                        crop_path: Path = (
                            Path(path)
                            / "cpm"
                            / region
                            / VariableOptions.cpm_value(var)
                            / run_type
                        )
                    else:
                        crop_path: Path = Path(path) / "cpm" / region / var / run_type
                    if append_cropped_path_dict:
                        self._cropped_path_dict[crop_path] = region
                    yield crop_path
