"""Resample UKHADS data and UKCP18 data.

- UKHADS is resampled spatially from 1km to 2.2km.
- UKCP18 is resampled temporally from a 360 day calendar to a standard (365/366 day) calendar and projected to British National Grid (BNG) (from rotated polar grid).

## Notes

"""

from dataclasses import dataclass, field
from datetime import date
from glob import glob
from logging import getLogger
from os import PathLike, cpu_count
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Iterator, Literal, Sequence

import dill as pickle
import numpy as np
import rioxarray  # nopycln: import
from numpy.typing import NDArray
from osgeo.gdal import GRA_NearestNeighbour
from rich import print
from tqdm.rich import trange
from xarray import Dataset, open_dataset
from xarray.core.types import T_Dataset

from clim_recal.debiasing.debias_wrapper import VariableOptions

from .utils.core import climate_data_mount_path, console, multiprocess_execute
from .utils.data import RegionOptions, RunOptions, VariableOptions
from .utils.gdal_formats import TIF_EXTENSION_STR
from .utils.xarray import (
    BRITISH_NATIONAL_GRID_EPSG,
    DEFAULT_RELATIVE_GRID_DATA_PATH,
    HADS_RAW_X_COLUMN_NAME,
    HADS_RAW_Y_COLUMN_NAME,
    NETCDF_EXTENSION_STR,
    ReprojectFuncType,
    apply_geo_func,
    cpm_reproject_with_standard_calendar,
    crop_xarray,
    hads_resample_and_reproject,
)

logger = getLogger(__name__)

DropDayType = set[tuple[int, int]]
ChangeDayType = set[tuple[int, int]]

CLIMATE_DATA_MOUNT_PATH: Path = climate_data_mount_path()
DEFAULT_INTERPOLATION_METHOD: str = "linear"
"""Default method to infer missing estimates in a time series."""

CFCalendarSTANDARD: Final[str] = "standard"

RESAMPLING_OUTPUT_PATH: Final[PathLike] = (
    CLIMATE_DATA_MOUNT_PATH / "Raw/python_refactor/"
)
RAW_HADS_PATH: Final[PathLike] = CLIMATE_DATA_MOUNT_PATH / "Raw/HadsUKgrid"
RAW_CPM_PATH: Final[PathLike] = CLIMATE_DATA_MOUNT_PATH / "Raw/UKCP2.2"
RAW_HADS_TASMAX_PATH: Final[PathLike] = RAW_HADS_PATH / "tasmax/day"
RAW_CPM_TASMAX_PATH: Final[PathLike] = RAW_CPM_PATH / "tasmax/01/latest"
REPROJECTED_CPM_TASMAX_05_LATEST_INPUT_PATH: Final[PathLike] = (
    CLIMATE_DATA_MOUNT_PATH / "Reprojected_infill/UKCP2.2/tasmax/05/latest"
)

CPRUK_RESOLUTION: Final[int] = 2200
CPRUK_RESAMPLING_METHOD: Final[str] = GRA_NearestNeighbour
ResamplingArgs = tuple[PathLike, np.ndarray, np.ndarray, PathLike]
ResamplingCallable = Callable[[list | tuple], int]
CPM_STANDARD_CALENDAR_PATH: Final[Path] = Path("cpm-standard-calendar")
CPM_SPATIAL_COORDS_PATH: Final[Path] = Path("cpm-to-27700-spatial")
HADS_2_2K_RESOLUTION_PATH: Final[Path] = Path("hads-to-27700-spatial-2.2km")
CPRUK_XDIM: Final[str] = "grid_longitude"
CPRUK_YDIM: Final[str] = "grid_latitude"

HADS_XDIM: Final[str] = HADS_RAW_X_COLUMN_NAME
HADS_YDIM: Final[str] = HADS_RAW_Y_COLUMN_NAME


CPM_START_DATE: Final[date] = date(1980, 12, 1)
CPM_END_DATE: Final[date] = date(2060, 11, 30)

HADS_START_DATE: Final[date] = date(1980, 1, 1)
HADS_END_DATE: Final[date] = date(2021, 12, 31)

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

    input_path: PathLike | None = RAW_HADS_TASMAX_PATH
    output_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_LOCAL_PATH
    variable_name: VariableOptions | str = VariableOptions.default()
    grid: PathLike | T_Dataset = DEFAULT_RELATIVE_GRID_DATA_PATH
    input_files: Iterable[PathLike] | None = None
    cpus: int | None = None
    crop_regions: tuple[RegionOptions | str, ...] | None = RegionOptions.all()
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_CROP_OUTPUT_LOCAL_PATH
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG
    grid_x_column_name: str = HADS_XDIM
    grid_y_column_name: str = HADS_YDIM
    input_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    export_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    resolution_relative_path: Path = HADS_2_2K_RESOLUTION_PATH
    input_file_x_column_name: str = HADS_XDIM
    input_file_y_column_name: str = HADS_YDIM
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
        self.set_grid_x_y()
        self.set_input_files()
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        if self.crop_regions:
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

    def set_grid(self, new_grid_data_path: PathLike | None = None) -> None:
        """Set check and set (if necessary) `grid` attribute of `self`.

        Notes
        -----
        To be depricated.

        Parameters
        ----------
        new_grid_data_path
            New `Path` to load to `self.grid`.
        """
        if new_grid_data_path:
            self.grid = new_grid_data_path
        if isinstance(self.grid, PathLike):
            self._grid_path = Path(self.grid)
            self.grid = open_dataset(self.grid)
        assert isinstance(self.grid, Dataset)

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

    def set_grid_x_y(
        self,
        grid_x_column_name: str | None = None,
        grid_y_column_name: str | None = None,
    ) -> None:
        """Set the `x` `y` values via `grid_x_column_name` and `grid_y_column_name`.

        Parameters
        ----------
        grid_x_column_name
            Name of column in `self.grid` `Dataset` to extract to `self.x`.
            If `None` use `self.grid_x_column_name`, else overwrite.
        grid_y_column_name
            Name of column in `self.grid` `Dataset` to extract to `self.y`.
            If `None` use `self.grid_y_column_name`, else overwrite.
        """
        if self.grid is None or isinstance(self.grid, PathLike):
            self.set_grid()
        assert isinstance(self.grid, Dataset)
        self.grid_x_column_name = grid_x_column_name or self.grid_x_column_name
        self.grid_y_column_name = grid_y_column_name or self.grid_y_column_name
        self.x: NDArray = self.grid[self.grid_x_column_name][:].values
        self.y: NDArray = self.grid[self.grid_y_column_name][:].values

    def execute(self, skip_spatial: bool = False, **kwargs) -> list[Path] | None:
        """Run all steps for processing"""
        return self.range_to_reprojection(**kwargs) if not skip_spatial else None

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
        regions: Iterable[str] | None = None,
        start: int | None = None,
        stop: int | None = None,
        step: int = 1,
        override_export_path: Path | None = None,
        return_results: bool = False,
        **kwargs,
    ) -> list[Path]:
        regions = regions or self.crop_regions
        start = start or self.start_index
        stop = stop or self.stop_index
        export_paths: list[Path | T_Dataset] = []
        if stop is None:
            stop = len(self)
        try:
            assert regions
        except:
            raise ValueError(f"Iterable 'regions' must be set.")
        for region in regions:
            console.print(f"Cropping to '{region}' from {self}...")
            for index in trange(start, stop, step):
                export_paths.append(
                    self.crop_projection(
                        region=region,
                        index=index,
                        override_export_path=override_export_path,
                        return_results=return_results,
                        **kwargs,
                    )
                )
        return export_paths

    def crop_projection(
        self,
        region: str,
        index: int = 0,
        override_export_path: Path | None = None,
        return_results: bool = False,
        sync_reprojection_paths: bool = True,
        **kwargs,
    ) -> Path | T_Dataset:
        """Crop a projection to `region` geometry."""
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
            assert region and region in self.crop_regions
        except AttributeError:
            raise IndexError(f"'{region}' not in 'crop_regions': '{self.crop_regions}'")
        path: PathLike = override_export_path or Path(self.crop_path) / (region)
        path.mkdir(exist_ok=True, parents=True)
        resampled_xr: Dataset = self._reprojected_paths[index]
        cropped: Dataset = crop_xarray(
            xr_time_series=resampled_xr,
            crop_box=RegionOptions.bounding_box(region),
            **kwargs,
        )
        cropped_file_name: str = "crop_" + region + "-" + resampled_xr.name
        export_path: Path = path / cropped_file_name
        cropped.to_netcdf(export_path)
        if not hasattr(self, "_cropped_paths"):
            self._cropped_paths: dict[str, list[PathLike]] = {}
        if region not in self._cropped_paths:
            self._cropped_paths[region] = []
        self._cropped_paths[region].append(export_path)
        if return_results:
            return cropped
        else:
            return export_path

    # def execute_crop(self, **kwargs) -> list[Path] | None:
    #     """Run crop for related reprojections."""
    #     if not self.output_path or not Path(self.output_path).exists():
    #         raise ValueError(f"Output path {self.output_path} required to crop.")
    #     else:


@dataclass(kw_only=True, repr=False)
class HADsResampler(ResamblerBase):
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    grid_data_path
        `Path` to load to `self.grid`.
    grid
        `Dataset` of grid (either passed via `grid_data_path` or as a parameter).
    input_files
        NCF or TIF files to process with `self.grid` etc.
    resampling_func
        Function to call on `self.input_files` with `self.grid`
    crop
        Path or file to spatially crop `input_files` with.
    final_crs
        Coordinate Reference System (CRS) to return final format in.
    grid_x_column_name
        Column name in `input_files` or `input` for `x` coordinates.
    grid_y_column_name
        Column name in `input_files` or `input` for `y` coordinates.
    input_file_extension
        File extensions to glob `input_files` with.

    Notes
    -----
    - [x] Try time projection first
    - [x] Combine with space (this worked)
    - [ ] Add crop step

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> hads_resampler: HADsResampler = HADsResampler(
    ...     output_path=resample_test_hads_output_path,
    ... )
    >>> hads_resampler
    <HADsResampler(...count=504,...
        ...input_path='.../tasmax/day',...
        ...output_path='...run-results_..._.../hads')>
    >>> pprint(hads_resampler.input_files)
    (...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc'),
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800201-19800229.nc'),
     ...,
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc'))
    """

    input_path: PathLike | None = RAW_HADS_TASMAX_PATH
    output_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_LOCAL_PATH
    variable_name: VariableOptions = VariableOptions.default()
    grid: PathLike | T_Dataset = DEFAULT_RELATIVE_GRID_DATA_PATH
    input_files: Iterable[PathLike] | None = None
    cpus: int | None = None
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_CROP_OUTPUT_LOCAL_PATH
    final_crs: str = BRITISH_NATIONAL_GRID_EPSG
    grid_x_column_name: str = HADS_XDIM
    grid_y_column_name: str = HADS_YDIM
    input_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    export_file_extension: NETCDF_OR_TIF = NETCDF_EXTENSION_STR
    # resolution_relative_path: Path = HADS_2_2K_RESOLUTION_PATH
    input_file_x_column_name: str = HADS_XDIM
    input_file_y_column_name: str = HADS_YDIM
    _resample_func: ReprojectFuncType = hads_resample_and_reproject
    _use_reference_grid: bool = True

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
        # path: PathLike = self._output_path(
        #     self.resolution_relative_path, override_export_path
        # )
        return apply_geo_func(
            source_path=source_path,
            # func=interpolate_coords,
            func=self._resample_func,
            export_folder=path,
            # Leaving in case we return to using warp
            # export_path_as_output_path_kwarg=True,
            # to_netcdf=False,
            to_netcdf=True,
            variable_name=self.variable_name,
            x_dim_name=self.input_file_x_column_name,
            y_dim_name=self.input_file_y_column_name,
            # x_grid=self.x,
            # y_grid=self.y,
            # source_x_coord_column_name=self.input_file_x_column_name,
            # source_y_coord_column_name=self.input_file_y_column_name,
            # use_reference_grid=self._use_reference_grid,
            new_path_name_func=reproject_2_2km_filename,
            return_results=return_results,
        )

    # def to_crop(
    #     self,
    #     index: int = 0,
    #     override_export_path: Path | None = None,
    #     return_results: bool = False,
    #     source_to_index: Sequence | None = None,
    # ) -> Path | T_Dataset:
    #     source_path: Path = self._output_path(
    #         index=index, source_to_index=source_to_index
    #     )
    #     path: PathLike = self._output_path(
    #         self.resolution_relative_path, override_export_path
    #     )
    #     return apply_geo_func(
    #         source_path=source_path,
    #         # func=interpolate_coords,
    #         func=self._resample_func,
    #         export_folder=path,
    #         # Leaving in case we return to using warp
    #         # export_path_as_output_path_kwarg=True,
    #         # to_netcdf=False,
    #         to_netcdf=True,
    #         variable_name=self.variable_name,
    #         x_dim_name=self.input_file_x_column_name,
    #         y_dim_name=self.input_file_y_column_name,
    #         # x_grid=self.x,
    #         # y_grid=self.y,
    #         # source_x_coord_column_name=self.input_file_x_column_name,
    #         # source_y_coord_column_name=self.input_file_y_column_name,
    #         # use_reference_grid=self._use_reference_grid,
    #         new_path_name_func=reproject_2_2km_filename,
    #         return_results=return_results,
    #     )


@dataclass(kw_only=True, repr=False)
class CPMResampler(ResamblerBase):
    """CPM specific changes to HADsResampler.

    Attributes
    ----------
    input_path
        `Path` to `CPM` files to process.
    output
        `Path` to save processed `CPM` files.
    grid_data_path
        `Path` to load to `self.grid`.
    grid
        `Dataset` of grid (either passed via `grid_data_path` or as a parameter).
    input_files
        NCF or TIF files to process with `self.grid` etc.
    cpus
        Number of `cpu` cores to use during multiprocessing.
    resampling_func
        Function to call on `self.input_files` with `self.grid`
    crop
        Path or file to spatially crop `input_files` with.
    final_crs
        Coordinate Reference System (CRS) to return final format in.
    grid_x_column_name
        Column name in `input_files` or `input` for `x` coordinates.
    grid_y_column_name
        Column name in `input_files` or `input` for `y` coordinates.
    input_file_extension
        File extensions to glob `input_files` with.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> cpm_resampler: CPMResampler = CPMResampler(
    ...     input_path=REPROJECTED_CPM_TASMAX_05_LATEST_INPUT_PATH,
    ...     output_path=resample_test_cpm_output_path,
    ...     input_file_extension=TIF_EXTENSION_STR,
    ... )
    >>> cpm_resampler
    <CPMResampler(...count=100,...
        ...input_path='.../tasmax/05/latest',...
        ...output_path='.../test-run-results_..._.../cpm')>
    >>> pprint(cpm_resampler.input_files)
    (...Path('.../tasmax/05/latest/tasmax_...-cpm_uk_2.2km_05_day_19801201-19811130.tif'),
     ...Path('.../tasmax/05/latest/tasmax_...-cpm_uk_2.2km_05_day_19811201-19821130.tif'),
     ...
     ...Path('.../tasmax/05/latest/tasmax_...-cpm_uk_2.2km_05_day_20791201-20801130.tif'))

    """

    input_path: PathLike | None = RAW_CPM_TASMAX_PATH
    output_path: PathLike = RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_LOCAL_PATH
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH / CPM_CROP_OUTPUT_LOCAL_PATH
    # standard_calendar_relative_path: Path = CPM_STANDARD_CALENDAR_PATH
    input_file_x_column_name: str = CPRUK_XDIM
    input_file_y_column_name: str = CPRUK_YDIM
    # resolution_relative_path: Path = CPM_SPATIAL_COORDS_PATH
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
        # path: PathLike = self._output_path(
        #     self.resolution_relative_path, override_export_path
        # )
        result: Path | T_Dataset | GDALDataset = apply_geo_func(
            source_path=source_path,
            func=self._resample_func,
            new_path_name_func=reproject_standard_calendar_filename,
            export_folder=path,
            to_netcdf=True,
            variable_name=self.cpm_variable_name,
            return_results=return_results,
        )
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
class HADsResamplerManager:

    """Class to manage processing HADs resampling.

    Attributes
    ----------
    input_paths
        `Path` or `Paths` to `CPM` files to process. If `Path`, will be propegated with files matching
    output_paths
        `Path` or `Paths` to to save processed `CPM` files to. If `Path` will be propagated to match `input_paths`.
    variables
        Which `VariableOptions` to include.
    sub_path
        `Path` to include at the stem of generating `input_paths`.
    cpus
        Number of `cpu` cores to use during multiprocessing.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> hads_resampler_manager: HADsResamplerManager = HADsResamplerManager(
    ...     variables=VariableOptions.all(),
    ...     output_paths=resample_test_hads_output_path,
    ...     )
    >>> hads_resampler_manager
    <HADsResamplerManager(variables_count=3, input_paths_count=3)>
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_HADS_PATH
    output_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_LOCAL_PATH
    )
    variables: Sequence[VariableOptions | str] = (VariableOptions.default(),)
    sub_path: Path = Path("day")
    start_index: int = 0
    stop_index: int | None = None
    start_date: date = HADS_START_DATE
    end_date: date = HADS_END_DATE
    configs: list[HADsResampler] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    resampler_class: type[HADsResampler] = HADsResampler
    cpus: int | None = None
    _var_path_dict: dict[VariableOptions | str, Sequence[Path]] = field(
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
        if hasattr(self, "_input_path"):
            return Path(self._input_path)
        else:
            return None

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"variables_count={len(self.variables)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

    def _gen_folder_paths(
        self, path: PathLike, append_var_path_dict: bool = False
    ) -> Iterator[Path]:
        """Return a Generator of paths of `self.variables` and `self.runs`."""
        var_path: Path
        for var in self.variables:
            var_path = Path(path) / var / self.sub_path
            if append_var_path_dict:
                self._var_path_dict[var_path] = var
            yield var_path

    def check_paths(self, run_set_data_paths: bool = True):
        """Check if all `self.input_paths` exist."""

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
                assert path in self._var_path_dict
            except:
                NotImplemented(
                    f"Syncing `self._var_path_dict` with changes to `self.input_paths`."
                )

    def set_input_paths(self):
        """Propagate `self.input_paths` if needed."""
        if isinstance(self.input_paths, PathLike):
            self._input_path = self.input_paths
            self.input_paths = tuple(
                self._gen_folder_paths(self.input_paths, append_var_path_dict=True)
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
            self.output_paths = tuple(self._gen_folder_paths(self.output_paths))

    def yield_configs(self) -> Iterable[HADsResampler]:
        """Generate a `CPMResampler` or `HADsResampler` for `self.input_paths`."""
        self.check_paths()
        assert isinstance(self.input_paths, Iterable)
        assert isinstance(self.output_paths, Iterable)
        for index, var_path in enumerate(self._var_path_dict.items()):
            yield self.resampler_class(
                input_path=var_path[0],
                output_path=self.output_paths[index],
                variable_name=var_path[1],
                start_index=self.start_index,
                stop_index=self.stop_index,
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
    ) -> tuple[CPMResampler | HADsResampler, ...]:
        """Run all resampler configurations

        Parameters
        ----------
        multiprocess
            If `True` run parameters in `resample_configs` with `multiprocess_execute`.
        cpus
            Number of `cpus` to pass to `multiprocess_execute`.
        """
        resamplers: tuple[CPMResampler | HADsResampler, ...] = tuple(
            self.yield_configs()
        )
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


@dataclass(kw_only=True, repr=False)
class CPMResamplerManager(HADsResamplerManager):

    """Class to manage processing CPM resampling.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> cpm_resampler_manager: CPMResamplerManager = CPMResamplerManager(
    ...     stop_index=10,
    ...     output_paths=resample_test_cpm_output_path,
    ...     )
    >>> cpm_resampler_manager
    <CPMResamplerManager(variables_count=1, runs_count=4,
                         input_paths_count=4)>
    >>> configs: tuple[CPMResampler, ...] = tuple(
    ...     cpm_resampler_manager.yield_configs())
    >>> pprint(configs)
    (<CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/05/latest',
                   output_path='.../tasmax/05/latest')>,
     <CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/06/latest',
                   output_path='.../tasmax/06/latest')>,
     <CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/07/latest',
                   output_path='.../tasmax/07/latest')>,
     <CPMResampler(count=10, max_count=100,
                   input_path='.../tasmax/08/latest',
                   output_path='.../tasmax/08/latest')>)
    """

    input_paths: PathLike | Sequence[PathLike] = RAW_CPM_PATH
    output_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_LOCAL_PATH
    )
    sub_path: Path = Path("latest")
    start_date: date = CPM_START_DATE
    end_date: date = CPM_END_DATE
    configs: list[CPMResampler] = field(default_factory=list)
    resampler_class: type[CPMResampler] = CPMResampler
    runs: Sequence[RunOptions] = RunOptions.preferred()

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

    def _gen_folder_paths(
        self, path: PathLike, append_var_path_dict: bool = False, cpm_paths: bool = True
    ) -> Iterator[Path]:
        """Return a Generator of paths of `self.variables` and `self.runs`."""
        var_path: Path
        for var in self.variables:
            for run_type in self.runs:
                if cpm_paths:
                    var_path: Path = (
                        Path(path)
                        / VariableOptions.cpm_value(var)
                        / run_type
                        / self.sub_path
                    )
                else:
                    var_path: Path = Path(path) / var / run_type / self.sub_path
                if append_var_path_dict:
                    self._var_path_dict[var_path] = var
                yield var_path


# Kept from previous structure for reference
# if __name__ == "__main__":
#     """
#     Script to resample UKHADs data from the command line
#     """
#     # Initialize parser
#     parser = argparse.ArgumentParser()
#
#     # Adding arguments
#     parser.add_argument(
#         "--input-path",
#         help="Path where the .nc files to resample is located",
#         required=True,
#         type=str,
#     )
#     parser.add_argument(
#         "--grid-data-path",
#         help="Path where the .nc file with the grid to resample is located",
#         required=False,
#         type=str,
#         default="../../data/rcp85_land-cpm_uk_2.2km_grid.nc",
#     )
#     parser.add_argument(
#         "--output-path",
#         help="Path to save the resampled data data",
#         required=False,
#         default=".",
#         type=str,
#     )
#     parser_args = parser.parse_args()
#     hads_run_manager = HADsResampler(
#         input_path=parser_args.input_path,
#         grid_data_path=parser_args.grid_data_path,
#         output_path=parser_args.output,
#     )
#     res = hads_run_manager.resample_multiprocessing()
#
#     parser_args = parser.parse_args()
#
#     # reading baseline grid to resample files to
#     grid = xr.open_dataset(parser_args.grid_data)
#
#     try:
#         # must have dimensions named projection_x_coordinate and projection_y_coordinate
#         x = grid["projection_x_coordinate"][:].values
#         y = grid["projection_y_coordinate"][:].values
#     except Exception as e:
#         print(f"Grid file: {parser_args.grid_data} produced errors: {e}")
#
#     # If output file do not exist create it
#     if not os.path.exists(parser_args.output):
#         os.makedirs(parser_args.output)
#
#     # find all nc files in input directory
#     files = glob.glob(f"{parser_args.input}/*.nc", recursive=True)
#     N = len(files)
#
#     args = [[f, x, y, parser_args.output] for f in files]
#
#     with multiprocessing.Pool(processes=os.cpu_count() - 1) as pool:
#         res = list(tqdm(pool.imap_unordered(resample_hadukgrid, args), total=N))
