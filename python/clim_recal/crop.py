from dataclasses import dataclass, field
from datetime import date
from logging import getLogger
from os import PathLike, cpu_count
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from tqdm.rich import trange
from xarray import Dataset
from xarray.core.types import T_Dataset

from .resample import (
    RAW_CPM_TASMAX_PATH,
    RAW_HADS_TASMAX_PATH,
    RESAMPLING_OUTPUT_PATH,
    ResamplerBase,
    ResamplerManagerBase,
)
from .utils.core import console, multiprocess_execute
from .utils.data import (
    CPM_CROP_OUTPUT_PATH,
    CPM_END_DATE,
    CPM_OUTPUT_PATH,
    CPM_START_DATE,
    HADS_CROP_OUTPUT_PATH,
    HADS_END_DATE,
    HADS_OUTPUT_PATH,
    HADS_START_DATE,
    RegionOptions,
    RunOptions,
    VariableOptions,
)
from .utils.xarray import crop_xarray, region_crop_file_name

logger = getLogger(__name__)

# RESAMPLING_OUTPUT_PATH: Final[PathLike] = (
#     CLIMATE_DATA_MOUNT_PATH / "CPM-365/andys-two-gdal-step-approach/resample"
# )
# CPM_CROP_OUTPUT_PATH: Final[Path] = Path("cpm-crop")
# HADS_CROP_OUTPUT_PATH: Final[Path] = Path("hads-crop")


@dataclass(kw_only=True, repr=False)
class RegionCropperBase(ResamplerBase):
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    input_files
        `Path` or `Paths` of `NCF` files to resample.
    crop
        Path or file to spatially crop `input_files` with.
    start_index
        First index of file to iterate processing from.
    stop_index
        Last index of files to iterate processing from as a count from `resample_start_index`.
        If `None`, this will simply iterate over all available files.
    """

    crop_region: RegionOptions | str | None = RegionOptions.GLASGOW
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH

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
    ) -> Iterator[Path]:
        start = start or self.stop_index
        stop = stop or self.stop_index
        self._export_paths: list[Path | T_Dataset] = []
        if stop is None:
            stop = len(self)
        console.print(f"Cropping to '{self.output_path}'")
        for index in trange(start, stop, step):
            self._export_paths.append(
                self.crop_projection(
                    # region=region,
                    index=index,
                    override_export_path=override_export_path,
                    return_results=return_results,
                    **kwargs,
                )
            )
            yield self._export_paths[-1]
        # return export_paths

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

    # def execute_crops(self, skip_crop: bool = False, **kwargs) -> list[Path] | None:
    #     """Run all specified crops."""
    #     return self.range_crop_projection(**kwargs) if not skip_crop else None

    def execute(self, skip_crop: bool = False, **kwargs) -> Iterator[Path] | None:
        """Run all specified crops."""
        return self.range_crop_projection(**kwargs) if not skip_crop else None


@dataclass(kw_only=True, repr=False)
class HADsRegionCropper(RegionCropperBase):
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    input_files
        `Path` or `Paths` of `NCF` files to resample.
    crop
        Path or file to spatially crop `input_files` with.
    start_index
        First index of file to iterate processing from.
    stop_index
        Last index of files to iterate processing from as a count from `resample_start_index`.
        If `None`, this will simply iterate over all available files.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_hads_output_path: Path = getfixture(
    ...         'resample_test_hads_output_path')
    >>> hads_cropper: HADsResampler = HADsResampler(  # doctest: +SKIP
    ...     output_path=resample_test_hads_output_path,
    ... )
    >>> hads_cropper  # doctest: +SKIP
    <HADsRegionCropper(...count=504,...
        ...input_path='.../tasmax/day',...
        ...output_path='...run-results_..._.../hads')>
    >>> pprint(hads_resampler.input_files)   # doctest: +SKIP
    (...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc'),
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800201-19800229.nc'),
     ...,
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc'))
    """

    input_path: PathLike | None = RAW_HADS_TASMAX_PATH
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH / HADS_CROP_OUTPUT_PATH


@dataclass(kw_only=True, repr=False)
class CPMRegionCropper(RegionCropperBase):
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    input_files
        `Path` or `Paths` of `NCF` files to resample.
    crop
        Path or file to spatially crop `input_files` with.
    start_index
        First index of file to iterate processing from.
    stop_index
        Last index of files to iterate processing from as a count from `resample_start_index`.
        If `None`, this will simply iterate over all available files.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_hads_output_path: Path = getfixture(
    ...         'resample_test_hads_output_path')
    >>> hads_cropper: HADsResampler = HADsResampler(  # doctest: +SKIP
    ...     output_path=resample_test_hads_output_path,
    ... )
    >>> hads_cropper  # doctest: +SKIP
    <HADsRegionCropper(...count=504,...
        ...input_path='.../tasmax/day',...
        ...output_path='...run-results_..._.../hads')>
    >>> pprint(hads_resampler.input_files)   # doctest: +SKIP
    (...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc'),
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_19800201-19800229.nc'),
     ...,
     ...Path('.../tasmax/day/tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc'))
    """

    input_path: PathLike | None = RAW_CPM_TASMAX_PATH
    crop_path: PathLike = RESAMPLING_OUTPUT_PATH / CPM_CROP_OUTPUT_PATH


@dataclass(kw_only=True, repr=False)
class RegionCropperManagerBase(ResamplerManagerBase):
    """Base class to inherit for `HADs` and `CPM` resampler managers."""

    # input_paths: PathLike | Sequence[PathLike] = Path()
    # resample_paths: PathLike | Sequence[PathLike] = Path()
    output_paths: PathLike | Sequence[PathLike] = Path()
    # variables: Sequence[VariableOptions | str] = (VariableOptions.default(),)
    crop_regions: tuple[RegionOptions | str, ...] = RegionOptions.all()
    # crop_paths: Sequence[PathLike] | PathLike = Path()
    # sub_path: Path = Path()
    # start_index: int = 0
    # stop_index: int | None = None
    # crop_start_index: int = 0
    # crop_stop_index: int | None = None
    # start_date: date | None = None
    # end_date: date | None = None
    configs: list[HADsRegionCropper | CPMRegionCropper] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    # resampler_class: type[HADsResampler | CPMResampler] | None = None
    calc_class: type[HADsRegionCropper | CPMRegionCropper] | None = None
    # cpus: int | None = None
    # _input_path_dict: dict[Path, str] = field(default_factory=dict)
    # _output_path_dict: dict[PathLike, VariableOptions | str] = field(
    #     default_factory=dict
    # )
    # _output_path_dict: dict[PathLike, VariableOptions | str] = field(
    #     default_factory=dict
    # )
    # _strict_fail_if_var_in_input_path: bool = True
    # _allow_check_fail: bool = False
    check_input_paths_exist: bool = True
    _raw_input_path_dict: dict[Path, VariableOptions | str] = field(
        default_factory=dict
    )

    # class VarirableInBaseImportPathError(Exception):
    #     """Checking import path validity for `self.variables`."""
    #
    #     pass

    def __post_init__(self) -> None:
        """Populate config attributes."""
        if not self.crop_regions:
            self.crop_regions = ()
        self.check_paths()
        self.total_cpus: int | None = cpu_count()
        if not self.cpus:
            self.cpus = 1 if not self.total_cpus else self.total_cpus
        # self.cpm_for_coord_alignment: T_Dataset | PathLike = RAW_CPM_TASMAX_PATH

    #
    # @property
    # def input_folder(self) -> Path | None:
    #     """Return `self._input_path` set by `set_input_paths()`."""
    #     if hasattr(self, "_input_path"):
    #         return Path(self._input_path)
    #     else:
    #         return None

    # @property
    # def resample_folder(self) -> Path | None:
    #     """Return `self._output_path` set by `set_resample_paths()`."""
    #     if hasattr(self, "_output_path"):
    #         return Path(self._output_path)
    #     else:
    #         return None

    # @property
    # def output_folder(self) -> Path | None:
    #     """Return `self._output_path` set by `set_resample_paths()`."""
    #     if hasattr(self, "_output_path"):
    #         return Path(self._output_path)
    #     else:
    #         return None

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

    # def _gen_output_folder_paths(
    #     self,
    #     path: PathLike,
    #     append_input_path_dict: bool = False,
    #     append_output_path_dict: bool = False,
    # ) -> Iterator[tuple[Path, Path]]:
    #     """Yield paths of resampled `self.variables` and `self.runs`."""
    #     for var in self.variables:
    #         input_path: Path = Path(path) / var / self.sub_path
    #         resample_path: Path = Path(path) / var
    #         if append_input_path_dict:
    #             self._input_path_dict[input_path] = var
    #         if append_output_path_dict:
    #             self._output_path_dict[resample_path] = var
    #         yield input_path, resample_path

    def check_paths(
        self,
        run_set_input_paths: bool = True,
        run_set_output_paths: bool = True,
        check_input_paths_exist: bool = False,
    ):
        """Check and set `input` and `output` paths."""
        check_input_paths_exist = (
            check_input_paths_exist or self.check_input_paths_exist
        )
        if run_set_input_paths:
            self.set_input_paths()

        # if run_set_data_paths:
        #     self.set_resample_paths()
        if run_set_output_paths:
            self.set_output_paths()
        assert isinstance(self.input_paths, Iterable)
        # assert isinstance(self.resample_paths, Iterable)
        if self.output_paths:
            try:
                assert isinstance(self.output_paths, Iterable)
            except AssertionError:
                raise ValueError(
                    f"'output_paths' not iterable for {self}. Hint: try setting 'run_set_output_paths' to 'True'."
                )
        # assert len(self.input_paths) == len(self.resample_paths)
        if check_input_paths_exist:
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
                for input_path, _ in self._gen_input_folder_paths(
                    self.input_paths,
                    append_input_path_dict=True,
                    append_raw_input_path_dict=True,
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

    #
    # def set_resample_paths(self):
    #     """Propagate `self.resample_paths` if needed."""
    #     self._set_input_paths()
    #     if isinstance(self.resample_paths, PathLike):
    #         self._output_path = self.resample_paths
    #         self.resample_paths = tuple(
    #             resample_path
    #             for _, resample_path in self._gen_output_folder_paths(
    #                 self.resample_paths, append_output_path_dict=True
    #             )
    #         )
    def _gen_output_folder_paths(
        self,
        path: PathLike,
        append_output_path_dict: bool = False,
    ) -> Iterator[Path | None]:
        """Return a Generator of paths of `self.variables` and `self.crops`."""
        raise NotImplementedError

    def _gen_input_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        # append_output_path_dict: bool = False,
        append_raw_input_path_dict: bool = False,
    ) -> Iterator[tuple[Path, Path]]:
        raise NotImplementedError

    def set_output_paths(self) -> None:
        """Propagate `self.resample_paths` if needed."""
        if isinstance(self.output_paths, PathLike):
            self._output_paths = self.output_paths
            self.output_paths = tuple(
                self._gen_output_folder_paths(
                    self.output_paths,
                    append_output_path_dict=True,  # append_raw_input_path_dict=True
                )
            )

    # def yield_configs(self) -> Iterable[ResamplerBase]:
    #     """Generate a `CPMResampler` or `HADsResampler` for `self.input_paths`."""
    #     self.check_paths()
    #     assert isinstance(self.resample_paths, Iterable)
    #     for index, var_path in enumerate(self._input_path_dict.items()):
    #         yield self.calc_class(
    #             input_path=var_path[0],
    #             output_path=self.resample_paths[index],
    #             variable_name=var_path[1],
    #             start_index=self.start_index,
    #             stop_index=self.stop_index,
    #             **self.config_default_kwargs,
    #         )
    #
    def yield_crop_configs(self) -> Iterable[ResamplerBase]:
        """Generate a `CPMResampler` or `HADsResampler` for `self.input_paths`."""
        self.check_paths()
        try:
            assert isinstance(self.input_paths, Iterable)
            assert isinstance(self.output_paths, Iterable)
        except AssertionError as error:
            raise error
        # assert isinstance(self.resample_paths, Iterable)
        # assert isinstance(self.output_paths, Iterable)
        for index, input_paths in enumerate(self._input_path_dict.items()):
            for crop_path, region in self._output_path_dict.items():
                yield self.calc_class(
                    input_path=input_paths[0],
                    # output_path=self.resample_paths[index],
                    output_path=crop_path,
                    variable_name=input_paths[1],
                    start_index=self.start_index,
                    stop_index=self.stop_index,
                    # crop_path=crop_path,
                    # Todo: remove below if single crop configs iterate over all
                    # crop_regions=self.crop_regions,
                    # crop_regions=(region,),
                    crop_region=region,
                    **self.config_default_kwargs,
                )

    # def __len__(self) -> int:
    #     """Return the length of `self.input_files`."""
    #     return (
    #         len(self.input_paths[self.start_index : self.stop_index])
    #         if isinstance(self.input_paths, Sequence)
    #         else 0
    #     )

    # @property
    # def max_count(self) -> int:
    #     """Maximum length of `self.input_files` ignoring `start_index` and `start_index`."""
    #     return len(self.input_paths) if isinstance(self.input_paths, Sequence) else 0

    # def __iter__(self) -> Iterator[Path] | None:
    #     if isinstance(self.input_paths, Sequence):
    #         for file_path in self.input_paths[
    #             self.start_index : self.stop_index
    #         ]:
    #             yield Path(file_path)
    #     else:
    #         return None

    # def __getitem__(self, key: int | slice) -> Path | tuple[Path, ...] | None:
    #     if not self.input_paths or not isinstance(self.input_paths, Sequence):
    #         return None
    #     elif isinstance(key, int):
    #         return Path(self.input_paths[key])
    #     elif isinstance(key, slice):
    #         return tuple(Path(path) for path in self.input_paths[key])
    #     else:
    #         raise IndexError(f"Can only index with 'int', not: '{key}'")
    # #
    # def execute_resample_configs(
    #     self, multiprocess: bool = False, cpus: int | None = None
    # ) -> tuple[ResamplerBase, ...]:
    #     """Run all resampler configurations
    #
    #     Parameters
    #     ----------
    #     multiprocess
    #         If `True` run parameters in `resample_configs` with `multiprocess_execute`.
    #     cpus
    #         Number of `cpus` to pass to `multiprocess_execute`.
    #     """
    #     resamplers: tuple[ResamplerBase, ...] = tuple(self.yield_configs())
    #     results: list[list[Path] | None] = []
    #     if multiprocess:
    #         cpus = cpus or self.cpus
    #         if self.total_cpus and cpus:
    #             cpus = min(cpus, self.total_cpus - 1)
    #         results = multiprocess_execute(resamplers, method_name="execute", cpus=cpus)
    #     else:
    #         for resampler in resamplers:
    #             print(resampler)
    #             results.append(resampler.execute())
    #     return resamplers

    def execute_configs(
        self, multiprocess: bool = False, cpus: int | None = None
    ) -> tuple[ResamplerBase, ...]:
        """Run all resampler configurations

        Parameters
        ----------
        multiprocess
            If `True` run parameters in `resample_configs` with `multiprocess_execute`.
        cpus
            Number of `cpus` to pass to `multiprocess_execute`.
        """
        croppers: tuple[ResamplerBase, ...] = tuple(self.yield_crop_configs())
        results: list[list[Path] | None] = []
        if multiprocess:
            cpus = cpus or self.cpus
            if self.total_cpus and cpus:
                cpus = min(cpus, self.total_cpus - 1)
            results = multiprocess_execute(croppers, method_name="execute", cpus=cpus)
        else:
            for cropper in croppers:
                print(cropper)
                results.append(cropper.execute())
        return croppers


@dataclass(kw_only=True, repr=False)
class HADsRegionCropManager(RegionCropperManagerBase):
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
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_hads_output_path: Path = getfixture(
    ...         'resample_test_hads_output_path')
    >>> hads_resampler_manager: HADsResamplerManager = HADsResamplerManager(
    ...     variables=VariableOptions.all(),
    ...     output_paths=resample_test_hads_output_path,
    ...     )
    >>> hads_resampler_manager
    <HADsResamplerManager(variables_count=3, input_paths_count=3)>
    """

    input_paths: PathLike | Sequence[PathLike] = HADS_OUTPUT_PATH
    # resample_paths: PathLike | Sequence[PathLike] = (
    #     RESAMPLING_OUTPUT_PATH / HADS_OUTPUT_PATH
    # )
    output_paths: Sequence[PathLike] | PathLike = (
        RESAMPLING_OUTPUT_PATH / HADS_CROP_OUTPUT_PATH
    )
    # sub_path: Path =  Path()
    start_date: date = HADS_START_DATE
    end_date: date = HADS_END_DATE
    configs: list[HADsRegionCropper] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    calc_class: type[HADsRegionCropper] = HADsRegionCropper
    # cpm_for_coord_alignment: T_Dataset | PathLike = RAW_CPM_TASMAX_PATH
    # cpm_for_coord_alignment_path_converted: bool = False

    def _gen_input_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        append_raw_input_path_dict: bool = False,
        # append_output_path_dict: bool = False,
    ) -> Iterator[tuple[Path, Path]]:
        """Yield paths of resampled `self.variables` and `self.runs`."""
        for var in self.variables:
            raw_input_path: Path = Path(path) / var / self.sub_path
            input_path: Path = Path(path) / var
            if append_input_path_dict:
                self._input_path_dict[input_path] = var
            if append_raw_input_path_dict:
                self._raw_input_path_dict[raw_input_path] = var
            # if append_output_path_dict:
            #     self._output_path_dict[output_path] = var
            yield input_path, raw_input_path

    def _gen_output_folder_paths(
        self, path: PathLike, append_output_path_dict: bool = False
    ) -> Iterator[Path | None]:
        """Return a Generator of paths of `self.variables` and `self.crops`."""
        if not self.crop_regions:
            return None
        if not self.input_paths:
            self._gen_input_folder_paths(
                self.input_paths,
                append_input_path_dict=True,
                # Assuming crop paths need to take over
                # append_output_path_dict=True
            )
        # if not self._output_path_dict:
        #     self._gen_output_folder_paths(
        #         self.input_paths,
        #         append_input_path_dict=True,
        #         append_output_path_dict=True,
        #     )
        for var in self.variables:
            for region in self.crop_regions:
                crop_path = Path(path) / HADS_OUTPUT_PATH / region / var
                if append_output_path_dict:
                    self._output_path_dict[crop_path] = region
                yield crop_path


@dataclass(kw_only=True, repr=False)
class CPMRegionCropManager(RegionCropperManagerBase):
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
    calc_class
        `class` to construct all `self.configs` instances with.
    cpus
        Number of `cpu` cores to use during multiprocessing.


    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> resample_test_cpm_output_path: Path = getfixture(
    ...         'resample_test_cpm_output_path')
    >>> cpm_crop_manager: CPMResamplerManager = CPMResamplerManager(
    ...     stop_index=9,
    ...     resample_paths=resample_test_cpm_output_path,
    ...     output_paths=resample_test_cpm_output_path,
    ...     )
    >>> cpm_crop_manager
    <CPMRegionCropManager(variables_count=1, runs_count=4,
                         input_paths_count=4)>
    >>> configs: tuple[CPMRegionCrop, ...] = tuple(
    ...     cpm_crop_manager.yield_configs())
    >>> pprint(configs)
    (<CPMRegionCrop(count=9, max_count=100,
                   input_path='.../tasmax/05/latest',
                   output_path='.../cpm/tasmax/05')>,
     <CPMRegionCrop(count=9, max_count=100,
                   input_path='.../tasmax/06/latest',
                   output_path='.../cpm/tasmax/06')>,
     <CPMRegionCrop(count=9, max_count=100,
                   input_path='.../tasmax/07/latest',
                   output_path='.../cpm/tasmax/07')>,
     <CPMRegionCrop(count=9, max_count=100,
                   input_path='.../tasmax/08/latest',
                   output_path='.../cpm/tasmax/08')>)
    """

    input_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_PATH
    )
    # resample_paths: PathLike | Sequence[PathLike] = (
    #     RESAMPLING_OUTPUT_PATH / CPM_OUTPUT_PATH
    # )
    output_paths: PathLike | Sequence[PathLike] = (
        RESAMPLING_OUTPUT_PATH / CPM_CROP_OUTPUT_PATH
    )
    # sub_path: Path = CPM_SUB_PATH
    start_date: date = CPM_START_DATE
    end_date: date = CPM_END_DATE
    configs: list[CPMRegionCropper] = field(default_factory=list)
    calc_class: type[CPMRegionCropper] = CPMRegionCropper
    # Runs are CPM simulations, not applicalbe to HADs
    runs: Sequence[RunOptions | str] = RunOptions.preferred()
    # crop_paths = RESAMPLING_OUTPUT_PATH / CPM_CROP_OUTPUT_PATH

    def _gen_input_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        # append_output_path_dict: bool = False,
        append_raw_input_path_dict: bool = False,
        cpm_paths: bool = True,
    ) -> Iterator[tuple[Path, Path]]:
        """Return a Generator of paths of `self.variables` and `self.runs`."""
        for var in self.variables:
            for run_type in self.runs:
                if cpm_paths:
                    input_raw_path: Path = (
                        Path(path)
                        / VariableOptions.cpm_value(var)
                        / run_type
                        / self.sub_path
                    )
                    input_path: Path = (
                        Path(path) / VariableOptions.cpm_value(var) / run_type
                    )
                else:
                    input_raw_path = Path(path) / var / run_type / self.sub_path
                    input_path = Path(path) / var / run_type
                if append_input_path_dict:
                    self._input_path_dict[input_path] = var
                if append_raw_input_path_dict:
                    self._raw_input_path_dict[input_raw_path] = var
                yield input_path, input_raw_path

    def _gen_output_folder_paths(
        self,
        path: PathLike,
        append_output_path_dict: bool = False,
        cpm_paths: bool = True,
    ) -> Iterator[Path]:
        """Return a Generator of paths of `self.variables` and `self.crops`."""
        for var in self.variables:
            for region in self.crop_regions:
                for run_type in self.runs:
                    if cpm_paths:
                        crop_path: Path = (
                            Path(path)
                            / CPM_OUTPUT_PATH
                            / region
                            / VariableOptions.cpm_value(var)
                            / run_type
                        )
                    else:
                        crop_path: Path = (
                            Path(path) / CPM_OUTPUT_PATH / region / var / run_type
                        )
                    if append_output_path_dict:
                        self._output_path_dict[crop_path] = region
                    yield crop_path
