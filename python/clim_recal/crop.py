from dataclasses import dataclass, field
from datetime import date
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence

from rich.progress import Progress
from xarray import Dataset
from xarray.core.types import T_Dataset

from .convert import IterCalcBase, IterCalcManagerBase
from .utils.core import _get_source_path
from .utils.data import (
    CONVERT_OUTPUT_PATH,
    CPM_CROP_OUTPUT_PATH,
    CPM_END_DATE,
    CPM_OUTPUT_PATH,
    CPM_START_DATE,
    CROP_OUTPUT_PATH,
    HADS_CROP_OUTPUT_PATH,
    HADS_END_DATE,
    HADS_OUTPUT_PATH,
    HADS_START_DATE,
    RegionOptions,
    RunOptions,
    VariableOptions,
)
from .utils.xarray import (
    _write_and_or_return_results,
    crop_xarray,
    data_path_to_date_range,
    execute_configs,
    progress_wrapper,
    region_crop_file_name,
)

logger = getLogger(__name__)


@dataclass(kw_only=True, repr=False)
class RegionCropperBase(IterCalcBase):
    """Manage resampling HADs datafiles for modelling.

    Attributes
    ----------
    input_path
        `Path` to `HADs` files to process.
    output
        `Path` to save processed `HADS` files.
    input_files
        `Path` or `Paths` of `NCF` files to crop.
    crop
        Path or file to spatially crop `input_files` with.
    start_index
        First index of file to iterate processing from.
    stop_index
        Last index of files to iterate processing from as a count from `resample_start_index`.
        If `None`, this will simply iterate over all available files.
    """

    input_path: PathLike | None = Path()
    output_path: PathLike = CROP_OUTPUT_PATH
    crop_region: RegionOptions | str | None = RegionOptions.GLASGOW
    _iter_calc_method_name: str = "range_crop_projection"

    def range_crop_projection(
        self,
        start: int = 0,
        stop: int | None = None,
        step: int = 1,
        override_export_path: Path | None = None,
        source_to_index: Sequence | None = None,
        return_path: bool = True,
        write_results: bool = True,
        progress_bar: bool = True,
        description_func: Callable[..., str] | None = data_path_to_date_range,
        progress_instance: Progress | None = None,
        **kwargs,
    ) -> Iterator[Path | T_Dataset]:
        return progress_wrapper(
            self,
            "to_reprojection",
            start=start,
            stop=stop,
            step=step,
            description="Cropping...",
            override_export_path=override_export_path,
            source_to_index=source_to_index,
            return_path=return_path,
            write_results=write_results,
            use_progress_bar=progress_bar,
            description_func=description_func,
            description_kwargs={"return_type": "string"},
            progress_instance=progress_instance,
            **kwargs,
        )
        # start = start or self.start_index
        # stop = stop or self.stop_index
        # if stop is None:
        #     stop = len(self)
        # logger.info(f"Cropping to '{self.output_path}'")
        # if progress_bar:
        #     for index in track(range(start, stop, step), description="Cropping..."):
        #         yield self.crop_projection(
        #             index=index,
        #             override_export_path=override_export_path,
        #             source_to_index=source_to_index,
        #             return_path=return_path,
        #             write_results=write_results,
        #         )
        # else:
        #     for index in track(range(start, stop, step), description="Cropping..."):
        #         yield self.crop_projection(
        #             index=index,
        #             override_export_path=override_export_path,
        #             source_to_index=source_to_index,
        #             return_path=return_path,
        #             write_results=write_results,
        #         )

    def crop_projection(
        self,
        index: int = 0,
        override_export_path: Path | None = None,
        return_path: bool = False,
        write_results: bool = True,
        source_to_index: Sequence | None = None,
        **kwargs,
    ) -> Path | T_Dataset:
        """Crop a projection to `region` geometry."""
        source_path: Path = _get_source_path(
            self, index=index, source_to_index=source_to_index
        )
        try:
            assert self.crop_region in RegionOptions
        except AttributeError:
            raise ValueError(
                f"'{self.crop_region}' not in 'RegionOptions': {RegionOptions.all()}"
            )
        logger.info(f"{self} cropping {source_path}")
        result: Dataset = crop_xarray(
            xr_time_series=source_path,
            crop_box=RegionOptions.bounding_box(self.crop_region),
            **kwargs,
        )
        logger.debug(f"Completed cropping index {index}...")
        return _write_and_or_return_results(
            self,
            result=result,
            output_path_func=region_crop_file_name,
            source_path=source_path,
            write_results=write_results,
            return_path=return_path,
            override_export_path=override_export_path,
            crop_region=self.crop_region,
        )


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
        `Path` or `Paths` of `NCF` files to crop.
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
    >>> hads_cropper: HADsRegionCropper = HADsRegionCropper(  # doctest: +SKIP
    ...     input_path=resample_test_hads_output_path,
    ... )
    """

    input_path: PathLike | None = CONVERT_OUTPUT_PATH / HADS_OUTPUT_PATH
    crop_path: PathLike = CROP_OUTPUT_PATH / HADS_CROP_OUTPUT_PATH


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
        `Path` or `Paths` of `NCF` files to crop.
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
    >>> resample_test_cpm_output_path: Path = getfixture(
    ...         'resample_test_cpm_output_path')
    >>> cpm_cropper: CPMRegionCropper = CPMRegionCropper(  # doctest: +SKIP
    ...     input_path=resample_test_cpm_output_path,
    ... )
    """

    input_path: PathLike | None = CONVERT_OUTPUT_PATH / CPM_OUTPUT_PATH
    crop_path: PathLike = CROP_OUTPUT_PATH / CPM_CROP_OUTPUT_PATH


@dataclass(kw_only=True, repr=False)
class RegionCropperManagerBase(IterCalcManagerBase):
    """Base class to inherit for `HADs` and `CPM` cropr managers."""

    crop_regions: tuple[RegionOptions | str, ...] = RegionOptions.all()
    configs: list[HADsRegionCropper | CPMRegionCropper] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    calc_class: type[HADsRegionCropper | CPMRegionCropper] | None = None
    check_input_paths_exist: bool = True
    _raw_input_path_dict: dict[Path, VariableOptions | str] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        """Populate config attributes."""
        super().__post_init__()
        if not self.crop_regions:
            self.crop_regions = ()
        # self.check_paths()
        # self.total_cpus: int | None = cpu_count()
        # if not self.cpus:
        #     self.cpus = 1 if not self.total_cpus else self.total_cpus

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"start_date={repr(self.start_date)[9:]}, "
            f"end_date={repr(self.end_date)[9:]}, "
            f"variables_count={len(self.variables)}, "
            f"input_paths_count={len(self.input_paths) if isinstance(self.input_paths, Sequence) else 1})>"
        )

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

        if run_set_output_paths:
            self.set_output_paths()
        assert isinstance(self.input_paths, Iterable)
        if self.output_paths:
            try:
                assert isinstance(self.output_paths, Iterable)
            except AssertionError:
                raise ValueError(
                    f"'output_paths' not iterable for {self}. Hint: try setting 'run_set_output_paths' to 'True'."
                )
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
        append_raw_input_path_dict: bool = False,
    ) -> Iterator[tuple[Path, Path]]:
        raise NotImplementedError

    def set_output_paths(self) -> None:
        """Propagate `self.crop_paths` if needed."""
        if isinstance(self.output_paths, PathLike):
            self._output_paths = self.output_paths
            self.output_paths = tuple(
                self._gen_output_folder_paths(
                    self.output_paths,
                    append_output_path_dict=True,  # append_raw_input_path_dict=True
                )
            )

    def yield_crop_configs(self) -> Iterable[IterCalcBase]:
        """Generate a `CPMRegionCrop` or `HADsRegionCrop` for `self.input_paths`."""
        self.check_paths()
        try:
            assert isinstance(self.input_paths, Iterable)
            assert isinstance(self.output_paths, Iterable)
        except AssertionError as error:
            raise error
        for index, input_paths in enumerate(self._input_path_dict.items()):
            for crop_path, region in self._output_path_dict.items():
                yield self.calc_class(
                    input_path=input_paths[0],
                    output_path=crop_path,
                    variable_name=input_paths[1],
                    start_index=self.start_index,
                    stop_index=self.stop_index,
                    crop_region=region,
                    **self.config_default_kwargs,
                )

    def execute_configs(
        self,
        multiprocess: bool = False,
        cpus: int | None = None,
        return_region_croppers: bool = False,
        return_path: bool = True,
        **kwargs,
    ) -> tuple[RegionCropperBase, ...] | list[T_Dataset | Path]:
        """Run all converter configurations

        Parameters
        ----------
        multiprocess
            If `True` run parameters in `resample_configs` with `multiprocess_execute`.
        cpus
            Number of `cpus` to pass to `multiprocess_execute`.
        """
        return execute_configs(
            self,
            configs_method="yield_crop_configs",
            multiprocess=multiprocess,
            cpus=cpus,
            return_instances=return_region_croppers,
            return_path=return_path,
            **kwargs,
        )


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
        List of `HADsRegionCrop` instances to iterate `resampling` or `cropping`.
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
    >>> tasmax_hads_1980_converted_path: Path = getfixture(
    ...         'tasmax_hads_1980_converted_path')
    >>> if not tasmax_hads_1980_converted_path:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> hads_crop_manager: HADsRegionCropManager = HADsRegionCropManager(
    ...     input_paths=tasmax_hads_1980_converted_path.parent,
    ...     variables=VariableOptions.all(),
    ...     output_paths=resample_test_hads_output_path,
    ...     )
    >>> hads_crop_manager
    <HADsRegionCropManager(variables_count=3, input_paths_count=3)>
    """

    input_paths: PathLike | Sequence[PathLike] = HADS_OUTPUT_PATH
    output_paths: Sequence[PathLike] | PathLike = (
        CONVERT_OUTPUT_PATH / HADS_CROP_OUTPUT_PATH
    )
    start_date: date = HADS_START_DATE
    end_date: date = HADS_END_DATE
    configs: list[HADsRegionCropper] = field(default_factory=list)
    config_default_kwargs: dict[str, Any] = field(default_factory=dict)
    calc_class: type[HADsRegionCropper] = HADsRegionCropper

    def _gen_input_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
        append_raw_input_path_dict: bool = False,
    ) -> Iterator[tuple[Path, Path]]:
        """Yield paths of resampled `self.variables` and `self.runs`."""
        for var in self.variables:
            raw_input_path: Path = Path(path) / var / self.sub_path
            input_path: Path = Path(path) / var
            if append_input_path_dict:
                self._input_path_dict[input_path] = var
            if append_raw_input_path_dict:
                self._raw_input_path_dict[raw_input_path] = var
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
            )
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
        List of `HADsRegionCrop` instances to iterate `resampling` or `cropping`.
    config_default_kwargs
        Parameters passed to all running `self.configs`.
    calc_class
        `class` to construct all `self.configs` instances with.
    cpus
        Number of `cpu` cores to use during multiprocessing.

    Examples
    --------
    >>> tasmax_cpm_1980_converted_path: Path = getfixture(
    ...         'tasmax_cpm_1980_converted_path')
    >>> if not tasmax_cpm_1980_converted_path:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> cpm_crop_manager: CPMRegionCropManager = CPMRegionCropManager(
    ...     input_paths=tasmax_cpm_1980_converted_path.parent,
    ...     output_paths=crop_test_cpm_output_path,
    ...     )
    >>> cpm_crop_manager
    <CPMRegionCropManager(variables_count=1, runs_count=4,
                         input_paths_count=1)>
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

    input_paths: PathLike | Sequence[PathLike] = CONVERT_OUTPUT_PATH / CPM_OUTPUT_PATH
    output_paths: PathLike | Sequence[PathLike] = (
        CONVERT_OUTPUT_PATH / CPM_CROP_OUTPUT_PATH
    )
    start_date: date = CPM_START_DATE
    end_date: date = CPM_END_DATE
    configs: list[CPMRegionCropper] = field(default_factory=list)
    calc_class: type[CPMRegionCropper] = CPMRegionCropper
    runs: Sequence[RunOptions | str] = RunOptions.preferred()

    def _gen_input_folder_paths(
        self,
        path: PathLike,
        append_input_path_dict: bool = False,
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
