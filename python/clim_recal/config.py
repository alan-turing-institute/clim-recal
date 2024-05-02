import subprocess
import warnings
from dataclasses import dataclass, field
from os import PathLike, chdir, cpu_count
from pathlib import Path
from typing import Any, Final, Sequence, TypedDict

from tqdm import TqdmExperimentalWarning, tqdm

from .debiasing.debias_wrapper import (
    BaseRunConfig,
    CityOptions,
    MethodOptions,
    RunConfig,
    RunConfigType,
    climate_data_mount_path,
)
from .resample import (
    CPM_OUTPUT_LOCAL_PATH,
    HADS_OUTPUT_LOCAL_PATH,
    CPMResamplerManager,
    HADsResamplerManager,
)
from .utils.core import product_dict
from .utils.data import RunOptions, VariableOptions

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)

DATA_PATH_DEFAULT: Final[Path] = climate_data_mount_path()


DEFAULT_OUTPUT_PATH: Final[Path] = Path("clim-recal-runs")
DEFAULT_RESAMPLE_FOLDER: Final[Path] = Path("resample")
DEFAULT_CPUS: Final[int] = 2


class ClimRecalRunsConfigType(TypedDict):

    """Lists of parameters to generate `RunConfigType` instances."""

    cities: Sequence[CityOptions] | None
    variables: Sequence[VariableOptions]
    runs: Sequence[RunOptions]
    methods: Sequence[MethodOptions]


ClimRecalRunResultsType = dict[RunConfig, dict[str, subprocess.CompletedProcess]]


@dataclass
class ClimRecalConfig(BaseRunConfig):

    """Manage creating command line scripts to run `debiasing` `cli`.

    Attributes
    ----------
    variables
        Variables to include in the model, eg. `tasmax`, `tasmin`.
    runs
        Which model runs to include, eg. "01", "08", "11".
    cities
        Which cities to crop data to. Future plans facilitate skipping to run for entire UK.
    methods
        Which debiasing methods to apply.
    multiprocess
        Whether to use `multiprocess` where available
    cpus
        Number of cpus to use if multiprocessing
    output_path
        `Path` to save all intermediate and final results to.
    resample_folder
        `Path` to append to `output_path` for resampling result files.
    hads_folder
        `Path` to append to `output_path` / `resample_folder` for resampling `HADs` files.
    cpm_folder
        `Path` to append to `output_path` / `resample_folder` for resampling `CPM` files.
    cpm_kwargs
        A `dict` of parameters to pass to a `CPMResamplerManager`.
    hads_kwargs
        A `dict` of parameters to pass to `HADsResamplerManager`.

    Examples
    --------
    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> run_config: ClimRecalConfig = ClimRecalConfig(
    ...     cities=('Manchester', 'Glasgow'),
    ...     output_path=resample_test_output_path,
    ...     cpus=1)
    >>> run_config
    <ClimRecalConfig(variables_count=1, runs_count=1, cities_count=2,
                     methods_count=1, cpm_folders_count=1,
                     hads_folders_count=1, start_index=0,
                     stop_index=None, cpus=1)>
    """

    variables: Sequence[VariableOptions] = (VariableOptions.default(),)
    runs: Sequence[RunOptions] = (RunOptions.default(),)
    cities: Sequence[CityOptions] | None = (CityOptions.default(),)
    methods: Sequence[MethodOptions] = (MethodOptions.default(),)
    multiprocess: bool = False
    cpus: int | None = DEFAULT_CPUS
    output_path: PathLike = DEFAULT_OUTPUT_PATH
    resample_folder: PathLike = DEFAULT_RESAMPLE_FOLDER
    hads_folder: PathLike = HADS_OUTPUT_LOCAL_PATH
    cpm_folder: PathLike = CPM_OUTPUT_LOCAL_PATH
    cpm_kwargs: dict = field(default_factory=dict)
    hads_kwargs: dict = field(default_factory=dict)
    start_index: int = 0
    stop_index: int | None = None

    @property
    def resample_path(self) -> Path:
        """The resample_path property."""
        return Path(self.output_path) / self.resample_folder

    @property
    def resample_hads_path(self) -> Path:
        """The resample_hads_path property."""
        return self.resample_path / self.hads_folder

    @property
    def resample_cpm_path(self) -> Path:
        """The resample_hads_path property."""
        return self.resample_path / self.cpm_folder

    def __post_init__(self) -> None:
        """Initiate related `HADs` and `CPM` managers.

        Notes
        -----
        The variagles passed to `CPMResamplerManager` do not apply
        `VariableOptions.cpm_values()`, that occurs within `CPMResamplerManager`
        for ease of coparability with HADs.
        """
        self.cpm_manager = CPMResamplerManager(
            variables=self.variables,
            runs=self.runs,
            output_paths=self.resample_cpm_path,
            start_index=self.start_index,
            stop_index=self.stop_index,
            **self.cpm_kwargs,
        )
        self.hads_manager = HADsResamplerManager(
            variables=self.variables,
            output_paths=self.resample_hads_path,
            start_index=self.start_index,
            stop_index=self.stop_index,
            **self.hads_kwargs,
        )
        self.total_cpus: int | None = cpu_count()
        if self.cpus == None or (self.total_cpus and self.cpus >= self.total_cpus):
            self.cpus = 1 if not self.total_cpus else self.total_cpus - 1

    def __repr__(self) -> str:
        """Summary of `self` configuration as a `str`."""
        return (
            f"<{self.__class__.__name__}("
            f"variables_count={len(self.variables)}, "
            f"runs_count={len(self.runs)}, "
            f"cities_count={len(self.cities) if self.cities else None}, "
            f"methods_count={len(self.methods)}, "
            f"cpm_folders_count={len(self.cpm_manager)}, "
            f"hads_folders_count={len(self.hads_manager)}, "
            f"start_index={self.start_index}, "
            f"stop_index={self.stop_index if self.stop_index else 'None'}, "
            f"cpus={self.cpus})>"
        )

    @property
    def model_vars(self) -> ClimRecalRunsConfigType:
        """Return provided run configurations.

        Examples
        --------
        >>> pprint(clim_runner.model_vars)
        {'cities': ('Glasgow', 'Manchester'),
         'methods': ('quantile_delta_mapping',),
         'runs': ('05',),
         'variables': ('tasmax',)}
        """
        return ClimRecalRunsConfigType(
            cities=self.cities,
            variables=self.variables,
            runs=self.runs,
            methods=self.methods,
        )

    @property
    def model_configs(self) -> tuple[RunConfigType, ...]:
        """`tuple` of all model `RunConfigType` runs.

        Examples
        --------
        >>> pprint(clim_runner.model_configs)
        ({'city': 'Glasgow',
          'method': 'quantile_delta_mapping',
          'run': '05',
          'variable': 'tasmax'},
         {'city': 'Manchester',
          'method': 'quantile_delta_mapping',
          'run': '05',
          'variable': 'tasmax'})
        """
        return tuple(
            RunConfigType(**params)
            for params in product_dict(
                city=self.cities,
                variable=self.variables,
                run=self.runs,
                method=self.methods,
            )
        )

    def _get_first_or_none(self, attr_name) -> Any | None:
        """Get the first value of self.name if iterable, else None."""
        assert hasattr(self, attr_name)
        val: Any | None = getattr(self, attr_name)
        if val is None:
            return None
        elif isinstance(val, Sequence):
            assert len(val) > 0
            return val[0]
        else:
            raise ValueError(f"{self} attribute should be iterable, not: '{val}'")

    @property
    def _first_conf_variable(self) -> VariableOptions:
        """Return the first `self.variables` value."""
        return self.variables[0]

    @property
    def _first_conf_run(self) -> RunOptions:
        """Return the first `self.variables` value."""
        return self.runs[0]

    @property
    def _first_conf_method(self) -> MethodOptions:
        """Return the first `self.variables` value."""
        return self.methods[0]

    @property
    def _first_conf_city(self) -> VariableOptions | None:
        """Return the first `self.variables` value."""
        return self._get_first_or_none(attr_name="cities")

    @property
    def _base_run_config(self) -> RunConfig:
        """Retun a base `RunConfig` from `self` attributes."""
        return RunConfig(
            command_dir=self.command_dir,
            variable=self._first_conf_variable,
            run=self._first_conf_run,
            city=self._first_conf_city,
            method=self._first_conf_method,
            run_prefix=self.run_prefix,
            preprocess_data_file=self.preprocess_data_file,
            run_cmethods_file=self.run_cmethods_file,
            data_path=self.data_path,
            mod_folder=self.mod_folder,
            obs_folder=self.obs_folder,
            preprocess_out_folder=self.preprocess_out_folder,
            cmethods_out_folder=self.cmethods_out_folder,
            calib_date_start=self.calib_date_start,
            calib_date_end=self.calib_date_end,
            valid_date_start=self.valid_date_start,
            valid_date_end=self.valid_date_end,
            processors=self.processors,
            date_format_str=self.date_format_str,
            date_split_str=self.date_split_str,
        )

    def run_models(self) -> ClimRecalRunResultsType:
        """Run all specified models.

        Examples
        --------
        >>> runs: dict[tuple, dict] = clim_runner.run_models()
        >>> pprint(tuple(runs.keys()))
        (('Glasgow', 'tasmax', '05', 'quantile_delta_mapping'),
         ('Manchester', 'tasmax', '05', 'quantile_delta_mapping'))
        """
        initial_folder: Path = Path().resolve()
        chdir(self._base_run_config.command_path)
        run_results: ClimRecalRunResultsType = {}

        for model_config in tqdm(self.model_configs):
            preprocess_run: subprocess.CompletedProcess = subprocess.run(
                self._base_run_config.to_cli_preprocess_tuple_strs(
                    variable=model_config["variable"],
                    run=model_config["run"],
                    city=model_config["city"],
                ),
                capture_output=True,
                text=True,
            )
            cmethods_run: subprocess.CompletedProcess = subprocess.run(
                self._base_run_config.to_cli_run_cmethods_tuple_strs(
                    city=model_config["city"],
                    run=model_config["run"],
                    variable=model_config["variable"],
                    method=model_config["method"],
                ),
                capture_output=True,
                text=True,
            )
            run_results[tuple(model_config.values())] = {
                "preprocess_run": preprocess_run,
                "cmethods_run": cmethods_run,
            }
        chdir(initial_folder)
        return run_results

    @property
    def command_path(self) -> Path:
        """Return command path relative to running tests."""
        return (Path() / self.command_dir).absolute()
