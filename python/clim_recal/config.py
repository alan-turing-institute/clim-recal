import subprocess
from dataclasses import dataclass
from os import chdir
from pathlib import Path
from typing import Any, Final, Sequence, TypedDict

from tqdm import tqdm

from .debiasing.debias_wrapper import (
    BaseRunConfig,
    CityOptions,
    MethodOptions,
    RunConfig,
    RunConfigType,
    RunOptions,
    VariableOptions,
    climate_data_mount_path,
)
from .utils.core import product_dict

DATA_PATH_DEFAULT: Final[Path] = climate_data_mount_path()


class ClimRecalRunsConfigType(TypedDict):

    """Lists of parameters to generate `RunConfigType` instances."""

    cities: Sequence[CityOptions] | None
    variables: Sequence[VariableOptions]
    runs: Sequence[RunOptions]
    methods: Sequence[MethodOptions]


ClimRecalRunResultsType = dict[RunConfig, dict[str, subprocess.CompletedProcess]]


@dataclass
class ClimRecalConfig(BaseRunConfig):

    """Manage creating command line scripts to run `debiasing` `cli`."""

    variables: Sequence[VariableOptions] = (VariableOptions.default(),)
    runs: Sequence[RunOptions] = (RunOptions.default(),)
    cities: Sequence[CityOptions] | None = (CityOptions.default(),)
    methods: Sequence[MethodOptions] = (MethodOptions.default(),)

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
