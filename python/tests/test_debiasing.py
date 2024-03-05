"""
Test generating and running `debiasing` scripts

"""
import subprocess
from os import chdir
from pathlib import Path

import pytest
from debiasing.debias_wrapper import (
    PREPROCESS_FILE_NAME,
    CityOptions,
    MethodOptions,
    RunConfig,
    RunOptions,
    VariableOptions,
)


@pytest.fixture
def run_config(tmp_path: Path) -> RunConfig:
    """Generate a `RunConfig` instance to ease paramaterizing tests."""
    return RunConfig(preprocess_out_folder=tmp_path)


def test_command_line_default(cli_preprocess_default_command_str_correct) -> None:
    """Test default generated cli `str`."""
    run_config: RunConfig = RunConfig()
    assert (
        run_config.to_cli_preprocess_str() == cli_preprocess_default_command_str_correct
    )


@pytest.mark.mount
@pytest.mark.server
@pytest.mark.slow
@pytest.mark.parametrize(
    "city, variable, run, method",
    (
        (
            CityOptions.default(),  # 'Manchester'
            VariableOptions.default(),  # 'tasmax`
            RunOptions.default(),  # '05'
            MethodOptions.default(),  # 'quantile_delta_mapping'
        ),
        (
            CityOptions.GLASGOW,
            VariableOptions.default(),
            RunOptions.default(),
            MethodOptions.default(),
        ),
        pytest.param(
            CityOptions.LONDON,
            VariableOptions.default(),
            RunOptions.default(),
            MethodOptions.default(),
            marks=pytest.mark.slow,
        ),
        pytest.param(
            CityOptions.LONDON,
            VariableOptions.RAINFALL,
            RunOptions.SIX,
            MethodOptions.DELTA_METHOD,
            marks=pytest.mark.slow,
        ),
    ),
)
def test_run(
    run_config,
    city,
    variable,
    run,
    method,
    mod_folder_files_count_correct,
    obs_folder_files_count_correct,
    preprocess_out_folder_files_count_correct,
) -> None:
    """Test running generated command script via a subprocess."""
    initial_folder: Path = Path().resolve()
    chdir(run_config.command_path)
    assert PREPROCESS_FILE_NAME in tuple(Path().iterdir())
    # run_config.method = method
    preprocess_run: subprocess.CompletedProcess = subprocess.run(
        run_config.to_cli_preprocess_tuple_strs(city=city, variable=variable, run=run),
        capture_output=True,
        text=True,
    )
    assert preprocess_run.returncode == 0
    assert (
        len(tuple(run_config.yield_mod_folder(city=city)))
        == mod_folder_files_count_correct
    )
    assert (
        len(tuple(run_config.yield_obs_folder(city=city)))
        == obs_folder_files_count_correct
    )

    if method == MethodOptions.default():
        assert (
            len(tuple(run_config.yield_preprocess_out_folder(city=city)))
            == preprocess_out_folder_files_count_correct
        )
    for log_txt in (
        "Saved observed (HADs) data for validation, period ('2010-01-01', '2010-12-30')",
        f"{city}/{run}/{variable}/modv_var-{variable}_run-{run}_20100101_20101230.nc",
    ):
        assert log_txt in preprocess_run.stdout
    cmethods_run: subprocess.CompletedProcess = subprocess.run(
        run_config.to_cli_run_cmethods_tuple_strs(
            city=city, run=run, variable=variable, method=method
        ),
        capture_output=True,
        text=True,
    )
    assert cmethods_run.returncode == 0
    for log_txt in (
        "Loading modelled calibration data (CPM)",
        # Todo: uncomment in future to check new paths
        # (
        #     f"Debiased/three.cities.cropped/{city}/{run}/{variable}/"
        #     f"debiased_{method}_result_var"
        #     f"-{variable}_quantiles-1000_kind-+_group-None_20100101_20101229.nc"
        # ),
        (
            f"Debiased/three.cities.cropped/{city}/{run}/{variable}/"
            f"debiased_{method}_result_var"
        ),
        "Saving to",
        # Todo: uncomment in future to check new paths
        # (
        #     f"Saving to {DATA_PATH_DEFAULT}/{city}/{run}/{variable}/"
        #     f"debiased_{method}_result_var-{variable}_kind-+None_20100101_20101229.nc"
        # ),
        (f"{city}/{run}/{variable}/debiased_{method}_result_var-"),
    ):
        assert log_txt in cmethods_run.stdout

    chdir(initial_folder)
