from pathlib import Path

import pytest

from clim_recal.pipeline import main
from clim_recal.utils.core import (
    CLIMATE_DATA_PATH,
    DARWIN_MOUNT_PATH,
    DEBIAN_MOUNT_PATH,
    climate_data_mount_path,
    is_platform_darwin,
)


def test_climate_data_mount_path() -> None:
    """Test OS specifc mount path."""
    if is_platform_darwin():
        assert climate_data_mount_path() == DARWIN_MOUNT_PATH / CLIMATE_DATA_PATH
    else:
        assert climate_data_mount_path() == DEBIAN_MOUNT_PATH / CLIMATE_DATA_PATH


@pytest.mark.parametrize(
    "execute",
    (
        False,
        pytest.param(
            True,
            marks=(
                pytest.mark.mount,
                pytest.mark.slow,
            ),
        ),
    ),
)
@pytest.mark.parametrize("multiprocess", (True, False))
@pytest.mark.parametrize("variables", (("rainfall",), ("rainfall", "tasmax")))
@pytest.mark.parametrize("regions", ("Glasgow", None))
def test_main(
    execute: bool,
    variables: tuple[str],
    test_runs_output_path: Path,
    multiprocess: bool,
    regions: str | tuple[str] | None,
    capsys,
) -> None:
    """Test running pipeline configurations."""
    run_folder_name: str = (
        f"{'-'.join(variables)}" f"-multi-{multiprocess}" f"-crop-regions-{regions}"
    )
    if execute:
        run_folder_name = "executed-" + run_folder_name
    output_path: Path = test_runs_output_path / run_folder_name

    results = main(
        execute=execute,
        variables=variables,
        output_path=output_path,
        hads_projection=True,
        cpm_projection=False,
        regions=regions,
        resample_stop_index=1,
        cpus=2,
        multiprocess=multiprocess,
        cpm_kwargs=dict(_allow_check_fail=True),
        hads_kwargs=dict(_allow_check_fail=True),
        local_dated_results_path_prefix="-".join(variables),
    )
    captured = capsys.readouterr()
    assert f"variables_count={len(variables)}" in captured.out
    assert results == None
    # if execute:
    #     assert False
