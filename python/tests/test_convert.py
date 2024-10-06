from pathlib import Path
from typing import Final

import numpy as np
import pytest
from numpy.testing import assert_allclose
from numpy.typing import NDArray
from xarray import open_dataset
from xarray.core.types import T_Dataset

from clim_recal.convert import (
    CPMConvert,
    CPMConvertManager,
    HADsConvert,
    HADsConvertManager,
    IterCalcBase,
    IterCalcManagerBase,
)
from clim_recal.utils.core import CLI_DATE_FORMAT_STR
from clim_recal.utils.data import CPM_NAME, HADS_NAME, RunOptions
from clim_recal.utils.xarray import (
    FINAL_CONVERTED_CPM_WIDTH,
    FINAL_RESAMPLE_LON_COL,
    plot_xarray,
)

from .utils import (
    CPM_TASMAX_DAY_SERVER_PATH,
    CPM_TASMAX_LOCAL_TEST_PATH,
    HADS_UK_TASMAX_DAY_SERVER_PATH,
    HADS_UK_TASMAX_LOCAL_TEST_PATH,
)

HADS_FIRST_DATES: Final[NDArray] = np.array(
    ["19800101", "19800102", "19800103", "19800104", "19800105"]
)


@pytest.fixture
def cpm_tasmax_raw_mount_path(data_mount_path: Path) -> Path:
    return data_mount_path / CPM_TASMAX_DAY_SERVER_PATH


@pytest.fixture
def hads_tasmax_raw_mount_path(data_mount_path: Path) -> Path:
    return data_mount_path / HADS_UK_TASMAX_DAY_SERVER_PATH


@pytest.fixture
def hads_tasmax_local_test_path(data_fixtures_path: Path) -> Path:
    return data_fixtures_path / HADS_UK_TASMAX_LOCAL_TEST_PATH


@pytest.fixture
def cpm_tasmax_local_test_path(data_fixtures_path: Path) -> Path:
    return data_fixtures_path / CPM_TASMAX_LOCAL_TEST_PATH


def check_tasmax_1980_cpm(export: T_Dataset) -> bool:
    CPM_FIRST_DATES: NDArray = np.array(
        ["19801201", "19801202", "19801203", "19801204", "19801205"]
    )
    CPM_CHECK_VALUES: NDArray = np.array(
        [
            12.325586,
            12.325586,
            12.312891,
            12.296778,
            12.296778,
            12.2792,
            12.2792,
            12.260401,
            12.260401,
            12.244776,
        ]
    )
    assert export.dims["time"] == 365
    assert export.dims[FINAL_RESAMPLE_LON_COL] == FINAL_CONVERTED_CPM_WIDTH
    # assert export.dims[FINAL_RESAMPLE_LON_COL] == CPM_CHECK_VALUES
    # assert_allclose(export.tasmax[10][5][:10].values, FINAL_CPM_DEC_10_X_2_Y_200_210)
    assert_allclose(export.tasmax[10][5][:10].values, CPM_CHECK_VALUES)
    assert (
        CPM_FIRST_DATES == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    return True


def check_tasmax_1980_hads(export: T_Dataset) -> bool:
    """Checks for 1980 hads."""
    assert len(export.time) == 31
    assert not np.isnan(export.tasmax[0][200:300].values).all()
    assert (
        HADS_FIRST_DATES.astype(object)
        == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    return True


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize(
    "config", ("direct", "range", "direct_provided", "range_provided")
)
def test_cpm_manager(
    resample_test_cpm_output_path, config: str, tasmax_cpm_1980_raw_path: Path
) -> None:
    """Test running default CPM calendar fix."""
    # CPM_FIRST_DATES: np.array = np.array(
    #     ["19801201", "19801202", "19801203", "19801204", "19801205"]
    # )
    output_path: Path = resample_test_cpm_output_path / config
    test_config = CPMConvert(
        input_path=tasmax_cpm_1980_raw_path.parent,
        output_path=output_path,
    )
    paths: list[Path]
    match config:
        case "direct":
            paths = [test_config.to_reprojection(return_path=True)]
        case "range":
            paths = test_config.range_to_reprojection(stop=1)
        case "direct_provided":
            paths = [
                test_config.to_reprojection(index=0, source_to_index=tuple(test_config))
            ]
        case "range_provided":
            paths = test_config.range_to_reprojection(
                stop=1, source_to_index=tuple(test_config)
            )
    export: T_Dataset = open_dataset(paths[0])
    assert check_tasmax_1980_cpm(export)
    plot_xarray(
        export.tasmax[0],
        path=resample_test_cpm_output_path / f"config-{config}.png",
        time_stamp=True,
    )


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("range", (False, True))
def test_hads_manager(
    resample_test_hads_output_path, range: bool, tasmax_hads_1980_raw_path: Path
) -> None:
    """Test running default HADs spatial projection."""
    test_config = HADsConvert(
        input_path=tasmax_hads_1980_raw_path.parent,
        output_path=resample_test_hads_output_path / f"range-{range}",
    )
    paths: list[Path]
    if range:
        paths = test_config.range_to_reprojection(stop=1)
    else:
        paths = [test_config.to_reprojection(return_path=True)]
    export: T_Dataset = open_dataset(paths[0])
    assert check_tasmax_1980_hads(export)
    plot_xarray(
        export.tasmax[0],
        path=resample_test_hads_output_path / f"range-{range}.png",
        time_stamp=True,
    )


@pytest.mark.localcache
@pytest.mark.mount
@pytest.mark.parametrize("strict_fail_bool", (True, False))
@pytest.mark.parametrize("manager", (HADsConvertManager, CPMConvertManager))
def test_variable_in_base_import_path_error(
    strict_fail_bool: bool,
    manager: HADsConvertManager | CPMConvertManager,
    tasmax_hads_1980_raw_path: Path,
) -> None:
    """Test checking import path validity for a given variable."""
    with pytest.raises(manager.VarirableInBaseImportPathError):
        manager(
            input_paths=tasmax_hads_1980_raw_path,
            stop_index=1,
        )
    if strict_fail_bool:
        with pytest.raises(FileExistsError):
            manager(
                input_paths=tasmax_hads_1980_raw_path,
                stop_index=1,
                _strict_fail_if_var_in_input_path=False,
            )


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("multiprocess", (False, True))
@pytest.mark.parametrize("manager_type", (CPM_NAME, HADS_NAME))
def test_execute_resample_configs(
    manager_type: str,
    tmp_path,
    hads_data_path: Path,
    cpm_data_path: Path,
    tasmax_cpm_1980_converted: Path,
    multiprocess: bool,
) -> None:
    """Test running default HADs spatial projection."""
    test_config: IterCalcManagerBase
    if manager_type == HADS_NAME:
        test_config = HADsConvertManager(
            input_paths=hads_data_path,
            output_paths=tmp_path,
            stop_index=1,
            cpm_for_coord_alignment=tasmax_cpm_1980_converted,
        )
    else:
        test_config = CPMConvertManager(
            input_paths=cpm_data_path,
            output_paths=tmp_path,
            runs=(RunOptions.ONE,),
            stop_index=1,
        )
    resamplers: tuple[IterCalcBase, ...] | list = test_config.execute_configs(
        multiprocess=multiprocess, return_resamplers=False, return_path=True, cpus=2
    )
    export: T_Dataset = open_dataset(resamplers[0][0])
    if manager_type == HADS_NAME:
        check_tasmax_1980_hads(export)
    else:
        assert check_tasmax_1980_cpm(export)
