from pathlib import Path
from typing import Final

import numpy as np
import pytest
from numpy.testing import assert_allclose
from numpy.typing import NDArray
from xarray import open_dataset
from xarray.core.types import T_Dataset

from clim_recal.resample import (
    CPMResampler,
    CPMResamplerManager,
    HADsResampler,
    HADsResamplerManager,
    ResamplerBase,
)
from clim_recal.utils.core import CLI_DATE_FORMAT_STR
from clim_recal.utils.xarray import (
    FINAL_CONVERTED_CPM_WIDTH,
    FINAL_RESAMPLE_LON_COL,
    plot_xarray,
)

from .utils import (
    CPM_TASMAX_DAY_SERVER_PATH,
    CPM_TASMAX_LOCAL_TEST_PATH,
    FINAL_CPM_DEC_10_X_2_Y_200_210,
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
    CPM_FIRST_DATES: np.array = np.array(
        ["19801201", "19801202", "19801203", "19801204", "19801205"]
    )
    output_path: Path = resample_test_cpm_output_path / config
    test_config = CPMResampler(
        input_path=tasmax_cpm_1980_raw_path.parent,
        output_path=output_path,
    )
    paths: list[Path]
    match config:
        case "direct":
            paths = [test_config.to_reprojection()]
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
    assert export.dims["time"] == 365
    assert export.dims[FINAL_RESAMPLE_LON_COL] == FINAL_CONVERTED_CPM_WIDTH
    assert_allclose(export.tasmax[10][5][:10].values, FINAL_CPM_DEC_10_X_2_Y_200_210)
    assert (
        CPM_FIRST_DATES == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
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
    test_config = HADsResampler(
        input_path=tasmax_hads_1980_raw_path.parent,
        output_path=resample_test_hads_output_path / f"range-{range}",
    )
    paths: list[Path]
    if range:
        paths = test_config.range_to_reprojection(stop=1)
    else:
        paths = [test_config.to_reprojection()]
    export: T_Dataset = open_dataset(paths[0])
    assert len(export.time) == 31
    assert not np.isnan(export.tasmax[0][200:300].values).all()
    assert (
        HADS_FIRST_DATES.astype(object)
        == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    plot_xarray(
        export.tasmax[0],
        path=resample_test_hads_output_path / f"range-{range}.png",
        time_stamp=True,
    )


@pytest.mark.localcache
@pytest.mark.mount
@pytest.mark.parametrize("strict_fail_bool", (True, False))
@pytest.mark.parametrize("manager", (HADsResamplerManager, CPMResamplerManager))
def test_variable_in_base_import_path_error(
    strict_fail_bool: bool,
    manager: HADsResamplerManager | CPMResamplerManager,
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
def test_execute_resample_configs(
    multiprocess: bool, tmp_path, tasmax_hads_1980_raw_path: Path
) -> None:
    """Test running default HADs spatial projection."""
    test_config = HADsResamplerManager(
        input_paths=tasmax_hads_1980_raw_path.parent,
        resample_paths=tmp_path,
        # crop_paths=tmp_path,
        stop_index=1,
    )
    resamplers: tuple[ResamplerBase, ...] = test_config.execute_configs(
        multiprocess=multiprocess
    )
    export: T_Dataset = open_dataset(resamplers[0][0])
    assert len(export.time) == 31
    assert not np.isnan(export.tasmax[0][200:300].values).all()
    assert (
        HADS_FIRST_DATES.astype(object)
        == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()


# @pytest.mark.localcache
# @pytest.mark.slow
# @pytest.mark.mount
# @pytest.mark.parametrize("manager", (CPMResamplerManager, HADsResamplerManager))
# # @pytest.mark.parametrize("multiprocess", (False, True))
# def test_execute_crop_configs(
#     manager: ResamplerManagerBase,
#     # multiprocess: bool,
#     tmp_path: Path,
#     resample_test_hads_output_path: Path,
#     resample_test_cpm_output_path: Path,
#     tasmax_hads_1980_raw_path: Path,
#     tasmax_cpm_1980_raw_path: Path,
#     tasmax_cpm_1980_converted_path: Path,
# ) -> None:
#     """Test running default HADs spatial projection."""
#     multiprocess: bool = False
#     input_path: Path
#     crop_path: Path
#     manager_kwargs: dict[str, Any] = {}
#     if manager is HADsResamplerManager:
#         input_path = tasmax_hads_1980_raw_path.parent
#         crop_path = (
#             resample_test_hads_output_path / "manage" / HADS_CROP_OUTPUT_LOCAL_PATH
#         )
#         manager_kwargs["cpm_for_coord_alignment"] = tasmax_cpm_1980_converted_path
#         manager_kwargs["cpm_for_coord_alignment_path_converted"] = True
#     else:
#         input_path = tasmax_cpm_1980_raw_path.parent
#         crop_path = (
#             resample_test_cpm_output_path / "manage" / CPM_CROP_OUTPUT_LOCAL_PATH
#         )
#         manager_kwargs["runs"] = (RunOptions.ONE,)
#     test_config: ResamplerManagerBase = manager(
#         input_paths=input_path,
#         resample_paths=tmp_path,
#         crop_paths=crop_path,
#         stop_index=1,
#         _strict_fail_if_var_in_input_path=False,
#         **manager_kwargs,
#     )
#     if isinstance(test_config, HADsResamplerManager):
#         test_config.set_cpm_for_coord_alignment = tasmax_cpm_1980_converted_path
#
#     _: tuple[HADsResampler | CPMResampler, ...] = test_config.execute_resample_configs(
#         multiprocess=multiprocess
#     )
#     region_crops: tuple[HADsResampler | CPMResampler, ...] = (
#         test_config.execute_crop_configs(multiprocess=multiprocess)
#     )
#     region_crop_dict: dict[str, tuple[Path, ...]] = {
#         crop.crop_region: tuple(Path(crop.crop_path).iterdir()) for crop in region_crops
#     }
#     assert len(region_crop_dict) == len(region_crops) == len(RegionOptions)
#     for region, path in region_crop_dict.items():
#         cropped_region: T_Dataset = open_dataset(path[0])
#         bbox = RegionOptions.bounding_box(region)
#         assert_allclose(cropped_region["x"].max(), bbox.xmax, rtol=0.1)
#         assert_allclose(cropped_region["x"].min(), bbox.xmin, rtol=0.1)
#         assert_allclose(cropped_region["y"].max(), bbox.ymax, rtol=0.1)
#         assert_allclose(cropped_region["y"].min(), bbox.ymin, rtol=0.1)
#         if isinstance(test_config, HADsResamplerManager):
#             assert len(cropped_region["time"]) == 31
#         else:
#             assert len(cropped_region["time"]) == 365
