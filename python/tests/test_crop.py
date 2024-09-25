from pathlib import Path
from typing import Any

import numpy as np
import pytest
from numpy.testing import assert_allclose
from numpy.typing import NDArray
from xarray import open_dataset
from xarray.core.types import T_Dataset

from clim_recal.crop import (
    CPMRegionCropManager,
    CPMRegionCropper,
    HADsRegionCropManager,
    HADsRegionCropper,
    RegionCropperManagerBase,
)
from clim_recal.resample import (
    CPMResamplerManager,
    HADsResamplerManager,
    ResamplerBase,
    ResamplerManagerBase,
)
from clim_recal.utils.core import CLI_DATE_FORMAT_STR
from clim_recal.utils.data import (
    CPM_CROP_OUTPUT_PATH,
    CPM_NAME,
    CPM_OUTPUT_PATH,
    HADS_CROP_OUTPUT_PATH,
    HADS_OUTPUT_PATH,
    GlasgowCoordsEPSG27700,
    RegionOptions,
    RunOptions,
)
from clim_recal.utils.xarray import FINAL_RESAMPLE_LON_COL, plot_xarray


def check_tasmax_1980_cpm_glasgow(export: T_Dataset) -> bool:
    CPM_FIRST_DATES: NDArray = np.array(
        ["19801201", "19801202", "19801203", "19801204", "19801205"]
    )
    CPM_CHECK_VALUES: NDArray = np.array(
        [
            6.471338,
            6.238672,
            5.943994,
            5.6705565,
            5.4742675,
            5.126611,
            4.7020507,
            4.2784667,
            4.1996093,
        ]
    )
    assert export.dims["time"] == 365
    assert export.dims[FINAL_RESAMPLE_LON_COL] == GlasgowCoordsEPSG27700.crop_width
    assert_allclose(export.tasmax[10][5].values, CPM_CHECK_VALUES)
    assert (
        CPM_FIRST_DATES == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    return True


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
# @pytest.mark.parametrize("crop_type", (CPM_NAME, HADS_NAME))
@pytest.mark.parametrize(
    "config", ("direct", "range", "direct_provided", "range_provided")
)
def test_cpm_crop(
    resample_test_cpm_output_path: Path,
    resample_test_hads_output_path: Path,
    config: str,
    tasmax_cpm_1980_raw_path: Path,
    tasmax_hads_1980_raw_path: Path,
    tasmax_cpm_1980_converted_path: Path,
    crop_type: str = CPM_NAME,
) -> None:
    """Test running default CPM calendar fix."""
    crop_path: Path = (
        resample_test_cpm_output_path / CPM_CROP_OUTPUT_PATH
        if crop_type == CPM_NAME
        else resample_test_hads_output_path / HADS_CROP_OUTPUT_PATH
    )
    crop_config: HADsRegionCropper | CPMRegionCropper = CPMRegionCropper(
        input_path=tasmax_cpm_1980_converted_path.parent,
        output_path=crop_path,
    )
    paths: list[Path]
    match config:
        case "direct":
            paths = [crop_config.crop_projection(return_path=True)]
        case "range":
            paths = crop_config.range_crop_projection(
                stop=1, return_results=True, return_path=True
            )
        case "direct_provided":
            paths = [
                crop_config.crop_projection(
                    index=0, source_to_index=tuple(crop_config), return_path=True
                )
            ]
        case "range_provided":
            paths = crop_config.range_crop_projection(
                stop=1,
                source_to_index=tuple(crop_config),
                return_results=True,
                return_path=True,
            )
    export: T_Dataset = open_dataset(paths[0])
    assert check_tasmax_1980_cpm_glasgow(export)
    plot_xarray(
        export.tasmax[0],
        path=crop_path / "glasgow" / f"config-{config}.png",
        time_stamp=True,
    )


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("crop_manager", (HADsRegionCropManager, CPMRegionCropManager))
# @pytest.mark.parametrize("multiprocess", (False, True))
def test_execute_crop_configs(
    crop_manager: type[RegionCropperManagerBase],
    # multiprocess: bool,
    tmp_path: Path,
    resample_test_hads_output_path: Path,
    resample_test_cpm_output_path: Path,
    tasmax_hads_1980_raw_path: Path,
    hads_data_path: Path,
    cpm_data_path: Path,
    tasmax_cpm_1980_raw_path: Path,
    tasmax_cpm_1980_converted_path: Path,
) -> None:
    """Test running default HADs spatial projection."""
    multiprocess: bool = False
    raw_input_path: Path
    resample_path: Path
    crop_path: Path
    resampler_manager_kwargs: dict[str, Any] = {}
    crop_manager_kwargs: dict[str, Any] = {}
    resampler: ResamplerManagerBase
    if crop_manager is HADsRegionCropManager:
        # raw_input_path = tasmax_hads_1980_raw_path.parents[2]
        raw_input_path = hads_data_path
        resample_path = tmp_path / HADS_OUTPUT_PATH
        crop_path = resample_test_hads_output_path / "manage" / HADS_CROP_OUTPUT_PATH
        resampler_manager_kwargs["cpm_for_coord_alignment"] = (
            tasmax_cpm_1980_converted_path
        )
        resampler_manager_kwargs["cpm_for_coord_alignment_path_converted"] = True
        resampler = HADsResamplerManager(
            input_paths=raw_input_path,
            output_paths=resample_path,
            # crop_paths=crop_path,
            stop_index=1,
            # _strict_fail_if_var_in_input_path=False,
            **resampler_manager_kwargs,
        )
    else:
        # raw_input_path = tasmax_cpm_1980_raw_path.parents[2]
        raw_input_path = cpm_data_path
        resample_path = tmp_path / CPM_OUTPUT_PATH
        crop_path = resample_test_cpm_output_path / "manage" / CPM_CROP_OUTPUT_PATH
        resampler_manager_kwargs["runs"] = crop_manager_kwargs["runs"] = (
            RunOptions.ONE,
        )
        resampler = CPMResamplerManager(
            input_paths=raw_input_path,
            output_paths=resample_path,
            stop_index=1,
            # _strict_fail_if_var_in_input_path=False,
            **resampler_manager_kwargs,
        )
    crop_config: RegionCropperManagerBase = crop_manager(
        input_paths=resample_path,
        output_paths=crop_path,
        # stop_index=1,
        # _strict_fail_if_var_in_input_path=False,
        check_input_paths_exist=False,
        **crop_manager_kwargs,
    )
    if isinstance(resampler, HADsResamplerManager):
        resampler.set_cpm_for_coord_alignment()

    _: tuple[ResamplerBase, ...] = resampler.execute_configs(multiprocess=multiprocess)
    region_crops: tuple[HADsRegionCropper | CPMRegionCropper] | list = (
        crop_config.execute_configs(
            multiprocess=multiprocess,
            return_region_croppers=True,
            return_path=True,
            cpus=2,
        )
    )
    region_crop_dict: dict[str, tuple[Path, ...]] = {
        crop.crop_region: tuple(Path(crop.output_path).iterdir())
        for crop in region_crops
    }
    assert len(region_crop_dict) == len(region_crops) == len(RegionOptions)
    for region, path in region_crop_dict.items():
        cropped_region: T_Dataset = open_dataset(path[0])
        bbox = RegionOptions.bounding_box(region)
        assert_allclose(cropped_region["x"].max(), bbox.xmax, rtol=0.1)
        assert_allclose(cropped_region["x"].min(), bbox.xmin, rtol=0.1)
        assert_allclose(cropped_region["y"].max(), bbox.ymax, rtol=0.1)
        assert_allclose(cropped_region["y"].min(), bbox.ymin, rtol=0.1)
        if isinstance(crop_config, HADsRegionCropManager):
            assert len(cropped_region["time"]) == 31
        else:
            assert len(cropped_region["time"]) == 365
