from pathlib import Path
from typing import Any

import pytest
from numpy.testing import assert_allclose
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
from clim_recal.utils.data import (
    CPM_CROP_OUTPUT_PATH,
    HADS_CROP_OUTPUT_PATH,
    RegionOptions,
    RunOptions,
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
    tasmax_cpm_1980_raw_path: Path,
    tasmax_cpm_1980_converted_path: Path,
) -> None:
    """Test running default HADs spatial projection."""
    multiprocess: bool = False
    input_path: Path
    crop_path: Path
    resampler_manager_kwargs: dict[str, Any] = {}
    crop_manager_kwargs: dict[str, Any] = {}
    resampler: ResamplerManagerBase
    if crop_manager is HADsRegionCropManager:
        input_path = tasmax_hads_1980_raw_path.parent
        crop_path = resample_test_hads_output_path / "manage" / HADS_CROP_OUTPUT_PATH
        resampler_manager_kwargs["cpm_for_coord_alignment"] = (
            tasmax_cpm_1980_converted_path
        )
        resampler_manager_kwargs["cpm_for_coord_alignment_path_converted"] = True
        resampler = HADsResamplerManager(
            input_paths=input_path,
            output_paths=tmp_path,
            # crop_paths=crop_path,
            stop_index=1,
            _strict_fail_if_var_in_input_path=False,
            **resampler_manager_kwargs,
        )
    else:
        input_path = tasmax_cpm_1980_raw_path.parent
        crop_path = resample_test_cpm_output_path / "manage" / CPM_CROP_OUTPUT_PATH
        resampler_manager_kwargs["runs"] = crop_manager_kwargs["runs"] = (
            RunOptions.ONE,
        )
        resampler = CPMResamplerManager(
            input_paths=input_path,
            output_paths=tmp_path,
            stop_index=1,
            _strict_fail_if_var_in_input_path=False,
            **resampler_manager_kwargs,
        )
    crop_config: RegionCropperManagerBase = crop_manager(
        input_paths=tmp_path,
        output_paths=crop_path,
        stop_index=1,
        _strict_fail_if_var_in_input_path=False,
        **crop_manager_kwargs,
    )
    if isinstance(resampler, HADsResamplerManager):
        resampler.set_cpm_for_coord_alignment()

    _: tuple[ResamplerBase, ...] = resampler.execute_configs(multiprocess=multiprocess)
    region_crops: tuple[HADsRegionCropper | CPMRegionCropper, ...] = (
        crop_config.execute_configs(multiprocess=multiprocess)
    )
    region_crop_dict: dict[str, tuple[Path, ...]] = {
        crop.crop_region: tuple(Path(crop.output_paths).iterdir())
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
        if isinstance(crop_config, HADsResamplerManager):
            assert len(cropped_region["time"]) == 31
        else:
            assert len(cropped_region["time"]) == 365
