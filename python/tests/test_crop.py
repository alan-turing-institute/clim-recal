from pathlib import Path
from typing import Any

import pytest
from numpy.testing import assert_allclose
from xarray import open_dataset
from xarray.core.types import T_Dataset

from clim_recal.convert import (
    CPMConvertManager,
    HADsConvertManager,
    IterCalcBase,
    IterCalcManagerBase,
)
from clim_recal.crop import (
    CPMRegionCropManager,
    CPMRegionCropper,
    HADsRegionCropManager,
    HADsRegionCropper,
    RegionCropperManagerBase,
)
from clim_recal.utils.core import CLI_DATE_FORMAT_STR
from clim_recal.utils.data import (
    CPM_CROP_OUTPUT_PATH,
    CPM_NAME,
    CPM_OUTPUT_PATH,
    HADS_CROP_OUTPUT_PATH,
    HADS_NAME,
    HADS_OUTPUT_PATH,
    ClimDataType,
    GlasgowCoordsEPSG27700,
    RegionOptions,
    RunOptions,
)
from clim_recal.utils.xarray import FINAL_RESAMPLE_LON_COL, plot_xarray

from .utils import (
    CPM_FIRST_DATES,
    CPM_GLASGOW_CONVERTED_FIRST_VALUES,
    HADS_FIRST_DATES,
    HADS_GLASGOW_CONVERTED_FIRST_VALUES,
    IterCheckTestType,
    match_convert_or_crop,
)


def check_tasmax_1980_glasgow(export: T_Dataset, data_type: ClimDataType) -> bool:
    if data_type == CPM_NAME:
        assert export.dims["time"] == 365
    else:
        assert export.dims["time"] == 31
    assert export.dims[FINAL_RESAMPLE_LON_COL] == GlasgowCoordsEPSG27700.crop_width
    check_values = (
        CPM_GLASGOW_CONVERTED_FIRST_VALUES
        if data_type == CPM_NAME
        else HADS_GLASGOW_CONVERTED_FIRST_VALUES
    )
    check_first_dates = CPM_FIRST_DATES if data_type == CPM_NAME else HADS_FIRST_DATES
    assert_allclose(export.tasmax[10][5].values, check_values)
    assert (
        check_first_dates == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    return True


@pytest.mark.localcache
@pytest.mark.mount
@pytest.mark.parametrize("crop_type", (CPM_NAME, HADS_NAME))
@pytest.mark.parametrize(
    "check_type", ("direct", "range", "direct_provided", "range_provided")
)
def test_crop(
    crop_type: ClimDataType,
    check_type: IterCheckTestType,
    resample_test_cpm_output_path: Path,
    resample_test_hads_output_path: Path,
    tasmax_cpm_1980_converted_path: Path,
    tasmax_hads_1980_converted_path: Path,
) -> None:
    """Test running default CPM calendar fix."""
    cropper: CPMRegionCropper | HADsRegionCropper
    results_path: Path
    if crop_type == CPM_NAME:
        results_path = resample_test_cpm_output_path / CPM_CROP_OUTPUT_PATH
        cropper = CPMRegionCropper(
            input_path=tasmax_cpm_1980_converted_path.parent, output_path=results_path
        )
    else:
        assert crop_type == HADS_NAME
        results_path = resample_test_hads_output_path / HADS_CROP_OUTPUT_PATH
        cropper = HADsRegionCropper(
            input_path=tasmax_hads_1980_converted_path.parent, output_path=results_path
        )
    export: T_Dataset = match_convert_or_crop(
        cropper, check_type=check_type, calc_type="crop"
    )
    assert check_tasmax_1980_glasgow(export, data_type=crop_type)
    plot_xarray(
        export.tasmax[0],
        path=results_path / "glasgow" / f"config-{check_type}.png",
        time_stamp=True,
    )


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("crop_manager", (HADsRegionCropManager, CPMRegionCropManager))
@pytest.mark.parametrize("multiprocess", (False, True))
def test_crop_managers(
    crop_manager: type[RegionCropperManagerBase],
    multiprocess: bool,
    tmp_path: Path,
    resample_test_hads_output_path: Path,
    resample_test_cpm_output_path: Path,
    hads_data_path: Path,
    cpm_data_path: Path,
    tasmax_cpm_1980_converted_path: Path,
) -> None:
    """Test running default HADs spatial projection."""
    raw_input_path: Path
    resample_path: Path
    crop_path: Path
    resampler_manager_kwargs: dict[str, Any] = {}
    crop_manager_kwargs: dict[str, Any] = {}
    resampler: IterCalcManagerBase

    if crop_manager is HADsRegionCropManager:
        raw_input_path = hads_data_path
        resample_path = tmp_path / HADS_OUTPUT_PATH
        crop_path = resample_test_hads_output_path / "manage" / HADS_CROP_OUTPUT_PATH
        resampler_manager_kwargs["cpm_for_coord_alignment"] = (
            tasmax_cpm_1980_converted_path
        )
        resampler_manager_kwargs["cpm_for_coord_alignment_path_converted"] = True
        resampler = HADsConvertManager(
            input_paths=raw_input_path,
            output_paths=resample_path,
            stop_index=1,
            **resampler_manager_kwargs,
        )
    else:
        raw_input_path = cpm_data_path
        resample_path = tmp_path / CPM_OUTPUT_PATH
        crop_path = resample_test_cpm_output_path / "manage" / CPM_CROP_OUTPUT_PATH
        resampler_manager_kwargs["runs"] = crop_manager_kwargs["runs"] = (
            RunOptions.ONE,
        )
        resampler = CPMConvertManager(
            input_paths=raw_input_path,
            output_paths=resample_path,
            stop_index=1,
            **resampler_manager_kwargs,
        )
    crop_config: RegionCropperManagerBase = crop_manager(
        input_paths=resample_path,
        output_paths=crop_path,
        check_input_paths_exist=False,
        **crop_manager_kwargs,
    )
    if isinstance(resampler, HADsConvertManager):
        resampler.set_cpm_for_coord_alignment()

    _: tuple[IterCalcBase, ...] = resampler.execute_configs(multiprocess=multiprocess)
    region_crops: tuple[IterCalcBase, ...] | list = crop_config.execute_configs(
        multiprocess=multiprocess,
        return_instances=True,
        return_path=True,
        cpus=2,
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
