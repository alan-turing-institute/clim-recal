from pathlib import Path

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
from clim_recal.utils.data import CPM_NAME, HADS_NAME, ClimDataType, RunOptions
from clim_recal.utils.xarray import (
    FINAL_CONVERTED_CPM_HEIGHT,
    FINAL_CONVERTED_CPM_WIDTH,
    FINAL_CONVERTED_HADS_HEIGHT,
    FINAL_CONVERTED_HADS_WIDTH,
    FINAL_RESAMPLE_LAT_COL,
    FINAL_RESAMPLE_LON_COL,
    plot_xarray,
)

from .utils import (
    CPM_CONVERTED_DEC_10_XY_300,
    CPM_FIRST_DATES,
    HADS_CONVERTED_DEC_10_XY_300,
    HADS_FIRST_DATES,
    IterCheckTestType,
    match_convert_or_crop,
)


def check_tasmax_1980(export: T_Dataset, data_type: ClimDataType) -> bool:
    check_values: NDArray
    first_dates: NDArray
    final_width: int
    final_height: int
    time_dims: int
    if data_type == CPM_NAME:
        first_dates = CPM_FIRST_DATES
        time_dims = 365
        check_values = CPM_CONVERTED_DEC_10_XY_300
        final_width = FINAL_CONVERTED_CPM_WIDTH
        final_height = FINAL_CONVERTED_CPM_HEIGHT
    else:
        first_dates = HADS_FIRST_DATES
        time_dims = 31
        check_values = HADS_CONVERTED_DEC_10_XY_300
        final_width = FINAL_CONVERTED_HADS_WIDTH
        final_height = FINAL_CONVERTED_HADS_HEIGHT

    assert export.dims["time"] == time_dims
    assert (
        first_dates == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    assert_allclose(export.tasmax[10][300][300:310].values, check_values)
    assert export.dims[FINAL_RESAMPLE_LON_COL] == final_width
    assert export.dims[FINAL_RESAMPLE_LAT_COL] == final_height
    return True


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("convert_type", (CPM_NAME, HADS_NAME))
@pytest.mark.parametrize(
    "check_type", ("direct", "range", "direct_provided", "range_provided")
)
def test_convert(
    convert_type: ClimDataType,
    check_type: IterCheckTestType,
    local_cache: bool,
    tasmax_hads_1980_raw_path: Path,
    resample_test_hads_output_path: Path,
    tasmax_cpm_1980_raw_path: Path,
    resample_test_cpm_output_path: Path,
    tasmax_cpm_1980_converted_path: Path,
) -> None:
    """Test running default CPM calendar fix."""
    converter: CPMConvert | HADsConvert
    plot_path: Path
    if convert_type == CPM_NAME:
        converter = CPMConvert(
            input_path=tasmax_cpm_1980_raw_path.parent,
            output_path=resample_test_cpm_output_path / f"range-{check_type}",
        )
        plot_path = resample_test_cpm_output_path / f"config-{convert_type}.png"
    else:
        assert convert_type == HADS_NAME
        converter = HADsConvert(
            input_path=tasmax_hads_1980_raw_path.parent,
            output_path=resample_test_hads_output_path / f"range-{check_type}",
        )
        if local_cache:
            converter.cpm_for_coord_alignment = tasmax_cpm_1980_converted_path
            converter.cpm_for_coord_alignment_path_converted = True
        plot_path = resample_test_hads_output_path / f"config-{convert_type}.png"
    export: T_Dataset = match_convert_or_crop(
        converter, check_type=check_type, calc_type="convert"
    )
    assert check_tasmax_1980(export, data_type=convert_type)
    plot_xarray(
        export.tasmax[0],
        path=plot_path,
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
def test_convert_managers(
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
            cpm_for_coord_alignment_path_converted=True,
        )
    else:
        test_config = CPMConvertManager(
            input_paths=cpm_data_path,
            output_paths=tmp_path,
            runs=(RunOptions.ONE,),
            stop_index=1,
        )
    resamplers: tuple[IterCalcBase, ...] | list = test_config.execute_configs(
        multiprocess=multiprocess, return_instances=True, return_path=True, cpus=2
    )
    export: T_Dataset = open_dataset(resamplers[0][0])
    assert check_tasmax_1980(export, data_type=manager_type)
