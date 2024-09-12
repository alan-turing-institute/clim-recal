import logging
from datetime import date
from pathlib import Path
from typing import Final

import numpy as np
import pytest
from numpy.testing import assert_allclose
from numpy.typing import NDArray
from osgeo.gdal import Dataset as GDALDataset
from xarray import Dataset, open_dataset
from xarray.core.types import T_DataArray, T_Dataset

from clim_recal.resample import (
    CPM_CROP_OUTPUT_LOCAL_PATH,
    HADS_CROP_OUTPUT_LOCAL_PATH,
    CPMResampler,
    HADsResampler,
)
from clim_recal.utils.core import (
    CLI_DATE_FORMAT_STR,
    DateType,
    date_range_generator,
    results_path,
)
from clim_recal.utils.data import (
    BRITISH_NATIONAL_GRID_EPSG,
    HadUKGrid,
    UKCPLocalProjections,
)
from clim_recal.utils.gdal_formats import NETCDF_EXTENSION_STR
from clim_recal.utils.xarray import (
    FINAL_CONVERTED_CPM_HEIGHT,
    FINAL_CONVERTED_CPM_WIDTH,
    FINAL_RESAMPLE_LAT_COL,
    FINAL_RESAMPLE_LON_COL,
    HADS_RAW_X_COLUMN_NAME,
    HADS_RAW_Y_COLUMN_NAME,
    NETCDF4_XARRAY_ENGINE,
    ConvertCalendarAlignOptions,
    convert_xr_calendar,
    cpm_check_converted,
    cpm_reproject_with_standard_calendar,
    cpm_xarray_to_standard_calendar,
    file_name_to_start_end_dates,
    get_cpm_for_coord_alignment,
    hads_resample_and_reproject,
    plot_xarray,
)

from .utils import (
    CPM_RAW_TASMAX_EXAMPLE_PATH,
    FINAL_CPM_DEC_10_X_2_Y_200_210,
    xarray_example,
    year_days_count,
)

CALENDAR_CONVERTED_CPM_WIDTH: Final[int] = 484
CALENDAR_CONVERTED_CPM_HEIGHT: Final[int] = 606

PROJECTED_CPM_TASMAX_1980_FIRST_5: Final[NDArray] = np.array(
    [13.406641, 13.376368, 13.361719, 13.354639, 13.334864], dtype="float32"
)
PROJECTED_CPM_TASMAX_1980_DEC_31_FIRST_5: Final[NDArray] = np.array(
    [10.645899, 10.508448, 10.546778, 10.547998, 10.553614], dtype="float32"
)

FINAL_HADS_JAN_10_430_X_200_210_Y: Final[NDArray] = np.array(
    (
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        7.57977839,
        7.47138044,
        7.27587694,
        7.27587694,
        7.07294578,
        7.04533059,
    )
)


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("is_path", (True, False))
@pytest.mark.parametrize("is_converted", (True, False))
def test_cpm_check_converted(
    is_path: bool,
    is_converted: bool,
    tasmax_cpm_1980_raw_path: Path,
    tasmax_cpm_1980_raw: T_Dataset,
    tasmax_cpm_1980_converted_path: Path,
    tasmax_cpm_1980_converted: T_Dataset,
) -> None:
    """Test if `cpm_check_converted` correctly checkes `CPM` converted files."""
    result: bool
    if is_converted:
        result = cpm_check_converted(
            tasmax_cpm_1980_converted_path if is_path else tasmax_cpm_1980_converted
        )
        assert result
    else:
        result = cpm_check_converted(
            tasmax_cpm_1980_raw_path if is_path else tasmax_cpm_1980_raw
        )
        assert not result


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("skip_reproject", (True, False))
@pytest.mark.parametrize("is_path", (True, False))
@pytest.mark.parametrize("is_converted", (True, False))
def test_get_cpm_for_coord_alignment(
    skip_reproject: bool,
    is_path: bool,
    is_converted: bool,
    tasmax_cpm_1980_raw: T_Dataset,
    tasmax_cpm_1980_raw_path: Path,
    tasmax_cpm_1980_converted_path: Path,
    tasmax_cpm_1980_converted: T_Dataset,
    caplog,
) -> None:
    """Test using `set_cpm_for_coord_alignment` to manage coord alignment."""
    with caplog.at_level(logging.INFO):
        path: Path = (
            tasmax_cpm_1980_converted_path if is_converted else tasmax_cpm_1980_raw_path
        )
        xr_dataset: T_Dataset = (
            tasmax_cpm_1980_converted if is_converted else tasmax_cpm_1980_raw
        )
        converter_dataset: T_Dataset = get_cpm_for_coord_alignment(
            cpm_for_coord_alignment=path if is_path else xr_dataset,
            skip_reproject=skip_reproject,
        )
    log_tuples: list[tuple[str, int, str]] = caplog.record_tuples
    if is_path:
        if skip_reproject:
            assert "Skipping reprojection and loading" in log_tuples[0][2]
            assert (
                "Variable 'tasmax' loaded for coord alignment from" in log_tuples[1][2]
            )
        elif is_converted:
            assert "Converting coordinates of" in log_tuples[0][2]
            assert "Checking if already converted..." in log_tuples[1][2]
            assert (
                "Similar to already converted. Returning unmodified" in log_tuples[2][2]
            )
            assert "Coordinates converted from" in log_tuples[3][2]
        else:
            assert "Converting coordinates of" in log_tuples[0][2]
            assert "Checking if already converted..." in log_tuples[1][2]
            assert "Coordinates converted from" in log_tuples[2][2]
    elif not skip_reproject:
        if is_converted:
            assert (
                "Converting coordinates of type <class 'xarray.core.dataset.Dataset'> ..."
                in log_tuples[0][2]
            )
            assert "Checking if already converted..." in log_tuples[1][2]
            assert (
                "Similar to already converted. Returning unmodified" in log_tuples[2][2]
            )
            assert "Coordinates converted to type" in log_tuples[3][2]
        else:
            assert "Converting coordinates of type" in log_tuples[0][2]
            assert "Checking if already converted..." in log_tuples[1][2]
            assert "Coordinates converted to type" in log_tuples[2][2]
    else:
        assert (
            f"Coordinate converter of type <class 'xarray.core.dataset.Dataset'> "
            f"loaded without processing."
        ) in log_tuples[0][2]
    if skip_reproject and not is_converted:
        assert isinstance(converter_dataset, Dataset)
        assert converter_dataset.dims["time"] == 360
        assert "x" not in converter_dataset.dims
        assert "y" not in converter_dataset.dims
    else:
        assert isinstance(converter_dataset, Dataset)
        assert converter_dataset.dims["time"] == 365
        assert converter_dataset.dims["x"] == FINAL_CONVERTED_CPM_WIDTH
        assert converter_dataset.dims["y"] == FINAL_CONVERTED_CPM_HEIGHT


@pytest.mark.slow
@pytest.mark.localcache
@pytest.mark.mount
def test_hads_resample_and_reproject(
    tasmax_hads_1980_raw: T_Dataset,
    tasmax_cpm_1980_raw: T_Dataset,
) -> None:
    variable_name: str = "tasmax"
    output_path: Path = Path("tests/runs/reample-hads")
    # First index is for month, in this case January 1980
    # The following could be replaced by a cached fixture
    cpm_to_match: T_Dataset = cpm_reproject_with_standard_calendar(tasmax_cpm_1980_raw)
    plot_xarray(
        tasmax_hads_1980_raw.tasmax[0],
        path=output_path / "tasmas-1980-JAN-1-raw.png",
        time_stamp=True,
    )

    assert tasmax_hads_1980_raw.dims["time"] == 31
    assert tasmax_hads_1980_raw.dims[HADS_RAW_X_COLUMN_NAME] == 900
    assert tasmax_hads_1980_raw.dims[HADS_RAW_Y_COLUMN_NAME] == 1450
    reprojected: T_Dataset = hads_resample_and_reproject(
        tasmax_hads_1980_raw,
        variable_name=variable_name,
        cpm_to_match=tasmax_cpm_1980_raw,
    )

    assert reprojected.rio.crs.to_epsg() == int(BRITISH_NATIONAL_GRID_EPSG[5:])
    export_netcdf_path: Path = results_path(
        "tasmax-1980-converted", path=output_path, extension="nc"
    )
    reprojected.to_netcdf(export_netcdf_path)
    read_from_export: T_Dataset = open_dataset(export_netcdf_path, decode_coords="all")
    plot_xarray(
        read_from_export.tasmax[0],
        path=output_path / "tasmax-1980-JAN-1-resampled.png",
        time_stamp=True,
    )
    assert_allclose(
        read_from_export.tasmax[10][430][200:210], FINAL_HADS_JAN_10_430_X_200_210_Y
    )
    assert read_from_export.dims["time"] == 31
    assert (
        read_from_export.dims[FINAL_RESAMPLE_LON_COL] == FINAL_CONVERTED_CPM_WIDTH
    )  # replaces projection_x_coordinate
    assert (
        read_from_export.dims[FINAL_RESAMPLE_LAT_COL] == FINAL_CONVERTED_CPM_HEIGHT
    )  # replaces projection_y_coordinate
    assert reprojected.rio.crs == read_from_export.rio.crs == BRITISH_NATIONAL_GRID_EPSG
    # Check projection coordinates match for CPM and HADs
    assert (
        reprojected.tasmax.rio.crs
        == read_from_export.tasmax.rio.crs
        == BRITISH_NATIONAL_GRID_EPSG
    )
    # Check projection coordinates are set at the variable level
    assert all(cpm_to_match.x == read_from_export.x)
    assert all(cpm_to_match.y == read_from_export.y)
    assert (
        read_from_export.spatial_ref.attrs["spatial_ref"]
        == cpm_to_match.spatial_ref.attrs["spatial_ref"]
    )


@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("interpolate_na", (True, False))
def test_convert_cpm_calendar(interpolate_na: bool) -> None:
    """Test `convert_calendar` on mounted `cpm` data.

    Notes
    -----
    If `interpolate_na` is `True`, there shouldn't be `tasmax` `nan` values, hence
    creating the `na_values` `bool` as the inverse of `interpolate_na`.
    """
    any_na_values_in_tasmax: bool = not interpolate_na
    raw_nc: T_Dataset = open_dataset(
        CPM_RAW_TASMAX_EXAMPLE_PATH, decode_coords="all", engine=NETCDF4_XARRAY_ENGINE
    )
    assert len(raw_nc.time) == 360
    assert len(raw_nc.time_bnds) == 360
    converted: T_Dataset = convert_xr_calendar(raw_nc, interpolate_na=interpolate_na)
    assert len(converted.time) == 365
    assert len(converted.time_bnds) == 365
    assert (
        np.isnan(converted.tasmax.head()[0][0][0].values).all()
        == any_na_values_in_tasmax
    )


@pytest.mark.localcache
@pytest.mark.mount
@pytest.mark.slow
@pytest.mark.parametrize("include_bnds_index", (True, False))
def test_cpm_xarray_to_standard_calendar(
    tasmax_cpm_1980_raw: T_Dataset,
    include_bnds_index: bool,
) -> None:
    """Test 360 raw to 365/366 calendar conversion.

    Notes
    -----
    Indexing differs between `include_bnds_index` ``bool`.
    ```
    """
    CORRECT_PROJ4: Final[str] = (
        "+proj=ob_tran +o_proj=longlat +o_lon_p=0 +o_lat_p=37.5 +lon_0=357.5 +R=6371229 +no_defs=True"
    )
    test_converted = cpm_xarray_to_standard_calendar(
        tasmax_cpm_1980_raw, include_bnds_index=include_bnds_index
    )
    assert test_converted.rio.width == CALENDAR_CONVERTED_CPM_WIDTH
    assert test_converted.rio.height == CALENDAR_CONVERTED_CPM_HEIGHT
    assert test_converted.rio.crs.to_proj4() == CORRECT_PROJ4
    assert test_converted.tasmax.rio.crs.to_proj4() == CORRECT_PROJ4
    assert len(test_converted.time) == 365

    tasmax_data_subset: NDArray
    if include_bnds_index:
        assert len(test_converted.tasmax.data) == 2  # second band
        assert len(test_converted.tasmax.data[0][0]) == 365  # first band
        assert len(test_converted.tasmax.data[1][0]) == 365  # second band
        tasmax_data_subset = test_converted.tasmax.data[0][0]  # first band
    else:
        assert len(test_converted.tasmax.data) == 1  # no band index
        tasmax_data_subset = test_converted.tasmax.data[0]
    assert len(tasmax_data_subset) == 365

    # By default December 1 in a 360 to 365 projection would
    # be null. The values matching below should indicate the
    # projection has interpolated null values on the first date
    assert (
        tasmax_data_subset[0][0][:5]
        == PROJECTED_CPM_TASMAX_1980_FIRST_5
        # test_converted.tasmax.data[0][0][0][0][:5] == PROJECTED_CPM_TASMAX_1980_FIRST_5
    ).all()
    # Check December 31 1980, which wouldn't be included in 360 day calendar
    assert (
        # test_converted.tasmax.data[0][0][31][0][:5]
        tasmax_data_subset[31][0][:5]
        == PROJECTED_CPM_TASMAX_1980_DEC_31_FIRST_5
    ).all()


@pytest.mark.localcache
@pytest.mark.mount
@pytest.mark.slow
def test_cpm_reproject_with_standard_calendar(
    tasmax_cpm_1980_raw_path: Path,
    test_runs_output_path: Path,
    variable_name: str = "tasmax",
) -> None:
    """Test all steps around calendar and warping CPM RAW data."""
    output_path: Path = results_path(
        "test-cpm-warp",
        path=test_runs_output_path,
        mkdir=True,
        extension=NETCDF_EXTENSION_STR,
    )
    plot_path: Path = output_path.parent / (output_path.stem + ".png")
    projected: T_Dataset = cpm_reproject_with_standard_calendar(
        tasmax_cpm_1980_raw_path,
    )
    assert projected.rio.crs == BRITISH_NATIONAL_GRID_EPSG
    projected.to_netcdf(output_path)
    results: T_Dataset = open_dataset(output_path, decode_coords="all")
    assert (results.time == projected.time).all()
    assert results.dims == {
        FINAL_RESAMPLE_LON_COL: FINAL_CONVERTED_CPM_WIDTH,
        FINAL_RESAMPLE_LAT_COL: FINAL_CONVERTED_CPM_HEIGHT,
        "time": 365,
    }
    assert results.rio.crs == BRITISH_NATIONAL_GRID_EPSG
    assert len(results.data_vars) == 1
    assert_allclose(
        results[variable_name][10][2][200:210], FINAL_CPM_DEC_10_X_2_Y_200_210
    )
    plot_xarray(results.tasmax[0], plot_path, time_stamp=True)


@pytest.mark.xfail(reason="test not complete")
def test_cpm_tif_to_standard_calendar(
    glasgow_example_cropped_cpm_rainfall_path: Path,
) -> None:
    test_converted: tuple[date, ...] = tuple(
        date_range_generator(
            *file_name_to_start_end_dates(glasgow_example_cropped_cpm_rainfall_path)
        )
    )
    assert len(test_converted) == 366
    assert False


# @pytest.mark.xfail(reason="not finished writing, will need refactor")
@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("region", ("Glasgow", "Manchester", "London", "Scotland"))
@pytest.mark.parametrize("data_type", (UKCPLocalProjections, HadUKGrid))
@pytest.mark.parametrize(
    # "config", ("direct", "range", "direct_provided", "range_provided")
    "config",
    ("direct", "range"),
)
def test_crop_xarray(
    tasmax_cpm_1980_raw_path,
    tasmax_hads_1980_raw_path,
    resample_test_cpm_output_path,
    resample_test_hads_output_path,
    config: str,
    data_type: str,
    region: str,
):
    """Test `cropping` `DataArray` to `standard` calendar."""
    CPM_FIRST_DATES: np.array = np.array(
        ["19801201", "19801202", "19801203", "19801204", "19801205"]
    )
    test_config: CPMResampler | HADsResampler
    if data_type == HadUKGrid:
        output_path: Path = resample_test_hads_output_path / config
        crop_path: Path = (
            resample_test_hads_output_path / config / HADS_CROP_OUTPUT_LOCAL_PATH
        )

        test_config = HADsResampler(
            input_path=tasmax_hads_1980_raw_path.parent,
            output_path=output_path,
            crop_path=crop_path,
        )
    else:
        assert data_type == UKCPLocalProjections
        output_path: Path = resample_test_cpm_output_path / config
        crop_path: Path = (
            resample_test_cpm_output_path / config / CPM_CROP_OUTPUT_LOCAL_PATH
        )
        test_config = CPMResampler(
            input_path=tasmax_cpm_1980_raw_path.parent,
            output_path=output_path,
            crop_path=crop_path,
        )
    paths: list[Path]
    try:
        reproject_result: GDALDataset = test_config.to_reprojection()
    except FileExistsError:
        test_config._sync_reprojected_paths(overwrite_output_path=output_path)

    match config:
        case "direct":
            paths = [test_config.crop_projection(region=region)]
        case "range":
            paths = test_config.range_crop_projection(stop=1)
        # case "direct_provided":
        #     paths = [
        #         test_config.to_reprojection(index=0, source_to_index=tuple(test_config))
        #     ]
        # case "range_provided":
        #     paths = test_config.range_to_reprojection(
        #         stop=1, source_to_index=tuple(test_config)
        #     )
    crop: T_Dataset = open_dataset(paths[0])
    # assert crop.dims[FINAL_RESAMPLE_LON_COL] == FINAL_CONVERTED_CPM_WIDTH
    # assert_allclose(export.tasmax[10][5][:10].values, FINAL_CPM_DEC_10_5_X_0_10_Y)
    if data_type == UKCPLocalProjections:
        assert crop.dims["time"] == 365
        assert (
            CPM_FIRST_DATES == crop.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
        ).all()
    plot_xarray(
        crop.tasmax[0],
        path=crop_path / region / f"config-{config}.png",
        time_stamp=True,
    )


def test_leap_year_days() -> None:
    """Test covering a leap year of 366 days."""
    start_date_str: str = "2024-03-01"
    end_date_str: str = "2025-03-01"
    xarray_2024_2025: T_DataArray = xarray_example(
        start_date=start_date_str,
        end_date=end_date_str,
        inclusive=True,
    )
    assert len(xarray_2024_2025) == year_days_count(leap_years=1)


# This is roughly what I had in mind for
# https://github.com/alan-turing-institute/clim-recal/issues/132
# This tests converting from a standard calendar to a 360_day calendar.
@pytest.mark.parametrize(
    # Only one of start_date and end_date are included the day counts
    "start_date, end_date, gen_date_count, days_360, converted_days, align_on",
    [
        pytest.param(
            # 4 years, including a leap year
            "2024-03-02",
            "2028-03-02",
            year_days_count(standard_years=3, leap_years=1),
            year_days_count(xarray_360_day_years=4),
            year_days_count(standard_years=3, leap_years=1),
            "year",
            id="years_4_annual_align",
        ),
        pytest.param(
            # A whole year, most of which is in a leap year, but avoids the leap day
            "2024-03-02",
            "2025-03-02",
            year_days_count(standard_years=1),
            year_days_count(xarray_360_day_years=1) - 1,
            year_days_count(standard_years=1),
            "year",
            id="leap_year_but_no_leap_day_annual_align",
        ),
        pytest.param(
            # A whole year, the same date range as the previous test,
            # but includes the leap day and the majority of the days are in a non-leap year
            # Note: the current final export configuration *adds* a day
            "2023-03-02",
            "2024-03-02",
            year_days_count(leap_years=1),
            year_days_count(xarray_360_day_years=1) + 1,
            year_days_count(leap_years=1) + 1,
            "year",
            id="leap_year_with_leap_day_annual_align",
        ),
        pytest.param(
            # An exact calendar year which *IS NOT* a leap year
            "2023-01-01",
            "2024-01-01",
            year_days_count(standard_years=1),
            year_days_count(xarray_360_day_years=1),
            year_days_count(standard_years=1),
            "year",
            id="non_leap_year_annual_align",
        ),
        pytest.param(
            # A leap day (just the days either side, in a leap year)
            "2024-02-28",
            "2024-03-01",
            2,
            2,
            2,
            "year",
            id="leap_day",
        ),
        pytest.param(
            # A non-leap day (just the days either side, in a non-leap year)
            "2023-02-28",
            "2023-03-01",
            1,
            1,
            1,
            "year",
            id="non_leap_day_date_align",
        ),
        # Add more test cases to cover the different scenarios and edge cases
        pytest.param(
            # 4 years, including a leap year
            # WARNING: the intermittent year days seems a week short
            "2024-03-02",
            "2028-03-02",
            year_days_count(standard_years=3, leap_years=1),
            year_days_count(xarray_360_day_years=4) - 7,
            year_days_count(standard_years=3, leap_years=1),
            "date",
            id="years_4_date_align",
            marks=pytest.mark.xfail(reason="raises `date_range_like` error"),
        ),
        pytest.param(
            # A whole year, most of which is in a leap year, but avoids the leap day
            "2024-03-02",
            "2025-03-02",
            year_days_count(standard_years=1),
            year_days_count(xarray_360_day_years=1) - 2,
            year_days_count(standard_years=1),
            "date",
            id="leap_year_but_no_leap_day_date_align",
            marks=pytest.mark.xfail(reason="raises `date_range_like` error"),
        ),
        pytest.param(
            # A whole year, the same date range as the previous test,
            # but includes the leap day and the majority of the days are in a non-leap year
            # Note: the current final export configuration *adds* a day
            "2023-03-02",
            "2024-03-02",
            year_days_count(leap_years=1),
            year_days_count(xarray_360_day_years=1) - 1,
            year_days_count(leap_years=1) + 1,
            "date",
            id="leap_year_with_leap_day_date_align",
            marks=pytest.mark.xfail(reason="raises `date_range_like` error"),
        ),
        pytest.param(
            # An exact calendar year which *IS NOT* a leap year
            "2023-01-01",
            "2024-01-01",
            year_days_count(standard_years=1),
            year_days_count(xarray_360_day_years=1) - 2,
            year_days_count(standard_years=1),
            "date",
            id="non_leap_year_date_align",
            marks=pytest.mark.xfail(reason="raises `date_range_like` error"),
        ),
        pytest.param(
            # A leap day (just the days either side, in a leap year)
            "2024-02-28",
            "2024-03-01",
            2,
            2,
            2,
            "date",
            id="leap_day",
        ),
        pytest.param(
            # A non-leap day (just the days either side, in a non-leap year)
            "2023-02-28",
            "2023-03-01",
            1,
            1,
            1,
            "date",
            id="non_leap_day_date_align",
        ),
    ],
)
def test_time_gaps_360_to_standard_calendar(
    start_date: DateType,
    end_date: DateType,
    gen_date_count: int,
    days_360: int,
    converted_days: int,
    align_on: ConvertCalendarAlignOptions,
):
    """Test `convert_xr_calendar` call of `360_day` `DataArray` to `standard` calendar."""
    # Potential paramaterized variables
    inclusive_date_range: bool = False  # includes the last day specified
    use_cftime: bool = True  # Whether to enforece using `cftime` over `datetime64`
    # align_on: ConvertCalendarAlignOptions = 'date'

    # Create a base
    base: T_Dataset = xarray_example(
        start_date, end_date, as_dataset=True, inclusive=inclusive_date_range
    )

    # Ensure the generated date range matches for later checks
    # This occurs for a sigle leap year
    assert len(base.time) == gen_date_count

    # Convert to `360_day` calendar example
    dates_360: T_Dataset = base.convert_calendar(
        calendar="360_day",
        align_on=align_on,
        use_cftime=use_cftime,
    )

    # Check the total number of days are as expected
    assert len(dates_360.time) == days_360

    if converted_days < 5:
        with pytest.raises(ValueError):
            convert_xr_calendar(dates_360, align_on=align_on, use_cftime=use_cftime)
    else:
        dates_converted: T_Dataset = convert_xr_calendar(
            dates_360, align_on=align_on, use_cftime=use_cftime
        )
        assert len(dates_converted.time) == converted_days

        # Optionally now check which dates have been dropped and added
        # Add more assertions here...
        assert all(base.time == dates_converted.time)
        assert all(base.time != dates_360.time)
