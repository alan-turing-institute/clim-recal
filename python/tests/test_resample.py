from datetime import date
from pathlib import Path
from typing import Any, Final

import numpy as np
import pytest

# from matplotlib import pyplot as plt
from numpy.testing import assert_allclose
from numpy.typing import NDArray
from osgeo.gdal import Dataset as GDALDataset
from xarray import open_dataset
from xarray.core.types import T_DataArray, T_Dataset

from clim_recal.resample import (
    BRITISH_NATIONAL_GRID_EPSG,
    CPM_CROP_OUTPUT_LOCAL_PATH,
    CPRUK_XDIM,
    CPRUK_YDIM,
    DEFAULT_RELATIVE_GRID_DATA_PATH,
    HADS_CROP_OUTPUT_LOCAL_PATH,
    HADS_XDIM,
    HADS_YDIM,
    CPMResampler,
    CPMResamplerManager,
    HADsResampler,
    HADsResamplerManager,
)
from clim_recal.utils.core import (
    CLI_DATE_FORMAT_STR,
    DateType,
    annual_data_paths_generator,
    date_range_generator,
    results_path,
)
from clim_recal.utils.data import HadUKGrid, UKCPLocalProjections
from clim_recal.utils.xarray import (
    FINAL_RESAMPLE_LAT_COL,
    FINAL_RESAMPLE_LON_COL,
    HADS_RAW_X_COLUMN_NAME,
    HADS_RAW_Y_COLUMN_NAME,
    NETCDF4_XARRAY_ENGINE,
    ConvertCalendarAlignOptions,
    convert_xr_calendar,
    cpm_reproject_with_standard_calendar,
    cpm_xarray_to_standard_calendar,
    file_name_to_start_end_dates,
    hads_resample_and_reproject,
    interpolate_coords,
    plot_xarray,
)
from conftest import resample_test_cpm_output_path

from .utils import (
    HADS_UK_TASMAX_DAY_SERVER_PATH,
    HADS_UK_TASMAX_LOCAL_TEST_PATH,
    UKCP_RAW_TASMAX_EXAMPLE_PATH,
    UKCP_TASMAX_DAY_SERVER_PATH,
    UKCP_TASMAX_LOCAL_TEST_PATH,
    xarray_example,
    year_days_count,
)

HADS_FIRST_DATES: np.array = np.array(
    ["19800101", "19800102", "19800103", "19800104", "19800105"]
)

CALENDAR_CONVERTED_CPM_WIDTH: Final[int] = 484
CALENDAR_CONVERTED_CPM_HEIGHT: Final[int] = 606

# FINAL_CONVERTED_CPM_WIDTH: Final[int] = 410
# FINAL_CONVERTED_CPM_HEIGHT: Final[int] = 660
FINAL_CONVERTED_CPM_WIDTH: Final[int] = 529
FINAL_CONVERTED_CPM_HEIGHT: Final[int] = 653

FINAL_CONVERTED_HADS_WIDTH: Final[int] = 410
FINAL_CONVERTED_HADS_HEIGHT: Final[int] = 660

RAW_CPM_TASMAX_1980_FIRST_5: np.array = np.array(
    [12.654932, 12.63711, 12.616358, 12.594385, 12.565821], dtype="float32"
)
RAW_CPM_TASMAX_1980_DEC_30_FIRST_5: np.array = np.array(
    [13.832666, 13.802149, 13.788477, 13.777491, 13.768946], dtype="float32"
)

PROJECTED_CPM_TASMAX_1980_FIRST_5: np.array = np.array(
    [13.406641, 13.376368, 13.361719, 13.354639, 13.334864], dtype="float32"
)
PROJECTED_CPM_TASMAX_1980_DEC_31_FIRST_5: np.array = np.array(
    [10.645899, 10.508448, 10.546778, 10.547998, 10.553614], dtype="float32"
)


FINAL_HADS_JAN_10_430_X_230_250_Y: Final[NDArray] = np.array(
    (
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        3.39634549,
        2.97574018,
        2.63433646,
        2.62451613,
        2.53676574,
        2.42130933,
        2.66667496,
        2.60239203,
        2.5052739,
    )
)


FINAL_CPM_DEC_10_5_X_0_10_Y: Final[NDArray] = np.array(
    (
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        5.131494,
        5.091943,
        5.091943,
        5.057275,
    )
)


@pytest.fixture(scope="session")
def reference_final_coord_grid() -> T_Dataset:
    return open_dataset(DEFAULT_RELATIVE_GRID_DATA_PATH, decode_coords="all")


@pytest.fixture
def ukcp_tasmax_raw_mount_path(data_mount_path: Path) -> Path:
    return data_mount_path / UKCP_TASMAX_DAY_SERVER_PATH


@pytest.fixture
def ukcp_tasmax_raw_5_years_paths(ukcp_tasmax_raw_path: Path) -> tuple[Path, ...]:
    """Return a `tuple` of valid paths for 5 years of"""
    return tuple(annual_data_paths_generator(parent_path=ukcp_tasmax_raw_path))


@pytest.fixture
def hads_tasmax_raw_mount_path(data_mount_path: Path) -> Path:
    return data_mount_path / HADS_UK_TASMAX_DAY_SERVER_PATH


@pytest.fixture
def hads_tasmax_local_test_path(data_fixtures_path: Path) -> Path:
    return data_fixtures_path / HADS_UK_TASMAX_LOCAL_TEST_PATH


@pytest.fixture
def ukcp_tasmax_local_test_path(data_fixtures_path: Path) -> Path:
    return data_fixtures_path / UKCP_TASMAX_LOCAL_TEST_PATH


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
        UKCP_RAW_TASMAX_EXAMPLE_PATH, decode_coords="all", engine=NETCDF4_XARRAY_ENGINE
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


# @pytest.mark.slow
# @pytest.mark.xfail
# @pytest.mark.parametrize("data_type", (UKCPLocalProjections, HadUKGrid))
# @pytest.mark.parametrize("data_source", ("mounted", "local"))
# @pytest.mark.parametrize("output_format", (GDALNetCDFFormatStr, GDALGeoTiffFormatStr))
# @pytest.mark.parametrize("glasgow_crop", (True, False))
# def test_geo_warp_format_type_crop(
#     data_type: CEDADataSources,
#     data_source: Literal["mounted", "local"],
#     output_format: GDALFormatsType,
#     glasgow_crop: bool,
#     test_runs_output_path: Path,
#     glasgow_shape_file_path: Path,
#     glasgow_epsg_27700_bounds: BoundsTupleType,
#     uk_rotated_grid_bounds,
#     ukcp_tasmax_raw_mount_path: Path,
#     hads_tasmax_raw_mount_path: Path,
#     hads_tasmax_local_test_path: Path,
#     ukcp_tasmax_local_test_path: Path,
#     is_data_mounted: bool,
# ) -> None:
#     """Test using `geo_warp` with mounted raw data."""
#     server_data_path: Path = (
#         ukcp_tasmax_raw_mount_path
#         if data_type == UKCPLocalProjections
#         else hads_tasmax_raw_mount_path
#     )
#     local_data_path: Path = (
#         ukcp_tasmax_local_test_path
#         if data_type == UKCPLocalProjections
#         else hads_tasmax_local_test_path
#     )
#     data_path: Path = server_data_path if data_source == "mounted" else local_data_path
#     if data_source == "mounted" and not is_data_mounted:
#         pytest.skip("requires external data mounted")
#     assert data_path.exists()
#
#     # cropped.rio.set_spatial_dims(x_dim="grid_longitude", y_dim="grid_latitude")
#     datetime_now = datetime.now()
#     warp_path: Path = test_runs_output_path / "geo_warp"
#     if glasgow_crop:
#         glasgow_test_fig_path = results_path(
#             name="glasgow",
#             path=warp_path,
#             time=datetime_now,
#             extension="png",
#             mkdir=True,
#         )
#     pre_warp_test_fig_path = results_path(
#         name="pre_warp", path=warp_path, time=datetime_now, extension="png", mkdir=True
#     )
#     warp_test_file_path = results_path(
#         name="test_warp_file",
#         path=warp_path,
#         time=datetime_now,
#         extension=GDALFormatExtensions[output_format],
#         mkdir=False,
#     )
#     warp_test_fig_path = results_path(
#         name="test_warp", path=warp_path, time=datetime_now, extension="png", mkdir=True
#     )
#
#     if glasgow_crop:
#         glasgow_geo_df: GeoDataFrame = read_file(glasgow_shape_file_path)
#         glasgow_geo_df.plot()
#         plt.savefig(glasgow_test_fig_path)
#         assert glasgow_geo_df.crs == BRITISH_NATIONAL_GRID_EPSG
#         assert (
#             tuple(glasgow_geo_df.bounds.values.tolist()[0]) == glasgow_epsg_27700_bounds
#         )
#
#     max_temp_data_path: Path = (
#         data_path
#         if data_source == "local"
#         else annual_data_path(
#             end_year=1981,
#             parent_path=data_path,
#         )
#     )
#     xarray_pre_warp: T_Dataset = open_dataset(
#         max_temp_data_path, decode_coords="all", engine=NETCDF4_XARRAY_ENGINE
#     )
#     xarray_pre_warp.isel(time=0).tasmax.plot()
#     plt.savefig(pre_warp_test_fig_path)
#
#     assert str(xarray_pre_warp.rio.crs) != BRITISH_NATIONAL_GRID_EPSG
#     assert xarray_pre_warp.rio.bounds() == uk_rotated_grid_bounds
#     # output_path: Path = warp_path / (max_temp_data_path.stem + ".tif" if output_format == GDALGeoTiffFormatStr else ".nc")
#     xarray_warped: GDALDataset
#     if glasgow_crop:
#         xarray_warped = gdal_warp_wrapper(
#             input_path=max_temp_data_path,
#             output_path=warp_test_file_path,
#             format=output_format,
#         )
#     else:
#         xarray_warped = gdal_warp_wrapper(
#             input_path=max_temp_data_path,
#             output_path=warp_test_file_path,
#             format=output_format,
#             output_bounds=uk_rotated_grid_bounds,
#         )
#     assert xarray_warped is not None
#     read_exported: T_Dataset = open_dataset(warp_test_file_path, decode_coords="all")
#
#     read_exported.Band1.plot()
#     plt.savefig(warp_test_fig_path)
#
#     assert str(read_exported.rio.crs) == BRITISH_NATIONAL_GRID_EPSG
#     if glasgow_crop:
#         assert read_exported.rio.bounds() == glasgow_epsg_27700_bounds


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
    CORRECT_PROJ4: Final[
        str
    ] = "+proj=ob_tran +o_proj=longlat +o_lon_p=0 +o_lat_p=37.5 +lon_0=357.5 +R=6371229 +no_defs=True"
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
    tasmax_cpm_1980_raw: T_Dataset,
    test_runs_output_path: Path,
    variable_name: str = "tasmax",
) -> None:
    """Test all steps around calendar and warping CPM RAW data."""
    output_path: Path = results_path(
        "test-cpm-warp", path=test_runs_output_path, mkdir=True, extension="nc"
    )
    plot_path: Path = output_path.parent / (output_path.stem + ".png")
    projected: T_Dataset = cpm_reproject_with_standard_calendar(
        tasmax_cpm_1980_raw,
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
    assert_allclose(results[variable_name][10][5][:10], FINAL_CPM_DEC_10_5_X_0_10_Y)
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


@pytest.mark.localcache
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize(
    "config", ("direct", "range", "direct_provided", "range_provided")
)
def test_ukcp_manager(
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
    assert_allclose(export.tasmax[10][5][:10].values, FINAL_CPM_DEC_10_5_X_0_10_Y)
    assert (
        CPM_FIRST_DATES == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    plot_xarray(
        export.tasmax[0],
        path=resample_test_cpm_output_path / f"config-{config}.png",
        time_stamp=True,
    )


# @pytest.mark.xfail(reason="checking `export.tasmax` values currently yields `nan`")
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
@pytest.mark.slow
@pytest.mark.parametrize("data_type", ("hads", "cpm"))
@pytest.mark.parametrize("use_reference_grid", (True, False))
def test_interpolate_coords(
    data_type: str,
    reference_final_coord_grid: T_Dataset,
    tasmax_cpm_1980_raw: T_Dataset,
    tasmax_hads_1980_raw: T_Dataset,
    use_reference_grid: bool,
) -> None:
    """Test reprojecting raw spatial files.

    Notes
    -----
    Still seems to run even when `-m "not mount"` is specified.
    """
    reprojected_xr_time_series: T_Dataset
    kwargs: dict[str, Any] = dict(
        variable_name="tasmax",
        x_grid=reference_final_coord_grid.projection_x_coordinate.values,
        y_grid=reference_final_coord_grid.projection_y_coordinate.values,
    )
    x_col_name: str = HADS_XDIM
    y_col_name: str = HADS_YDIM
    if data_type == "hads":
        reprojected_xr_time_series = interpolate_coords(
            tasmax_hads_1980_raw,
            x_coord_column_name=x_col_name,
            y_coord_column_name=y_col_name,
            use_reference_grid=use_reference_grid,
            **kwargs,
        )
        assert reprojected_xr_time_series.dims["time"] == 31
        assert_allclose(
            reprojected_xr_time_series.tasmax[10][430][230:250],
            FINAL_HADS_JAN_10_430_X_230_250_Y,
        )
        if use_reference_grid:
            assert reprojected_xr_time_series.rio.crs == BRITISH_NATIONAL_GRID_EPSG
        else:
            assert reprojected_xr_time_series.rio.crs == tasmax_hads_1980_raw.rio.crs
    else:
        x_col_name = CPRUK_XDIM
        y_col_name = CPRUK_YDIM
        # We are now using gdal_warp_wrapper. See test_cpm_warp_steps
        reprojected_xr_time_series = interpolate_coords(
            tasmax_cpm_1980_raw,
            x_coord_column_name=x_col_name,
            y_coord_column_name=y_col_name,
            use_reference_grid=use_reference_grid,
            **kwargs,
        )
        # Note: this test is to a raw file without 365 day projection
        assert reprojected_xr_time_series.dims["time"] == 360
        assert np.isnan(reprojected_xr_time_series.tasmax[0][10][5][:10].values).all()
        if use_reference_grid:
            assert reprojected_xr_time_series.rio.crs == BRITISH_NATIONAL_GRID_EPSG
        else:
            assert reprojected_xr_time_series.rio.crs == tasmax_cpm_1980_raw.rio.crs
    assert reprojected_xr_time_series.dims[x_col_name] == 528
    assert reprojected_xr_time_series.dims[y_col_name] == 651


@pytest.mark.localcache
@pytest.mark.mount
def test_hads_resample_and_reproject(
    tasmax_hads_1980_raw: T_Dataset,
    tasmax_cpm_1980_raw: T_Dataset,
) -> None:
    variable_name: str = "tasmax"
    output_path: Path = Path("tests/runs/reample-hads")
    # First index is for month, in this case January 1980
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
    )

    # plot_xarray(
    #     reprojected.tasmax[0], path=output_path / "tasmas-1980.png", time_stamp=True
    # )
    # assert_allclose(
    #     reprojected.tasmax[10][430][230:250], FINAL_HADS_JAN_10_430_X_230_250_Y
    # )
    assert reprojected.rio.crs.to_epsg() == int(BRITISH_NATIONAL_GRID_EPSG[5:])
    export_netcdf_path: Path = results_path(
        "tasmax-1980-converted", path=output_path, extension="nc"
    )
    reprojected.to_netcdf(export_netcdf_path)
    read_from_export: T_Dataset = open_dataset(export_netcdf_path, decode_coords="all")
    assert read_from_export.dims["time"] == 31
    assert (
        read_from_export.dims[FINAL_RESAMPLE_LON_COL] == FINAL_CONVERTED_HADS_WIDTH
    )  # replaces projection_x_coordinate
    assert (
        read_from_export.dims[FINAL_RESAMPLE_LAT_COL] == FINAL_CONVERTED_HADS_HEIGHT
    )  # replaces projection_y_coordinate
    assert reprojected.rio.crs == read_from_export.rio.crs == BRITISH_NATIONAL_GRID_EPSG
    # Check projection coordinates match for CPM and HADs
    assert all(cpm_to_match.x == read_from_export.x)
    assert all(cpm_to_match.y == read_from_export.y)


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
        output_paths=tmp_path,
        stop_index=1,
    )
    resamplers: tuple[
        HADsResampler | CPMResampler, ...
    ] = test_config.execute_resample_configs(multiprocess=multiprocess)
    export: T_Dataset = open_dataset(resamplers[0][0])
    assert len(export.time) == 31
    assert not np.isnan(export.tasmax[0][200:300].values).all()
    assert (
        HADS_FIRST_DATES.astype(object)
        == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()


# Todo: rearrange test below for crop testing or delete
# @pytest.mark.xfail(reason="test still in development")
# @pytest.mark.slow
# @pytest.mark.mount
# def test_crop_merged_nc(
#     # align_on: ConvertCalendarAlignOptions,
#     # ukcp_tasmax_raw_path
#     glasgow_shape_file_path,
#     data_mount_path,
# ):
#     """Test `cropping` `DataArray` to `standard` calendar."""
#     # Create a base
#     result_bounds: BoundsTupleType = (
#         353.92520902961434,
#         -4.693282346489016,
#         364.3162765660888,
#         8.073382596733156
#      )
#
#     cropped = crop_xarray(
#         'tests/data/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19821201-19831130.nc',
#         crop_geom=glasgow_shape_file_path, invert=True)
#     assert cropped.rio.bounds == result_bounds
#     ts_to_crop: dict[Path, T_Dataset] = {}
#     for path in ukcp_tasmax_raw_5_years_paths:
#         assert path.exists()
#         ts_to_crop[path] = open_dataset(path, decode_coords="all")
#
#     assert False
#     test_crop = crop_xarray()
