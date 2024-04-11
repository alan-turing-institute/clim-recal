from datetime import datetime
from pathlib import Path
from typing import Final, Literal

import pytest
from geopandas import GeoDataFrame, read_file
from matplotlib import pyplot as plt
from osgeo.gdal import Dataset as GDALDataset
from xarray import DataArray, Dataset, open_dataset

from clim_recal.resample import (
    UK_SPATIAL_PROJECTION,
    ConvertCalendarAlignOptions,
    convert_xr_calendar,
    crop_nc,
    geo_warp,
)
from clim_recal.utils.core import (
    CPM_YEAR_DAYS,
    LEAP_YEAR_DAYS,
    NORMAL_YEAR_DAYS,
    DateType,
    annual_data_path,
    annual_data_paths_generator,
    run_path,
)
from clim_recal.utils.data import CEDADataSources, HadUKGrid, UKCPLocalProjections
from clim_recal.utils.xarray import (
    NETCDF4_XARRAY_ENGINE,
    BoundsTupleType,
    GDALFormatExtensions,
    GDALFormatsType,
    GDALGeoTiffFormatStr,
    GDALNetCDFFormatStr,
    xarray_example,
)

HADS_UK_TASMAX_DAY_SERVER_PATH: Final[Path] = Path("Raw/HadsUKgrid/tasmax/day")
HADS_UK_RESAMPLED_DAY_SERVER_PATH: Final[Path] = Path(
    "Processed/HadsUKgrid/resampled_2.2km/tasmax/day"
)
# Todo: Change "tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc"
# to "tasmax_hads_example.nc"
HADS_UK_TASMAX_LOCAL_TEST_PATH: Final[Path] = (
    Path(HadUKGrid.slug) / "tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc"
)

UKCP_TASMAX_DAY_SERVER_PATH: Final[Path] = Path("Raw/UKCP2.2/tasmax/01/latest")
# Todo: Change "tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc"
# to "tasmax_cpm_example.nc"
UKCP_TASMAX_LOCAL_TEST_PATH: Final[Path] = (
    Path(UKCPLocalProjections.slug)
    / "tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc"
)


@pytest.fixture
def ukcp_tasmax_raw_mount_path(data_mount_path: Path) -> Path:
    return data_mount_path / UKCP_TASMAX_DAY_SERVER_PATH


@pytest.fixture
def ukcp_tasmax_raw_5_years_paths(ukcp_tasmax_raw_path: Path) -> tuple[Path, ...]:
    """Return a `tuple` of valid paths for 5 years of"""
    return tuple(annual_data_paths_generator(parent_path=ukcp_tasmax_raw_path))


#
# @pytest.mark.slow
# @pytest.mark.mount
# def ukcp_tasmax_raw_5_years(ukcp_tasmax_raw_5_years_paths) -> Dataset:


@pytest.fixture
def hads_tasmax_raw_mount_path(data_mount_path: Path) -> Path:
    return data_mount_path / HADS_UK_TASMAX_DAY_SERVER_PATH


@pytest.fixture
def hads_tasmax_local_test_path(data_fixtures_path: Path) -> Path:
    return data_fixtures_path / HADS_UK_TASMAX_LOCAL_TEST_PATH


@pytest.fixture
def ukcp_tasmax_local_test_path(data_fixtures_path: Path) -> Path:
    return data_fixtures_path / UKCP_TASMAX_LOCAL_TEST_PATH


# May be replaced by `glasgow_shape_file_path` fixture
# @pytest.mark.mount
# @pytest.fixture
# def glasgow_server_shape(data_mount_path) -> GeoDataFrame:
#     yield read_file(data_mount_path / GLASGOW_GEOM_LOCAL_PATH)


class StandardWith360DayError(Exception):
    ...


def year_days_count(
    standard_years: int = 0,
    leap_years: int = 0,
    xarray_360_day_years: int = 0,
    strict: bool = True,
) -> int:
    """Return the number of days for the combination of learn lengths.

    Parameters
    ----------
    standard_years
        Count of 365 day years.
    leap_years
        Count of 366 day years.
    xarray_360_day_years
        Count of 360 day years following xarray's specification.
    strict
        Whether to prevent combining `standard_years` or `leap_years`
        with `xarray_360_day_years`.

    Returns
    -------
    Sum of all year type counts

    Examples
    --------
    >>> year_days_count(standard_years=4) == NORMAL_YEAR_DAYS*4 == 365*4
    True
    >>> year_days_count(xarray_360_day_years=4) == CPM_YEAR_DAYS*4 == 360*4
    True
    >>> (year_days_count(standard_years=3, leap_years=1)
    ...  == NORMAL_YEAR_DAYS*3 + LEAP_YEAR_DAYS
    ...  == 365*3 + 366)
    True
    """
    if strict and (standard_years or leap_years) and xarray_360_day_years:
        raise StandardWith360DayError(
            f"With 'strict == True', "
            f"{standard_years} standard (365 day) years and/or "
            f"{leap_years} leap (366 day) years "
            f"cannot be combined with "
            f"xarray_360_day_years ({xarray_360_day_years})."
        )
    return (
        standard_years * NORMAL_YEAR_DAYS
        + leap_years * LEAP_YEAR_DAYS
        + xarray_360_day_years * CPM_YEAR_DAYS
    )


def test_leap_year_days() -> None:
    """Test covering a leap year of 366 days."""
    start_date_str: str = "2024-03-01"
    end_date_str: str = "2025-03-01"
    xarray_2024_2025: DataArray = xarray_example(
        start_date=start_date_str,
        end_date=end_date_str,
        inclusive=True,
    )
    assert len(xarray_2024_2025) == year_days_count(leap_years=1)


# This is roughly what I had in mind for
# https://github.com/alan-turing-institute/clim-recal/issues/132
# This tests converting from a standard calendar to a 360_day calendar.
# It would be good to have an equivalent test would be to convert from a 360_day
# calendar to a standard calendar.
# These should be two separate tests. Trying to generalise the test to cover
# both cases would overcomplicate
# the code and make it harder to understand.
@pytest.mark.skip("this may be causing too much lost space")
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
    base: Dataset = xarray_example(
        start_date, end_date, as_dataset=True, inclusive=inclusive_date_range
    )

    # Ensure the generated date range matches for later checks
    # This occurs for a sigle leap year
    assert len(base.time) == gen_date_count

    # Convert to `360_day` calendar example
    dates_360: Dataset = base.convert_calendar(
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
        dates_converted: Dataset = convert_xr_calendar(
            dates_360, align_on=align_on, use_cftime=use_cftime
        )
        assert len(dates_converted.time) == converted_days

        # Optionally now check which dates have been dropped and added
        # Add more assertions here...
        assert all(base.time == dates_converted.time)
        assert all(base.time != dates_360.time)


@pytest.mark.skip("this may be causing too much lost space")
@pytest.mark.slow
@pytest.mark.xfail
@pytest.mark.parametrize("data_type", (UKCPLocalProjections, HadUKGrid))
@pytest.mark.parametrize("data_source", ("mounted", "local"))
@pytest.mark.parametrize("output_format", (GDALNetCDFFormatStr, GDALGeoTiffFormatStr))
@pytest.mark.parametrize("glasgow_crop", (True, False))
def test_geo_warp(
    data_type: CEDADataSources,
    data_source: Literal["mounted", "local"],
    output_format: GDALFormatsType,
    glasgow_crop: bool,
    resample_test_runs_output_path: Path,
    glasgow_shape_file_path: Path,
    glasgow_epsg_27700_bounds: BoundsTupleType,
    uk_epsg_27700_bounds,
    ukcp_tasmax_raw_mount_path: Path,
    hads_tasmax_raw_mount_path: Path,
    hads_tasmax_local_test_path: Path,
    ukcp_tasmax_local_test_path: Path,
    is_data_mounted: bool,
) -> None:
    """Test using `geo_warp` with mounted raw data."""
    server_data_path: Path = (
        ukcp_tasmax_raw_mount_path
        if data_type == UKCPLocalProjections
        else hads_tasmax_raw_mount_path
    )
    local_data_path: Path = (
        ukcp_tasmax_local_test_path
        if data_type == UKCPLocalProjections
        else hads_tasmax_local_test_path
    )
    data_path: Path = server_data_path if data_source == "mounted" else local_data_path
    if data_source == "mounted" and not is_data_mounted:
        pytest.skip("requires external data mounted")
    assert data_path.exists()

    # cropped.rio.set_spatial_dims(x_dim="grid_longitude", y_dim="grid_latitude")
    datetime_now = datetime.now()
    warp_path: Path = resample_test_runs_output_path / "geo_warp"
    if glasgow_crop:
        glasgow_test_fig_path = run_path(
            name="glasgow",
            path=warp_path,
            time=datetime_now,
            extension="png",
            mkdir=True,
        )
    pre_warp_test_fig_path = run_path(
        name="pre_warp", path=warp_path, time=datetime_now, extension="png", mkdir=True
    )
    warp_test_file_path = run_path(
        name="test_warp_file",
        path=warp_path,
        time=datetime_now,
        extension=GDALFormatExtensions[output_format],
        mkdir=False,
    )
    warp_test_fig_path = run_path(
        name="test_warp", path=warp_path, time=datetime_now, extension="png", mkdir=True
    )

    if glasgow_crop:
        glasgow_geo_df: GeoDataFrame = read_file(glasgow_shape_file_path)
        glasgow_geo_df.plot()
        plt.savefig(glasgow_test_fig_path)
        assert glasgow_geo_df.crs == UK_SPATIAL_PROJECTION
        assert (
            tuple(glasgow_geo_df.bounds.values.tolist()[0]) == glasgow_epsg_27700_bounds
        )

    max_temp_data_path: Path = (
        data_path
        if data_source == "local"
        else annual_data_path(
            end_year=1981,
            parent_path=data_path,
        )
    )
    xarray_pre_warp: Dataset = open_dataset(
        max_temp_data_path, decode_coords="all", engine=NETCDF4_XARRAY_ENGINE
    )
    xarray_pre_warp.isel(time=0).tasmax.plot()
    plt.savefig(pre_warp_test_fig_path)

    assert str(xarray_pre_warp.rio.crs) != UK_SPATIAL_PROJECTION
    assert xarray_pre_warp.rio.bounds() == uk_epsg_27700_bounds
    # output_path: Path = warp_path / (max_temp_data_path.stem + ".tif" if output_format == GDALGeoTiffFormatStr else ".nc")
    xarray_warped: GDALDataset
    if glasgow_crop:
        xarray_warped = geo_warp(
            max_temp_data_path, output_path=warp_test_file_path, format=output_format
        )
    else:
        xarray_warped = geo_warp(
            max_temp_data_path,
            output_path=warp_test_file_path,
            format=output_format,
            output_bounds=uk_epsg_27700_bounds,
        )
    assert xarray_warped is not None
    read_exported: Dataset = open_dataset(warp_test_file_path, decode_coords="all")

    read_exported.Band1.plot()
    plt.savefig(warp_test_fig_path)

    assert str(read_exported.rio.crs) == UK_SPATIAL_PROJECTION
    if glasgow_crop:
        assert read_exported.rio.bounds() == glasgow_epsg_27700_bounds


@pytest.mark.skip("this may be causing too much lost space")
@pytest.mark.xfail
def test_crop_nc(
    # align_on: ConvertCalendarAlignOptions,
    # ukcp_tasmax_raw_path
    glasgow_shape_file_path,
    data_fixtures_path,
    glasgow_epsg_27700_bounds,
    uk_epsg_27700_bounds,
):
    """Test `cropping` `DataArray` to `standard` calendar."""
    # cropped.rio.set_spatial_dims(x_dim="grid_longitude", y_dim="grid_latitude")
    datetime_now_str: str = str(datetime.now()).replace(" ", "-")

    # plot_path: Path = data_fixtures_path / 'output'
    plot_path: Path = Path("tests") / "runs"
    plot_path.mkdir(parents=True, exist_ok=True)

    glasgow_fig_file_name: Path = Path(f"glasgow_{datetime_now_str}.png")
    pre_crop_fig_file_name: Path = Path(f"pre_crop_{datetime_now_str}.png")
    crop_fig_file_name: Path = Path(f"test_crop_{datetime_now_str}.png")

    glasgow_test_fig_path: Path = plot_path / glasgow_fig_file_name
    pre_crop_test_fig_path: Path = plot_path / pre_crop_fig_file_name
    crop_test_fig_path: Path = plot_path / crop_fig_file_name

    glasgow_geo_df: GeoDataFrame = read_file(glasgow_shape_file_path)
    glasgow_geo_df.plot()
    plt.savefig(glasgow_test_fig_path)
    assert glasgow_geo_df.crs == UK_SPATIAL_PROJECTION
    assert tuple(glasgow_geo_df.bounds.values.tolist()[0]) == glasgow_epsg_27700_bounds

    max_temp_1981_path: Path = annual_data_path(
        end_year=1981,
        parent_path=data_fixtures_path,
    )
    xarray_pre_crop: Dataset = open_dataset(max_temp_1981_path, decode_coords="all")
    xarray_pre_crop.isel(time=0).tasmax.plot()
    plt.savefig(pre_crop_test_fig_path)

    assert str(xarray_pre_crop.rio.crs) != UK_SPATIAL_PROJECTION
    assert xarray_pre_crop.rio.bounds() == uk_epsg_27700_bounds

    cropped: Dataset = crop_nc(
        xr_time_series=max_temp_1981_path,
        crop_geom=glasgow_shape_file_path,
        enforce_xarray_spatial_dims=True,
        invert=True,
        initial_clip_box=False,
    )

    cropped.isel(time=0).tasmax.plot()
    plt.savefig(crop_test_fig_path)

    assert str(cropped.rio.crs) == UK_SPATIAL_PROJECTION
    assert cropped.rio.bounds() == glasgow_epsg_27700_bounds
    assert False


# @pytest.mark.xfail("test still in development")
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
#     cropped = crop_nc(
#         'tests/data/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19821201-19831130.nc',
#         crop_geom=glasgow_shape_file_path, invert=True)
#     assert cropped.rio.bounds == result_bounds
#     ts_to_crop: dict[Path, Dataset] = {}
#     for path in ukcp_tasmax_raw_5_years_paths:
#         assert path.exists()
#         ts_to_crop[path] = open_dataset(path, decode_coords="all")
#
#     assert False
#     test_crop = crop_nc()


#
#
# @pytest.mark.server
# def test_ukcp_raw(
#         start_date: DateType = '1980-12-01',
#     end_date: DateType = '1985-12-01',
#     align_on: ConvertCalendarAlignOptions,
# ):
#     """Test `convert_xr_calendar` call of `360_day` `DataArray` to `standard` calendar."""
#     # Potential paramaterized variables
#     inclusive_date_range: bool = False  # includes the last day specified
#     use_cftime: bool = True  # Whether to enforece using `cftime` over `datetime64`
#     # align_on: ConvertCalendarAlignOptions = 'date'
#
#     # Create a base
#     base: Dataset = xarray_example(
#         start_date, end_date, as_dataset=True, inclusive=inclusive_date_range
#     )
#     assert False
#
#     # Ensure the generated date range matches for later checks
#     # This occurs for a sigle leap year
#     assert len(base.time) == gen_date_count
#
#     # Convert to `360_day` calendar example
#     dates_360: Dataset = base.convert_calendar(
#         calendar="360_day",
#         align_on=align_on,
#         use_cftime=use_cftime,
#     )
