from datetime import date, datetime
from pathlib import Path
from typing import Any, Final, Literal

import numpy as np
import pytest
from geopandas import GeoDataFrame, read_file
from matplotlib import pyplot as plt
from numpy.typing import NDArray
from osgeo.gdal import Dataset as GDALDataset
from xarray import DataArray, Dataset, open_dataset

from clim_recal.resample import (
    BRITISH_NATIONAL_GRID_EPSG,
    CPRUK_XDIM,
    CPRUK_YDIM,
    DEFAULT_RELATIVE_GRID_DATA_PATH,
    HADS_XDIM,
    HADS_YDIM,
    RAW_CPM_TASMAX_PATH,
    RAW_HADS_PATH,
    RAW_HADS_TASMAX_PATH,
    ConvertCalendarAlignOptions,
    CPMResampler,
    CPMResamplerManager,
    HADsResampler,
    HADsResamplerManager,
    cpm_reproject_with_standard_calendar,
    interpolate_coords,
)
from clim_recal.utils.core import (
    CLI_DATE_FORMAT_STR,
    CPM_YEAR_DAYS,
    LEAP_YEAR_DAYS,
    NORMAL_YEAR_DAYS,
    DateType,
    annual_data_path,
    annual_data_paths_generator,
    date_range_generator,
    results_path,
)
from clim_recal.utils.data import CEDADataSources, HadUKGrid, UKCPLocalProjections
from clim_recal.utils.gdal_formats import (
    GDALFormatExtensions,
    GDALFormatsType,
    GDALGeoTiffFormatStr,
    GDALNetCDFFormatStr,
)
from clim_recal.utils.xarray import (
    CPM_365_OR_366_27700_FINAL,
    CPM_LOCAL_INTERMEDIATE_PATH,
    NETCDF4_XARRAY_ENGINE,
    BoundsTupleType,
    convert_xr_calendar,
    cpm_xarray_to_standard_calendar,
    crop_nc,
    file_name_to_start_end_dates,
    gdal_warp_wrapper,
    xarray_example,
)

HADS_UK_TASMAX_DAY_SERVER_PATH: Final[Path] = Path("Raw/HadsUKgrid/tasmax/day")
HADS_UK_RESAMPLED_DAY_SERVER_PATH: Final[Path] = Path(
    "Processed/HadsUKgrid/resampled_2.2km/tasmax/day"
)

UKCP_RAW_TASMAX_1980_FILE: Final[Path] = Path(
    "tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc"
)
HADS_RAW_TASMAX_1980_FILE: Final[Path] = Path(
    "tasmax_hadukgrid_uk_1km_day_19800101-19800131.nc"
)

HADS_UK_TASMAX_LOCAL_TEST_PATH: Final[Path] = (
    Path(HadUKGrid.slug) / HADS_RAW_TASMAX_1980_FILE
)

UKCP_TASMAX_DAY_SERVER_PATH: Final[Path] = Path("Raw/UKCP2.2/tasmax/01/latest")
# Todo: Change "tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc"
# to "tasmax_cpm_example.nc"
UKCP_TASMAX_LOCAL_TEST_PATH: Final[Path] = (
    Path(UKCPLocalProjections.slug) / UKCP_RAW_TASMAX_1980_FILE
)

UKCP_RAW_TASMAX_EXAMPLE_PATH: Final[Path] = (
    RAW_CPM_TASMAX_PATH / UKCP_RAW_TASMAX_1980_FILE
)

HADS_RAW_TASMAX_EXAMPLE_PATH: Final[Path] = (
    RAW_HADS_TASMAX_PATH / HADS_RAW_TASMAX_1980_FILE
)

HADS_FIRST_DATES: np.array = np.array(
    ["19800101", "19800102", "19800103", "19800104", "19800105"]
)
FINAL_CONVERTED_CPM_WIDTH: Final[int] = 484
FINAL_CONVERTED_CPM_HEIGHT: Final[int] = 606

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


@pytest.mark.mount
@pytest.fixture(scope="session")
def tasmax_cpm_1980_raw() -> Dataset:
    return open_dataset(UKCP_RAW_TASMAX_EXAMPLE_PATH, decode_coords="all")


@pytest.mark.mount
@pytest.fixture(scope="session")
def tasmax_hads_1980_raw() -> Dataset:
    return open_dataset(HADS_RAW_TASMAX_EXAMPLE_PATH, decode_coords="all")


@pytest.fixture(scope="session")
def reference_final_coord_grid() -> Dataset:
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
    raw_nc: Dataset = open_dataset(
        UKCP_RAW_TASMAX_EXAMPLE_PATH, decode_coords="all", engine=NETCDF4_XARRAY_ENGINE
    )
    assert len(raw_nc.time) == 360
    assert len(raw_nc.time_bnds) == 360
    converted: Dataset = convert_xr_calendar(raw_nc, interpolate_na=interpolate_na)
    assert len(converted.time) == 365
    assert len(converted.time_bnds) == 365
    assert (
        np.isnan(converted.tasmax.head()[0][0][0].values).all()
        == any_na_values_in_tasmax
    )


@pytest.mark.slow
@pytest.mark.xfail
@pytest.mark.parametrize("data_type", (UKCPLocalProjections, HadUKGrid))
@pytest.mark.parametrize("data_source", ("mounted", "local"))
@pytest.mark.parametrize("output_format", (GDALNetCDFFormatStr, GDALGeoTiffFormatStr))
@pytest.mark.parametrize("glasgow_crop", (True, False))
def test_geo_warp_format_type_crop(
    data_type: CEDADataSources,
    data_source: Literal["mounted", "local"],
    output_format: GDALFormatsType,
    glasgow_crop: bool,
    test_runs_output_path: Path,
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
    warp_path: Path = test_runs_output_path / "geo_warp"
    if glasgow_crop:
        glasgow_test_fig_path = results_path(
            name="glasgow",
            path=warp_path,
            time=datetime_now,
            extension="png",
            mkdir=True,
        )
    pre_warp_test_fig_path = results_path(
        name="pre_warp", path=warp_path, time=datetime_now, extension="png", mkdir=True
    )
    warp_test_file_path = results_path(
        name="test_warp_file",
        path=warp_path,
        time=datetime_now,
        extension=GDALFormatExtensions[output_format],
        mkdir=False,
    )
    warp_test_fig_path = results_path(
        name="test_warp", path=warp_path, time=datetime_now, extension="png", mkdir=True
    )

    if glasgow_crop:
        glasgow_geo_df: GeoDataFrame = read_file(glasgow_shape_file_path)
        glasgow_geo_df.plot()
        plt.savefig(glasgow_test_fig_path)
        assert glasgow_geo_df.crs == BRITISH_NATIONAL_GRID_EPSG
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

    assert str(xarray_pre_warp.rio.crs) != BRITISH_NATIONAL_GRID_EPSG
    assert xarray_pre_warp.rio.bounds() == uk_epsg_27700_bounds
    # output_path: Path = warp_path / (max_temp_data_path.stem + ".tif" if output_format == GDALGeoTiffFormatStr else ".nc")
    xarray_warped: GDALDataset
    if glasgow_crop:
        xarray_warped = gdal_warp_wrapper(
            input_path=max_temp_data_path,
            output_path=warp_test_file_path,
            format=output_format,
        )
    else:
        xarray_warped = gdal_warp_wrapper(
            input_path=max_temp_data_path,
            output_path=warp_test_file_path,
            format=output_format,
            output_bounds=uk_epsg_27700_bounds,
        )
    assert xarray_warped is not None
    read_exported: Dataset = open_dataset(warp_test_file_path, decode_coords="all")

    read_exported.Band1.plot()
    plt.savefig(warp_test_fig_path)

    assert str(read_exported.rio.crs) == BRITISH_NATIONAL_GRID_EPSG
    if glasgow_crop:
        assert read_exported.rio.bounds() == glasgow_epsg_27700_bounds


@pytest.mark.mount
@pytest.mark.slow
@pytest.mark.parametrize("include_bnds_index", (True, False))
def test_cpm_xarray_to_standard_calendar(
    tasmax_cpm_1980_raw: Dataset,
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
    assert test_converted.rio.width == FINAL_CONVERTED_CPM_WIDTH
    assert test_converted.rio.height == FINAL_CONVERTED_CPM_HEIGHT
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


@pytest.mark.mount
@pytest.mark.slow
def test_cpm_warp_steps(
    tasmax_cpm_1980_raw: Dataset,
    test_runs_output_path: Path,
) -> None:
    """Test all steps around calendar and warping CPM RAW data."""
    file_name_prefix: str = "test-1980-"
    output_path: Path = test_runs_output_path / "test-cpm-warp"
    projected = cpm_reproject_with_standard_calendar(
        tasmax_cpm_1980_raw,
        variable_name="tasmax",
        output_path=output_path,
        file_name_prefix=file_name_prefix,
    )
    assert projected.rio.crs == BRITISH_NATIONAL_GRID_EPSG
    # Previous checks, worth re-wroking/expanding
    # test_projected = open_dataset(intermediate_warp_path)
    # assert test_projected.rio.crs == BRITISH_NATIONAL_GRID_EPSG
    # assert len(test_projected.time) == len(expanded_calendar.time)
    # assert len(test_projected.x) == len(tasmax_cpm_1980_raw.grid_longitude)
    # assert len(test_projected.y) == len(tasmax_cpm_1980_raw.grid_latitude)
    # test_projected.to_netcdf(final_nc_path)
    final_nc_path: Path = Path(
        "3-" + file_name_prefix + "tasmax-" + CPM_365_OR_366_27700_FINAL
    )
    intermediate_dir = tuple(output_path.iterdir())[0]
    assert intermediate_dir.name.startswith(CPM_LOCAL_INTERMEDIATE_PATH.name)
    final_results = open_dataset(intermediate_dir / final_nc_path, decode_coords="all")
    assert (final_results.time == projected.time).all()


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


@pytest.mark.xfail(reason="not finished writing, will need refactor")
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
    assert glasgow_geo_df.crs == BRITISH_NATIONAL_GRID_EPSG
    assert tuple(glasgow_geo_df.bounds.values.tolist()[0]) == glasgow_epsg_27700_bounds

    max_temp_1981_path: Path = annual_data_path(
        end_year=1981,
        parent_path=data_fixtures_path,
    )
    xarray_pre_crop: Dataset = open_dataset(max_temp_1981_path, decode_coords="all")
    xarray_pre_crop.isel(time=0).tasmax.plot()
    plt.savefig(pre_crop_test_fig_path)

    assert str(xarray_pre_crop.rio.crs) != BRITISH_NATIONAL_GRID_EPSG
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

    assert str(cropped.rio.crs) == BRITISH_NATIONAL_GRID_EPSG
    assert cropped.rio.bounds() == glasgow_epsg_27700_bounds
    assert False


@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize(
    "config", ("direct", "range", "direct_provided", "range_provided")
)
def test_ukcp_manager(resample_test_cpm_output_path, config: str) -> None:
    """Test running default CPM calendar fix."""
    CPM_FIRST_DATES: np.array = np.array(
        ["19801201", "19801202", "19801203", "19801204", "19801205"]
    )
    output_path: Path = resample_test_cpm_output_path / config
    test_config = CPMResampler(
        input_path=RAW_CPM_TASMAX_PATH,
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
                test_config.to_reprojection(index=1, source_to_index=tuple(test_config))
            ]
        case "range_provided":
            paths = test_config.range_to_reprojection(
                stop=1, source_to_index=tuple(test_config)
            )
    export: Dataset = open_dataset(paths[0])
    assert export.dims["time"] == 365
    assert export.dims["x"] == 492
    assert export.dims["y"] == 608  # previously 603
    assert not np.isnan(export.tasmax.head()[0].values).all()
    # Todo: reapply these checks to intermediary files
    # assert export.dims[CPRUK_XDIM] == 484
    # assert export.dims[CPRUK_YDIM] == 606
    # assert not np.isnan(export.tasmax.head()[0][0][0].values).all()
    assert (
        CPM_FIRST_DATES == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()
    # assert (
    #     CPM_FIRST_DATES
    #     == export.time_bnds.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    # ).all()
    # assert (CPM_FIRST_DATES == export.yyyymmdd.head().values).all()


# @pytest.mark.xfail(reason="checking `export.tasmax` values currently yields `nan`")
@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("range", (False, True))
def test_hads_manager(resample_test_hads_output_path, range: bool) -> None:
    """Test running default HADs spatial projection."""
    test_config = HADsResampler(
        input_path=RAW_HADS_TASMAX_PATH,
        output_path=resample_test_hads_output_path,
    )
    paths: list[Path]
    if range:
        paths = test_config.range_to_reprojection(stop=1)
    else:
        paths = [test_config.to_reprojection()]
    export: Dataset = open_dataset(paths[0])
    assert len(export.time) == 31
    assert not np.isnan(export.tasmax[0][200:300].values).all()
    assert (
        HADS_FIRST_DATES.astype(object)
        == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()


@pytest.mark.mount
@pytest.mark.slow
@pytest.mark.parametrize("data_type", ("hads", "cpm"))
def test_interpolate_coords(
    data_type: str,
    reference_final_coord_grid: Dataset,
    tasmax_cpm_1980_raw: Dataset,
    tasmax_hads_1980_raw: Dataset,
) -> None:
    """Test reprojecting raw spatial files.

    Notes
    -----
    Still seems to run even when `-m "not mount"` is specified.
    """
    reprojected_xr_time_series: Dataset
    kwargs: dict[str, Any] = dict(
        variable_name="tasmax",
        x_grid=reference_final_coord_grid.projection_x_coordinate,
        y_grid=reference_final_coord_grid.projection_y_coordinate,
    )
    if data_type == "hads":
        reprojected_xr_time_series = interpolate_coords(
            tasmax_hads_1980_raw,
            xr_time_series_x_column_name=HADS_XDIM,
            xr_time_series_y_column_name=HADS_YDIM,
            **kwargs,
        )
        assert reprojected_xr_time_series.dims["time"] == 31
        # assert reprojected_xr_time_series.dims[HADS_XDIM] == 528
        # assert reprojected_xr_time_series.dims[HADS_YDIM] == 651
    else:
        # We are now using gdal_warp_wrapper. See test_cpm_warp_steps
        reprojected_xr_time_series = interpolate_coords(
            tasmax_cpm_1980_raw,
            xr_time_series_x_column_name=CPRUK_XDIM,
            xr_time_series_y_column_name=CPRUK_YDIM,
            **kwargs,
        )
        # Note: this test is to a raw file without 365 day projection
        assert reprojected_xr_time_series.dims["time"] == 360
    assert reprojected_xr_time_series.dims[HADS_XDIM] == 528
    assert reprojected_xr_time_series.dims[HADS_YDIM] == 651


@pytest.mark.mount
@pytest.mark.parametrize("strict_fail_bool", (True, False))
@pytest.mark.parametrize("manager", (HADsResamplerManager, CPMResamplerManager))
def test_variable_in_base_import_path_error(
    strict_fail_bool: bool, manager: HADsResamplerManager | CPMResamplerManager
) -> None:
    """Test checking import path validity for a given variable."""
    with pytest.raises(manager.VarirableInBaseImportPathError):
        HADsResamplerManager(
            input_paths=RAW_HADS_TASMAX_PATH,
            stop_index=1,
        )
    if strict_fail_bool:
        with pytest.raises(FileExistsError):
            HADsResamplerManager(
                input_paths=RAW_HADS_TASMAX_PATH,
                stop_index=1,
                _strict_fail_if_var_in_input_path=False,
            )


@pytest.mark.slow
@pytest.mark.mount
@pytest.mark.parametrize("multiprocess", (False, True))
def test_execute_resample_configs(multiprocess: bool, tmp_path) -> None:
    """Test running default HADs spatial projection."""
    test_config = HADsResamplerManager(
        input_paths=RAW_HADS_PATH,
        output_paths=tmp_path,
        stop_index=1,
    )
    resamplers: tuple[
        HADsResampler | CPMResampler, ...
    ] = test_config.execute_resample_configs(multiprocess=multiprocess)
    export: Dataset = open_dataset(resamplers[0][0])
    assert len(export.time) == 31
    assert not np.isnan(export.tasmax[0][200:300].values).all()
    assert (
        HADS_FIRST_DATES.astype(object)
        == export.time.dt.strftime(CLI_DATE_FORMAT_STR).head().values
    ).all()


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
