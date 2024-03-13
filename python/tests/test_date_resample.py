from pathlib import Path
from typing import Callable, Final

import numpy as np
import pytest
import xarray as xr
from clim_recal.resample import (
    MONTH_DAY_XARRAY_LEAP_YEAR_DROP,
    ConvertCalendarAlignOptions,
    convert_xr_calendar,
    xarray_example,
)
from clim_recal.utils import DateType, year_days
from xarray import DataArray, Dataset, cftime_range, open_dataset

HADS_UK_TASMAX_DAY_LOCAL_PATH: Final[Path] = Path("Raw/HadsUKgrid/tasmax/day")
HADS_UK_RESAMPLED_DAY_LOCAL_PATH: Final[Path] = Path(
    "Processed/HadsUKgrid/resampled_2.2km/tasmax/day"
)


@pytest.fixture
def hads_tasmax_day_path(data_mount_path: Path) -> Path:
    return data_mount_path / HADS_UK_TASMAX_DAY_LOCAL_PATH


@pytest.fixture
def hads_tasmax_resampled_day_path(data_mount_path: Path) -> Path:
    return data_mount_path / HADS_UK_TASMAX_DAY_LOCAL_PATH


def test_leap_year_days(xarray_spatial_temporal: Callable) -> None:
    """Test covering a leap year of 366 days."""
    start_date_str: str = "2024-03-01"
    end_date_str: str = "2025-03-01"
    xarray_2024_2025: DataArray = xarray_spatial_temporal(
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        inclusive=True,
    )
    assert len(xarray_2024_2025) == year_days(leaps=1)


# This is roughly what I had in mind for
# https://github.com/alan-turing-institute/clim-recal/issues/132
# This tests converting from a standard calendar to a 360_day calendar.
# It would be good to have an equivalent test would be to convert from a 360_day
# calendar to a standard calendar.
# These should be two separate tests. Trying to generalise the test to cover
# both cases would overcomplicate
# the code and make it harder to understand.
@pytest.mark.parametrize(
    # Only one of start_date and end_date are included the day counts
    "start_date, end_date, gen_date_count, days_360, converted_days, align_on",
    [
        pytest.param(
            # 4 years, including a leap year
            "2024-03-02",
            "2028-03-02",
            year_days(stds=3, leaps=1),
            year_days(cpms=4),
            year_days(stds=3, leaps=1),
            "year",
            id="years_4_annual_align",
        ),
        pytest.param(
            # A whole year, most of which is in a leap year, but avoids the leap day
            "2024-03-02",
            "2025-03-02",
            year_days(stds=1),
            year_days(cpms=1) - 1,
            year_days(stds=1),
            "year",
            id="leap_year_but_no_leap_day_annual_align",
        ),
        pytest.param(
            # A whole year, the same date range as the previous test,
            # but includes the leap day and the majority of the days are in a non-leap year
            # Note: the current final export configuration *adds* a day
            "2023-03-02",
            "2024-03-02",
            year_days(leaps=1),
            year_days(cpms=1) + 1,
            year_days(leaps=1) + 1,
            "year",
            id="leap_year_with_leap_day_annual_align",
        ),
        pytest.param(
            # An exact calendar year which *IS NOT* a leap year
            "2023-01-01",
            "2024-01-01",
            year_days(stds=1),
            year_days(cpms=1),
            year_days(stds=1),
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
            year_days(stds=3, leaps=1),
            year_days(cpms=4) - 7,
            year_days(stds=3, leaps=1),
            "date",
            id="years_4_date_align",
            marks=pytest.mark.xfail(reason="raises `date_range_like` error"),
        ),
        pytest.param(
            # A whole year, most of which is in a leap year, but avoids the leap day
            "2024-03-02",
            "2025-03-02",
            year_days(stds=1),
            year_days(cpms=1) - 2,
            year_days(stds=1),
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
            year_days(leaps=1),
            year_days(cpms=1) - 1,
            year_days(leaps=1) + 1,
            "date",
            id="leap_year_with_leap_day_date_align",
            marks=pytest.mark.xfail(reason="raises `date_range_like` error"),
        ),
        pytest.param(
            # An exact calendar year which *IS NOT* a leap year
            "2023-01-01",
            "2024-01-01",
            year_days(stds=1),
            year_days(cpms=1) - 2,
            year_days(stds=1),
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


# The test below was originally a rework of python/resampling/check_calendar.py
# This will likely be deleted prior to a pull request to the main branch
# @pytest.mark.mount
# def test_raw_vs_resampled_dates(
#     hads_tasmax_day_path: Path, hads_tasmax_resampled_day_path: Path
# ) -> None:
#     """Test dates generated via original (raw) and resambling."""
#     # example files to be compared :
#     # after resampling: tasmax_hadukgrid_uk_1km_day_2.2km_resampled_19800101-19800131.ncr
#     # before resampling: tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc
#
#     # open log file and write both input paths on top:
#     with open("check_calendar_log.txt", "w") as f:
#         f.write(f"{'*'*20} Comparing raw data:  {hads_tasmax_day_path} {'*'*20}\n")
#         f.write(
#             f"{'*'*20} to resampled data: {hads_tasmax_resampled_day_path} {'*'*20}\n"
#         )
#
#     # iterate through dir at path and loop through files
#     files: tuple[Path, ...] = tuple(
#         Path(f)
#         for f in glob.glob(str(hads_tasmax_day_path) + "**/*.nc", recursive=True)
#     )
#
#     all_dates = np.array([], dtype="datetime64[ns]")  # Specify the correct data type
#     for file in files:
#         # separate filename from flag '2.2km_resamples' from date
#         output_name: str = f"{'_'.join(str(file).split('_')[:-1])}_2.2km_resampled_{str(file).split('_')[-1]}"
#
#         # raw_f = os.path.join(hads_tasmax_day_path, file)
#         raw_f: Path = hads_tasmax_day_path / file
#         # preproc_f = os.path.join(hads_tasmax_resampled_day_path, output_name)
#         preproc_f: Path = hads_tasmax_resampled_day_path / output_name
#         # load before and after resampling files
#         try:
#             data_raw = open_dataset(raw_f, decode_coords="all")
#             data_preproc = open_dataset(preproc_f, decode_coords="all")
#         # catch OSError and KeyError
#         except (OSError, KeyError) as e:
#             with open("check_calendar_log.txt", "a") as f:
#                 f.write(f"File: {file} produced errors: {e}\n")
#             continue
#
#         # convert to string
#         time_raw = [str(t).split("T")[0] for t in data_raw.coords["time"].values]
#         time_pre = [str(t).split(" ")[0] for t in data_preproc.coords["time"].values]
#
#         # Use sets to find differences
#         dates_in_raw_not_in_pre = set(time_raw) - set(time_pre)
#         dates_in_pre_not_in_raw = set(time_pre) - set(time_raw)
#
#         # check if dates are empty
#         if dates_in_raw_not_in_pre | dates_in_pre_not_in_raw:
#             # write to log file
#             with open("check_calendar_log.txt", "a") as f:
#                 f.write(
#                     f"raw # days: {len(set(time_raw))} - resampled # days: {len(set(time_pre))}\n"
#                 )
#                 f.write(f"Dates in raw not in resampled: {dates_in_raw_not_in_pre}\n")
#                 f.write(f"Dates in resampled not in raw: {dates_in_pre_not_in_raw}\n")
#
#         # save dates for later overall comparison
#         all_dates = np.concatenate((all_dates, data_preproc.coords["time"].values))
#
#     # generating expected dates
#     start = files[0].split("_")[-1].split("-")[0]
#     stop = files[-1].split("_")[-1].split("-")[1][:-5] + "30"
#     time_index = cftime_range(
#         start, stop, freq="D", calendar="360_day", inclusive="both"
#     )
#
#     # convert to strings
#     x_dates_str = [
#         f"{date.year}-{date.month:02d}-{date.day:02d}" for date in time_index
#     ]
#     y_dates_str = [f"{date.year}-{date.month:02d}-{date.day:02d}" for date in all_dates]
#     # compare if all present
#     not_in_y = [date_x for date_x in x_dates_str if date_x not in y_dates_str]
#     with open("check_calendar_log.txt", "a") as f:
#         f.write(f"______________________________\n")
#         f.write(f"missing dates: {len(not_in_y)}\n")
#         # find duplicates
#         counts = Counter(y_dates_str)
#         for string, count in counts.items():
#             if count > 1:
#                 f.write(f"date '{string}' appears {count} times.\n")
