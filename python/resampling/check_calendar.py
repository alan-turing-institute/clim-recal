import os
import xarray as xr
import glob
import numpy as np
from collections import Counter

path_ukcp = '/Volumes/vmfileshare/ClimateData/Raw/UKCP2.2/tasmax/01/latest/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc'
data_raw = xr.open_dataset(path_ukcp, decode_coords="all")

path_raw = '/Volumes/vmfileshare/ClimateData/Raw/HadsUKgrid/tasmax/day'
path_preproc = '/Volumes/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/tasmax/day'
#example files to be compared :
# after resampling: tasmax_hadukgrid_uk_1km_day_2.2km_resampled_19800101-19800131.ncr
# before resampling: tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc

# open log file and write both input paths on top:
with open('check_calendar_log.txt', 'w') as f:
    f.write(f"{'*'*20} Comparing raw data:  {path_raw} {'*'*20}\n")
    f.write(f"{'*'*20} to resampled data: {path_preproc} {'*'*20}\n")

#iterate through dir at path and loop through files
files = [os.path.basename(f) for f in glob.glob(path_raw + "**/*.nc", recursive=True)]

all_dates = np.array([], dtype='datetime64[ns]')  # Specify the correct data type
for i,file in enumerate(files):
    #separate filename from flag '2.2km_resamples' from date
    output_name = f"{'_'.join(file.split('_')[:-1])}_2.2km_resampled_{file.split('_')[-1]}"

    raw_f = os.path.join(path_raw, file)
    preproc_f = os.path.join(path_preproc, output_name)
    #load before and after resampling files
    data_raw = xr.open_dataset(raw_f, decode_coords="all")
    data_preproc = xr.open_dataset(preproc_f, decode_coords="all")

    #convert to string
    time_raw = [str(t).split('T')[0] for t in data_raw.coords['time'].values]
    time_pre = [str(t).split(' ')[0] for t in data_preproc.coords['time'].values]

    # Use sets to find differences
    dates_in_raw_not_in_pre = set(time_raw) - set(time_pre)
    dates_in_pre_not_in_raw = set(time_pre) - set(time_raw)

    # check if dates are empty
    if dates_in_raw_not_in_pre | dates_in_pre_not_in_raw:
        # write to log file
        with open('check_calendar_log.txt', 'a') as f:
            f.write(f"File: {file} produced errors:\n")
            f.write(f"raw # days: {len(set(time_raw))} - resampled # days: {len(set(time_pre))}\n")
            f.write(f"Dates in raw not in resampled: {dates_in_raw_not_in_pre}\n")
            f.write(f"Dates in resampled not in raw: {dates_in_pre_not_in_raw}\n")

    # save dates for later overall comparison
    all_dates = np.concatenate((all_dates, data_preproc.coords['time'].values))

# generating expected dates
start = files[0].split('_')[-1].split('-')[0]
stop = files[-1].split('_')[-1].split('-')[1][:-5]+'30'
time_index = xr.cftime_range(start, stop, freq='D', calendar='360_day', inclusive='both')

# convert to strings
x_dates_str = [f"{date.year}-{date.month:02d}-{date.day:02d}" for date in time_index]
y_dates_str = [f"{date.year}-{date.month:02d}-{date.day:02d}" for date in all_dates]
# compare if all present
not_in_y = [date_x for date_x in x_dates_str if date_x not in y_dates_str]
with open('check_calendar_log.txt', 'a') as f:
            f.write(f'______________________________\n')
            f.write(f'missing dates: {len(not_in_y)}\n')
            # find duplicates
            counts = Counter(y_dates_str)
            for string, count in counts.items():
                if count > 1:
                    f.write(f"date '{string}' appears {count} times.\n")









