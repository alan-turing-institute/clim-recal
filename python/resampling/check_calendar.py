from datetime import datetime
import os
import xarray as xr
import glob
import numpy as np

path_raw = '/Volumes/vmfileshare/ClimateData/Raw/HadsUKgrid/tasmax/day'
path_preproc = '/Volumes/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/tasmax/day'
#example file names:
#tasmax_hadukgrid_uk_1km_day_2.2km_resampled_19800101-19800131.ncr
#tasmax_hadukgrid_uk_1km_day_20211201-20211231.nc

# open log file and write both input paths on top:
with open('check_calendar_log.txt', 'w') as f:
    f.write(f"{'*'*20} Comparing raw data:  {path_raw} {'*'*20}\n")
    f.write(f"{'*'*20} to resampled data: {path_preproc} {'*'*20}\n")

#iterate through dir at path and loop through files
files = [os.path.basename(f) for f in glob.glob(path_raw + "**/*.nc", recursive=True)]

for i,file in enumerate(files):
    if i%10==0:
        print(i)
    #separate filename from flag '2.2km_resamples' from date
    output_name = f"{'_'.join(file.split('_')[:-1])}_2.2km_resampled_{file.split('_')[-1]}"

    raw_f = os.path.join(path_raw, file)
    preproc_f = os.path.join(path_preproc, output_name)
    #load before and after resampling files
    data_raw = xr.open_dataset(raw_f, decode_coords="all")
    data_preproc = xr.open_dataset(preproc_f, decode_coords="all")
    time_raw = [str(t).split('T')[0] for t in data_raw.coords['time'].values]
    time_pre = [str(t).split(' ')[0] for t in data_preproc.coords['time'].values]

    # Use sets to find differences
    dates_in_raw_not_in_pre = set(time_raw) - set(time_pre)
    dates_in_pre_not_in_raw = set(time_pre) - set(time_raw)

    # check if dates are empty
    if dates_in_raw_not_in_pre | dates_in_pre_not_in_raw:
        #if date in raw not in pre ends in 31
        if list(dates_in_raw_not_in_pre)[0][-2:]!='31':
            # write to log file
            with open(os.path.join(path_preproc,'check_calendar_log.txt'), 'a') as f:
                f.write(f"File: {file} produced errors:\n")
                f.write(f"raw # days: {len(set(time_raw))} - resampled # days: {len(set(time_pre))}\n")
                f.write(f"Dates in raw not in resampled: {dates_in_raw_not_in_pre}\n")
                f.write(f"Dates in resampled not in raw: {dates_in_pre_not_in_raw}\n")










