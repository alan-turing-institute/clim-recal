#!/bin/python3

# Script to pre-process control, scenario and observation data (including combining files to cover a range of dates),
# before running debiasing methods.

import argparse
import glob
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime
from pathlib import Path

sys.path.insert(1, '../load_data')
from data_loader import load_data

# * ----- L O G G I N G -----
formatter = logging.Formatter(
    fmt='%(asctime)s %(module)s,line: %(lineno)d %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

log = logging.getLogger()
log.setLevel(logging.INFO)
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
logging.getLogger().addHandler(screen_handler)

# * ----- I N P U T - H A N D L I N G -----
parser = argparse.ArgumentParser(description='Pre-process data before bias correction.')
parser.add_argument('--obs', '--observation', dest='obs_fpath', type=str, help='Path to observation datasets')
parser.add_argument('--contr', '--control', dest='contr_fpath', type=str, help='Path to control datasets')
parser.add_argument('--scen', '--scenario', dest='scen_fpath', type=str,
                    help='Path to scenario datasets (data to adjust)')
parser.add_argument('--contr_dates', '--control_date_range', dest='control_date_range', type=str,
                    help='Start and end dates for control and observation data (historic CPM/HADs data used to '
                         'calibrate the debiasing model) - in YYYYMMDD-YYYYMMDD format',
                    default='19801201-19991130')
parser.add_argument('--scen_dates', '--scenario_date_range', dest='scenario_date_range', type=str,
                    help='Start and end dates for scenario data (CPM data to be debiased using the '
                         'calibrated debiasing model) - multiple date ranges can be passed, '
                         'separated by "_", each in YYYYMMDD-YYYYMMDD format e.g., '
                         '"20100101-20191231_20200101-20291231"',
                    default='20201201-20291130_20301201-20391130')
parser.add_argument('--shp', '--shapefile', dest='shapefile_fpath', type=str, help='Path to shapefile', default=None)
parser.add_argument('--out', '--output', dest='output_fpath', type=str, help='Path to save output files', default='.')
parser.add_argument('-v', '--variable', dest='var', type=str, default='tasmax', help='Variable to adjust')
parser.add_argument('-u', '--unit', dest='unit', type=str, default='°C', help='Unit of the varible')
parser.add_argument('-r', '--run_number', dest='run_number', type=str, default=None,
                    help='Run number to process (out of 13 runs in the CPM data)')

params = vars(parser.parse_args())

obs_fpath = params['obs_fpath']
contr_fpath = params['contr_fpath']
scen_fpath = params['scen_fpath']
calibration_date_range = params['control_date_range']
projection_date_range = params['scenario_date_range']
shape_fpath = params['shapefile_fpath']
out_fpath = params['output_fpath']
var = params['var']
unit = params['unit']
run_number = params['run_number']

calib_list = calibration_date_range.split('-')
h_date_period = (datetime.strptime(calib_list[0], '%Y%m%d').strftime('%Y-%m-%d'),
                 datetime.strptime(calib_list[1], '%Y%m%d').strftime('%Y-%m-%d'))
proj_list = projection_date_range.split('_')
future_time_periods = [(p.split('-')[0], p.split('-')[1]) for p in proj_list]
future_time_periods = [(datetime.strptime(p[0], '%Y%m%d').strftime('%Y-%m-%d'),
                        datetime.strptime(p[1], '%Y%m%d').strftime('%Y-%m-%d'))
                       for p in future_time_periods]


# * ----- ----- -----M A I N ----- ----- -----
def preprocess_data() -> None:
    start = time.time()

    # load every file found with extension in the path and selects only the input time period.from
    # coordinates are renamed for compatibility with the cmethods-library
    use_pr = False
    if var == "rainfall":
        use_pr = True
    if run_number is not None:
        ds_simh = \
            load_data(contr_fpath, date_range=h_date_period, variable=var, filter_filenames_on_variable=True,
                      run_number=run_number, filter_filenames_on_run_number=True, use_pr=use_pr, shapefile_path=shape_fpath,
                      extension='tif')[var].rename({"projection_x_coordinate": "lon",
                                                    "projection_y_coordinate": "lat"})
    else:
        ds_simh = \
            load_data(contr_fpath, date_range=h_date_period, variable=var, filter_filenames_on_variable=True,
                      use_pr=use_pr, shapefile_path=shape_fpath,
                      extension='tif')[var].rename({"projection_x_coordinate": "lon",
                                                    "projection_y_coordinate": "lat"})

    # find file extensions for observation data
    files_obs_nc = glob.glob(f"{obs_fpath}/*.nc", recursive=True)
    files_obs_tif = glob.glob(f"{obs_fpath}/*.tif", recursive=True)

    if len(files_obs_nc) > 0 and len(files_obs_tif) == 0:
        ext = 'nc'
    elif len(files_obs_nc) == 0 and len(files_obs_tif) > 0:
        ext = 'tif'
    elif len(files_obs_nc) == 0 and len(files_obs_tif) == 0:
        raise Exception(f"No observation files found in {obs_fpath} with extensions .nc or .tif")
    else:
        raise Exception(f"A mix of .nc and .tif observation files found in {obs_fpath}, file extension should be the "
                        f"same for all files in the directory.")
    ds_obs = load_data(obs_fpath, date_range=h_date_period, variable=var, filter_filenames_on_variable=True,
                       shapefile_path=shape_fpath, extension=ext)[var].rename({"projection_x_coordinate": "lon",
                                                                               "projection_y_coordinate": "lat"})
    log.info('Historical data Loaded.')

    # aligning calendars, e.g there might be a few extra days on the scenario data that has to be dropped.
    ds_simh = ds_simh.sel(time=ds_obs.time, method='nearest')

    if ds_obs.shape != ds_simh.shape:
        raise RuntimeError('Error, observed and simulated historical data must have same dimensions.')

    log.info('Resulting datasets with shape')
    log.info(ds_obs.shape)

    # masking coordinates where the observed data has no values
    ds_simh = ds_simh.where(~np.isnan(ds_obs.isel(time=0)))
    ds_simh = ds_simh.where(ds_simh.values < 1000)
    log.info('Historical data Masked')

    ds_obs.attrs['unit'] = unit
    ds_simh.attrs['unit'] = unit

    # write simh to .nc file in output directory
    simh_filename = f'simh_var-{var}_run-{run_number}_{calib_list[0]}_{calib_list[1]}'
    simh_path = os.path.join(out_fpath, f'{simh_filename}.nc')
    if not os.path.exists(os.path.dirname(simh_path)):
        folder_path = Path(os.path.dirname(simh_path))
        folder_path.mkdir(parents=True)
    print(f"Saving historical control data to {simh_path}")
    ds_simh.to_netcdf(simh_path)
    log.info(f'Saved CPM data for calibration (historic) period to {simh_path}')

    # write ds_obs to .nc file in output directory
    obsh_filename = f'obsh_var-{var}_run-{run_number}_{calib_list[0]}_{calib_list[1]}'
    obsh_path = os.path.join(out_fpath, f'{obsh_filename}.nc')
    if not os.path.exists(os.path.dirname(obsh_path)):
        folder_path = Path(os.path.dirname(obsh_path))
        folder_path.mkdir(parents=True)
    print(f"Saving historical observation data to {obsh_path}")
    ds_obs.to_netcdf(obsh_path)
    log.info(f'Saved HADs data for calibration (historic) period to {obsh_path}')

    # looping over time periods
    # this is done because the full time period for the scenario dataset is too large for memory.
    for f_date_period in future_time_periods:

        log.info(f'Running for {f_date_period} time period')

        try:
            use_pr = False
            if var == "rainfall":
                use_pr = True
            if run_number is not None:
                ds_simp = \
                    load_data(scen_fpath, date_range=f_date_period, variable=var, run_number=run_number,
                              filter_filenames_on_run_number=True, use_pr=use_pr, shapefile_path=shape_fpath,
                              extension='tif')[
                        var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
            else:
                ds_simp = \
                    load_data(scen_fpath, date_range=f_date_period, variable=var,
                              use_pr=use_pr, shapefile_path=shape_fpath, extension='tif')[
                        var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
        except Exception as e:
            log.info(f'No data available for {f_date_period} time period')
            continue

        # masking coordinates where the observed data has no values
        ds_simp = ds_simp.where(~np.isnan(ds_obs.isel(time=0)))
        ds_simp = ds_simp.where(ds_simp.values < 1000)

        ds_simp.attrs['unit'] = unit

        # write ds_simp to .nc file in output directory
        simp_filename = f'simp_var-{var}_run-{run_number}_{f_date_period[0]}_{f_date_period[1]}'
        simp_path = os.path.join(out_fpath, f'{simp_filename}.nc')
        if not os.path.exists(os.path.dirname(simp_path)):
            folder_path = Path(os.path.dirname(simp_path))
            folder_path.mkdir(parents=True)
        print(f"Saving future scenario data to {simp_path}")
        ds_simp.to_netcdf(simp_path)
        log.info(f'Saved CPM data for projection (future) period {f_date_period} to {simp_path}')

    end = time.time()
    log.info(f'total time in seconds: {end - start}')
    log.info('Done')


if __name__ == '__main__':
    preprocess_data()

# * ----- ----- E O F ----- -----
