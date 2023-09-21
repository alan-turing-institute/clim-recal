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
parser.add_argument('--mod', '--modelled', dest='mod_fpath', type=str,
                    help='Path to modelled (CPM) datasets')
parser.add_argument('--obs', '--observed', dest='obs_fpath', type=str,
                    help='Path to observation (HADs) datasets')
parser.add_argument('--calib_dates', '--calibration_date_range', dest='calibration_date_range', type=str,
                    help='Start and end dates for calibration (historic CPM/HADs data used to '
                         'calibrate the debiasing model) - in YYYYMMDD-YYYYMMDD format',
                    default='19801201-19991130')
parser.add_argument('--valid_dates', '--validation_date_range', dest='validation_date_range', type=str,
                    help='Start and end dates for validation data (CPM data to be debiased using the '
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
mod_fpath = params['mod_fpath']
calibration_date_range = params['calibration_date_range']
validation_date_range = params['validation_date_range']
shape_fpath = params['shapefile_fpath']
out_fpath = params['output_fpath']
var = params['var']
unit = params['unit']
run_number = params['run_number']

calib_list = calibration_date_range.split('-')
h_date_period = (datetime.strptime(calib_list[0], '%Y%m%d').strftime('%Y-%m-%d'),
                 datetime.strptime(calib_list[1], '%Y%m%d').strftime('%Y-%m-%d'))
val_list = validation_date_range.split('_')
future_time_periods = [(p.split('-')[0], p.split('-')[1]) for p in val_list]
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

    # load modelled data (CPM) for calibration period and place into ds_modc
    if run_number is not None:
        ds_modc = \
            load_data(mod_fpath, date_range=h_date_period, variable=var, filter_filenames_on_variable=True,
                      run_number=run_number, filter_filenames_on_run_number=True, use_pr=use_pr,
                      shapefile_path=shape_fpath,
                      extension='tif')[var].rename({"projection_x_coordinate": "lon",
                                                    "projection_y_coordinate": "lat"})
    else:
        ds_modc = \
            load_data(mod_fpath, date_range=h_date_period, variable=var, filter_filenames_on_variable=True,
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

    # load observation data (HADs) for calibration period and place into ds_obsc
    ds_obsc = load_data(obs_fpath, date_range=h_date_period, variable=var, filter_filenames_on_variable=True,
                        shapefile_path=shape_fpath, extension=ext)[var].rename({"projection_x_coordinate": "lon",
                                                                                "projection_y_coordinate": "lat"})
    log.info('Calibration data (modelled and observed) loaded.')

    # aligning calendars, there might be extra days in the modelled data that need to be dropped
    ds_modc = ds_modc.sel(time=ds_obsc.time, method='nearest')

    if ds_obsc.shape != ds_modc.shape:
        raise RuntimeError('Error, observed and modelled calibration data must have same dimensions.')

    log.info('Resulting datasets with shape')
    log.info(ds_obsc.shape)

    # masking coordinates where the observed data has no values
    ds_modc = ds_modc.where(~np.isnan(ds_obsc.isel(time=0)))
    ds_modc = ds_modc.where(ds_modc.values < 1000)
    log.info('Calibration data masked')

    ds_obsc.attrs['unit'] = unit
    ds_modc.attrs['unit'] = unit

    # write modc to .nc file in output directory
    modc_filename = f'modc_var-{var}_run-{run_number}_{calib_list[0]}_{calib_list[1]}'
    modc_path = os.path.join(out_fpath, f'{modc_filename}.nc')
    if not os.path.exists(os.path.dirname(modc_path)):
        folder_path = Path(os.path.dirname(modc_path))
        folder_path.mkdir(parents=True)
    print(f"Saving modelled (CPM) data for calibration to {modc_path}")
    ds_modc.to_netcdf(modc_path)
    log.info(f'Saved modelled (CPM) data for calibration to {modc_path}')

    # write ds_obsc to .nc file in output directory
    obsc_filename = f'obsc_var-{var}_run-{run_number}_{calib_list[0]}_{calib_list[1]}'
    obsc_path = os.path.join(out_fpath, f'{obsc_filename}.nc')
    if not os.path.exists(os.path.dirname(obsc_path)):
        folder_path = Path(os.path.dirname(obsc_path))
        folder_path.mkdir(parents=True)
    print(f"Saving observation data (HADs) for calibration to {obsc_path}")
    ds_obsc.to_netcdf(obsc_path)
    log.info(f'Saved observation data (HADs) for calibration period to {obsc_path}')

    # looping over validation time periods
    for f_date_period in future_time_periods:

        log.info(f'Running for {f_date_period} time period')
        # load modelled (CPM) data for validation period and store in ds_modv
        try:
            use_pr = False
            if var == "rainfall":
                use_pr = True
            # load
            if run_number is not None:
                ds_modv = \
                    load_data(mod_fpath, date_range=f_date_period, variable=var, run_number=run_number,
                              filter_filenames_on_run_number=True, use_pr=use_pr, shapefile_path=shape_fpath,
                              extension='tif')[
                        var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
            else:
                ds_modv = \
                    load_data(mod_fpath, date_range=f_date_period, variable=var,
                              use_pr=use_pr, shapefile_path=shape_fpath, extension='tif')[
                        var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
        except Exception as e:
            log.info(f'No data available for {f_date_period} time period')
            continue

        # masking coordinates where the observed data has no values
        ds_modv = ds_modv.where(~np.isnan(ds_obsc.isel(time=0)))
        ds_modv = ds_modv.where(ds_modv.values < 1000)

        ds_modv.attrs['unit'] = unit

        # write ds_modv to .nc file in output directory
        ds_modv_filename = f'modv_var-{var}_run-{run_number}_{f_date_period[0]}_{f_date_period[1]}'
        ds_modv_path = os.path.join(out_fpath, f'{ds_modv_filename}.nc')
        if not os.path.exists(os.path.dirname(ds_modv_path)):
            folder_path = Path(os.path.dirname(ds_modv_path))
            folder_path.mkdir(parents=True)
        print(f"Saving modelled (CPM) data for validation to {ds_modv_path}")
        ds_modv.to_netcdf(ds_modv_path)
        log.info(f'Saved modelled (CPM) data for validation, period {f_date_period} to {ds_modv_path}')

    end = time.time()
    log.info(f'total time in seconds: {end - start}')
    log.info('Done')


if __name__ == '__main__':
    preprocess_data()

# * ----- ----- E O F ----- -----
