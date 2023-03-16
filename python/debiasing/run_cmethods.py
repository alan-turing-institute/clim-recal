#!/bin/python3

# Script to adjust climate biases in climate data using the python-cmethods library This script is inspired in the
# script by Benjamin Thomas Schwertfeger (https://github.com/btschwertfeger/python-cmethods/blob/master/examples/do_bias_correction.py)
# and adapted to function with UKCP and HADs data.


import argparse
import logging
import sys
import time
import numpy as np
import matplotlib.pyplot as plt
import os

sys.path.insert(1, 'python-cmethods')
from cmethods.CMethods import CMethods

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
parser = argparse.ArgumentParser(description='Adjust climate data based on bias correction algorithms.')
parser.add_argument('--obs', '--observation', dest='obs_fpath', type=str, help='Path to observation datasets')
parser.add_argument('--contr', '--control', dest='contr_fpath', type=str, help='Path to control datasets')
parser.add_argument('--scen', '--scenario', dest='scen_fpath', type=str,
                    help='Path to scenario datasets (data to adjust)')

parser.add_argument('--shp', '--shapefile', dest='shapefile_fpath', type=str, help='Path to shapefile', default=None)
parser.add_argument('--out', '--output', dest='output_fpath', type=str, help='Path to save output files', default='.')
parser.add_argument('-m', '--method', dest='method', type=str, help='Correction method',
                    default='quantile_delta_mapping')
parser.add_argument('-v', '--variable', dest='var', type=str, default='tas', help='Variable to adjust')
parser.add_argument('-u', '--unit', dest='unit', type=str, default='°C', help='Unit of the varible')
parser.add_argument('-g', '--group', dest='group', type=str, default=None,
                    help='Value grouping, default: time, (options: time.month, time.dayofyear, time.year')
parser.add_argument('-k', '--kind', dest='kind', type=str, default='+', help='+ or *, default: +')
parser.add_argument('-n', '--nquantiles', dest='n_quantiles', type=int, default=1000, help='Nr. of Quantiles to use')
parser.add_argument('-p', '--processes', dest='p', type=int, default=1,
                    help='Multiprocessing with n processes, default: 1')
params = vars(parser.parse_args())

obs_fpath = params['obs_fpath']
contr_fpath = params['contr_fpath']
scen_fpath = params['scen_fpath']
shape_fpath = params['shapefile_fpath']
out_fpath = params['output_fpath']

method = params['method']
var = params['var']
unit = params['unit']
group = params['group']
kind = params['kind']
n_quantiles = params['n_quantiles']
n_jobs = params['p']

h_date_period = ('1980-12-01', '1999-11-30')
future_time_periods = [('2020-12-01', '2030-11-30'), ('2030-12-01', '2040-11-30'), ('2060-12-01', '2070-11-30'),
                       ('2070-12-01', '2080-11-30')]


# for testing
future_time_periods = [('2020-12-01', '2022-11-30'),('2022-12-01', '2023-11-30')]
h_date_period = ('1980-12-01', '1981-11-30')
# * ----- ----- -----M A I N ----- ----- -----
def run_debiasing() -> None:
    start = time.time()
    cm = CMethods()

    if method not in cm.get_available_methods():
        raise ValueError(f'Unknown method {method}. Available methods: {cm.get_available_methods()}')

    # load every file found with extension in the path and selects only the input time period.from
    # coordinates are renamed for compatibility with the cmethods-library
    ds_simh = \
        load_data(contr_fpath, date_range=h_date_period, variable=var, shapefile_path=shape_fpath, extension='tif')[
            var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
    ds_obs = load_data(obs_fpath, date_range=h_date_period, variable=var, shapefile_path=shape_fpath)[var].rename(
        {"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
    log.info('Historical data Loaded.')

    # aligning calendars, e.g there might be a few extra days on the scenario data that has to be droped.
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

    # looping over time periods
    # this is done because the full time period for the scenario dataset is too large for memory.
    for f_date_period in future_time_periods:

        log.info(f'Running for {f_date_period} time period')

        try:
            ds_simp = \
                load_data(scen_fpath, date_range=f_date_period, variable=var, shapefile_path=shape_fpath,
                          extension='tif')[
                    var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
        except Exception as e:
            log.info(f'No data available for {f_date_period} time period')
            continue

        # masking coordinates where the observed data has no values
        ds_simp = ds_simp.where(~np.isnan(ds_obs.isel(time=0)))
        ds_simp = ds_simp.where(ds_simp.values < 1000)

        ds_simp.attrs['unit'] = unit

        start_date: str = ds_simp['time'][0].dt.strftime('%Y%m%d').values.ravel()[0]
        end_date: str = ds_simp['time'][-1].dt.strftime('%Y%m%d').values.ravel()[0]

        descr1, descr2 = '', ''
        if method in cm.DISTRIBUTION_METHODS:
            descr1 = f'_quantiles-{n_quantiles}'

        # If output file do not exist create it
        result_path = os.path.join(out_fpath, var)
        if not os.path.exists(result_path):
            os.makedirs(result_path)

        # ----- Adjustment -----
        log.info(f'Starting {method} adjustment')
        result = cm.adjust_3d(
            method=method,
            obs=ds_obs,
            simh=ds_simh,
            simp=ds_simp,
            n_quantiles=n_quantiles,
            kind=kind,
            group=group,
            n_jobs=n_jobs
        )
        log.info('Saving now')
        result.name = var
        result['time'] = ds_simp['time']
        result = result.rename({"lon": "projection_x_coordinate", "lat": "projection_y_coordinate"})

        # define output name
        output_name = f'{method}_result_var-{var}{descr1}_kind-{kind}_group-{group}{descr2}_{start_date}_{end_date}'
        file_name = os.path.join(result_path, f'debiased_{output_name}.nc')

        log.info('Results')
        log.info(result.head())

        plt.figure(figsize=(10, 5), dpi=216)
        ds_simh.groupby('time.dayofyear').mean(...).plot(label='$T_{sim,h}$')
        ds_obs.groupby('time.dayofyear').mean(...).plot(label='$T_{obs,h}$')
        ds_simp.groupby('time.dayofyear').mean(...).plot(label='$T_{sim,p}$')
        result.groupby('time.dayofyear').mean(...).plot(label='$T^{*Debiased}_{sim,p}$')
        plt.title(
            f'Debiased {var} projected to {start_date} and {end_date}')
        plt.gca().grid(alpha=.3)
        plt.legend()
        fig_name = os.path.join(result_path, f'time-series-{output_name}.png')
        plt.savefig(fig_name)

        index = list(np.linspace(0, len(result.time.values) - 1, 6, dtype=int))
        plt.figure(figsize=(10, 5), dpi=216)
        g_simple = result.isel(time=index).plot(x='projection_x_coordinate', y='projection_y_coordinate', col='time',
                                                col_wrap=3)
        fig_name = os.path.join(result_path, f'maps-{output_name}.png')
        plt.savefig(fig_name)

        print('Saving to', file_name)
        result.to_netcdf(file_name)

    end = time.time()
    log.info(f'total time in seconds: {end - start}')
    log.info('Done')


if __name__ == '__main__':
    run_debiasing()

# * ----- ----- E O F ----- -----
