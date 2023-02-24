#!/bin/python3

#Script to adjust climate biases in 3D Climate data using cmethods library
#inspired in script by Benjamin Thomas Schwertfeger script https://github.com/btschwertfeger/python-cmethods/blob/master/examples/do_bias_correction.py

import argparse
import logging, sys
import time
import numpy as np
import matplotlib.pyplot as plt

from CMethods import CMethods_climrecal

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
parser = argparse.ArgumentParser(description='Adjust climate data based on bias correction algorithms and magic.')
parser.add_argument('--obs', '--observation', dest='obs_fpath', type=str, help='Observation dataset')
parser.add_argument('--contr', '--control', dest='contr_fpath', type=str, help='Control dataset')
parser.add_argument('--scen', '--scenario', dest='scen_fpath', type=str, help='Scenario dataset (data to adjust)')

parser.add_argument('--shp', '--shapefile', dest='shapefile_fpath', type=str, help='Path to shapefile', default=None)


parser.add_argument('-m', '--method', dest='method', type=str, help='Correction method', default='quantile_delta_mapping')
parser.add_argument('-v', '--variable', dest='var', type=str, default='tas', help='Variable to adjust')
parser.add_argument('-u', '--unit', dest='unit', type=str, default='°C', help='Unit of the varible')

parser.add_argument('-g', '--group', dest='group', type=str, default=None, help='Value grouping, default: time, (options: time.month, time.dayofyear, time.year')
parser.add_argument('-k', '--kind', dest='kind', type=str, default='+', help='+ or *, default: +')
parser.add_argument('-n', '--nquantiles', dest='n_quantiles', type=int, default=1000, help='Nr. of Quantiles to use')

parser.add_argument('-p', '--processes', dest='p', type=int, default=1, help='Multiprocessing with n processes, default: 1')
params = vars(parser.parse_args())

obs_fpath = params['obs_fpath']
contr_fpath = params['contr_fpath']
scen_fpath = params['scen_fpath']
shape_fpath = params['shapefile_fpath']

method = params['method']
var = params['var']
unit = params['unit']
group = params['group']
kind = params['kind']
n_quantiles = params['n_quantiles']
n_jobs = params['p']

h_date_period = ('1980-12-01', '1999-11-30')

future_time_periods = [('2020-12-01', '2030-11-30'),('2030-12-01', '2040-11-30'),('2040-12-01', '2050-11-30'),
               ('2050-12-01', '2060-11-30'),('2060-12-01', '2070-11-30'),('2070-12-01', '2080-11-30')]

#for testing
#future_time_periods = [('2020-12-01', '2021-01-01'),('2021-01-02', '2021-05-01'),('2021-06-02', '2021-12-01')]

# * ----- ----- -----M A I N ----- ----- -----
def main() -> None:
    import time

    start = time.time()
    cm = CMethods_climrecal()

    if method not in cm.get_available_methods(): raise ValueError(f'Unknown method {method}. Available methods: {cm.get_available_methods()}')

    # data loader
    ds_obs = load_data(obs_fpath, date_range=h_date_period, variable=var, shapefile_path=shape_fpath)[var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
    ds_simh = load_data(contr_fpath, date_range=h_date_period, variable=var, shapefile_path=shape_fpath, extension='tif')[var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
    log.info('Historical data Loaded')

    ds_simh = ds_simh.where(~np.isnan(ds_obs.isel(time=0)))
    ds_simh = ds_simh.where(ds_simh.values<1000)

    log.info('Historical data Masked')

    ds_obs.attrs['unit'] = unit
    ds_simh.attrs['unit'] = unit


    for f_date_period in future_time_periods:

        try:
            ds_simp = load_data(scen_fpath, date_range=f_date_period, variable=var, shapefile_path=shape_fpath, extension='tif')[var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
        except Exception as e:
            print ('No data available for time period',f_date_period)
            pass


        ds_simp = ds_simp.where(~np.isnan(ds_obs.isel(time=0)))
        ds_simp = ds_simp.where(ds_simp.values<1000)

        ds_simp.attrs['unit'] = unit

        start_date: str = ds_simp['time'][0].dt.strftime('%Y%m%d').values.ravel()[0]
        end_date: str = ds_simp['time'][-1].dt.strftime('%Y%m%d').values.ravel()[0]

        descr1, descr2 = '', ''
        if method in cm.DISTRIBUTION_METHODS:
            descr1 = f'_quantiles-{n_quantiles}'

        # ----- Adjustment -----
        log.info(f'Starting {method} adjustment')
        result = cm.adjust_3d(
            method = method,
            obs = ds_obs,
            simh = ds_simh,
            simp = ds_simp,
            n_quantiles = n_quantiles,
            kind = kind,
            group = group,
            n_jobs = n_jobs
        )
        log.info('Saving now')
        result.name = var
        result['time'] = ds_simp['time']
        result = result.rename({"lon": "projection_x_coordinate", "lat": "projection_y_coordinate"})
        result.to_netcdf(f'{method}_result_var-{var}{descr1}_kind-{kind}_group-{group}{descr2}_{start_date}_{end_date}.nc')


        plt.figure(figsize=(10, 5), dpi=216)
        ds_simh.groupby('time.dayofyear').mean(...).plot(label='$T_{sim,h}$')
        ds_obs.groupby('time.dayofyear').mean(...).plot(label='$T_{obs,h}$')
        ds_simp.groupby('time.dayofyear').mean(...).plot(label='$T_{sim,p}$')
        result.groupby('time.dayofyear').mean(...).plot(label='$T^{*Debiased}_{sim,p}$')
        plt.title(
            f'Historical modeled and obseved temperatures between December {start_date} and {end_date}')  # ; and predicted temperatures')
        plt.gca().grid(alpha=.3)
        plt.legend();
        plt.savefig(f'time-series-{method}_result_var-{var}{descr1}_kind-{kind}_group-{group}{descr2}_{start_date}_{end_date}.png')

        index = list(np.linspace(0, len(result.time.values) - 1, 6, dtype=int))
        plt.figure(figsize=(10, 5), dpi=216)
        g_simple = result.isel(time=index).plot(x='projection_x_coordinate', y='projection_y_coordinate', col='time', col_wrap=3)
        plt.savefig(f'maps-{method}_result_var-{var}{descr1}_kind-{kind}_group-{group}{descr2}_{start_date}_{end_date}.png')

    end = time.time()
    print('total time in seconds',end - start)
    log.info('Done')

if __name__ == '__main__':
    main()


# * ----- ----- E O F ----- -----