#!/bin/python3

#Script to adjust climate biases in 3D Climate data using cmethods library
#inspired in script by Benjamin Thomas Schwertfeger script https://github.com/btschwertfeger/python-cmethods/blob/master/examples/do_bias_correction.py

import argparse
import logging, sys

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

h_date_period = ('1980-12-01', '1981-11-30')
f_date_period = ('2020-01-01', '2021-11-30')

# * ----- ----- -----M A I N ----- ----- -----
def main() -> None:
    cm = CMethods_climrecal()

    if method not in cm.get_available_methods(): raise ValueError(f'Unknown method {method}. Available methods: {cm.get_available_methods()}')

    # data loader
    ds_obs = load_data(obs_fpath, date_range=h_date_period, variable=var, shapefile_path=shape_fpath)[var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
    ds_simh = load_data(contr_fpath, date_range=h_date_period, variable=var, shapefile_path=shape_fpath, extension='tif')[var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})
    ds_simp = load_data(scen_fpath, date_range=f_date_period, variable=var, shapefile_path=shape_fpath, extension='tif')[var].rename({"projection_x_coordinate": "lon", "projection_y_coordinate": "lat"})

    log.info('Data Loaded')

    ds_obs.attrs['unit'] = unit
    ds_simh.attrs['unit'] = unit
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
    result.to_netcdf(f'{method}_result_var-{var}{descr1}_kind-{kind}_group-{group}{descr2}_{start_date}_{end_date}.nc')
    log.info('Done')


if __name__ == '__main__':
    main()


# * ----- ----- E O F ----- -----