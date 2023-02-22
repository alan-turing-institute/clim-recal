import logging, sys 
import argparse

# A file with common functions and variables for the scripts in the debiasing folder

h_date_period = ('1980-01-01', '2000-01-01')
f_date_period = ('2020-01-01', '2080-01-01')

def get_params():
    """
    Get parameters from command line
    """
    parser = argparse.ArgumentParser(description='Adjust climate data based on bias correction algorithms and magic.')
    parser.add_argument('--obs', '--observation', dest='obs_fpath', type=str, help='Observation dataset')
    parser.add_argument('--contr', '--control', dest='contr_fpath', type=str, help='Control dataset')
    parser.add_argument('--scen', '--scenario', dest='scen_fpath', type=str, help='Scenario dataset (data to adjust)')
    parser.add_argument('--shp', '--shapefile', dest='shapefile_fpath', type=str, help='Path to shapefile', default=None)
    parser.add_argument('-m', '--method', dest='method', type=str, help='Correction method',default='linear_scaling')
    parser.add_argument('-v', '--variable', dest='var', type=str, default='tas', help='Variable to adjust')
    parser.add_argument('-u', '--unit', dest='unit', type=str, default='°C', help='Unit of the varible')
    parser.add_argument('-g', '--group', dest='group', type=str, default='time.dayofyear', help='Value grouping, default: time, (options: time.month, time.dayofyear, time.year')
    parser.add_argument('-k', '--kind', dest='kind', type=str, default='+', help='+ or *, default: +')
    parser.add_argument('-n', '--nquantiles', dest='n_quantiles', type=int, default=100, help='Nr. of Quantiles to use')
    parser.add_argument('-p', '--processes', dest='p', type=int, default=1, help='Multiprocessing with n processes, default: 1')
    return vars(parser.parse_args())


def create_logger():
    """
    Create logger
    """
    formatter = logging.Formatter(
        fmt='%(asctime)s %(module)s,line: %(lineno)d %(levelname)8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    log = logging.getLogger()
    log.setLevel(logging.INFO)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logging.getLogger().addHandler(screen_handler)
    return log

