from common_tools import get_params, create_logger, h_date_period, f_date_period


def main():
    log = create_logger()
    log.info('Loading in parameters ...')

    params = get_params()

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

    

if __name__ == '__main__':
    main()