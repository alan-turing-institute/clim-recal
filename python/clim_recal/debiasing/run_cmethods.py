#!/bin/python3

# Script to adjust climate biases in climate data using the python-cmethods library. This script is inspired by the
# script by Benjamin Thomas Schwertfeger
# (https://github.com/btschwertfeger/python-cmethods/blob/master/examples/do_bias_correction.py)
# and adapted to function with UKCP/CPM and HADs data.

import argparse
import glob
import logging
import os
import sys
import time

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

sys.path.insert(1, "python-cmethods")
from cmethods.CMethods import CMethods

# * ----- L O G G I N G -----
formatter = logging.Formatter(
    fmt="%(asctime)s %(module)s,line: %(lineno)d %(levelname)8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger()
log.setLevel(logging.INFO)
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
logging.getLogger().addHandler(screen_handler)

# * ----- I N P U T - H A N D L I N G -----
parser = argparse.ArgumentParser(
    description="Adjust climate data based on bias correction algorithms."
)
parser.add_argument(
    "--input_data_folder",
    "--input_data_folder",
    dest="input_dir",
    type=str,
    help="Directory that contains all data files. NetCDF (.nc) files with names starting with "
    "`modc` and `obsc` should be found in the directory (containing "
    "modelled calibration data (CPM) and observed calibration data (HADs) respectively), "
    "as well as at least one file with name "
    "starting with `modv` (containing modelled validation data (CPM). Calibration data "
    "are used to calibrate the debiasing methods and validation data are debiased.",
)
parser.add_argument(
    "--out",
    "--output",
    dest="output_fpath",
    type=str,
    help="Path to save output files",
    default=".",
)
parser.add_argument(
    "-m",
    "--method",
    dest="method",
    type=str,
    help="Correction method",
    default="quantile_delta_mapping",
)
parser.add_argument(
    "-v", "--variable", dest="var", type=str, default="tas", help="Variable to adjust"
)
parser.add_argument(
    "-g",
    "--group",
    dest="group",
    type=str,
    default=None,
    help="Value grouping, default: time, (options: time.month, time.dayofyear, time.year",
)
parser.add_argument(
    "-k", "--kind", dest="kind", type=str, default="+", help="+ or *, default: +"
)
parser.add_argument(
    "-n",
    "--nquantiles",
    dest="n_quantiles",
    type=int,
    default=1000,
    help="Nr. of Quantiles to use",
)
parser.add_argument(
    "-p",
    "--processes",
    dest="p",
    type=int,
    default=1,
    help="Multiprocessing with n processes, default: 1",
)
params = vars(parser.parse_args())

input_dir = params["input_dir"]
out_fpath = params["output_fpath"]

method = params["method"]
var = params["var"]
group = params["group"]
kind = params["kind"]
n_quantiles = params["n_quantiles"]
n_jobs = params["p"]


# * ----- ----- -----M A I N ----- ----- -----
def run_debiasing() -> None:
    start = time.time()
    cm = CMethods()

    if method not in cm.get_available_methods():
        raise ValueError(
            f"Unknown method {method}. Available methods: {cm.get_available_methods()}"
        )

    modc_files = glob.glob(f"{input_dir}/modc*.nc")
    if len(modc_files) == 0:
        raise Exception(
            f"No .nc files with filename starting with modc were "
            f"found in the input directory {input_dir}"
        )
    elif len(modc_files) > 1:
        raise Exception(
            f"More than one .nc file with filenames starting with modc were "
            f"found in the input directory {input_dir}"
        )
    else:
        print("Loading modelled calibration data (CPM) from ", modc_files[0], "...")
        with xr.open_dataset(modc_files[0], engine="netcdf4") as ds:
            ds_modc = ds.load()[var]
    log.info(f"Modelled calibration data (CPM) loaded with shape {ds_modc.shape}.")

    obsc_files = glob.glob(f"{input_dir}/obsc*.nc")
    if len(obsc_files) == 0:
        raise Exception(
            f"No .nc files with filename starting with obsc were "
            f"found in the input directory {input_dir}"
        )
    elif len(obsc_files) > 1:
        raise Exception(
            f"More than one .nc file with filenames starting with obsc were "
            f"found in the input directory {input_dir}"
        )
    else:
        print("Loading observation data for calibration from ", obsc_files[0], "...")
        with xr.open_dataset(obsc_files[0], engine="netcdf4") as ds:
            ds_obsc = ds.load()[var]
        log.info(f"Observation data for calibration loaded with shape {ds_obsc.shape}.")

    if ds_obsc.shape != ds_modc.shape:
        raise RuntimeError(
            "Error, observed and modelled calibration data must have same dimensions."
        )

    # looping over future time periods for which debiased data need to be generated
    modv_files = glob.glob(f"{input_dir}/modv*.nc")
    if len(modv_files) == 0:
        raise Exception(
            f"No .nc files with filename starting with modv were "
            f"found in the input directory {input_dir}"
        )
    else:
        for modv_file in modv_files:
            print("Loading modelled data (CPM) for validation from ", modv_file, "...")
            with xr.open_dataset(modv_file, engine="netcdf4") as ds:
                ds_modv = ds.load()[var]
            log.info(
                f"Modelled data (CPM) for validation loaded with shape {ds_modv.shape}."
            )

            start_date: str = ds_modv["time"][0].dt.strftime("%Y%m%d").values.ravel()[0]
            end_date: str = ds_modv["time"][-1].dt.strftime("%Y%m%d").values.ravel()[0]

            descr1, descr2 = "", ""
            if method in cm.DISTRIBUTION_METHODS:
                descr1 = f"_quantiles-{n_quantiles}"

            # If output file do not exist create it
            result_path = os.path.join(out_fpath, var)
            if not os.path.exists(result_path):
                os.makedirs(result_path)

            # ----- Adjustment -----
            log.info(f"Starting {method} adjustment")
            result = cm.adjust_3d(
                method=method,
                obs=ds_obsc,
                simh=ds_modc,
                simp=ds_modv,
                n_quantiles=n_quantiles,
                kind=kind,
                group=group,
                n_jobs=n_jobs,
            )
            log.info("Saving now")
            result.name = var
            result["time"] = ds_modv["time"]
            result = result.rename(
                {"lon": "projection_x_coordinate", "lat": "projection_y_coordinate"}
            )

            # define output name
            output_name = f"{method}_result_var-{var}{descr1}_kind-{kind}_group-{group}{descr2}_{start_date}_{end_date}"
            file_name = os.path.join(result_path, f"debiased_{output_name}.nc")

            log.info("Results")
            log.info(result.head())

            plt.figure(figsize=(10, 5), dpi=216)
            ds_modc.groupby("time.dayofyear").mean(...).plot(label="$T_{sim,h}$")
            ds_obsc.groupby("time.dayofyear").mean(...).plot(label="$T_{obs,h}$")
            ds_modv.groupby("time.dayofyear").mean(...).plot(label="$T_{sim,p}$")
            result.groupby("time.dayofyear").mean(...).plot(
                label="$T^{*Debiased}_{sim,p}$"
            )
            plt.title(f"Debiased {var} projected to {start_date} and {end_date}")
            plt.gca().grid(alpha=0.3)
            plt.legend()
            fig_name = os.path.join(result_path, f"time-series-{output_name}.png")
            plt.savefig(fig_name)

            index = list(np.linspace(0, len(result.time.values) - 1, 6, dtype=int))
            plt.figure(figsize=(10, 5), dpi=216)
            g_simple = result.isel(time=index).plot(
                x="projection_x_coordinate",
                y="projection_y_coordinate",
                col="time",
                col_wrap=3,
            )
            fig_name = os.path.join(result_path, f"maps-{output_name}.png")
            plt.savefig(fig_name)

            print("Saving to", file_name)
            result.to_netcdf(file_name)

    end = time.time()
    log.info(f"total time in seconds: {end - start}")
    log.info("Done")


if __name__ == "__main__":
    run_debiasing()

# * ----- ----- E O F ----- -----
