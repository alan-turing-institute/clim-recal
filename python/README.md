# Methods implemented in Python

*WARNING*: the documentation below predates a significant refactor.

## Resampling HADs grid from 1 km to 2.2 km

The raw [UKHAD observational data](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km)
needs to be resampled to the same grid of the [RCP8.5 data](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/).
<!--
This can be done with the `python/resampling/resampling_hads.py` script, which takes an input
grid and uses to resample the data using [linear interpolation](https://docs.xarray.dev/en/stable/generated/xarray.DataArray.interp.html) (for simplicity have added a
default grid in `data/rcp85_land-cpm_uk_2.2km_grid.nc`).


The script runs under the conda environment created on the main [README.md](../README.md) and has several options that can be understood by
running the following from the `resampling` directory:

```sh
$ python resampling_hads.py --help

usage: resampling_hads.py [-h] --input INPUT [--output OUTPUT] [--grid_data GRID_DATA]

options:
  -h, --help            show this help message and exit
  --input INPUT         Path where the .nc files to resample is located
  --output OUTPUT       Path to save the resampled data data
  --grid_data GRID_DATA
                        Path where the .nc file with the grid to resample is located

```

The script expects the data to be files of `.nc` extension, have dimensions named `projection_x_coordinate` and `projection_y_coordinate` and to follow the format of the [CEDA Archive](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km).

Furthermore, the layer/variable to be resampled must be on the beginning of the name of the file before any `_` (e.g for `tasmax` is `tasmax_hadukgrid_uk_1km_day_19930501-19930531.nc`).

### Quickstart

For example, to run the resampling on `tasmax` daily data found in the fileshare (https://dymestorage1.file.core.windows.net/vmfileshare).

```sh
$ cd python/resampling
$ python resampling_hads.py --input /Volumes/vmfileshare/ClimateData/Raw/HadsUKgrid/tasmax/day --output <local-directory-path>
```

as there is not a `--grid_data` flag, the default file described above is used.


## Loading UKCP and HADs data

In [python/clim_recal/data_loader.py] we have written a few functions for loading and concatenating data into a single xarray which can be used for running debiasing methods. Instructions in how to use these functions can be found in [python/notebooks/load_data_python.ipynb](../notebooks/load_data_python.ipynb).
-->

## Running debiasing methods

The code in the [debiasing](debiasing) directory contains scripts that interface with implementations of the debiasing methods
implemented by different libraries.

Note: By March 2023 we have only implemented the [python-cmethods](https://github.com/alan-turing-institute/python-cmethods) library.


### The cmethods library

---
>  **_NOTE:_** `python-cmethod` has been removed from this project for the reasons given in the main README file. The original citation for the library is below:
>
> python-cmethods citation: Benjamin T. Schwertfeger. (2024). btschwertfeger/python-cmethods: v2.3.0 (v2.3.0). Zenodo. https://doi.org/10.5281/zenodo.12168002
> 
> There are still many references to the `python-cmethods` library in the codebase and documentation. This will not work in their current state and are not maintained. These references will be removed in due course.
>
> Users may download `python-cmethods` independently for their own use, but it will no longer be included in this project. Users must not redistribute the combination of `python-cmethods` and `clim-recal` as a single package, as this would violate the licence of `python-cmethods`.

---

This repository contains two python scripts one for preprocessing/grouping data and one to run debiasing in climate data using a fork of the [original python-cmethods](https://github.com/btschwertfeger/python-cmethods) module written by Benjamin Thomas Schwertfeger's , which has been modified to function with the dataset used in the clim-recal project. ~~This library has been included as a submodule to this project, so you must run the following command to pull the submodules required.~~


- The [preprocess_data.py](clim_recal/debiasing/preprocess_data.py) script allows the user to specify directories from which the modelled (CPM/UKCP) data and observation (HADs) data should be loaded, as well as time periods to use for calibration and validation. The script parses the necessary files and combines them into two files for calibration (modelled and observed), and two files for validation (modelled and observed) - with the option to specify multiple validation periods. These can then be used as inputs to `run_cmethods.py`.
- The [run_cmethods.py](clim_recal/debiasing/run_cmethods.py) script allow us to adjust climate biases in climate data using the python-cmethods library.
It takes as input observation data (HADs data) and modelled data (historical CPM/UKCP data) for calibration, as well as observation and modelled data for validation (generated by `preprocess_data.py`). It calibrates the debiasing method using the calibration period data and debiases the modelled data for the validation period. The resulting output is saved as a `.nc` to a specified directory. The script will also produce a time-series and a map plot of the debiased data.

**Usage**:

The scripts can be run from the command line using the following arguments:

```sh
$ python3 preprocess_data.py --mod <path to modelled datasets> --obs <path to observation datasets> --shp <shapefile> --out <output file path> -v <variable> -u <unit> -r <CPM model run number> --calib_dates <date range for calibration> --valid_dates <date range for validation>

$ python3 run_cmethods.py --input_data_folder <input files directory> --out <output directory> -m <method> -v <variable> -g <group> -k <kind> -n <number of quantiles> -p <number of processes>
```

For more details on the scripts and options you can run:
```sh
$ python3 preprocess_data.py --help
```
and
```sh
python3 run_cmethods.py --help
```
**Main Functionality**:

The `preprocess_data.py` script performs the following steps:

- Parses the input arguments.
- Loads, merges and clips (if shapefile is provided) all calibration datasets and merges them into two distinct datasets: the m modelled and observed datasets.
- Aligns the calendars of the two datasets, ensuring that they have the same time dimension.
- Saves the calibration datasets to the output directory.
- Loops over the validation time periods specified in the `calib_dates` variable and performs the following steps:
  - Loads the modelled data for the current time period.
  - Loads the observed data for the current time period.
  - Aligns and saves the datasets to the output directory.

The `run_cmethods.py` script performs the following steps:
  - Reads the input calibration and validation datasets from the input directory.
  - Applies the specified debiasing method, combining the calibration and valiation data.
  - Saves the resulting output to the specified directory.
  - Creates diagnotic figues of the output dataset (time series and time dependent maps) and saves it into the specified directory.

**Working example**.

Example of how to run the two scripts using data stored in the Azure fileshare, running the scripts locally (uses input data that have been cropped to contain only the city of Glasgow. The two scripts will debias only the `tasmax` variable, run 05 of the CPM, for calibration years 1980-2009 and validation years 2010-2019. It uses the `quantile_delta_mapping` debiasing method:
```sh
$ python3 preprocess_data.py --mod /Volumes/vmfileshare/ClimateData/Cropped/three.cities/CPM/Glasgow/ --obs /Volumes/vmfileshare/ClimateData/Cropped/three.cities/Hads.original360/Glasgow/ -v tasmax --out ./preprocessed_data/ --calib_dates 19800101-20091230 --valid_dates 20100101-20191230 --run_number 05

$ python3 run_cmethods.py --input_data_folder ./preprocessed_data/  --out ./debiased_data/  --method quantile_delta_mapping --v tasmax -p 4
```

## Testing

Testing for `python` components uses `pytest`, with configuration specified in `pyproject.toml`. To run tests, ensure the `conda-lock.yml` environment is installed and activated, then run `pytest` from within the `clim-recal/python` checkout directory. Note: some tests are skipped unless run on a specific linux server wth data mounted to a specific path.

```sh
$ cd clim-recal
$ conda-lock install --name clim-recal conda-lock.yml
$ conda activate clim-recal
(clim-recal)$ cd python
(clim-recal)$ pytest
...sss........sss.....                                                         [100%]
============================== short test summary info ===============================
SKIPPED [1] <doctest test_debiasing.RunConfig.mod_path[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.obs_path[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.preprocess_out_path[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.yield_mod_folder[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.yield_obs_folder[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.yield_preprocess_out_folder[0]>:2: requires linux server mount paths
16 passed, 6 skipped, 4 deselected in 0.26s
```

<!--
### Python

In the `python` subdirectory you can find code for the different data download, processing and debiasing steps:
   - **Data download** for a script to download data from the CEDA archive.
   - **Resampling** for the HADsUK datasets from 1km to a 2.2 km grid to match the UKCP re-projected grid.
   - **Data loaders** functions for loading and concatenating data into a single xarray which can be used for running debiasing methods.
   - **Debiasing scripts** that interface with implementations of the debiasing (bias correction) methods implemented by different libraries (by March 2023 we have only implemented the python-cmethods library).

More details in how to use this code can be found in [the python README file](python/README.md) and the environment used in this [environment setup file](setup-instructions.md).

-->
