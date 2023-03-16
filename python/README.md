# Methods implemented in Python

## Resampling HADs grid from 1 km to 2.2 km

The raw [UKHAD observational data](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km) 
needs to be resampled to the same grid of the [RCP8.5 data](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/).
This can be done with the `python/resampling/resampling_hads.py` script, which takes an input
grid and uses to resample the data using [linear interpolation](https://docs.xarray.dev/en/stable/generated/xarray.DataArray.interp.html) (for simplicity have added a
default grid in `data/rcp85_land-cpm_uk_2.2km_grid.nc`).


The script runs under the conda environment created on the main [README.md](../README.md) and has several options that can be understood by 
running the following from the `resampling` directory:

```
python resampling_hads.py --help

usage: resampling_hads.py [-h] --input INPUT [--output OUTPUT] [--grid_data GRID_DATA]

options:
  -h, --help            show this help message and exit
  --input INPUT         Path where the .nc files to resample is located
  --output OUTPUT       Path to save the resampled data data
  --grid_data GRID_DATA
                        Path where the .nc file with the grid to resample is located

```

The script expects the data to be files of `.nc` extension, have dimensions named `projection_x_coordinate` and `projection_y_coordinate` and to follow the format
of the [CEDA Archive](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km).
Furthermore, the layer/variable to be resampled must be on the beginning of the name of the file before any `_` (e.g for `tasmax` is `tasmax_hadukgrid_uk_1km_day_19930501-19930531.nc`).

### Quickstart 

For example, to run the resampling on `tasmax` daily data found in the fileshare (https://dymestorage1.file.core.windows.net/vmfileshare).

```
cd python/resampling
python resampling_hads.py --input /Volumes/vmfileshare/ClimateData/Raw/HadsUKgrid/tasmax/day --output <local-directory-path>
```

as there is not a `--grid_data` flag, the default file described above is used. 


## Loading UKCP and HADs data

In [python/load_data/data_loader.py] we have written a few functions for loading and concatenating data into a single xarray which
can be used for running debiasing methods. Instructions in how to use these functions can be found in [python/notebooks/load_data_python.ipynb](../notebooks/load_data_python.ipynb).

## Running debiasing methods 

The code in the [debiasing](debiasing) directory contains scripts that interface with implementations of the debiasing methods 
implemented by different libraries.

Note: By March 2023 we have only implemented the [python-cmethods](https://github.com/alan-turing-institute/python-cmethods) library.


### The cmethods library

This repository contains a python script used to run debiasing in climate data using a fork of the [original python-cmethods](https://github.com/btschwertfeger/python-cmethods) module written by Benjamin Thomas Schwertfeger's , which has 
been modified to function with the dataset used in the clim-recal project. This library has been included as a 
submodule to this project, so you must run the following command to pull the submodules required.

```
cd debiasing
git submodule update --init --recursive
```

The [run_cmethods.py](debiasing/run_cmethods.py) allow us to adjusts climate biases in climate data using the python-cmethods library. 
It takes as input observation data (HADs data), control data (historical UKCP data), and scenario data (future UKCP data), 
and applies a correction method to the scenario data. The resulting output is saved as a `.nc` to a specified directory.
The script will also produce a time-series and a map plot of the debiased data.

**Usage**:

The script can be run from the command line using the following arguments:

```
python3 run_cmethods.py.py --obs <path to observation datasets> --contr <path to control datasets> --scen <path to scenario datasets> --shp <shapefile> 
--out <output file path> -m <method> -v <variable> -u <unit> -g <group> -k <kind> -n <number of quantiles> -p <number of processes>
```

where:

where:

- `--obs` specifies the path to the observation datasets
- `--contr` specifies the path to the control datasets
- `--scen`  specifies the path to the scenario datasets (data to adjust)
- `--shp`  specifies the path to a shapefile, in case we want to select a smaller region (default: None)
- `--out` specifies the path to save the output files (default: current directory)
- `--method` specifies the correction method to use (default: quantile_delta_mapping)
- `-v` specifies the variable to adjust (default: tas)
- `-u`  specifies the unit of the variable (default: °C)
- `-g`  specifies the value grouping (default: time)
- `-k`  specifies the method kind (+ or *, default: +)
- `-n`  specifies the number of quantiles to use (default: 1000)
- `-p`  specifies the number of processes to use for multiprocessing (default: 1)

For more details on the script and options you can run:

```
python run_cmethods.py --help
```
**Main Functionality**:

The script applies corrections extracted from historical observed and simulated data between `1980-12-01` and `1999-11-30`.
Corrections are applied to future scenario data between `2020` and `2080` (however there is no available scenario data between `2040` to `2060`, so this time
period is skipped.


The script performs the following steps:

- Parses the input arguments.
- Loads, merges and clips (if shapefile is provided) the all input datasets and merges them into two distinct datasets: the observation and control datasets.
- Aligns the calendars of the historical simulation data and observed data, ensuring that they have the same time dimension 
and checks that the observed and simulated historical data have the same dimensions.
- Loops over the future time periods specified in the `future_time_periods` variable and performs the following steps:
  - Loads the scenario data for the current time period.
  - Applies the specified correction method to the scenario data.
  - Saves the resulting output to the specified directory.
  - Creates diagnotic figues of the output dataset (time series and time dependent maps) and saves it into the specified directory.

In this script 
datasets are debiased in periods of 10 years, in a consecutive loop, for each time period it will produce an `.nc` output file
with the adjusted data and a time-series plot and a time dependent map plot of the adjusted data. 

**Working example**.

Example of code working on the **clim-recal** dataset:
```
python run_cmethods.py --scen /Volumes/vmfileshare/ClimateData/Reprojected/UKCP2.2/tasmax/01/latest --contr /Volumes/vmfileshare/ClimateData/Reprojected/UKCP2.2/tasmax/01/latest/ --obs /Volumes/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/tasmax/day/ --shape ../../data/Scotland/Scotland.bbox.shp -v tasmax --method delta_method --group time.month -p 5
```



