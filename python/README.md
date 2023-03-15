# Methods implemented in Python


## Resampling HADs grid from 1 km to 2.2 km

The raw [UKHAD observational data](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km) 
needs to be resampled to the same grid of the [RCP8.5 data](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/).
This can be done with the `python/resampling/resampling_hads.py` script, which takes an input
grid and uses to resample the data using [linear interpolation](https://docs.xarray.dev/en/stable/generated/xarray.DataArray.interp.html) (for simplicity have added a
default grid in `data/rcp85_land-cpm_uk_2.2km_grid.nc`).


The script runs under the conda environment created on [../README.md] and has several options that can be understood by 
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
python resampling_hads.py --input /mnt/vmfileshare/ClimateData/Raw/HadsUKgrid/tasmax/day --output <local-directory-path>
```

as there is not a `--grid_data` flag, the default file described above is used. 


## Loading UKCP and HADs data

In [python/load_data/data_loader.py] we have written a few functions for loading and concatenating data into a single xarray which
can be used for running debiasing methods. Instructions in how to use these functions can be found in [python/notebooks/load_data_python.ipynb](../notebooks/load_data_python.ipynb).

## Running debiasing methods 

The code in the [debiasing](debiasing) directory contains scripts that interface with implementations of the debiasing methods 
implemented by different libraries (up to now we are using the [python-cmethods]()).

## python-cmethods library

This repository contains a Python script used to adjust biases in 3D climate data using a fork of the original python-cmethods module written by Benjamin Thomas Schwertfeger's , which has 
been modified to function with the dataset used in the clim-recal project. 

The script requires two input datasets: an observation dataset and a control dataset. It also requires a scenario dataset 
to be adjusted, as well as a shapefile for the geographical region. The user can specify a correction method, 
the variable to be adjusted, the unit of the variable and the value grouping (i.e. time, time.month, time.dayofyear, time.year). 
The user can also specify the number of quantiles to use for the correction, and the kind of correction (‘+’ or ‘*’). 

The script will run the correction method to adjust the climate data from the scenario dataset, and produce an output file
with the adjusted data. It will also produce a time-series plot and a map plot of the adjusted data. 

Usage:

```python3 run_cmethods.py.py --obs <path to observation dataset> --contr <path to control dataset> --scen <path to scenario dataset> --shp <shapefile> 
--out <output file path> -m <method> -v <variable> -u <unit> -g <group> -k <kind> -n <number of quantiles> -p <number of processes>
```
Example:
```
python run_cmethods.py --scen /Volumes/vmfileshare/ClimateData/Reprojected/UKCP2.2/tasmax/01/latest --contr /Volumes/vmfileshare/ClimateData/Reprojected/UKCP2.2/tasmax/01/latest/  --obs /Volumes/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/tasmax/day/ --shape ../../data/Scotland/Scotland.bbox.shp -v tasmax --method delta_method --group time.month -p 5
```



