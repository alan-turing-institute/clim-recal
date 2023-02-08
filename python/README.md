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
