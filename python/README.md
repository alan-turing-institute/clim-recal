# Methods implemented in Python

## Resampling HADs grid from 1 km to 2.2 km

We need to resample the raw [UKHAD observational data](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km) to the same grid of the [RCP8.5 data](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/).
For this we have writen the script found in the `python/resampling` directory, which takes an input
grid and uses to resample the data using linear interpolation (for simplicity have added a
default grid in `data/rcp85_land-cpm_uk_2.2km_grid.nc`).


The script has several options that can be understood by running the following from the `resampling` directory:

```python spc-hpc-client.py --help

usage: resampling_hads.py [-h] --input INPUT [--output OUTPUT] [--grid_data GRID_DATA]

options:
  -h, --help            show this help message and exit
  --input INPUT         Path where the .nc files to resample is located
  --output OUTPUT       Path to save the resampled data data
  --grid_data GRID_DATA
                        Path where the .nc file with the grid to resample is located

```

The script expects the data to be .nc files, and to have dimensions named `projection_x_coordinate` and `projection_y_coordinate` and follow the format
of the [CEDA Archive](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km).
Furthermore, the layer/variable to be resampled must be on the beginning of the name of the file, before any `_` (e.g for `tasmax` is `tasmax_hadukgrid_uk_1km_day_19930501-19930531.nc`).

### Quickstart 

For example, to run the resampling on `tasmax` daily data found in the fileshare ([link to how to mount the fileshare on your machine]).

```
cd python/resampling
python resampling_hads.py --input /mnt/vmfileshare/ClimateData/Raw/HadsUKgrid/tasmax/day --output <local-directory-path>
```

as there is not a `--grid_data` flag, the default file described above is used. 
