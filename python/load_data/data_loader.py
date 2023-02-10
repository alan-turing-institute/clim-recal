import xarray as xr
import glob
import geopandas as gp
from datetime import datetime
import numpy as np 


def load_hads(input_path, date_range, variable, shapefile_path=None, extension='.nc'):
    '''
    This function takes a date range and a list of variables and loads and merges xarrays based on those parameters.

    Parameters
    ----------
    input_path: str
        Path to where .nc files are found
    date_range : tuple
        A tuple of datetime objects representing the start and end date
    variable : string
        A strings representing the variables to be loaded
    shapefile_path: str
        Path to a shape file used to clip resulting dataset.

    Returns
    -------
    merged_xarray : xarray
        An xarray containing all loaded and merged and clipped variables
    '''

    files = glob.glob(f"{input_path}/*.{extension}", recursive=True)

    xa = load_and_merge(date_range, files)

    if shapefile_path:
        print ('clipping',datetime.now())
        xa = clip_dataset(xa, variable, shapefile_path)

    return xa


def clip_dataset(xa, variable, shapefile):
    """
    Parameters
    ----------
    xa: xArray Dataset
        xArray containing a giving variable
    variable : string
        A strings representing the variable to be loaded
    shapefile: str
        Path to a shape file used to clip resulting dataset, must be in the same CRS of the input xArray.

    """
    geodf = gp.read_file(shapefile)

    # assign projection
    xa_mask = xa[variable].rename({"projection_x_coordinate": "x", "projection_y_coordinate": "y"}) \
        .rio.write_crs('epsg:27700')

    # clip and turn back to Dataset with original coordinate names
    xa = xa_mask.rio.clip(geodf['geometry']).to_dataset().rename({
        "x": "projection_x_coordinate",
        "y": "projection_y_coordinate",
    })

    del xa[variable].attrs['grid_mapping']

    return xa


def load_and_merge(date_range, files):
    # Create an empty list to store xarrays
    xarray_list = []
    # Iterate through the variables
    for file in files:
        # Load the xarray
        try:
            print (file, datetime.now())
            try:
                x = xr.open_dataset(file).sel(time=slice(*date_range))
            except Exception as e:
                print(f"File: {file} is needs rasterio library, trying...")
                x = xr.open_rasterio(file)
                st, sp = file.rsplit("_")[-1][:-4].split('-')
                start = f"{st[:4]}-{st[4:6]}-{st[6:]}"
                stop = f"{sp[:4]}-{sp[4:6]}-{sp[6:]}"
                dates = np.linspace(0, 1, len(x['band'])) * (np.datetime64(stop) - np.datetime64(start)) + np.datetime64(start)
                x= x.expand_dims(dim={"time":dates}, axis=0).sel(time=slice(*date_range))
            # Select the date range
            if x.time.size != 0:
                # Append the xarray to the list
                xarray_list.append(x)
            del x
        except Exception as e:
            print(f"File: {file} produced errors: {e}")

    # Merge all xarrays in the list
    print ('merging', datetime.now())
    if len(xarray_list) == 0:
        raise RuntimeError('No files passed the time selection. No merged output produced.')
    else:
        merged_xarray = xr.concat(xarray_list, dim="time")
        return merged_xarray.sortby('time')




if __name__ == "__main__":
    """
    Load xarrays
    """


    input = '../../data/pr'
    hads = load_hads(input, ('1980-01-01', '2000-01-01'), 'pr', extension='tif')#, '../../data/Scotland/Scotland.bbox.shp')

