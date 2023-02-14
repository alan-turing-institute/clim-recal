import xarray as xr
import glob
import geopandas as gp

def load_data(input_path, date_range, variable, shapefile_path=None, extension='nc'):
    '''
    This function takes a date range and a variable and loads and merges xarrays based on those parameters.
    If shapefile is provided it crops the data to that region.

    Parameters
    ----------
    input_path: str
        Path to where .nc or .tif files are found
    date_range : tuple
        A tuple of datetime objects representing the start and end date
    variable : string
        A string representing the variable to be loaded
    shapefile_path: str
        Path to a shape file used to clip resulting dataset.
    extension: str
        Extension of the files to be loaded, it can be .nc or .tif files.

    Returns
    -------
    merged_xarray : xarray
        An xarray containing all loaded and merged and clipped data
    '''

    if extension not in ('nc', 'tif'):
        raise Exception("We only accept .nc or .tif extension for the input data")

    files = glob.glob(f"{input_path}/*.{extension}", recursive=True)

    if len(files)==0:
        raise Exception(f"No files found in {input_path} with {extension}")


    #TODO: Load using mfdataset avoiding errors from HDF5
    #try:
        # loading files with dedicated function
    #    xa = xr.open_mfdataset(files).sel(time=slice(*date_range)).sortby('time')
    #except Exception as e:
    #    print(f"Not able to load using open_mfdataset, with errors: {e}. "
    #          f"Looping and loading individual files.")
    #    # files with wrong format wont load with open_mfdataset, need to be reformated.

    xa = load_and_merge(date_range, files, variable)

    # clipping
    if shapefile_path:
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

     Returns
    -------
    xa : xarray
        A clipped xarray dataset

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

    try:
        # this is creating issues after clipping for hads
        del xa[variable].attrs['grid_mapping']
    except:
        pass


    return xa

def reformat_file(file, variable):
    """
    Load tif file and reformat xarray into expected format.

    """
    print(f"File: {file} is needs rasterio library, trying...")
    x = xr.open_rasterio(file)
    st, sp = file.rsplit("_")[-1][:-4].split('-')
    start = f"{st[:4]}-{st[4:6]}-{st[6:]}"
    stop = f"{sp[:4]}-{sp[4:6]}-{sp[6:]}"
    time_index = xr.cftime_range(start, stop, freq='D', calendar='360_day')

    x_renamed = x.rename({"x": "projection_x_coordinate", "y": "projection_y_coordinate", "band": "time"}) \
        .rio.write_crs('epsg:27700')
    x_renamed.coords['time'] = time_index

    xa = x_renamed.transpose('time', 'projection_y_coordinate',
                                                        'projection_x_coordinate').to_dataset(
        name=variable)

    return xa


def load_and_merge(date_range, files, variable):
    """
    Load files into xarrays, select a time range and a variable and merge into a sigle xarray.

    Parameters
    ----------

    date_range : tuple
        A tuple of datetime objects representing the start and end date
    files: list (str)
        List of strings with path to files to be loaded.
    variable : string
        A string representing the variable to be loaded

    Returns
    -------
    merged_xarray : xarray
        An xarray containing all loaded and merged data
    """

    # Create an empty list to store xarrays
    xarray_list = []
    # Iterate through the variables
    for file in files:
        # Load the xarray
        try:
            try:
                x = xr.open_dataset(file).sel(time=slice(*date_range))
            except Exception as e:
                x = reformat_file(file,variable).sel(time=slice(*date_range))

            # Select the date range
            if x.time.size != 0:
                # Append the xarray to the list
                xarray_list.append(x)
            del x
        except Exception as e:
            print(f"File: {file} produced errors: {e}")

    # Merge all xarrays in the list
    if len(xarray_list) == 0:
        raise RuntimeError('No files passed the time selection. No merged output produced.')
    else:
        merged_xarray = xr.concat(xarray_list, dim="time").sortby('time')

    return merged_xarray
