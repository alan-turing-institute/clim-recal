import xarray as xr
import glob

def load_hads(date_range, variable, input_path = '/Volumes/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/'):
    '''
    This function takes a date range and a list of variables and loads and merges xarrays based on those parameters.

    Parameters
    ----------
    date_range : tuple
        A tuple of datetime objects representing the start and end date
    variable : string
        A strings representing the variables to be loaded

    Returns
    -------
    merged_xarray : xarray
        An xarray containing all loaded and merged variables
    '''

    files = glob.glob(f"{input_path}/{variable}/*/*.nc", recursive=True)

    xa = load_and_merge(date_range, None, files)

    return xa

def load_and_merge(date_range, mask, files):

    # Create an empty list to store xarrays
    xarray_list = []
    # Iterate through the variables
    for file in files:
        # Load the xarray
        try:
            x = xr.open_dataset(file).sel(time=slice(*date_range))
            # Select the date range
            if x.time.size != 0:
                # Append the xarray to the list
                xarray_list.append(x)
            del x
        except Exception as e:
            print(f"File: {file} produced errors: {e}")

    # Merge all xarrays in the list
    merged_xarray = xr.concat(xarray_list, dim="time")

    return merged_xarray


if __name__ == "__main__":
    """
    Load xarrays
    """

    import rioxarray
    import geopandas
    from shapely.geometry import mapping

    geodf = geopandas.read_file('../../data/Wales_ctry_2022/wales_ctry_2022.shp', crs="epsg:27700")
    xds = xr.open_dataarray('../../data/tasmax_hadukgrid_uk_1km_day_19930501-19930531.nc')
    xds.rio.set_spatial_dims(x_dim="projection_x_coordinate", y_dim="projection_x_coordinate", inplace=True)

    xds.rio.write_crs('epsg:27700',inplace=True)
    xds.rio.clip(geodf.geometry.apply(mapping), geodf.crs, drop=False)


    test = load_hads(('1981','1983'),'tasmax')

    test.to_netcdf('output.nc')

    print (test.head())
