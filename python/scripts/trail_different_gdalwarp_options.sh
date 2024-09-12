this_path=$(dirname $0)
echo $this_path
outdir=$this_path/trail_reprojection_outputs
mkdir -p $outdir

source_cpm=$this_path/../tests/data/cpm/tasmax_rcp85_land-cpm_uk_2.2km_05_day_19811201-19821130_cpm_example.nc
echo $source_cpm


# All of these options produce netCDF files which align correctly with the vector boundary files and the HADs data.
# Oddly when the resulting files are opened in QGIS, QGIS does not automatically recognise the CRS of the files, but if the user manually specifies the CRS, they align with other files
#
# The `-oo` parameters are passed to the NetCDF driver for input files. (It is also possible to use the `-doo` parameter to pass options to the NetCDF driver for output files.
# The `VARIABLES_AS_BANDS=YES` option gets past the error message: "ERROR 1: Input file <filename> has no raster bands"
# The `-oo GDAL_NETCDF_VERIFY_DIMS=STRICT` option prevents the driver mistakenly using the 'longitude' and 'latitude' dimensions instead of the "grid_longitude","grid_latitude" dimensions (which is what causes the results where the data ends up on the west coast of Africa). 
#
# I have not tested how will the time dimension is handled by these options. If needed this might be improved by using the `-doo` options and creation options described here https://gdal.org/drivers/raster/netcdf.html#raster-netcdf


# # Used with GDAL 3.8.3, released 2024/01/04
# gdalwarp --version

# # Reproject - No regridding
# gdalwarp -t_srs 'EPSG:27700' -r near $source_cpm $outdir/cpm_27700_no_regrid.nc -oo VARIABLES_AS_BANDS=YES -oo GDAL_NETCDF_VERIFY_DIMS=STRICT

# # Reproject - with regridding
# gdalwarp -t_srs 'EPSG:27700' -tr 2200 2200 -r near $source_cpm $outdir/cpm_27700_regridded.nc -oo VARIABLES_AS_BANDS=YES -oo GDAL_NETCDF_VERIFY_DIMS=STRICT

# # Transform - Just rotate axis to a sphere
# gdalwarp -t_srs 'ESRI:104047' -r near $source_cpm $outdir/cpm_104047_no_regrid.nc -oo VARIABLES_AS_BANDS=YES -oo GDAL_NETCDF_VERIFY_DIMS=STRICT

# # Minimal reprojection - just to WGS1984
# gdalwarp -t_srs 'EPSG:4326'  -r near -r near $source_cpm $outdir/cpm_4326_no_regrid.nc -oo VARIABLES_AS_BANDS=YES -oo GDAL_NETCDF_VERIFY_DIMS=STRICT


# # # Reproject via a virtual layer
# # THIS IS SLLOOOWWWWW (~2.5 hours)
# # But the output is correct
# # - Algins correctly with the vector boundary files and the HADs data
# # - CRS is automatically reckonised by QGIS.
# # - Bands appear in the same ways as the original file
gdalwarp -t_srs 'EPSG:27700' -tr 2200 2200 -r near -of VRT  $source_cpm $outdir/tempfile.vrt -oo VARIABLES_AS_BANDS=YES -oo GDAL_NETCDF_VERIFY_DIMS=STRICT
gdal_translate -of netCDF $outdir/tempfile.vrt $outdir/two_step_cpm_27700_regridded.nc


# Now try doing the temporal resampling, on the reprojected file
python process_cpm_single_file.py --cpm-file $outdir/two_step_cpm_27700_regridded.nc --output-path $outdir/cpm_27700_regridded_365_days.nc
