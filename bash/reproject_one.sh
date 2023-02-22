f=$1 # The first argument is the file to reproject
fn=${f/Raw/Reprojected} # Replace Raw with Reprojected in the filename
folder=`dirname $fn` # Get the folder name
mkdir -p $folder # Create the folder if it doesn't exist
gdalwarp -t_srs 'EPSG:27700' -tr 2200 2200 -r near -overwrite $f "${fn%.nc}.tif" # Reproject the file