f=$1
fn=${f/Raw/Reprojected}
folder=`dirname $fn`
mkdir -p $folder
gdalwarp -t_srs 'EPSG:27700' -tr 2200 2200 -r near -overwrite $f "${fn%.nc}.tif"