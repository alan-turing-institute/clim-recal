f=$1
fn=${f/Raw/Reprojected}
folder=`dirname $fn`
mkdir -p $folder
gdalwarp -t_srs 'EPSG:27700' $f "${f/Raw/Reprojected}"