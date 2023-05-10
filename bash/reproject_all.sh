files=`find /Volumes/vmfileshare/ClimateData/Raw/UKCP2.2/ -type f -name "*.nc"` # Find all netCDF files in the UKCP2.2 directory
parallel ./reproject_one.sh {} ::: $files # Run reproject_one.sh on each file in parallel
