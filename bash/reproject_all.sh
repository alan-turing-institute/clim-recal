files=`find /mnt/vmfileshare/ClimateData/Raw/UKCP2.2/ -type f -name "*.nc"`
parallel reproject_one.sh {} ::: $files