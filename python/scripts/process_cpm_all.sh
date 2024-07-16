#!/bin/bash

# Default path (update for your system)
default_path="/mnt/vmfileshare/ClimateData/Raw/UKCP2.2"
default_path="/Volumes/vmfileshare/ClimateData/Raw/UKCP2.2"

# Use the provided path or the default path if none is given
path_to_search="${1:-$default_path}"

files=`find $path_to_search -type f -name "*.nc"` # Find all netCDF files in the specified or default directory
parallel ./process_cpm_single_wrapper.sh {} ::: $files # Run reproject_one.sh on each file in parallel
# for f in $files; do
#     echo $f
#     ./process_cpm_single_wrapper.sh $f
# done