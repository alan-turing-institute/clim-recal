#!/bin/bash

# Default path (update for your system)
# Linux VM
default_path="/mnt/vmfileshare/ClimateData/Raw/UKCP2.2"

# Mac laptop
# default_path="/Volumes/vmfileshare/ClimateData/Raw/UKCP2.2"

# Use the provided path or the default path if none is given
path_to_search="${1:-$default_path}"

files=`find $path_to_search -type f -name "*.nc"` # Find all netCDF files in the specified or default directory
parallel --tag ./process_cpm_single_wrapper.sh {} ::: $files # Run reproject_one.sh on each file in parallel
# parallel echo {} ::: $files

# for f in $files; do
#     echo $f
#     ./process_cpm_single_wrapper.sh $f
# done