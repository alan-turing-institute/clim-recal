#!/bin/bash

# Default path
default_path="/mnt/vmfileshare/ClimateData/Raw/UKCP2.2/"

# Use the provided path or the default path if none is given
path_to_search="${1:-$default_path}"

files=`find $path_to_search -type f -name "*.nc"` # Find all netCDF files in the specified or default directory
parallel ./reproject_one.sh {} ::: $files # Run reproject_one.sh on each file in parallel
