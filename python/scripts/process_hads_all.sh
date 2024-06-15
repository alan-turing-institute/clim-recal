#!/bin/bash

# Default path (update for your system) (no trailing slash)
default_path="/mnt/vmfileshare/ClimateData/Raw/HadsUKgrid"
default_path="/Volumes/vmfileshare/ClimateData/Raw/HadsUKgrid"

# Use the provided path or the default path if none is given
path_to_search="${1:-$default_path}"

files=`find $path_to_search -type f -name "*.nc"` # Find all netCDF files in the specified or default directory
# parallel ./process_hads_single_wrapper.sh {} ::: $files # Run reproject_one.sh on each file in parallel
for f in $files; do
    echo $f
    ./process_hads_single_wrapper.sh $f
done