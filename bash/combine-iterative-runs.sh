#!/bin/bash

# Run this script from the root of the `group_run_*` directory

# For example, the directory for "nearest" interpolation method (eg `data-v1.0-rc.2`) is:
#   `/datadrive/clim-recal-results/group_run_2024-10-14-16-27``
# but this directory is temporary before publishing

combined_dir=./combined_output

# combined_dir=/mnt/vmfileshare/ClimateData/processed_2024_09_26/combined_output

mkdir -p $combined_dir

# Get all output directories
output_dirs=`find . -type d -name "run_*"`

for output_dir in $output_dirs; do

    # The trailling slash on the `$output_dir` is required!
    rsync \
      --recursive \
      --verbose \
      --ignore-existing \
      $output_dir/ \
      $combined_dir
done
