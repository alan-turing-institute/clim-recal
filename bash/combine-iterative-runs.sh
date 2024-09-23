#!/bin/bash

# Run this script from the root of the `group_run_*` directory

combined_dir=./combined_output
mkdir -p $combined_dir

# Get all output directories
output_dirs=`find . -type d -name "run_*"`

for output_dir in $output_dirs; do

    # The trailling slash on the `$output_dir` is required!
    rsync \
      --recursive \
      --verbose \
      $output_dir/ \
      $combined_dir
done
