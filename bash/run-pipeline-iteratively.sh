#!/bin/bash
set -e
set -x

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Script dir: $script_dir"


# Input and output paths
hads_input_path="/datadrive/HadsUKgrid/"
cpm_input_path="/datadrive/UKCP2.2/"
output_path="/datadrive/clim-recal-results/group_run_`date +%F-%H-%M`"

# Other values used in local development
# hads_input_path="/Volumes/vmfileshare/ClimateData/Raw/HadsUKgrid"
# hads_input_path="$script_dir/example_files/HadsUKgrid"
# cpm_input_path="/Volumes/vmfileshare/ClimateData/Raw/UKCP2.2"
# cpm_input_path="$script_dir/example_files/UKCP2.2"
# output_path="$script_dir/clim-recal-results/group_run_`date +%F-%H-%M`"

log_path="$output_path/logs"

# Temporary directories which will hold one year of data at a time
hads_working_dir="$output_path/working/HadsUKgrid"
cpm_working_dir="$output_path/working/UKCP2.2"


mkdir -p $hads_working_dir
mkdir -p $cpm_working_dir
mkdir -p $log_path


cpm_start_year=1982
cpm_end_year=1982
# cpm_end_year=1982

# First and last year that we have CPM data for
for year in $(seq $cpm_start_year $cpm_end_year); do
  echo "Running for year={$year}"

  # Including `1201` in the filter, guarantees that we only match on the
  # start year for each file, not the end year.
  cpm_filter="*_${year}1201-*.nc"
  # cpm_filter="*_198?1201-*.nc"

  # Copy the relevant CPM files into the working directory
  # These options:
  # 1. Maintain the directory structure
  # 2. Include only the files that match the current year filter
  # 3. Exclude all other files
  rsync \
    --include="$cpm_filter" \
    --filter='-! */' \
    --recursive \
    --delete-excluded \
    $cpm_input_path \
    $cpm_working_dir

  # Copy the HADS files into the working directory
  hads_filter="*_${year}??01-*.nc"

  rsync \
    --include="$hads_filter" \
    --filter='-! */' \
    --recursive \
    --delete-excluded \
    $hads_input_path \
    $hads_working_dir

  {
    clim-recal \
    --hads-input-path $hads_working_dir \
    --cpm-input-path $cpm_working_dir \
    --output-path $output_path \
    --all-variables \
    --all-regions \
    --run 01 \
    --run 05 \
    --run 06 \
    --run 07 \
    --run 08 \
    --execute
   } 2>&1 | tee $log_path/log_$year.txt

  # Delete extraneous crop files
  find $output_path -type d -name 'run_*' | xargs -I {} python $script_dir/remove-extra-cropfiles.py {} --I-am-really-sure-I-want-to-delete-lots-of-files

done
