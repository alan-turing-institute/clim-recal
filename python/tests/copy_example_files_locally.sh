#!/bin/bash
# This ensure that there are some example files in the data directory for testing
# By using rsync, is a local file is corrupted, it can be easily updated, without needing to re-download the whole file.
this_path=$(dirname $0)
echo $this_path

# Even this much directory structure could be more complex than is strictly needed
mkdir -p $this_path/data/cpm
mkdir -p $this_path/data/hads

# Example raw CPM file
rsync \
    /Volumes/vmfileshare/ClimateData/Raw/UKCP2.2/tasmax/05/latest/tasmax_rcp85_land-cpm_uk_2.2km_05_day_19811201-19821130.nc \
    $this_path/data/cpm/example-cpm-tasmax_rcp85_land-cpm_uk_2.2km_05_day_19811201-19821130.nc

# Example raw HADs file
rsync \
    /Volumes/vmfileshare/ClimateData/Raw/HadsUKgrid/tasmax/day/tasmax_hadukgrid_uk_1km_day_19940101-19940131.nc \
    $this_path/data/hads/example-hads-tasmax_hadukgrid_uk_1km_day_19940101-19940131.nc
