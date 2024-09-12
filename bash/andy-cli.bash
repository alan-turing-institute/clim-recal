#!/bin/bash
set -e
set -x

# Start-index goes from 1
max_index=500

# Input and output paths
hads_input_path="/datadrive/HadsUKgrid/"
cpm_input_path="/datadrive/UKCP2.2/"
output_path="/datadrive/clim-recal-results/group_run_2024_09_11_1500"

for i in $(seq 1 $max_index); do
  echo "Running for index={$i}"
  {
    clim-recal \
    --start-index $i \
    --total-from-index 1 \
    --hads-input-path $hads_input_path \
    --cpm-input-path $cpm_input_path \
    --output-path $output_path \
    --execute
   } 2>&1 | tee /datadrive/clim-recal-results/group_run_2024_09_12_1000/logs/log_$i.txt

done
