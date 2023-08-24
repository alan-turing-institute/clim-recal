#!/bin/sh

declare -a vars=("tasmax" "rainfall" "tasmin")
declare -a runs=("05" "07" "08" "06")
declare -a methods=("quantile_delta_mapping" "quantile_mapping" "variance_scaling" "delta_method")
declare -a cities=("Glasgow" "Manchester" "London")

for var in "${vars[@]}"; do
  for run in "${runs[@]}"; do
    for method in "${methods[@]}"; do
      for city in "${cities[@]}"; do
        python run_cmethods.py --scen /Volumes/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --contr /Volumes/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --obs /Volumes/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/$var/day/ --shape /Volumes/vmfileshare/ClimateData/shapefiles/three.cities/$city/$city.shp -v $var --method $method -p 5 --out ./debiasing_test/output/$city/$run/ --contr_dates 19800101-19801230 --scen_dates 20100101-20100330
      done
    done
  done
done
