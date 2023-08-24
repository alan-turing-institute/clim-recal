#!/bin/sh

declare -a vars=("rainfall" "tasmax" "tasmin")
declare -a runs=("05" "07" "08" "06")
declare -a cities=("Glasgow" "Manchester" "London")
declare -a methods=("quantile_delta_mapping" "quantile_mapping")
declare -a methods_2=("variance_scaling" "delta_method")

for var in "${vars[@]}"; do
  for run in "${runs[@]}"; do
    for city in "${cities[@]}"; do
      for method in "${methods[@]}"; do
        python run_cmethods.py --scen /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --contr /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --obs /mnt/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/$var/day/ --shape /mnt/vmfileshare/ClimateData/shapefiles/three.cities/$city/$city.shp -v $var --method $method -p 32 --out /mnt/vmfileshare/ClimateData/Debiased/three.cities/$city/$run/ --contr_dates 19800101-20091230 --scen_dates 20100101-20191230
      done

      for method in "${methods_2[@]}"; do
        python run_cmethods.py --scen /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --contr /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --obs /mnt/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/$var/day/ --shape /mnt/vmfileshare/ClimateData/shapefiles/three.cities/$city/$city.shp -v $var --method $method --group time.month -p 32 --out /mnt/vmfileshare/ClimateData/Debiased/three.cities/$city/$run/ --contr_dates 19800101-20091230 --scen_dates 20100101-20191230
      done
    done
  done
done
