#!/bin/sh

declare -a vars=("tasmax" "tasmin")
declare -a runs=("05" "07" "08" "06")
declare -a cities=("Glasgow" "Manchester" "London")
declare -a methods=("quantile_delta_mapping" "quantile_mapping")
declare -a methods_2=("variance_scaling" "delta_method")


for run in "${runs[@]}"; do
  for city in "${cities[@]}"; do
    for method in "${methods[@]}"; do

      python run_cmethods.py --mod /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/pr/$run/latest/ --obs /mnt/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/rainfall/day/ --shape /mnt/vmfileshare/ClimateData/shapefiles/three.cities/$city/$city.shp -v rainfall --method $method -p 32 --out /mnt/vmfileshare/ClimateData/Debiased/three.cities/$city/$run/ --calib_dates 19810101-19811230 --valid_dates 20100101-20100330

      for var in "${vars[@]}"; do
        python run_cmethods.py --mod /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --obs /mnt/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/$var/day/ --shape /mnt/vmfileshare/ClimateData/shapefiles/three.cities/$city/$city.shp -v $var --method $method -p 32 --out /mnt/vmfileshare/ClimateData/Debiased/three.cities/$city/$run/ --calib_dates 19810101-19811230 --valid_dates 20100101-20100330
      done
    done

    for method in "${methods_2[@]}"; do

      python run_cmethods.py --mod /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/pr/$run/latest/ --obs /mnt/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/rainfall/day/ --shape /mnt/vmfileshare/ClimateData/shapefiles/three.cities/$city/$city.shp -v rainfall --method $method --group time.month -p 32 --out /mnt/vmfileshare/ClimateData/Debiased/three.cities/$city/$run/ --calib_dates 19810101-19811230 --valid_dates 20100101-20100330

      for var in "${vars[@]}"; do
        python run_cmethods.py --mod /mnt/vmfileshare/ClimateData/Reprojected_infill/UKCP2.2/$var/$run/latest/ --obs /mnt/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/$var/day/ --shape /mnt/vmfileshare/ClimateData/shapefiles/three.cities/$city/$city.shp -v $var --method $method --group time.month -p 32 --out /mnt/vmfileshare/ClimateData/Debiased/three.cities/$city/$run/ --calib_dates 19810101-19811230 --valid_dates 20100101-20100330
      done
    done

  done
done
