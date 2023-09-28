#!/bin/sh

declare -a vars=("tasmax" "rainfall" "tasmin")
declare -a runs=("05" "07" "08" "06")
declare -a cities=("Glasgow" "Manchester" "London")
declare -a methods=("quantile_delta_mapping" "quantile_mapping")
declare -a methods_2=("variance_scaling" "delta_method")


for run in "${runs[@]}"; do
  for city in "${cities[@]}"; do
    for var in "${vars[@]}"; do

      python preprocess_data.py --mod /mnt/vmfileshare/ClimateData/Cropped/three.cities/CPM/$city --obs /mnt/vmfileshare/ClimateData/Cropped/three.cities/Hads.original360/$city -v $var -r $run --out /mnt/vmfileshare/ClimateData/Cropped/three.cities/Preprocessed/$city/$run/$var --calib_dates 19810101-19831230 --valid_dates 20100101-20101230

      for method in "${methods[@]}"; do
        python run_cmethods.py --input_data_folder /mnt/vmfileshare/ClimateData/Cropped/three.cities/Preprocessed/$city/$run/$var --out /mnt/vmfileshare/ClimateData/Debiased/three.cities.cropped/$city/$run --method $method --v $var -p 32
      done

      for method in "${methods_2[@]}"; do
        python run_cmethods.py --input_data_folder /mnt/vmfileshare/ClimateData/Cropped/three.cities/Preprocessed/$city/$run/$var --out /mnt/vmfileshare/ClimateData/Debiased/three.cities.cropped/$city/$run --method $method --group time.month --v $var -p 32
      done

    done
  done
done
