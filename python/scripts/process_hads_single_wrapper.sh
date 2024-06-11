f=$1 # The first argument is the file to reproject
fn=${f/Raw/processed_2024_june} # Replace Raw with Reprojected_infill in the filename
folder=`dirname $fn` # Get the folder name
# mkdir -p $folder # Create the folder if it doesn't exist

# Example (adjust as needed):
referencefile="/Volumes/vmfileshare/ClimateData/processed_2024_june/UKCP2.2/tasmax/05/latest/tasmax_rcp85_land-cpm_uk_2.2km_05_day_19801201-19811130.nc"
/Users/a.smith/miniconda3/envs/clim-recal2/bin/python3.12 process_hads_single_file.py --dry-run --hads-file $f --output-dir $folder --reference-file $referencefile
