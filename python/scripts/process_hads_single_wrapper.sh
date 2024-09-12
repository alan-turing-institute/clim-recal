f=$1 # The first argument is the file to reproject
fn=${f/Raw/processed_2024_june} # Replace Raw with Reprojected_infill in the filename
folder=`dirname $fn` # Get the folder name
mkdir -p $folder # Create the folder if it doesn't exist

# Example (adjust as needed):
referencefile="/home/jovyan/python/scripts/reference_data/pr_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.nc"

python process_hads_single_file.py --hads-file $f --output-dir $folder --reference-file $referencefile
