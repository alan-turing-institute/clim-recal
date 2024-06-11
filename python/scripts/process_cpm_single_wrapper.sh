f=$1 # The first argument is the file to reproject
file_new=${f/Raw/processed_2024_june} # Replace Raw with Reprojected_infill in the filename
folder=`dirname $file_new` # Get the folder name
# mkdir -p $folder # Create the folder if it doesn't exist
python process_cpm_single_file.py --dry-run --cpm-file $f --output-dir $folder
