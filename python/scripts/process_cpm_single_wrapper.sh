f=$1 # The first argument is the file to reproject
file_new=${f/Raw/processed_2024_08_30} # Replace Raw with Reprojected_infill in the filename
folder=`dirname $file_new` # Get the folder name
mkdir -p $folder # Create the folder if it doesn't exist
echo "starting output: $file_new"
python process_cpm_single_file.py --cpm-file $f --output-dir $folder
