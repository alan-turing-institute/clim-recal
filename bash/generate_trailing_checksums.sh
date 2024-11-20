#!/bin/bash

# This script requires two arguments:
# - The directory of files to create checksums for. All "*.nc" file within this directory
# - The number of trailing bytes to use in teh checksum calculation (this is passed as an argument to `tail`)

set -e

manifest=manifest_last_bytes_$2.txt
offset=$2
echo 'file,checksum' > $manifest

for file in `find $1 -name "*.nc" -type f`; do
        echo $file
        checksum=`tail -c $offset $file | md5sum | cut -d ' ' -f1`
        echo "$file,$checksum" >> $manifest
done

tail -n +2 $manifest | sort | sudo tee $manifest
