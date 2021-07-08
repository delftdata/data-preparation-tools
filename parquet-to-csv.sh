#!/bin/bash

source_data_folder=$1
dest_data_folder=`pwd`/$2
current_dir=`pwd`

cd $source_data_folder
for file in `ls`;
do
	base_filename=`basename -- "$file" .parquet`
	if [ ! -f "$dest_data_folder/$base_filename.csv" ];
	then
		python3 $current_dir/parquet-to-csv.py $dest_data_folder $base_filename
		echo "Created file $dest_data_folder/$base_filename.csv"
	fi
done
