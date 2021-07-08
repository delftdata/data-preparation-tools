#!/bin/bash

source_data_folder=$1
match_threshold=$2
current_dir=`pwd`
log_file=examined_pairs_of_files.log

cd $source_data_folder

# Read already examined pairs from log file (resume after failure)
examined_pairs_of_files=()
pairs_flushed_to_log=0
if [ -f "$log_file" ];
then
	while IFS= read -r line;
	do
		examined_pairs_of_files+=($line)
	done <$log_file
	echo "Found and read log file $log_file:"
	printf '  %s\n' "${examined_pairs_of_files[@]}"
	pairs_flushed_to_log=${#examined_pairs_of_files[@]}
fi

for file in `ls`;
do
	for pair_file in `ls`;
	do
		# If we haven't examined this pair of files
		if [ $file != $pair_file ] && \
			! printf '%s\n' "${examined_pairs_of_files[@]}" | \
			egrep -q -- "^($file$pair_file|$pair_file$file)$";
		then
			# DEBUG: Uncomment commented out statements
			#echo
			#echo "Examined pairs: ${examined_pairs_of_files[@]}"
			examined_pairs_of_files+=($file$pair_file)
			#echo "Pair of files to examine: $file$pair_file"

			# Get column header from file
			file_header=`head -n 1 -- $file | tr -d " "`
			#echo "file header: $file_header"
			pair_file_header=`head -n 1 -- $pair_file | tr -d " "`
			#echo "pair file header: $pair_file_header"

			# Pass the two headers to the matcher for comparison
			python3 $current_dir/fuzzy_match.py "$file" \
				"$pair_file" $file_header $pair_file_header \
				$match_threshold
		fi
	done

	# Flush examined pairs to log file
	pairs_to_flush=$(( ${#examined_pairs_of_files[@]}-$pairs_flushed_to_log ))
	#echo
	#echo "Pairs to flush: $pairs_to_flush (${#examined_pairs_of_files[@]} - $pairs_flushed_to_log)"
	for pair in `printf '%s\n' "${examined_pairs_of_files[@]}" | tail -n $pairs_to_flush`;
	do
		echo "$pair" >>$log_file
		#echo "$pair"
		(( pairs_flushed_to_log++ ))
	done
done
