# Analyze Parquet files

`./parquet-to-csv.sh <source dir> <dest dir>`

Notes:
- `\<source dir\>`: path to source directory where the parquet files are stored
- `\<dest dir\>`: path to destination directory where the parquet files are stored. Destination directory should exist.

Example usage:

`./parquet-to-csv.sh ../parquet/physical_entity_tables ../csv/physical_entity_tables`

# Find similarity between column headers of CSV files

`./match-column-headers-csv.sh <source dir> <match threshold>`

Notes:
- `\<source dir\>`: path to source directory where the csv files are stored
- `\<match threshold\>`: two column headers are considered to match when their similarity is equal or over the provided threshold
- In the source dir you will also find a log file named `examined_pairs_of_files.log` that retains the progress of the comparisons and is used to resume from the current state after a failure.
- You can save the matches in an output file using output redirection: `\>matches.out`

Example usage:

`./match-column-headers-csv.sh ../csv/physical_entity_tables 70 >matches.out`

# Find similarity between column headers of CSV files (faster)

Equivalent implementation in full Python with multiprocessing support

`python match-column-headers-csv.py <source dir> <match threshold> <output file> <parallelism>`

Notes:
- `\<source dir\>`: path to source directory where the csv files are stored
- `\<match threshold\>`: two column headers are considered to match when their similarity is equal or over the provided threshold
- `\<output file\>`: file name (without path) where the matches will be stored. By default, the file will be saved in the `source dir`.
- `\<parallelism\>`: level of parallelism (think of number of CPU cores roughly) available to the program
- In the source dir you will also find a log file named `examined_pairs_of_files.log` that retains the progress of the comparisons and is used to resume from the current state after a failure.

Example usage:

`python match-column-headers-csv.py ../csv/physical_entity_tables 70 matches.out 4`

# Dependencies

- Python version 3 (should be the default of `python` command)
- Python Library fuzzywuzzy
  - install with `pip install fuzzywuzzy[speedup]`
