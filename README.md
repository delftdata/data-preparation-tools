# Analyze Parquet files

`./parquet-to-csv.sh <source dir> <dest dir>`

Notes:
- \<source dir\>: path to source directory where the parquet files are stored
- \<dest dir\>: path to destination directory where the parquet files are stored. Destination directory should exist.

Example usage:

`./parquet-to-csv.sh ../parquet/physical_entity_tables ../csv/physical_entity_tables`

# Find similarity between column headers of CSV files

`./match-column-headers-csv.sh <source dir> <match threshold>`

Notes:
- \<source dir\>: path to source directory where the csv files are stored
- \<match threshold\>: two column headers are considered to match when their similarity is equal or over the provided threshold

Example usage:

`./match-column-headers-csv.sh ../csv/physical_entity_tables 70`

# Dependencies

- Python 3
- Python Library fuzzywuzzy
  - install with `pip install fuzzywuzzy[speedup]`
