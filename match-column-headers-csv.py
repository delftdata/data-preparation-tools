import sys
import os
import multiprocessing as mp
import pandas as pd
from fuzzywuzzy import fuzz

def read_input_file(file, q):
    data = pd.read_csv(file, nrows=0)
    columns = data.columns.tolist()
    q.put([file] + columns)

def build_index(index_file, index, q):
    while True:
        line = q.get()
        if line == "done":
            with open(index_file, 'w') as f:
                for file, columns in index.items():
                    f.write(file + ',' + ','.join(columns) + '\n')
            print("Wrote index to file " + index_file)
            break
        index[line[0]] = line[1:]

# Function runs in parallel executed by multiple processes
def match_pair(file, columns, pair_file, pair_columns, match_threshold, q):
    match_found = False
    for col in columns:
        # Empty column; skip
        if len(col) == 0 or 'Unnamed' in col: continue

        for pair_col in pair_columns:
            if len(pair_col) == 0 or 'Unnamed' in pair_col: continue

            match_ratio = fuzz.ratio(col, pair_col)
            if match_ratio >= match_threshold:
                if not match_found:
                    q.put("{} <-> {} files matched:".format(file, pair_file))
                    match_found = True
                q.put("  {} <-> {}".format(col, pair_col))
                # print("  {} <-> {}      ({})".format(col, pair_col,
                #       match_ratio))

# Single process writes to file to avoid concurrency control.
def writer(file, q):
    with open(file, 'a+') as f:
        while True:
            line = q.get()
            if line == "kill":
                break
            f.write(str(line + '\n'))
            f.flush()

def main():
    source_data_folder = sys.argv[1]
    match_threshold = int(sys.argv[2])
    output_file = sys.argv[3]
    parallelism = int(sys.argv[4])

    # Change working dir to where the data is
    os.chdir(source_data_folder)

    index_file = 'file_column_headers.index'

    manager = mp.Manager()
    q = manager.Queue()
    index = manager.dict()
    pool = mp.Pool(parallelism)

    # Build index
    if not os.path.exists(index_file):
        pool.apply_async(build_index, (index_file, index, q))

        jobs = []
        for file in os.listdir():
            job = pool.apply_async(read_input_file, (file, q))
            jobs.append(job)

        for i, job in enumerate(jobs):
            job.get()
            if i % 100 == 0:
                print("{} jobs completed for index build.".format(i))
        q.put("done")
    else:
        with open(index_file, 'r') as f:
            for line in f.readlines():
                tokenized_line = line.split(',')
                index[tokenized_line[0]] = tokenized_line[1:]
    print("Built in-memory index with " + str(len(index)) + " column headers.")

    # Try recover processing state from output file
    processing_recovery = False
    if os.path.exists(output_file):
        print("Try recover processing state..")
        for line in reversed(list(open(output_file))):
            if line.endswith(" files matched:\n"):
                tokenized_line = line.split()
                file = tokenized_line[0]
                recovered_pair_file = tokenized_line[2]
                if not file.endswith(".csv") and pair_file.endswith(".csv"):
                    print("Recovery failed: unexpected last pair of files encountered {} <-> {}".format(file, pair_file))
                    sys.exit(1)

                key_list = list(index.keys())
                remaining_keys = key_list[key_list.index(file):]
                print("Original index size: {}, index of file last matched: {}, remaining files to compare: {}".format(len(index), key_list.index(file), len(remaining_keys)))
                index = {k:v for k,v in index.items() if k in remaining_keys}
                processing_recovery = True
                print("Recovered processing state: {} <-> {}.".format(file, recovered_pair_file))
                break

    # Start writer first
    pool.apply_async(writer, (output_file, q))

    pair_index = index.copy()
    comparisons = len(index) * len(pair_index) / 2
    jobs = []
    for i, (file, columns) in enumerate(index.items()):
        for pair_i, (pair_file, pair_columns) in enumerate(pair_index.items()):
            if file == pair_file: continue
            if processing_recovery and pair_file <= recovered_pair_file: continue

            # Allocate work to processes from the process pool
            job = pool.apply_async(match_pair, (file, columns, pair_file,
                        pair_columns, match_threshold, q))
            jobs.append(job)

        # 'file' has been compared with all other files now. Remove it.
        # About warning: same key-value pairs are not copied over to pair_index.
        #       This is really strange. Probable cause: large number of columns.
        try:
            del pair_index[file]
        except KeyError:
            print("Warning: Key {} not found in index copy".format(file))

        # Iteration interrupted by failure just completed. Proceed normally.
        if processing_recovery:
            processing_recovery = False

    # Wait for job set to complete.
    for j, job in enumerate(jobs):
        job.get()
        if j % 1000 == 0:
            print("{} file comparisons completed (total: {})".format(j, comparisons))

    # Done. Kill the writer.
    q.put("kill")
    # Close the process pool
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()
