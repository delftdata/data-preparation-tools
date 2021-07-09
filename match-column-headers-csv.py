import sys
import os
import multiprocessing as mp
import pandas as pd
from fuzzywuzzy import fuzz

# Function runs in parallel executed by multiple processes
def match_pair(file, columns, pair_file, match_threshold, q):
    pair_data = pd.read_csv(pair_file, nrows=0)
    pair_columns = pair_data.columns.tolist()
    # print("{} vs {}".format(columns, pair_columns))

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

    log_file = 'examined_pairs_of_files.log'
    examined_pairs_of_files = []
    previously_examined_pairs_of_files = 0
    try:
        with open(log_file, 'r') as f:
            examined_pairs_of_files = [line.rstrip('\n') for line in f.readlines()]
    except FileNotFoundError:
        pass
    previously_examined_pairs_of_files = len(examined_pairs_of_files)
    # print(examined_pairs_of_files)
    # print(previously_examined_pairs_of_files)

    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(parallelism)

    # Start writer first
    pool.apply_async(writer, (output_file, q))

    for file in os.listdir():
        data = pd.read_csv(file, nrows=0)
        columns = data.columns.tolist()

        jobs = []
        for pair_file in os.listdir():
            if not file == pair_file and \
                    file + pair_file not in examined_pairs_of_files and \
                    pair_file + file not in examined_pairs_of_files:
                        # print("{} vs {}".format(file, pair_file))
                        examined_pairs_of_files.append(file + pair_file)

                        # Allocate work to processes from the process pool
                        job = pool.apply_async(match_pair, (file, columns, pair_file,
                            match_threshold, q))
                        jobs.append(job)

        # Wait for job set to complete.
        for job in jobs:
            job.get()

        with open(log_file, 'a') as f:
            for line in examined_pairs_of_files[previously_examined_pairs_of_files:]:
                f.write(line + '\n')
                # print("Append pair to log " + line)
        previously_examined_pairs_of_files = len(examined_pairs_of_files)

    # Done. Kill the writer.
    q.put("kill")
    # Close the process pool
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()
