import sys
from fuzzywuzzy import fuzz

file = sys.argv[1]
pair_file = sys.argv[2]
columns = sys.argv[3].split(",")
pair_columns = sys.argv[4].split(",")
match_threshold = sys.argv[5]
#print(sys.argv)

match_found = False

for col in columns:
    # Empty column; skip
    if len(col) == 0: continue

    for pair_col in pair_columns:
        if len(pair_col) == 0: continue

        match_ratio = fuzz.ratio(col, pair_col)
        if match_ratio >= int(match_threshold):
            if not match_found:
                print("{} <-> {} files matched:".format(file, pair_file))
                match_found = True
            print("  {} <-> {}".format(col, pair_col))
            #print("  {} <-> {}      ({})".format(col, pair_col, match_ratio))
