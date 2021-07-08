import pandas as pd
import sys

dest_folder = sys.argv[1]
filename = sys.argv[2]

df = pd.read_parquet(filename + '.parquet')
df.to_csv(dest_folder + "/" + filename + '.csv')
