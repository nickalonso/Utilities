import gpudb
import os, glob
import pandas as pd
import numpy as np
import datetime
import random

# Establish connection with a locally-running instance of Kinetica using
# binary encoding to save memory
h_db = gpudb.GPUdb(
    encoding="BINARY",
    host="172.31.33.26",
    port="9191",
    username="<username>",
    password="<password>")

# Confirm target table exists
if h_db.has_table(table_name="stores")['table_exists']:
    print("Table successfully reached.")
else:
    print("Table not found.")

# Pull data from target and store as variable
data = h_db.get_records(
    table_name="stores",
    offset=0,
    limit=gpudb.GPUdb.END_OF_SET,
    encoding="binary")

# Hydrate pandas df, adding a datetime column as a start date for the desired month
store_df = pd.DataFrame(gpudb.GPUdbRecord.decode_binary_data(data["type_schema"], data["records_binary"]))
store_df['dates'] = pd.to_datetime("'2020-12-01'".replace("'",""))

# Function for random float generation given a range
def random_float(low,high):
    return random.random()*(high-low)+low

num = 4
for i in range(num):

    # Random demand generation and 7 day timedelta
    store_df['demand'] = np.random.randint(0, 4808, len(store_df))
    store_df['dates'] = store_df['dates'] + datetime.timedelta(days=7)

    # Random price per unit and margins using random float module
    store_df['avg_ppu'] = [random_float(3.7,9.0) for j in store_df.index]
    store_df['profit_margin'] = [random_float(.043, .084) for k in store_df.index]
    store_df.to_csv(str("_test_week") + str(i+1) + '.csv', index=False)

# Merge
extension = 'csv'

# Merging individual week projections to a single month
os.chdir("/Users/nickalonso/PycharmProjects/logistics-dev")
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

# Write combined pandas df to csv
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
combined_csv.to_csv("stores2020.csv", index=False, encoding='utf-8-sig')



















