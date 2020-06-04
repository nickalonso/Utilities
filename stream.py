import gpudb
import collections
import time
import pandas as pd
pd.options.display.max_columns = 100
pd.set_option('display.width', 10000)

# init
TABLE = "risk_inputs"
COLLECTION = "RISK"
NEW_TABLE = "bs_stream"
HOST = "<ipaddress>"
ENCODING = "binary"
PORT = "9191"
DATA_PACK = 1
INGEST_FREQ = 3

"Execute python scripts on Kinetica servers using"
"/opt/gpudb/bin/gpudb_python"

# Establish connection to database with necessary credentials
# Pull data from Kinetica and put it directly into a Pandas df
h_db = gpudb.GPUdb(
    encoding=ENCODING,
    host=HOST,
    port=PORT,
    username="<username>",
    password="<password>")

if h_db.has_table(table_name=TABLE)['table_exists']:
    print("Table successfully reached.")
else:
    print("Table not found.")

# Pull data from Kinetica and put it directly into a Pandas df
data = h_db.get_records(table_name=TABLE,offset=0,limit=gpudb.GPUdb.END_OF_SET,encoding=ENCODING)
df = pd.DataFrame(gpudb.GPUdbRecord.decode_binary_data(data["type_schema"], data["records_binary"]))

# Column instantiation for the target table
columns = [
    ["symbol","string"],
    ["spot_price","float"],
    ["option_type","string"],
    ["exposure","string"],
    ["strike_price","float"],
    ["maturity_y","int"],
    ["maturity_m","int"],
    ["maturity_d","int"],
    ["calendar","string"],
    ["day_count","string"],
    ["risk_free_rate","float"],
    ["dividend_rate","float"],
    ["calc_dt_y","int"],
    ["calc_dt_m","int"],
    ["calc_dt_d","int"],
    ["volatility","float"]
]

# Clear the table at run time the create the table
no_error_option = {"no_error_if_not_exists": "true"}
h_db.clear_table(table_name=NEW_TABLE, options=no_error_option)
collection_option_object = gpudb.GPUdbTableOptions.default().collection_name(COLLECTION)
print("Table cleared")

try:
    table_gps_obj = gpudb.GPUdbTable(
        columns,
        NEW_TABLE,
        collection_option_object,
        h_db
    )
    print("Table created succesfully")
except gpudb.GPUdbException as e:
    print("Table creation failure: {}".format(str(e)))
print(df.head(5))

index = 0
h_db = gpudb.GPUdb(encoding=ENCODING,host=HOST,port=PORT)
# Implement the GpuDB table class instead of manual JSON
my_type = """
{
  "type": "record",
  "name": "type_name",
  "fields": [
             {"name": "symbol","type": "string"},
             {"name": "spot_price","type": "float"},
             {"name": "option_type","type": "string"},
             {"name": "exposure","type": "string"},
             {"name": "strike_price","type": "float"},
             {"name": "maturity_y","type": "int"},
             {"name": "maturity_m","type": "int"},
             {"name": "maturity_d","type": "int"},
             {"name": "calendar","type": "string"},
             {"name": "day_count","type": "string"},
             {"name": "risk_free_rate","type": "float"},
             {"name": "dividend_rate","type": "float"},
             {"name": "calc_dt_y","type": "int"},
             {"name": "calc_dt_m","type": "int"},
             {"name": "calc_dt_d","type": "int"},
             {"name": "volatility","type": "float"}         
            ]
}""".replace('\n', '').replace(' ', '')

def stream_ingest(df):
    """This method parses the df and inserts the data into Kinetica row by row
    with a 3 second delay in between rows"""
    global index

    i=0
    coords= []
    datum = collections.OrderedDict()
    for index, row in df.iterrows():
        datum["symbol"]=str(df.iloc[index,0])
        datum["spot_price"]=float(df.iloc[index,1])
        datum["option_type"] = str(df.iloc[index, 4])
        datum["exposure"] = str(df.iloc[index, 6])
        datum["strike_price"] = float(df.iloc[index, 7])
        datum["maturity_y"] = int(df.iloc[index, 8])
        datum["maturity_m"] = int(df.iloc[index, 9])
        datum["maturity_d"] = int(df.iloc[index, 10])
        datum["calendar"] = str(df.iloc[index, 11])
        datum["day_count"] = str(df.iloc[index, 12])
        datum["risk_free_rate"] = float(df.iloc[index, 13])
        datum["dividend_rate"] = float(df.iloc[index, 14])
        datum["calc_dt_y"] = int(df.iloc[index, 15])
        datum["calc_dt_m"] = int(df.iloc[index, 16])
        datum["calc_dt_d"] = int(df.iloc[index, 17])
        datum["volatility"] = float(df.iloc[index, 18])
        coords.append(h_db.encode_datum(my_type, datum))

        i= i + 1
        # Pump data in batches
        if i % DATA_PACK == 0:
            response = h_db.insert_records(
                table_name=NEW_TABLE,
                data=coords,
                list_encoding=ENCODING,
                options={})
            coords = []
            time.sleep(INGEST_FREQ)
            print(response)

    # Flush the last batch
    if i % DATA_PACK != 0:
        response = h_db.insert_records(
            table_name=NEW_TABLE,
            data=coords,
            list_encoding=ENCODING,
            options={})

        # 3 second delay to mimic real time ingest
        time.sleep(INGEST_FREQ)
        print(response)
    return coords

if __name__ == "__main__":
    stream_ingest(df)
