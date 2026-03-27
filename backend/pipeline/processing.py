#why pipeline?
#this pipeline injects the raw log data using dask bag. parse them into structured data with the help of dask data frame, we can filter the invalid log entries and converts into data frame using meta data.

#why meta data?
#dask cannot guess the column type automatically , we tell dask what is key and what is the value(passing key and value pairs) like what are column names and data types of the particular column.

#why objects?
#raw log data are strings intially ,which are in text files, using objects it is safer browser for better understanding


# example
# meta_data = {
#     # pandas/dask expects a valid dtype; use datetime64 for timestamp
#     "timestamp": "datetime64[ns]", 
#     "level" : "string", 
#     "service" : "string", 
#     "message" : "string",
# }

import dask.bag as db
import dask.dataframe as dd
from backend.injection.parser import parse_log_line

def process_pipeline(file_path):

    # Read as Bag
    bag = db.read_text(file_path)

    # Parse
    parsed = bag.map(parse_log_line)

    # Remove failed parses
    parsed = parsed.filter(lambda x: x is not None)

    # Metadata for DataFrame
    meta = {
        "timestamp": "datetime64[ns]",
        "level": "string",
        "service": "string",
        "message": "string",
    }

    # Convert to DataFrame
    df = parsed.to_dataframe(meta=meta)

    return df