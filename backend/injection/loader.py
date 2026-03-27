#which is used to handle the data and reuseable code which is seperate from the dask 
#loading the data
#parsing the data
#structure the data and load the data using injection files
#parser: parser is a python method which is used to convert the raw data into structured data based on the schema which we have defined(translater)
#without parser:no columns are defined 
#no filtering is done
#no anamoloy is detection or no aggregation
#with parsing: we can filter error logs
#detect the unsual patterns
#it converts raw log data to the machine readable language
import dask.bag as db
import pandas as pd
from .parser import parse_log_line

def load_logs(file_path):
    bag = db.read_text(file_path)

    parsed = (
        bag.map(parse_log_line)
           .filter(lambda x: x is not None)
    )

    meta = pd.DataFrame({
        "timestamp": pd.Series(dtype="datetime64[ns]"),
        "level": pd.Series(dtype="string"),
        "service": pd.Series(dtype="string"),
        "message": pd.Series(dtype="string"),
    })

    df = parsed.to_dataframe(meta=meta)
    return df