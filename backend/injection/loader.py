
# which is used to handle the data and
# reusable code which is separate from the task

# loading the data

# parsing the data

# structure the data and load the data
# using injection files

# parser: parser is a python method
# which is used to convert the raw
# data into structured data based
# on the schema which we have
# defined (translator)

# without parser:
# no columns are defined
# no filtering is done
# no anomaly detection or aggregation

# with parsing:
# we can filter error logs
# detect the unusual patterns

# it converts raw log data to
# machine readable language

import dask.bag as db
from injection.parser import parse_log_line
from schema.schema import LOG_SCHEMA

def load_logs(file_path):
    bag = db.read_text(file_path)

    parsed = (
        bag.map(parse_log_line)
           .filter(lambda x: x is not None)   # Filter out lines that failed to parse
    )

    df = parsed.to_dataframe()
    return df.astype(LOG_SCHEMA)