
#schema.py
#it ensures all rows follow the same structure of data
#helps to validate data
#faster processing,easier analytics(filtering,mapping,grouping and anomaly detection becomes simple and faster)

log_schema = {
    "timestamp": "datetime",
    "level": "string",
    "service": "string",
    "message": "string"
}

#astype(LOG_SCHEMA)