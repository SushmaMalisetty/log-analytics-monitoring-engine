#Here we detect anamolies from log data by comparing current errors and normal behavior using stastics 

import dask.dataframe as dd

def detect_anamoly(log_df,z_threshold=3):
    log_df["timestamp"] = dd.to_datetime(log_df["timestamp"])
    
    #count error per minute, per service
    error_logs = log_df[log_df["level"]=="ERROR"]
    error_logs["minute"]  = error_logs["timestamp"].dt.floor("min")

    error_counts = (
        error_logs.groupby("minute").size().to_frame("error_count")
    )

    #Normal behavior
    mean = error_counts["error_count"].mean().compute()
    #suppose assume mean value as 20 the value will be stored in the key named error_count
    std = error_counts["error_count"].std().compute()

    if std == 0:
        return error_counts.compute()

    error_counts["z_score"] = (error_counts["error_count"] - mean) / std
    error_counts["is_anomaly"] = error_counts["z_score"].abs() > z_threshold

    return error_counts[error_counts["is_anomaly"]].compute()