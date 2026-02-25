


    # Now you can use the client to submit tasks to the Dask cluster
    # For example, you can use client.submit() to run a function on the cluster
    # result = client.submit(your_function, your_arguments)

    # Don't forget to close the client when you're done
    
from datetime import time
from config.dask_config import start_dask
from injection.loader import load_logs
from processing.pipeline import build_pipeline
from processing.pipeline import build_pipeline

def main():
    print("Starting Log Processing...")

    client = start_dask()
    print("Dask Started Successfully")

    df = load_logs("data/sample_log.log")
    print("Logs Loaded Successfully")

    print("\nFirst 5 Parsed Logs:")
    print(df.head())

    print("\nLog Count by Level:")
    result = df.count().compute()
    print(result)

    client.close()
    print("\nProcessing Finished Successfully!")

if __name__ == "__main__":
    main()