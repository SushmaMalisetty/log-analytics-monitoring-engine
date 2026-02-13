
from backend.config.dask_config import create_dask_client

def main():
    # Create a Dask client
    client = create_dask_client()

    print(client)
    print(f"Dashboard: {client.dashboard_link}")

    # Now you can use the client to submit tasks to the Dask cluster
    # For example, you can use client.submit() to run a function on the cluster
    # result = client.submit(your_function, your_arguments)

    # Don't forget to close the client when you're done
    client.close()

if __name__ == "__main__":
    main()