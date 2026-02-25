


# client(): Client is a Python class used to make a connection
# between the Dask cluster and Python code (local machine).

# LocalCluster: It is a cluster used to develop and test
# distributed computing locally before deploying to a main cluster.

# Without Client:
# - We cannot know the number of workers in the cluster.
# - We cannot know what task is running.
# - We cannot monitor the execution process.
# - We cannot see the status of Dask.

# With Client:
# - We can assign tasks to workers.
# - We can monitor task execution.
# - We can easily connect Python code with Dask.
# - We can see the status of Dask.

from dask.distributed import LocalCluster,Client
def create_dask_client():
    cluster = LocalCluster(
        n_workers=4,            # Number of workers in the cluster
        threads_per_worker=2,   # Number of threads per worker
        memory_limit="16GB" ,    # Memory limit for each worker
        dashboard_address = ".8790"
    )

    
    return Client(cluster)

# Backwards-compatible alias expected by some modules
start_dask = create_dask_client