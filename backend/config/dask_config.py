
#client (): Client is a python methods which is used to make a connection between dask cluster and python code which is mean that local machine
#Local cluster :It is a cluster used to develop and test or prove distributed computing locally before deploying into main cluster
#without client 
#we cannot know number of workers exist in cluster
#we cannot know what is task and where task is running
#we cannot know the status of the task
#we cannot monitor the process of execution
#with client
#we can asssign task to the workers
#we can easy connect python code with dask
#we can easyly see the status 
from dask.distributed import Client, LocalCluster
def create_dask_client():
    print("Creating Dask cluster...")

    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=2,
        memory_limit="1GB"
    )
    client = Client(cluster)   # IMPORTANT: Capital C
    print("Dask cluster created successfully!")
    return client

