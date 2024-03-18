# Creating a new workflow

## Relevant Components
In addition to the core LUTE package, a number of components are generally involved to run a workflow. The current set of scripts and objects are used to interface with Airflow, and the SLURM job scheduler. The core LUTE library can also be used to run workflows using different backends, and in the future these may be supported.

For building and running workflows using SLURM and Airflow, the following components are necessary, and will be described in more detail below:
- Airflow launch script: `launch_airflow.py`
- SLURM submission script: `submit_slurm.sh`
- Airflow operators:
  - `JIDSlurmOperator`

## Launch Scripts
## `launch_airflow.py`
Sends a request to an Airflow instance to submit a specific DAG. This script prepares an HTTP request with the appropriate parameters in a specific format.

A request involves the following information:
```py
dag_run_data: Dict[str, Union[str, Dict[str, Union[str, int, List[str]]]]] = {
    "dag_run_id": str(uuid.uuid4()),
    "conf": {
        "experiment": os.environ.get("EXPERIMENT"),
        "run_id": f"{os.environ.get('RUN_NUM')}{datetime.datetime.utcnow().isoformat()}",
        "JID_UPDATE_COUNTERS": os.environ.get("JID_UPDATE_COUNTERS"),
        "ARP_ROOT_JOB_ID": os.environ.get("ARP_JOB_ID"),
        "ARP_LOCATION": os.environ.get("ARP_LOCATION", "S3DF"),
        "Authorization": os.environ.get("Authorization"),
        "user": getpass.getuser(),
        "lute_params": params,
        "slurm_params": extra_args,
    },
}
```

## `submit_slurm.sh`

## Operators
### `JIDSlurmOperator` arguments
- `task_id`: This is nominally the name of the task on the Airflow side. However, for simplicity this is used 1-1 to match the name of a **managed** Task defined in LUTE's `managed_tasks.py` module. I.e., it should the name of an `Executor("Task")` object which will run the specific Task of interest. This **must** match the name of a defined managed Task.
- `max_cores`: Used to cap the maximum number of cores which should be requested of SLURM. By default all jobs will run with the same number of cores, which should be specified when running the `launch_airflow.py` script (either from the ARP, or by hand). This behaviour was chosen because in general we want to increase or decrease the core-count for all Tasks uniformly, and we don't want to have to specify core number arguments for each job individually. Nonetheless, on occassion it may be necessary to cap the number of cores a specific job will use. E.g. if the default value specified when launching the Airflow DAG is multiple cores, and one job is single threaded, the core count can be capped for that single job to 1, while the rest run with multiple cores.
