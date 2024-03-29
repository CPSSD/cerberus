Command Line Interface
======================

**Version: 0.2.0**
This document outlines the command line interface options.

---

## Usage

### Global Options

The application has 1 global option.

* `--master`: This option is used to specify the master address of the
  MapReduce cluster. It is shared across all commands.

### Commands

There are 4 main Commands

* `run`: This command tells the master to perform a MapReduce job with the
  given binary and input directory. It has the following flags:
  * `--input`: This specifies the location of the input directory on the
    shared filesystem. If it is not provided the application will fail.
  * `--binary`: The user must specify the binary location that implements the
    `libcerberus` library. If the binary is not a libcerberus one, or is not
    found an error will be raised.
  * `--output`: Optional location where the output of the MapReduce will be
    placed. The location must also be accessible in the shared filesystem,
    otherwise an error will be raised. If the flag is not set, the output
    will match the following format: `$shared_location/$map_reduce_ID/output`.
  * `--priority`: Optional value from 1-10 giving a priority for the scheduled
    MapReduce job. The master will allocate the tasks for jobs with higher
    priority first. The default priority is 3.


Example:
```
$ cli --master=locahost:10456 run --input=/shared/shakespeare --binary=/shared/bin/shakespeare
Scheduling MapReduce....
MapReduce mr42 scheduled. Place in queue: 3

Run cli status --job_id=mr42 to get the status.
```

* `cluster_status`: This command gives basic information about the curent
  state that the cluster is in. Amongs the given information is the size of
  the working pool, as well as the queue size.

Example:
```
$ cli cluster_status
Getting Cluster Status...
Worker: 6
Queue:  3
```

* `status`: This command is used to get the status of the scheduled MapReduce.
  If a specific job ID is specified only one result will be returned,
  otherwise a list of MapReduce jobs scheduled by the client will be returned.
  It has 1 flag.
  * `--job_id`: This specifies the outlined above MapReduce ID.

Example:
```
$ cli status
|====================================|
| MRID    | mr42                     |
| Status  | SCHEDULED (3rd in queue) |
| Output  | /shared/mr42/output      |
|====================================|

$ cli status --job_id=mr13
|====================================|
| MRID    | mr13                     |
| Status  | COMPLETED (45m13s)       |
| Output  | /shared/super_secret     |
|====================================|
```

* `cancel`: This command cancels a running MapReduce job.
  It has 1 flag:
  * `--id`: This specifies the MapReduce ID of the job to cancel.

### Distributed Filesystem Commands

There are 2 additional commands that can be used when using DFS for storage
on the cluster.

* `upload`: This command uploads a file or directory to the cluster distributed
  filesystem. It has the following flags:
  * `--local_path`: Path of the file or directory on the local machine.
  * `--remote_path`: Path of the file or directory on the cluster.
    The local file path will be used if this is not provided.

* `download`: This command downloads a file or directory from the cluster
  distributed filesystem. It has the following flags:
  * `--remote_path`: Path of the file or directory on the cluster.
  * `--local_path`: Path of location to store the downloaded file or directory.
