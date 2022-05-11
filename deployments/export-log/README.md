# README

## Overview

For better tracking and debugging Milvus, the script `export-milvus-log.sh` is provided for exporting all Milvus logs at once. For those pods that have been restarted, this script can export the logs of the running pods and the logs of the previously pods.

## Parameter Description

| Parameters | Description                                       | Default      |
| ---------- | ------------------------------------------------- | ------------ |
| i          | Specify the milvus instance name                  | None         |
| n          | Specify the namespace that milvus is installed in | default      |
| p          | Specify the log storage path                      | ./milvus-log |

## Usage

1. Milvus instance name is required to be specified

```shell
./export-milvus-log.sh -i my-release
```

> This command will generate a directory named milvus-log in the current directory.
> For a pod that have not been restarted, the command will generate a log named ${podname}.log for the pod and store it in `milvus-log`.
> For a pod that has been restarted, this command will generate a log named ${podname}.log and a log ${podname}-pre.log for the pod.

2. If your milvus is not installed in the k8s default namespace, please specify namespace with `-n`. You can also customize the log storage path with `-p`.

```shell
./export-milvus-log.sh -i my-release -n milvus -p ./logs
```

