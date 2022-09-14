# README

## Overview

For better tracking and debugging Milvus, the script `export-milvus-log.sh` is provided for exporting all Milvus logs at once. For those pods that have been restarted, this script can export the logs of the running pods and the logs of the previously pods.

> Note: This script only works with Milvus installed with k8s.
>
> For Milvus installed with docker-compose, you can use `docker-compose logs > milvus.log` to export the logs.

## Parameter Description

| Parameters | Description                                       | Default      |
| ---------- | ------------------------------------------------- | ------------ |
| i          | Specify the milvus instance name                  | None         |
| n          | Specify the namespace that milvus is installed in | default      |
| p          | Specify the log storage path                      | ./milvus-log |
| e          | Export etcd logs                                  | false        |
| m          | Export Minio logs                                 | false        |
| u          | Export pulsar logs                                | false        |
| k          | Export Kafka logs                                 | False        |
| s          | Only return logs newer than a relative duration like 5s, 2m,or 3h. Defaults to all logs                                | all        |
> By default, the script only exports the logs of the Milvus component.
>
> If you need to export the logs of etcd, minio, and pulsar components, you need to add the parameters -e, -m, -u.

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

3. Export the logs of milvus, etcd, minio, and pulsar components.

```shell
./export-milvus-log.sh -i my-release -n milvus -p ./logs -e -m -u
```

4. Export the logs of milvus and Kafka components.

```
./export-milvus-log.sh -i my-release -n milvus -p ./logs -k
```

5. Export the logs for only latest 24h.

```
./export-milvus-log.sh -i my-release -s 24h
```

