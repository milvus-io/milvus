# README

## Overview

Milvus 2.2.3 supports rolling update. This script helps you to perform a rolling update with zero downtime. 


> Note: 
> 1. Milvus version must be after 2.2.0.
> 2. This script only applies to update Milvus installed with Helm and does not apply to Milvus installed with Milvus Operator.
> 3. This script only supports update operation now.
> 4. Rolling update is **not supported** in Milvus standalone with **RocksMQ**.

## Parameters


| Parameters   | Description                                               | Default value                    | Required                |
| ------------ | ----------------------------------------------------------| -------------------------------- | ----------------------- |
| `i`          | The Milvus instance name.                                 | `None`                           | True                    |
| `n`          | The namespace that Milvus is installed in.                | `default`                        | False                   |
| `t`          | The target Milvus version.                                | `None`                           | True                    |
| `w`          | The new Milvus image tag.                                 | `milvusdb/milvus:v2.2.3`         | True                    |
| `o`          | The operation.                                            | `update`                         | False                   |


## Overview of update procedures
1. Check all deployments of the Milvus instance. Make sure no deployment is blocked.
2. Update the deployments one by one. The update order is hard-coded for now.
3. Specify the namespace, Milvus instance name, target Milvus version, and target image in the script. 
4. Run the script. The following script updates Milvus to 2.2.3.

```shell
sh rollingUpdate.sh -n default -i my-release -o update -t 2.2.3 -w 'milvusdb/milvus:v2.2.3'
```

> Note
> 1. This script update the deployment by `kubectl patch` and watch the deployment's status by `kubectl rollout status`.
> 2. The `target version` is the deployment's label `app.kubernetes.io/version` and is updated by `kubectl patch`.



