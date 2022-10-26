# README

## Overview

Milvus 2.2 has changed the meta structure for segment index. To upgrade a Milvus cluster of 2.1.x version you have installed, run this script to migrate the meta and upgrade the Milvus image version.


> Note: 
> 1. This script only applies to Milvus installed on a K8s cluster.
> 2. This script only supports upgrading from Milvus v2.1.x to v2.2.0.

## Parameters

| Parameters   | Description                                                      | Default value                    | Required                |
| ------------ | ---------------------------------------------------------------- | ---------------------------- | ----------------------- |
| `i`          | The Milvus instance name.                                 | `None`                         | True                    |
| `n`          | The namespace that Milvus is installed in.                | `default`                      | False                   |
| `s`          | The source Milvus version.                                | `None`                         | True                    |
| `t`          | The target Milvus version.                               | `None`                         | True                    |
| `r`          | The root path of Milvus meta.                             | `by-dev`                       | False                   |
| `w`          | The new Milvus image tag.                                 | `milvusdb/milvus:v2.2.0`       | False                   |
| `m`          | The meta migration image tag.                             | `harbor.milvus.io/milvus/meta-migration:20221025-e54b6181b`       | False                   |
| `o`          | The meta migration operation.                             | `migrate`                      | False                   |
| `d`          | Whether to delete migration pod after the migration is completed.          | `false`                        | False                   |
> By default, the script only applies to migration from v2.1.x to v2.2.x. Rollback to the older version with the `rollback` operation first if an error occurs.


## Overview of migration procedures
1. Stop the Milvus components. Any live session in the Milvus Etcd can cause the migration to fail. 
2. Create a backup for Milvus meta.
3. Migrate the Milvus meta.
4. Start Milvus components with a new image.


## Migration guide

1. Specify Milvus instance name, source Milvus version, and target Milvus version.

```shell
./migrate.sh -i my-release -s 2.1.1 -t 2.2.0
```

2. Specify namespace with `-n` if your Milvus is not installed in the default K8s namespace.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0
```

3. Specify rootpath with `-r` if your Milvus is installed with the custom `rootpath`.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev
```

4. Specify the image tag with `-w` if your Milvus is installed with custom `image`.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev -w milvusdb/milvus:master-20221016-15878781
```

5. Set `-d true` if you want to automatically remove the migration pod after the migration is completed.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -w milvusdb/milvus:master-20221016-15878781 -d true
```

6. Rollback and migrate again if the migration fails.

```
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev -o rollback -w <milvus-2-1-1-image>
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev -o migrate -w <milvus-2-2-0-image>
```
