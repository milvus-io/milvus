# README

## Overview

Milvus 2.2 has changed the meta structure for segment index. If you have installed a milvus cluster of version v2.1.x, you'd run this script to migrate the meta and upgrade the milvus image version.

> Note: 
> 1. This script only works with Milvus installed on k8s cluster.
> 2. You can only upgrade from v2.1.x to v2.2.

## Parameter Description

| Parameters | Description                                                      | Default                      | Required                |
| ---------- | ---------------------------------------------------------------- | ---------------------------- | ----------------------- |
| i          | Specify the milvus instance name                                 | None                         | true                    |
| n          | Specify the namespace that milvus is installed in                | default                      | false                   |
| s          | Specify the milvus source version                                | None                         | true                    |
| t          | Specify the milvus target version                                | None                         | true                    |
| r          | Specify the milvus meta root path                                | by-dev                       | false                   |
| w          | Specify the milvus new image tag                                 | milvusdb/milvus:v2.2.0       | false                   |
| o          | Specify the meta migration operation                             | migrate                      | false                   |
| d          | Whether delete migration pod after successful migration          | false                        | false                   |
> By default, the script only migrate from v2.1.x to v2.2.x. If there is anything wrong, you'd first rollback to the older version using the `rollback` operation.

## Migrate Procedures
The migration will take four steps:
1. Stop the milvus components; if there are any live session in milvus etcd, the migration will fail.
2. Backup the milvus meta;
3. Migrate the milvus meta;
4. Startup milvus components with provided new image;

> Note:
> 1. if step 1) or 2) are failed, you can rerun the migration again
> 2. if step 3) or 4) are failed, you'd first rollback and migrate again

## Usage

1. Milvus instance name, milvus source version, milvus target vresion are required to be specified.

```shell
./migrate.sh -i my-release -s 2.1.1 -t 2.2.0
```

2. If your milvus is not installed in the k8s default namespace, please specify namespace with `-n`.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0
```

3. If your milvus is installed with custom `rootpath`, please specify rootpath with `-r`.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev
```

4. If your milvus is installed with custom `image`, please specify the image tag with `-w`.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev -w milvusdb/milvus:master-20221016-15878781
```

5. If you want to automatically remove the migration pod after successful migration, plese specify `-d true`.

```shell
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -w milvusdb/milvus:master-20221016-15878781 -d true
```

6. If the migrate is failed, you'd first rollback migration and rerun migrate again.

```
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev -o rollback -w <milvus-2-1-1-image>
./migrate.sh -i my-release -n milvus -s 2.1.1 -t 2.2.0 -r by-dev -o migrate -w <milvus-2-2-0-image>
```
