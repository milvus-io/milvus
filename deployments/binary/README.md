# Run Milvus standalone through binary files

To quickly install Milvus standalone without docker or kubernetes, this document provides a tutorial for installing Milvus and dependencies, etcd and MinIO, through binary files.

Before installing, you can refer to [docker-compose.yml](https://github.com/milvus-io/milvus/blob/master/deployments/docker/standalone/docker-compose.yml) to check the versions required by etcd and MinIO.

## Start etcd service

#### Refer: https://github.com/etcd-io/etcd/releases

```shell
$ wget https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz
$ tar zxvf etcd-v3.5.0-linux-amd64.tar.gz
$ cd etcd-v3.5.0-linux-amd64
$ ./etcd -advertise-client-urls=http://127.0.0.1:2379 -listen-client-urls http://0.0.0.0:2379 --data-dir /etcd
```

## Start MinIO service

#### Refer: https://min.io/download#/linux

```shell
$ wget https://dl.min.io/server/minio/release/linux-amd64/minio
$ chmod +x minio
$ ./minio server /minio
```

## Start Milvus standalone

To start Milvus standalone, you need a Milvus binary file. Currently you can get the latest version of Milvus binary file through the Milvus docker image. (We will upload Milvus binary files in the future)

```shell
$ docker run -d --name milvus milvusdb/milvus:v2.5.0-beta /bin/bash
$ docker cp milvus:/milvus .
```

### Install Milvus dependencies

```shell
$ sudo apt-get install libopenblas-dev
$ sudo apt-get install libgomp1
$ sudo apt-get install libtbb2
```

### Start Milvus service

```shell
$ cd milvus
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/lib
$ ./bin/milvus run standalone
```
