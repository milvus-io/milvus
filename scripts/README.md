# Compile and install milvus cluster

## Environment

```
OS: Ubuntu 20.04
go：1.21
cmake: >=3.18
gcc: >= 11
```

## Install dependencies

Install compile dependencies

```shell
$ sudo apt install -y g++ gcc make libssl-dev zlib1g-dev libboost-regex-dev \
    libboost-program-options-dev libboost-system-dev libboost-filesystem-dev \
    libboost-serialization-dev python3-dev libboost-python-dev libcurl4-openssl-dev gfortran libtbb-dev
$ export GO111MODULE=on
$ go get github.com/golang/protobuf/protoc-gen-go@v1.3.2
```

OpenBLAS is managed by Conan for C++ core builds. Install the compiler toolchain and Fortran runtime needed by Conan, then run the normal third-party build step.

## Compile

Generate the go files from proto file

```shell
$ make check-proto-product
```

Check code specifications

```shell
$ make verifiers
```

Compile milvus

```shell
$ make milvus
```

## Install docker-compose

refer: https://docs.docker.com/compose/install/

```shell
$ sudo curl -L "https://github.com/docker/compose/releases/download/1.27.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
$ sudo chmod +x /usr/local/bin/docker-compose
$ sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
$ docker-compose --version
$ docker compose --version
```

## Start service

Start third-party service:

```shell
$ cd [milvus project path]/deployments/docker/cluster
$ docker-compose up -d
$ docker compose up -d
```

Start milvus cluster:

```shell
$ cd [milvus project path]
$ ./scripts/start_cluster.sh
```

## Run unittest

Run all unittest including go and cpp cases:

```shell
$ make unittest
```

You also can run go unittest only:

```shell
$ make test-go
```

Run cpp unittest only:

```shell
$ make test-cpp
```

## Run code coverage

Run code coverage including go and cpp:

```shell
$ make codecov
```

You also can run go code coverage only:

```shell
$ make codecov-go
```

Run cpp code coverage only:

```shell
$ make codecov-cpp
```
