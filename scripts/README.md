# Compile and install milvus cluster

## Environment

```
OS: Ubuntu 20.04
go：1.20
cmake: >=3.18
gcc： 7.5
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

Install OpenBlas library

```shell
$ wget https://github.com/xianyi/OpenBLAS/archive/v0.3.9.tar.gz && \
$ tar zxvf v0.3.9.tar.gz && cd OpenBLAS-0.3.9 && \
$ make TARGET=CORE2 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 USE_THREAD=0 USE_OPENMP=0 FC=gfortran CC=gcc COMMON_OPT="-O3 -g -fPIC" FCOMMON_OPT="-O3 -g -fPIC -frecursive" NMAX="NUM_THREADS=128" LIBPREFIX="libopenblas" INTERFACE64=0 NO_STATIC=1 && \
$ make PREFIX=/usr install
```

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
```

## Start service

Start third-party service:

```shell
$ cd [milvus project path]/deployments/docker/cluster
$ docker-compose up -d
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
