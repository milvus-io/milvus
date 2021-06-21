# Build and Start Milvus from Source Code

This article describes how to build and start Milvus Standalone and Cluster from source code.

- [Prerequisites](#prerequisites)
- [Build Milvus](#build-milvus)
- [Start Milvus](#start-milvus)

## Prerequisites

Install the following before building Milvus from source code.

- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) for version control.
- [Golang](https://golang.org/doc/install) version 1.15 or higher and associated toolkits.
- [CMake](https://cmake.org/install/) version 3.14 or higher for compilation.
- [OpenBLAS](https://github.com/xianyi/OpenBLAS/wiki/Installation-Guide) (Basic Linear Algebra Subprograms) library version 0.3.9 or higher for matrix operations.



## Build Milvus

1. Clone Milvus' GitHub repository:

```
$ cd /home/$USER
$ git clone https://github.com/milvus-io/milvus.git
```

2. Install third-party dependencies:

```
$ cd milvus
$ ./scripts/install_deps.sh
```

3. Compile executed binary for Milvus:

```
$ make milvus
```

## Start Milvus

1. Start infrastructure services:

```
$ cd /home/$USER/milvus/deployments/docker/dev
$ sudo docker-compose up -d
```

2. Start Milvus:

- For Milvus Standalone

```
$ cd /home/$USER/milvus
./bin/milvus run standalone    > /tmp/standalone.log     2>&1  &
```

- For Milvus Cluster:

```
$ cd /home/$USER/milvus
#start RootCoord
./bin/milvus run rootcoord     > /tmp/rootcoord.log      2>&1  &
#start coord
./bin/milvus run datacoord     > /tmp/datacoord.log      2>&1  &
./bin/milvus run indexcoord    > /tmp/indexcoord.log     2>&1  &
./bin/milvus run querycoord    > /tmp/querycoord.log     2>&1  &
#start node
./bin/milvus run proxy         > /tmp/proxy.log          2>&1  &
./bin/milvus run datanode      > /tmp/data_node.log      2>&1  &
./bin/milvus run indexnode     > /tmp/index_node.log     2>&1  &
./bin/milvus run querynode     > /tmp/query_node.log     2>&1  &
```

