# Development

This document will help to setup your development environment and running tests. If you encounter a problem, please file an issue.

## Building Milvus on a local OS/shell environment

The details below outline the hardware and software requirements for building on Linux.

### Hardware Requirements

Milvus is written in Go and C++, compiling it can use a lot of resources. We recommend the following for any physical or virtual machine being used for building Milvus.

```
- 8GB of RAM
- 50GB of free disk space
```

### Installing Required Software

In fact, all Linux distributions is available to develop Milvus. The following only contains commands on Ubuntu, because we mainly use it. If you develop Milvus on other distributions, you are welcome to improve this document.

#### Debian/Ubuntu

```shell
sudo apt update
sudo apt install -y build-essential ccache gfortran \
    libssl-dev zlib1g-dev python3-dev libcurl4-openssl-dev libtbb-dev\
    libboost-regex-dev libboost-program-options-dev libboost-system-dev \
    libboost-filesystem-dev libboost-serialization-dev libboost-python-dev
```

Once you have finished, confirm that `gcc` and `make` are installed:

```shell
gcc --version
make --version
```

#### CMake

The algorithm library of Milvus, Knowhere is written in c++. CMake is required in the Milvus compilation. If you don't have it, please follow the instructions in the [Installing CMake](https://cmake.org/install/).

Confirm that cmake is available:

```shell
cmake --version
```

#### Go

Milvus is written in [Go](http://golang.org/). If you don't have a Go development environment, please follow the instructions in the [Go Getting Started guide](https://golang.org/doc/install).

Confirm that your `GOPATH` and `GOBIN` environment variables are correctly set as detailed in [How to Write Go Code](https://golang.org/doc/code.html) before proceeding.

```shell
go version
```

#### Docker & Docker Compose

Milvus depends on Etcd, Pulsar and minIO. Using Docker Compose to manage these is an easy way in a local development. To install Docker and Docker Compose in your development environment, follow the instructions from the Docker website below:

-   Docker: https://docs.docker.com/get-docker/
-   Docker Compose: https://docs.docker.com/compose/install/

### Building Milvus

To build the Milvus project, run the following command:

```shell
make all
```

If this command succeed, you will now have an executable at `bin/milvus` off of your Milvus project directory.

## A Quick Start for Testing Milvus

### Presubmission Verification

Presubmission verification provides a battery of checks and tests to give your pull request the best chance of being accepted. Developers need to run as many verification tests as possible locally.

To run all presubmission verification tests, use this command:

```shell
make verifiers
```

### Unit Tests

Pull requests need to pass all unit tests. To run every unit test, use this command:

```shell
make unittest
```

### E2E Tests

Milvus uses Python SDK to write test cases to verify the correctness of Milvus functions. Before run E2E tests, you need a running Milvus:

#### Standalone

```shell
cd deployments/docker/standalone
docker-compose up -d
cd ../../../
./bin/milvus run standalone
```

#### Cluster

```shell
./scripts/start.sh
```

To run E2E tests, use these command:

```shell
cd test20/python_client
pip install -r requirements.txt
pytest .
```

## GitHub Flow

To check out code to work on, please refer to the [GitHub Flow](https://guides.github.com/introduction/flow/).