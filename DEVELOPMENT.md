# Development

This document will help to setup your development environment and running tests. If you encounter a problem, please file an issue.

Table of contents
=================
- [Development](#development)
- [Table of contents](#table-of-contents)
  - [Building Milvus with Docker](#building-milvus-with-docker)
  - [Building Milvus on a local OS/shell environment](#building-milvus-on-a-local-osshell-environment)
    - [Hardware Requirements](#hardware-requirements)
    - [Installing Required Software](#installing-required-software)
      - [Dependencies](#dependencies)
      - [CMake](#cmake)
      - [Go](#go)
      - [Docker & Docker Compose](#docker--docker-compose)
    - [Building Milvus](#building-milvus)
  - [A Quick Start for Testing Milvus](#a-quick-start-for-testing-milvus)
    - [Presubmission Verification](#presubmission-verification)
    - [Unit Tests](#unit-tests)
    - [Code coverage](#code-coverage)
    - [E2E Tests](#e2e-tests)
  - [GitHub Flow](#github-flow)


## Building Milvus with Docker

Official releases are built using Docker containers. To build Milvus using Docker please follow [these instructions](https://github.com/milvus-io/milvus/blob/master/build/README.md).

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

#### Dependencies
- Debian/Ubuntu

```shell
$ sudo apt update
$ sudo apt install -y build-essential ccache gfortran \
      libssl-dev zlib1g-dev python3-dev libcurl4-openssl-dev libtbb-dev\
      libboost-regex-dev libboost-program-options-dev libboost-system-dev \
      libboost-filesystem-dev libboost-serialization-dev libboost-python-dev
```

- CentOS

```shell
$ sudo yum install -y epel-release centos-release-scl-rh && \
$ sudo yum install -y git make automake openssl-devel zlib-devel \
        libcurl-devel python3-devel \
        devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran \
        llvm-toolset-7.0-clang llvm-toolset-7.0-clang-tools-extra \
        ccache lcov

$ echo "source scl_source enable devtoolset-7" | sudo tee -a /etc/profile.d/devtoolset-7.sh
$ echo "source scl_source enable llvm-toolset-7.0" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
$ echo "export CLANG_TOOLS_PATH=/opt/rh/llvm-toolset-7.0/root/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
$ source "/etc/profile.d/llvm-toolset-7.sh"

# Install tbb
$ git clone https://github.com/wjakob/tbb.git && \
      cd tbb/build && \
      cmake .. && make -j && \
      sudo make install && \
      cd ../../ && rm -rf tbb/

# Install boost
$ wget -q https://boostorg.jfrog.io/artifactory/main/release/1.65.1/source/boost_1_65_1.tar.gz && \
      tar zxf boost_1_65_1.tar.gz && cd boost_1_65_1 && \
      ./bootstrap.sh --prefix=/usr/local --with-toolset=gcc --without-libraries=python && \
      sudo ./b2 -j2 --prefix=/usr/local --without-python toolset=gcc install && \
      cd ../ && rm -rf ./boost_1_65_1*

```

Once you have finished, confirm that `gcc` and `make` are installed:

```shell
$ gcc --version
$ make --version
```

#### CMake

The algorithm library of Milvus, Knowhere is written in c++. CMake is required in the Milvus compilation. If you don't have it, please follow the instructions in the [Installing CMake](https://cmake.org/install/).

Confirm that cmake is available:

```shell
$ cmake --version
```

#### Go

Milvus is written in [Go](http://golang.org/). If you don't have a Go development environment, please follow the instructions in the [Go Getting Started guide](https://golang.org/doc/install).

Confirm that your `GOPATH` and `GOBIN` environment variables are correctly set as detailed in [How to Write Go Code](https://golang.org/doc/code.html) before proceeding.

```shell
$ go version
```

#### Docker & Docker Compose

Milvus depends on Etcd, Pulsar and minIO. Using Docker Compose to manage these is an easy way in a local development. To install Docker and Docker Compose in your development environment, follow the instructions from the Docker website below:

-   Docker: https://docs.docker.com/get-docker/
-   Docker Compose: https://docs.docker.com/compose/install/

### Building Milvus

To build the Milvus project, run the following command:

```shell
$ make all
```

If this command succeed, you will now have an executable at `bin/milvus` off of your Milvus project directory.

## A Quick Start for Testing Milvus

### Presubmission Verification

Presubmission verification provides a battery of checks and tests to give your pull request the best chance of being accepted. Developers need to run as many verification tests as possible locally.

To run all presubmission verification tests, use this command:

```shell
$ make verifiers
```

### Unit Tests

Pull requests need to pass all unit tests. To run every unit test, use this command:

```shell
$ make unittest
```

To run single test case, for instance, run TestSearchTask in /internal/proxy directory, use
```shell
$ go test -v ./internal/proxy/ -test.run TestSearchTask
```

### Code coverage

Before submitting your Pull Request, make sure your code change is covered by unit test. Use the following commands to check code coverage rate:

Install lcov(cpp code coverage tool)
```shell
$ sudo apt-get install lcov
```

Run unit test and generate report for code coverage
```shell
$ make codecov
```
This command will generate html report for Golang and C++ respectively.
For Golang report, open the `go_coverage.html` under milvus project path.
For C++ report, open the `cpp_coverage/index.html` under milvus project path.

You also can generate Golang coverage report by:
```shell
$ make codecov-go
```
Or C++ coverage report by:
```shell
$ make codecov-cpp
```

### E2E Tests

Milvus uses Python SDK to write test cases to verify the correctness of Milvus functions. Before run E2E tests, you need a running Milvus:

```shell
$ cd deployments/docker/dev
$ docker-compose up -d
$ cd ../../../
# Running Milvus standalone
$ ./scripts/start_standalone.sh

# or running running a Milvus cluster
$ ./scripts/start_cluster.sh
```

To run E2E tests, use these command:

```shell
$ cd tests/python_client
$ pip install -r requirements.txt
$ pytest --tags=L0 -n auto
```

## GitHub Flow

To check out code to work on, please refer to the [GitHub Flow](https://guides.github.com/introduction/flow/).