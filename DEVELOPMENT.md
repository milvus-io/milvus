# Development

This document will help to set up your Milvus development environment and to run tests. Please [file an issue](https://github.com/milvus-io/milvus/issues/new/choose) if there's a problem.

# Table of contents

- [Development](#development)
- [Table of contents](#table-of-contents)
  - [Building Milvus with Docker](#building-milvus-with-docker)
  - [Building Milvus on a local OS/shell environment](#building-milvus-on-a-local-osshell-environment)
    - [Hardware Requirements](#hardware-requirements)
    - [Software Requirements](#software-requirements)
      - [Prerequisites](#prerequisites)
      - [Installing Dependencies](#installing-dependencies)
      - [Caveats](#caveats)
      - [CMake \& Conan](#cmake--conan)
      - [Go](#go)
      - [Docker \& Docker Compose](#docker--docker-compose)
    - [Building Milvus](#building-milvus)
  - [A Quick Start for Testing Milvus](#a-quick-start-for-testing-milvus)
    - [Pre-submission Verification](#pre-submission-verification)
    - [Unit Tests](#unit-tests)
    - [Code coverage](#code-coverage)
    - [E2E Tests](#e2e-tests)
    - [Test on local branch](#test-on-local-branch)
      - [With Linux and MacOS](#with-linux-and-macos)
      - [With docker](#with-docker)
  - [GitHub Flow](#github-flow)
  - [FAQs](#faqs)

## Building Milvus with Docker

Our official Milvus versions are releases as Docker images. To build Milvus Docker on your own, please follow [these instructions](https://github.com/milvus-io/milvus/blob/master/build/README.md).

## Building Milvus on a local OS/shell environment

The details below outline the hardware and software requirements for building on Linux and MacOS.

### Hardware Requirements

The following specification (either physical or virtual machine resources) is recommended for Milvus to build and run from source code.

```
- 8GB of RAM
- 50GB of free disk space
```

### Software Requirements

All Linux distributions are available for Milvus development. However a majority of our contributor worked with Ubuntu or CentOS systems, with a small portion of Mac (both x86_64 and Apple Silicon) contributors. If you would like Milvus to build and run on other distributions, you are more than welcome to file an issue and contribute!

Here's a list of verified OS types where Milvus can successfully build and run:

- Debian/Ubuntu
- Amazon Linux
- MacOS (x86_64)
- MacOS (Apple Silicon)

#### Prerequisites

Linux systems (Recommend Ubuntu 20.04 or later):

```bash
go: >= 1.20
cmake: >= 3.18
gcc: 7.5
```

MacOS systems with x86_64 (Big Sur 11.5 or later recommended):

```bash
go: >= 1.20
cmake: >= 3.18
llvm: >= 15
```

MacOS systems with Apple Silicon (Monterey 12.0.1 or later recommended):

```bash
go: >= 1.20 (Arch=ARM64)
cmake: >= 3.18
llvm: >= 15
```

#### Installing Dependencies

In the Milvus repository root, simply run:

```bash
$ ./scripts/install_deps.sh
```

#### Caveats

- [Google Test](https://github.com/google/googletest.git) is automatically cloned from GitHub, which in some case could conflict with your local google test library.

Once you have finished, confirm that `gcc` and `make` are installed:

```shell
$ gcc --version
$ make --version
```

#### CMake & Conan

The algorithm library of Milvus, Knowhere is written in c++. CMake is required in the Milvus compilation. If you don't have it, please follow the instructions in the [Installing CMake](https://cmake.org/install/).

Confirm that cmake is available:

```shell
$ cmake --version
```

Note: 3.25 or higher cmake version is required to build Milvus.

Milvus uses Conan to manage third-party dependencies for c++.

Install Conan

```shell
pip install conan==1.61.0
```

Note: Conan version 2.x is not currently supported, please use version 1.58.

#### Go

Milvus is written in [Go](http://golang.org/). If you don't have a Go development environment, please follow the instructions in the [Go Getting Started guide](https://golang.org/doc/install).

Confirm that your `GOPATH` and `GOBIN` environment variables are correctly set as detailed in [How to Write Go Code](https://golang.org/doc/code.html) before proceeding.

```shell
$ go version
```
Note: go >= 1.20 is required to build Milvus.

#### Docker & Docker Compose

Milvus depends on etcd, Pulsar and MinIO. Using Docker Compose to manage these is an easy way in local development. To install Docker and Docker Compose in your development environment, follow the instructions from the Docker website below:

- Docker: https://docs.docker.com/get-docker/
- Docker Compose: https://docs.docker.com/compose/install/

### Building Milvus

To build the Milvus project, run the following command:

```shell
$ make
```

If this command succeed, you will now have an executable at `bin/milvus` off of your Milvus project directory.

If you want to update proto file before `make`, we can use the following command:

```shell
$ make generated-proto-go
```

If you want to know more, you can read Makefile.

## A Quick Start for Testing Milvus

### Pre-submission Verification

Pre-submission verification provides a battery of checks and tests to give your pull request the best chance of being accepted. Developers need to run as many verification tests as possible locally.

To run all pre-submission verification tests, use this command:

```shell
$ make verifiers
```

### Unit Tests

It is required that all pull request candidates should pass all Milvus unit tests.

Beforce running unit tests, you need to first bring up the Milvus deployment environment.
You may set up a local docker environment with our docker compose yaml file to start unit testing.
For Apple Silicon users (Apple M1):

```shell
$ cd deployments/docker/dev
$ docker-compose -f docker-compose-apple-silicon.yml up -d
$ cd ../../../
$ make unittest
```

For others:

```shell
$ cd deployments/docker/dev
$ docker-compose up -d
$ cd ../../../
$ make unittest
```

To run only cpp test:

```shell
$ make test-cpp
```

To run only go test:

```shell
$ make test-go
```

To run a single test case (TestSearchTask in /internal/proxy directory, for example):

```shell
$ source scripts/setenv.sh && go test -v ./internal/proxy/ -test.run TestSearchTask
```

If using Mac with M1 chip

```
$ source scripts/setenv.sh && go test -tags=dynamic -v ./internal/proxy/ -test.run TestSearchTask
```

### Code coverage

Before submitting your pull request, make sure your code change is covered by unit test. Use the following commands to check code coverage rate:

Run unit test and generate code coverage report:

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

Milvus uses Python SDK to write test cases to verify the correctness of Milvus functions. Before running E2E tests, you need a running Milvus:

```shell
# Running Milvus cluster
$ cd deployments/docker/dev
$ docker-compose up -d
$ cd ../../../
$ ./scripts/start_cluster.sh

# Or running Milvus standalone
$ cd deployments/docker/dev
$ docker-compose up -d
$ cd ../../../
$ ./scripts/start_standalone.sh
```

To run E2E tests, use these commands:

```shell
$ cd tests/python_client
$ pip install -r requirements.txt
$ pytest --tags=L0 -n auto
```

### Test on local branch

#### With Linux and MacOS

After preparing deployment environment, we can start the cluster on your host machine

```shell
$ ./scripts/start_cluster.sh
```

#### With docker

start the cluster on your host machine

```shell
$ ./build/builder.sh make install // build milvus
$ ./build/build_image.sh // build milvus latest docker image
$ docker images // check if milvus latest image is ready
REPOSITORY                 TAG                                 IMAGE ID       CREATED          SIZE
milvusdb/milvus            latest                              63c62ff7c1b7   52 minutes ago   570MB
$ install with docker compose
```

## GitHub Flow

To check out code to work on, please refer to the [GitHub Flow](https://guides.github.com/introduction/flow/).

## FAQs

Q: The go building phase fails on Apple Silicon (Mac M1) machines.

A: Please double-check that you have [right Go version](https://go.dev/dl/) installed, i.e. with OS=macOS and Arch=ARM64.

---

Q: "make" fails with "_ld: library not found for -lSystem_" on MacOS.

A: There are a couple of things you could try:

1. Use **Software Update** (from **About this Mac** -> **Overview**) to install updates.
2. Try the following commands:

```bash
sudo rm -rf /Library/Developer/CommandLineTools
sudo xcode-select --install
```

---

Q: Rocksdb fails to compile with "_ld: warning: object file was built for newer macOS version (11.6) than being linked (11.0)._" on MacOS.

A: Use **Software Update** (from **About this Mac** -> **Overview**) to install updates.

---

Q: Some Go unit tests failed.

A: We are aware that some tests can be flaky occasionally. If there's something you believe is abnormal (i.e. tests that fail every single time). You are more than welcome to [file an issue](https://github.com/milvus-io/milvus/issues/new/choose)!
