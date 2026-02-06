# Development

This document will help to set up your Milvus development environment and to run tests. Please [file an issue](https://github.com/milvus-io/milvus/issues/new/choose) if there's a problem.

# Table of contents

- [Development](#development)
- [Table of contents](#table-of-contents)
  - [Quick Start](#quick-start)
  - [Building Milvus with Docker](#building-milvus-with-docker)
  - [Building Milvus on a local OS/shell environment](#building-milvus-on-a-local-osshell-environment)
    - [Hardware Requirements](#hardware-requirements)
    - [Software Requirements](#software-requirements)
      - [Supported Platforms](#supported-platforms)
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

## Quick Start

For a clean machine, you can build Milvus with just 2 commands:

```bash
# Step 1: Install Go (>= 1.21) first, then install all dependencies
./scripts/install_deps.sh

# Step 2: Build Milvus
make
```

The compiled binary will be at `bin/milvus`.

## Building Milvus with Docker

Our official Milvus versions are releases as Docker images. To build Milvus Docker on your own, please follow [these instructions](https://github.com/milvus-io/milvus/blob/master/build/README.md).

## Building Milvus on a local OS/shell environment

The details below outline the hardware and software requirements for building on Linux and MacOS.

### Hardware Requirements

The following specification (either physical or virtual machine resources) is recommended for Milvus to build and run from source code.

```yaml
- 8GB of RAM
- 50GB of free disk space
```

### Software Requirements

#### Supported Platforms

| Platform | Versions | Architecture | Notes |
|----------|----------|--------------|-------|
| macOS | 12, 13, 14, 15 | Intel (x86_64), Apple Silicon (arm64) | Requires Homebrew |
| Ubuntu | 20.04, 22.04, 24.04 | x86_64, arm64 | |
| Rocky Linux | 8, 9 | x86_64, arm64 | RHEL compatible |
| Amazon Linux | 2023 | x86_64, arm64 | |
| CentOS | 7 (legacy) | x86_64 | EOL, use Rocky Linux instead |

#### Prerequisites

**Compiler Requirements:**

| Platform | Compiler | Supported Versions |
|----------|----------|-------------------|
| macOS | LLVM/Clang | 14, 15, 16, 17, 18 |
| Linux | GCC | 9, 10, 11, 12, 13, 14 |

**Tool Requirements:**

| Tool | Minimum Version | Notes |
|------|----------------|-------|
| Go | 1.21 | Required. Install from https://go.dev/dl/ |
| CMake | 3.26 | Auto-installed by install_deps.sh |
| Conan | 1.64.1 | Auto-installed. Conan 2.x not supported |
| Rust | 1.89 | Auto-installed by install_deps.sh |

### Compiler Setup

You can use Vscode to integrate C++ and Go together. Please replace user.settings file with below configs:

```json
{
    "go.toolsEnvVars": {
        "PKG_CONFIG_PATH": "${env:PKG_CONFIG_PATH}:${workspaceFolder}/internal/core/output/lib/pkgconfig:${workspaceFolder}/internal/core/output/lib64/pkgconfig",
        "LD_LIBRARY_PATH": "${env:LD_LIBRARY_PATH}:${workspaceFolder}/internal/core/output/lib:${workspaceFolder}/internal/core/output/lib64",
        "RPATH": "${env:RPATH}:${workspaceFolder}/internal/core/output/lib:${workspaceFolder}/internal/core/output/lib64"
    },
    "go.testEnvVars": {
        "PKG_CONFIG_PATH": "${env:PKG_CONFIG_PATH}:${workspaceFolder}/internal/core/output/lib/pkgconfig:${workspaceFolder}/internal/core/output/lib64/pkgconfig",
        "LD_LIBRARY_PATH": "${env:LD_LIBRARY_PATH}:${workspaceFolder}/internal/core/output/lib:${workspaceFolder}/internal/core/output/lib64",
        "RPATH": "${env:RPATH}:${workspaceFolder}/internal/core/output/lib:${workspaceFolder}/internal/core/output/lib64"
    },
    "go.buildFlags": [
        "-ldflags=-r=${workspaceFolder}/internal/core/output/lib"
    ],
    "terminal.integrated.env.linux": {
        "PKG_CONFIG_PATH": "${env:PKG_CONFIG_PATH}:${workspaceFolder}/internal/core/output/lib/pkgconfig:${workspaceFolder}/internal/core/output/lib64/pkgconfig",
        "LD_LIBRARY_PATH": "${env:LD_LIBRARY_PATH}:${workspaceFolder}/internal/core/output/lib:${workspaceFolder}/internal/core/output/lib64",
        "RPATH": "${env:RPATH}:${workspaceFolder}/internal/core/output/lib:${workspaceFolder}/internal/core/output/lib64"
    },
    "go.useLanguageServer": true,
    "gopls": {
        "formatting.gofumpt": true
    },
    "go.formatTool": "gofumpt",
    "go.lintTool": "golangci-lint",
    "go.testTags": "test,dynamic",
    "go.testTimeout": "10m"
}
```

#### Installing Dependencies

In the Milvus repository root, simply run:

```bash
./scripts/install_deps.sh
```

This script will:
- Detect your OS and version automatically
- Install the appropriate compiler (LLVM on macOS, GCC on Linux)
- Install CMake, Ninja, ccache, and other build tools
- Install Conan package manager
- Install Rust toolchain

#### Caveats

- [Google Test](https://github.com/google/googletest.git) is automatically cloned from GitHub, which in some case could conflict with your local google test library.

Once you have finished, confirm that the compiler is installed:

```shell
# On macOS
clang --version

# On Linux
gcc --version
```

#### CMake & Conan

The algorithm library of Milvus, Knowhere is written in c++. CMake is required in the Milvus compilation. If you don't have it, please follow the instructions in the [Installing CMake](https://cmake.org/install/).

Confirm that cmake is available:

```shell
cmake --version
```

Note: CMake 3.26 or higher is required to build Milvus.

Milvus uses Conan to manage third-party dependencies for c++.

Install Conan:

```shell
pip install conan==1.64.1
```

Note: Conan version 2.x is not currently supported, please use version 1.64.1.

#### Go

Milvus is written in [Go](http://golang.org/). If you don't have a Go development environment, please follow the instructions in the [Go Getting Started guide](https://golang.org/doc/install).

Confirm that your `GOPATH` and `GOBIN` environment variables are correctly set as detailed in [How to Write Go Code](https://golang.org/doc/code.html) before proceeding.

```shell
go version
```

Note: Go >= 1.21 is required to build Milvus.

#### Docker & Docker Compose

Milvus depends on etcd, Pulsar and MinIO. Using Docker Compose to manage these is an easy way in local development. To install Docker and Docker Compose in your development environment, follow the instructions from the Docker website below:

- Docker: <https://docs.docker.com/get-docker/>
- Docker Compose: <https://docs.docker.com/compose/install/>

### Building Milvus

(Note: if you are building Milvus on an operating system that uses large memory page size, ensure the MILVUS_JEMALLOC_LG_PAGE configuration variable is adjusted in the build script. For step-by-step guidance, refer to the FAQs section.)

To build the Milvus project, run the following command:

```shell
make
```

Milvus uses `conan` to manage 3rd-party dependencies. `conan` will check the consistency of these dependencies every time you run `make`. This process can take a considerable amount of time, especially if the network is poor. If you make sure that the 3rd-party dependencies are consistent, you can use the following command to skip this step:

```shell
make SKIP_3RDPARTY=1
```

If this command succeeds, you will now have an executable at `bin/milvus` in your Milvus project directory.

If you want to run the `bin/milvus` executable on the host machine, you need to set `LD_LIBRARY_PATH` temporarily:

```shell
# Linux
LD_LIBRARY_PATH=./internal/core/output/lib:lib:$LD_LIBRARY_PATH ./bin/milvus

# macOS
DYLD_LIBRARY_PATH=./internal/core/output/lib:lib:$DYLD_LIBRARY_PATH ./bin/milvus
```

If you want to update proto file before `make`, we can use the following command:

```shell
make generated-proto-go
```

If you want to know more, you can read Makefile.

## A Quick Start for Testing Milvus

### Pre-submission Verification

Pre-submission verification provides a battery of checks and tests to give your pull request the best chance of being accepted. Developers need to run as many verification tests as possible locally.

To run all pre-submission verification tests, use this command:

```shell
make verifiers
```

### Unit Tests

It is required that all pull request candidates should pass all Milvus unit tests.

Before running unit tests, you need to first bring up the Milvus deployment environment.
You may set up a local docker environment with our docker compose yaml file to start unit testing.
For Apple Silicon users (Apple M1):

```shell
cd deployments/docker/dev
docker compose -f docker-compose-apple-silicon.yml up -d
cd ../../../
make unittest
```

For others:

```shell
cd deployments/docker/dev
docker compose up -d
cd ../../../
make unittest
```

To run only cpp test:

```shell
make test-cpp
```

To run only go test:

```shell
make test-go
```

To run a single test case (TestSearchTask in /internal/proxy directory, for example):

```shell
source scripts/setenv.sh && go test -v ./internal/proxy/ -test.run TestSearchTask
```

If using Mac with M1 chip

```
source scripts/setenv.sh && go test -tags=dynamic -v ./internal/proxy/ -test.run TestSearchTask
```

### Code coverage

Before submitting your pull request, make sure your code change is covered by unit test. Use the following commands to check code coverage rate:

Run unit test and generate code coverage report:

```shell
make codecov
```

This command will generate html report for Golang and C++ respectively.
For Golang report, open the `go_coverage.html` under milvus project path.
For C++ report, open the `cpp_coverage/index.html` under milvus project path.

You also can generate Golang coverage report by:

```shell
make codecov-go
```

Or C++ coverage report by:

```shell
make codecov-cpp
```

### E2E Tests

Milvus uses Python SDK to write test cases to verify the correctness of Milvus functions. Before running E2E tests, you need a running Milvus. There are two modes of operation to build Milvus — Milvus Standalone and Milvus Cluster. Milvus Standalone operates independently as a single instance. Milvus Cluster operates across multiple nodes. All milvus instances are clustered together to form a unified system to support larger volumes of data and higher traffic loads.

Both include three components:

1. Milvus: The core functional component.
2. Etcd: The metadata engine. Access and store metadata of Milvus' internal components.
3. MinIO: The storage engine. Responsible for data persistence for Milvus.
Milvus Cluster includes further component — Pulsar, to be distributed through Pub/Sub mechanism.

```shell
# Running Milvus cluster
$ cd deployments/docker/dev
$ docker compose up -d
$ cd ../../../
$ ./scripts/start_cluster.sh

# Or running Milvus standalone
$ cd deployments/docker/dev
$ docker compose up -d
$ cd ../../../
$ ./scripts/start_standalone.sh
```

To run E2E tests, use these commands:

```shell
cd tests/python_client
pip install -r requirements.txt
pytest --tags=L0 -n auto
```

### Test on local branch

#### With Linux and MacOS

After preparing deployment environment, we can start the cluster on your host machine

```shell
./scripts/start_cluster.sh
```

#### With docker

start the cluster on your host machine

```shell
$ ./build/builder.sh make install // build milvus
$ ./build/build_image.sh // build milvus latest docker image
$ docker images // check if milvus latest image is ready
REPOSITORY                 TAG                                 IMAGE ID       CREATED          SIZE
milvusdb/milvus            latest                              63c62ff7c1b7   52 minutes ago   570MB
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

---

Q: Brew: Unexpected Disconnect while reading sideband packet

```bash
==> Tapping homebrew/core
remote: Enumerating objects: 1107077, done.
remote: Counting objects: 100% (228/228), done.
remote: Compressing objects: 100% (157/157), done.
error: 545 bytes of body are still expected.44 MiB | 341.00 KiB/s
fetch-pack: unexpected disconnect while reading sideband packet
fatal: early EOF
fatal: index-pack failed
Failed during: git fetch --force origin refs/heads/master:refs/remotes/origin/master
```

A: try to increase http post buffer

```bash
git config --global http.postBuffer 1M
```

---

Q: Brew: command not found" after installation

A: set up git config

```bash
git config --global user.email xxx
git config --global user.name xxx
```

---

Q: Docker: error getting credentials - err: exit status 1, out: ``

A: removing "credsStore":from ~/.docker/config.json

---

Q: ModuleNotFoundError: No module named 'imp'

A: Python 3.12 has removed the imp module, please downgrade to 3.11 for now.

---

Q: Conan: Unrecognized arguments: — install-folder conan

A: The version is not correct. Please use Conan 1.64.1.

---

Q: Conan command not found

A: Fixed by exporting Python bin PATH in your bash:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

---

Q: LLVM: use of undeclared identifier 'kSecFormatOpenSSL'

A: This is a known issue on macOS 15+. Try using a newer version of LLVM:

```bash
brew install llvm@17
```

The build scripts will automatically detect and use LLVM 14-18.

---

Q: The binary fails to open on my OS with the error "Unsupported system page size"?

A: This occurs when your OS uses a large memory page size, which must be explicitly declared during compilation. Follow these steps to resolve it:

First, check your OS page size by running the following command:

```bash
getconf PAGESIZE
```

If the output is 65536, your system uses 64KB pages.

The `MILVUS_JEMALLOC_LG_PAGE` variable's primary function is to specify the size of large pages during the compilation of jemalloc. Jemalloc is a memory allocator designed to enhance the performance and efficiency of applications in a multi-threaded environment. By specifying the size of large pages, memory management and access can be optimized, thereby improving performance.

Large page support allows the operating system to manage and allocate memory in larger blocks, reducing the number of page table entries, thereby decreasing the time for page table lookups and improving the efficiency of memory access. This is particularly important when processing large amounts of data, as it can significantly reduce page faults and Translation Lookaside Buffer (TLB) misses, enhancing application performance.

On ARM64 architectures, different systems may support different page sizes, such as 4KB and 64KB. The `MILVUS_JEMALLOC_LG_PAGE` setting allows developers to customize the compilation of jemalloc for the target platform, ensuring it can efficiently operate on systems with varying page sizes. By specifying the `--with-lg-page` configuration option, jemalloc can utilize the optimal page size supported by the system when managing memory.

For example, if a system supports a 64KB page size, by setting `MILVUS_JEMALLOC_LG_PAGE` to the corresponding value (the power of 2, 64KB is 2 to the 16th power, so the value is 16), jemalloc can allocate and manage memory in 64KB units, which can improve the performance of applications running on that system.

Modify the make configuration file, located at: `./milvus/scripts/core_build.sh`, with the following changes:

```diff
arch=$(uname -m)
CMAKE_CMD="cmake \
${CMAKE_EXTRA_ARGS} \
 -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
 -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
 -DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
 -DCMAKE_LIBRARY_ARCHITECTURE=${arch} \
 -DBUILD_COVERAGE=${BUILD_COVERAGE} \
 -DMILVUS_GPU_VERSION=${GPU_VERSION} \
 -DMILVUS_CUDA_ARCH=${CUDA_ARCH} \
 -DEMBEDDED_MILVUS=${EMBEDDED_MILVUS} \
 -DBUILD_DISK_ANN=${BUILD_DISK_ANN} \
+ -DMILVUS_JEMALLOC_LG_PAGE=16 \
 -DUSE_ASAN=${USE_ASAN} \
 -DUSE_DYNAMIC_SIMD=${USE_DYNAMIC_SIMD} \
 -DCPU_ARCH=${CPU_ARCH} \
 -DINDEX_ENGINE=${INDEX_ENGINE} \
 -DENABLE_GCP_NATIVE=${ENABLE_GCP_NATIVE} \
 -DENABLE_AZURE_FS=${ENABLE_AZURE_FS} "
CMAKE_CMD=${CMAKE_CMD}"${CPP_SRC_DIR}"
```

Using `-DMILVUS_JEMALLOC_LG_PAGE=16` as a compilation option for jemalloc is because it specifies the size of "large pages" as 2 to the 16th power bytes, which equals 65536 bytes or 64KB. This value is set to optimize memory management and improve performance, especially on systems that support or prefer using large pages to reduce the overhead of page table management.

Specifying `-DMILVUS_JEMALLOC_LG_PAGE=16` during the compilation of jemalloc informs jemalloc to assume the system's large page size is 64KB. This allows jemalloc to work more efficiently with the operating system's memory manager, using large pages to optimize performance. This is crucial for ensuring optimal performance on systems with different default page sizes, particularly in environments that might have different memory management needs due to varying hardware or system configurations.
