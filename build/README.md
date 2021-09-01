# Building Milvus with Docker

Building Milvus is easy if you take advantage of the containerized build environment. This document will help guide you through understanding this build process.

1. Docker, using one of the following configurations:
  * **macOS** Install Docker for Mac. See installation instructions [here](https://docs.docker.com/docker-for-mac/).
     **Note**: You will want to set the Docker VM to have at least 2 vCPU and 8GB of initial memory or building will likely fail.
  * **Linux with local Docker**  Install Docker according to the [instructions](https://docs.docker.com/installation/#installation) for your OS.
  * **Windows with Docker Desktop WSL2 backend**  Install Docker according to the [instructions](https://docs.docker.com/docker-for-windows/wsl-tech-preview/). Be sure to store your sources in the local Linux file system, not the Windows remote mount at `/mnt/c`.
2. **Optional** [Google Cloud SDK](https://developers.google.com/cloud/sdk/)

You must install and configure Google Cloud SDK if you want to upload your release to Google Cloud Storage and may safely omit this otherwise.

## Overview

While it is possible to build Milvus using a local golang installation, we have a build process that runs in a Docker container.  This simplifies initial set up and provides for a very consistent build and test environment.


## Before You Begin

Before building Milvus, you must check the eligibility of your Docker, Docker Compose, and hardware in line with Milvus' requirement.

<details><summary>Check your Docker and Docker Compose version</summary>

<li>Docker version 19.03 or higher is required. </li>

<div class="alert note">
Follow <a href="https://docs.docker.com/get-docker/">Get Docker</a> to install Docker on your system.
</div>

<li>Docker Compose version 1.25.1 or higher is required. </li>

<div class="alert note">
See <a href="https://docs.docker.com/compose/install/">Install Docker Compose</a> for Docker Compose installation guide.
</div>

</details>


<details><summary>Check whether your CPU supports SIMD extension instruction set</summary>

Milvus' computing operations depend on CPU’s support for SIMD (Single Instruction, Multiple Data) extension instruction set. Whether your CPU supports SIMD extension instruction set is crucial to index building and vector similarity search within Milvus. Ensure that your CPU supports at least one of the following SIMD instruction sets:

- SSE4.2
- AVX
- AVX2
- AVX512

Run the lscpu command to check if your CPU supports the SIMD instruction sets mentioned above:

```
$ lscpu | grep -e sse4_2 -e avx -e avx2 -e avx512
```
</details>


## Key scripts

The following scripts are found in the [`build/`](.) directory. Note that all scripts must be run from the Milvus root directory.

* [`build/builder.sh`](builder.sh): Run a command in a build docker container.  Common invocations:
  * `build/builder.sh make` Build just linux binaries in the container.  Pass options and packages as necessary.
  * `build/builder.sh make verifiers`: Run all pre-submission verification check
  * `build/builder.sh make unittest`: Run all unit tests
  * `build/builder.sh make clean`: Clean up all the generated files

You can specify a different OS for builder by setting `OS_NAME` which defaults to `ubuntu18.04`. Valid OS name are `ubuntu18.04`, `centos7`.

To specify `centos7` builder, use these command:

```shell
export OS_NAME=centos7
build/builder.sh make
```

## E2E Tests

Milvus uses Python SDK to write test cases to verify the correctness of Milvus functions. Before run E2E tests, you need a running Milvus:

```shell
$ cd deployments/docker/dev
$ docker-compose up -d
$ cd ../../../
# Running Milvus
$ build/builder.sh /bin/bash -c "export ROCKSMQ_PATH='/tmp/milvus/rdb_data' && ./scripts/start_standalone.sh && cat"

# or

$ build/builder.sh /bin/bash -c "./scripts/start_cluster.sh && cat"
```

To run E2E tests, use these command:

```shell
MILVUS_SERVICE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker-compose ps -q builder))
cd tests/docker
docker-compose run --rm pytest /bin/bash -c "pytest --host ${MILVUS_SERVICE_IP}"
```


## Basic Flow

The scripts directly under [`build/`](.) are used to build and test. They will ensure that the `builder` Docker image is built (based on [`build/docker/builder`] ) and then execute the appropriate command in that container. These scripts will both ensure that the right data is cached from run to run for incremental builds and will copy the results back out of the container. You can specify a different registry/name for `builder` by setting `IMAGE_REPO` which defaults to  `milvusdb`.

The `builder.sh` is execute by first creating a “docker volume“ directory in `.docker/`. The `.docker/` directory is used to cache the third-party package and compiler cache data. It speeds up recompilation by caching previous compilations and detecting when the same compilation is being done again.
