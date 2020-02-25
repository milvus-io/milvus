# Install Milvus from Source Code

<!-- TOC -->

- [Build from source](#build-from-source)
  - [Requirements](#requirements)
  - [Compilation](#compilation)
  - [Launch Milvus server](#launch-milvus-server)
- [Compile Milvus on Docker](#compile-milvus-on-docker)
  - [Step 1 Pull Milvus Docker images](#step-1-pull-milvus-docker-images)
  - [Step 2 Start the Docker container](#step-2-start-the-docker-container)
  - [Step 3 Download Milvus source code](#step-3-download-milvus-source-code)
  - [Step 4 Compile Milvus in the container](#step-4-compile-milvus-in-the-container)
- [Troubleshooting](#troubleshooting)
  - [Error message: `protocol https not supported or disabled in libcurl`](#error-message-protocol-https-not-supported-or-disabled-in-libcurl)
  - [Error message: `internal compiler error`](#error-message-internal-compiler-error)
  - [Error message: `error while loading shared libraries: libmysqlpp.so.3`](#error-message-error-while-loading-shared-libraries-libmysqlppso3)
  - [CMake version is not supported](#cmake-version-is-not-supported)

<!-- /TOC -->

## Build from source

### Requirements

- Operating system
  - Ubuntu 18.04 or higher
  - CentOS 7

  If your operating system does not meet the requirements, we recommend that you pull a Docker image of [Ubuntu 18.04](https://docs.docker.com/install/linux/docker-ce/ubuntu/) or [CentOS 7](https://docs.docker.com/install/linux/docker-ce/centos/) as your compilation environment.
  
- GCC 7.0 or higher to support C++ 17
- CMake 3.12 or higher

For GPU-enabled version, you will also need:

- CUDA 10.0 or higher
- NVIDIA driver 418 or higher

### Compilation

#### Step 1 Install dependencies

##### Install in Ubuntu

```shell
$ cd [Milvus root path]/core
$ ./ubuntu_build_deps.sh
```

##### Install in CentOS

```shell
$ cd [Milvus root path]/core
$ ./centos7_build_deps.sh
```

#### Step 2 Build

```shell
$ cd [Milvus root path]/core
$ ./build.sh -t Debug
```

or

```shell
$ ./build.sh -t Release
```

By default, it will build CPU-only version. To build GPU version, add `-g` option.

```shell
$ ./build.sh -g
```

If you want to know the complete build options, run the following command.

```shell
$./build.sh -h
```

When the build is completed, everything that you need in order to run Milvus will be installed under `[Milvus root path]/core/milvus`.

### Launch Milvus server

```shell
$ cd [Milvus root path]/core/milvus
```

Add `lib/` directory to `LD_LIBRARY_PATH`

```shell
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:[Milvus root path]/core/milvus/lib
```

Then start Milvus server:

```shell
$ cd scripts
$ ./start_server.sh
```

To stop Milvus server, run:

```shell
$ ./stop_server.sh
```

## Compile Milvus on Docker

With the following Docker images, you should be able to compile Milvus on any Linux platform that run Docker. To build a GPU supported Milvus, you need to install [NVIDIA Docker](https://github.com/NVIDIA/nvidia-docker/) first.

### Step 1 Pull Milvus Docker images

Pull CPU-only image:

```shell
$ docker pull milvusdb/milvus-cpu-build-env:latest
```

Pull GPU-enabled image:

```shell
$ docker pull milvusdb/milvus-gpu-build-env:latest
```
### Step 2 Start the Docker container

Start a CPU-only container:

```shell
$ docker run -it -p 19530:19530 -d milvusdb/milvus-cpu-build-env:latest
```

Start a GPU container:

```shell
$ docker run --runtime=nvidia -it -p 19530:19530 -d milvusdb/milvus-gpu-build-env:latest
```
To enter the container:

```shell
$ docker exec -it [container_id] bash
```

### Step 3 Download Milvus source code

Download latest Milvus source code:

```shell
$ cd /home
$ git clone https://github.com/milvus-io/milvus
```

To enter its core directory:

```shell
$ cd ./milvus/core
```

### Step 4 Compile Milvus in the container

If you are using a CPU-only image, compile it like this:

```shell
$ ./build.sh -t Release
```

If you are using a GPU-enabled image, you need to add a `-g` parameter:

```shell
$ ./build.sh -g -t Release
```

Then start Milvus server：

```shell
$ ./start_server.sh
```

## Troubleshooting

### Error message: `protocol https not supported or disabled in libcurl`

Follow the steps below to solve this problem:

1. Make sure you have `libcurl4-openssl-dev` installed in your system.
2. Try reinstalling the latest CMake from source with `--system-curl` option:

   ```shell
   $ ./bootstrap --system-curl
   $ make
   $ sudo make install
   ```

   If the `--system-curl` command doesn't work, you can also reinstall CMake in **Ubuntu Software** on your local computer.

### Error message: `internal compiler error`

Try increasing the memory allocated to Docker. If this doesn't work, you can reduce the number of threads in CMake build in `[Milvus root path]/core/build.sh`.

```shell
make -j 8 install || exit 1 # The default number of threads is 8.
```

Note: You might also need to configure CMake build for faiss in `[Milvus root path]/core/src/index/thirdparty/faiss`.

### Error message: `error while loading shared libraries: libmysqlpp.so.3`

Follow the steps below to solve this problem:

1. Check whether `libmysqlpp.so.3` is correctly installed.
2. If `libmysqlpp.so.3` is installed, check whether it is added to `LD_LIBRARY_PATH`.

### CMake version is not supported

Follow the steps below to install a supported version of CMake:

1. Remove the unsupported version of CMake.
2. Get CMake 3.12 or higher. Here we get CMake 3.12.

    ```shell
    $ wget https://cmake.org/files/v3.12/cmake-3.12.2-Linux-x86_64.tar.gz
    ```

3. Extract the file and install CMake.

    ```shell
    $ tar zxvf cmake-3.12.2-Linux-x86_64.tar.gz
    $ mv cmake-3.12.2-Linux-x86_64 /opt/cmake-3.12.2
    $ ln -sf /opt/cmake-3.12.2/bin/* /usr/bin/
    ```
