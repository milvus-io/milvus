# Install Milvus from Source Code

- [Build from source](#build-from-source)
- [Compile Milvus on Docker](#compile-milvus-on-docker)

If you encounter any problems/issues compiling Milvus from source, please refer to [Troubleshooting](#troubleshooting).

## Build from source

### Requirements

- Ubuntu 18.04 or higher

  If your operating system is not Ubuntu 18.04 or higher, we recommend you to pull a [docker image of Ubuntu 18.04](https://docs.docker.com/install/linux/docker-ce/ubuntu/) as your compilation environment.
  
- GCC 7.0 or higher to support C++17
- CMake 3.12 or higher

##### For GPU-enabled version, you will also need:

- CUDA 10.0 or higher
- NVIDIA driver 418 or higher

### Compilation

#### Step 1 Install dependencies

```shell
$ cd [Milvus root path]/core
$ ./ubuntu_build_deps.sh
```

#### Step 2 Build

```shell
$ cd [Milvus root path]/core
$ ./build.sh -t Debug
or 
$ ./build.sh -t Release
```

By default, it will build CPU-only version. To build GPU version, add `-g` option
```shell
$ ./build.sh -g
```

If you want to know the complete build options, run
```shell
$./build.sh -h
```

When the build is completed, all the stuff that you need in order to run Milvus will be installed under `[Milvus root path]/core/milvus`.

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

With the following Docker images, you should be able to compile Milvus on any Linux platform that run Docker. To build a GPU supported Milvus, you neeed to install [NVIDIA Docker](https://github.com/NVIDIA/nvidia-docker/) first.

### Step 1 Pull Milvus Docker images

Pull CPU-only image:

```shell
$ docker pull milvusdb/milvus-cpu-build-env:v0.6.0-ubuntu18.04
```

Pull GPU-enabled image:

```shell
$ docker pull milvusdb/milvus-gpu-build-env:v0.6.0-ubuntu18.04
```
### Step 2 Start the Docker container

Start a CPU-only container:

```shell
$ docker run -it -p 19530:19530 -d milvusdb/milvus-cpu-build-env:v0.6.0-ubuntu18.04
```

Start a GPU container:

```shell
$ docker run --runtime=nvidia -it -p 19530:19530 -d milvusdb/milvus-gpu-build-env:v0.6.0-ubuntu18.04
```
To enter the container:

```shell
$ docker exec -it [container_id] bash
```
### Step 3 Download Milvus source code

Download Milvus source code:

```shell
$ cd /home
$ wget https://github.com/milvus-io/milvus/archive/0.6.0.tar.gz
```

Extract the source package:

```shell
$ tar xvf ./v0.6.0.tar.gz
```

The source code is extracted into a folder called `milvus-0.6.0`. To enter its core directory:

```shell
$ cd ./milvus-0.6.0/core
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
1. If you encounter the following error when compiling: 
`protocol https not supported or disabled in libcurl`.
First, make sure you have `libcurl4-openssl-dev` installed in your system.
Then try reinstalling the latest CMake from source with `--system-curl` option:

   ```shell
   $ ./bootstrap --system-curl 
   $ make 
   $ sudo make install
   ```
If the `--system-curl` command doesn't work, you can also reinstall CMake in **Ubuntu Software** on your local computer.
