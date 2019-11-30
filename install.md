# Install Milvus from Source Code

## Software requirements

- Ubuntu 18.04 or higher

  If your operating system is not Ubuntu 18.04 or higher, we recommend you to pull a [docker image of Ubuntu 18.04](https://docs.docker.com/install/linux/docker-ce/ubuntu/) as your compilation environment.
  
- CMake 3.12 or higher

##### For GPU version, you will also need:

- CUDA 10.0 or higher
- NVIDIA driver 418 or higher

## Compilation

### Step 1 Install dependencies

```shell
$ cd [Milvus root path]/core
$ ./ubuntu_build_deps.sh
```

### Step 2 Build

```shell
$ cd [Milvus root path]/core
$ ./build.sh -t Debug
or 
$ ./build.sh -t Release
```

By default, it will build CPU version. To build GPU version, add `-g` option
```shell
$ ./build.sh -g
```

If you want to know the complete build options, run
```shell
$./build.sh -h
```

When the build is completed, all the stuff that you need in order to run Milvus will be installed under `[Milvus root path]/core/milvus`.

## Launch Milvus server

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

## Troubleshooting
1. If you encounter the following error when compiling: 
`protocol https not supported or disabled in libcurl`.
First, make sure you have `libcurl4-openssl-dev` installed in your system.
Then try reinstall CMake from source with `--system-curl` option:
```shell
$ ./bootstrap --system-curl 
$ make 
$ sudo make install
```

