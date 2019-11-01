# Install Milvus from Source Code

## Software requirements

- Ubuntu 18.04 or higher
- CMake 3.14 or higher
- CUDA 10.0 or higher
- NVIDIA driver 418 or higher

## Compilation

### Step 1 Install dependencies

```shell
$ cd [Milvus sourcecode path]/core
$ ./ubuntu_build_deps.sh
```

### Step 2 Build

```shell
$ cd [Milvus sourcecode path]/core
$ ./build.sh -t Debug
or 
$ ./build.sh -t Release
```

When the build is completed, all the stuff that you need in order to run Milvus will be installed under `[Milvus root path]/core/milvus`.

## Launch Milvus server

```shell
$ cd [Milvus root path]/core/milvus
```

Add `lib/` directory to `LD_LIBRARY_PATH`

```
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/milvus/lib
```

Then start Milvus server:

```
$ cd scripts
$ ./start_server.sh
```

To stop Milvus server, run:

```shell
$ ./stop_server.sh
```
