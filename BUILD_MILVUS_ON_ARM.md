# Build Milvus on ARM

## Build from source

### Requirements

- Ubuntu 18.04 or higher
- GCC 7.0 or higher to support C++17
- CMake 3.12 or higher

### Compilation

#### Step 1 Install dependencies

```
$ cd [Milvus root path]/core
$ ./ubuntu_build_deps.sh
```

#### Step 2 Build

```
$ cd [Milvus root path]/core
$ ./build.sh -t Release
```

When the build is completed, all the stuff that you need in order to run Milvus will be installed under [Milvus root path]/core/milvus.

### Launch Milvus server

```
$ cd [Milvus root path]/core/milvus
```

Add lib/ directory to LD_LIBRARY_PATH

```
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:[Milvus root path]/core/milvus/lib
```

Then start Milvus server:

```
$ cd scripts
$ ./start_server.sh
```

To stop Milvus server, run:

```
$ ./stop_server.sh
```
