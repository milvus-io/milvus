# Compile and install milvus-dustributed

## Environment

```
    OS: Ubuntu 18.04
    go：1.15
    cmake: >=3.16
    gcc： 7.5
```

### Install dependencies

```shell script
    sudo apt install -y g++ gcc make libssl-dev zlib1g-dev libboost-regex-dev \
    libboost-program-options-dev libboost-system-dev libboost-filesystem-dev \
    libboost-serialization-dev python3-dev libboost-python-dev libcurl4-openssl-dev gfortran libtbb-dev

    export GO111MODULE=on
    go get github.com/golang/protobuf/protoc-gen-go@v1.3.2
```

#### Install OpenBlas library

```shell script
    wget https://github.com/xianyi/OpenBLAS/archive/v0.3.9.tar.gz && \
    tar zxvf v0.3.9.tar.gz && cd OpenBLAS-0.3.9 && \
    make TARGET=CORE2 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 USE_THREAD=0 USE_OPENMP=0 FC=gfortran CC=gcc COMMON_OPT="-O3 -g -fPIC" FCOMMON_OPT="-O3 -g -fPIC -frecursive" NMAX="NUM_THREADS=128" LIBPREFIX="libopenblas" LAPACKE="NO_LAPACKE=1" INTERFACE64=0 NO_STATIC=1 && \
    make PREFIX=/usr install
```

### Compile

#### Generate the go files from proto file

```shell script
    cd milvus-distributed
    pwd_dir=`pwd`
    export PATH=$PATH:$(go env GOPATH)/bin
    export protoc=${pwd_dir}/internal/core/cmake_build/thirdparty/protobuf/protobuf-build/protoc
    ./ci/scripts/proto_gen_go.sh
```

#### Check code specifications

```shell script
    make verifiers
```

#### Compile

```shell script
    make all
```

#### Start service

```shell script
    cd deployments
    docker-compose up -d
```

#### Run unittest

```shell script
    make unittest
```
