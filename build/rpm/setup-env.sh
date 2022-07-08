#!/bin/bash

# ref: github.com/milvus-io/milvus/docker/buidler/cpu/centos7/Dockerfile github.com/milvus-io/milvus/docker/openblas/centos7/Dockerfile

# Install devltoolset
yum install -y epel-release centos-release-scl-rh && yum install -y wget make automake \
    devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran && \
    rm -rf /var/cache/yum/* && \
    echo "source scl_source enable devtoolset-7" >> /etc/profile.d/devtoolset-7.sh

# Install openblas
source /etc/profile.d/devtoolset-7.sh && \
    wget https://github.com/xianyi/OpenBLAS/archive/v0.3.10.tar.gz && \
    tar zxvf v0.3.10.tar.gz && cd OpenBLAS-0.3.10 && \
    make TARGET=CORE2 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 USE_THREAD=0 USE_OPENMP=0 FC=gfortran CC=gcc COMMON_OPT="-O3 -g -fPIC" FCOMMON_OPT="-O3 -g -fPIC -frecursive" NMAX="NUM_THREADS=128" LIBPREFIX="libopenblas" LAPACKE="NO_LAPACKE=1" INTERFACE64=0 NO_STATIC=1 && \
    make PREFIX=/usr NO_STATIC=1 install && \
    cd .. && rm -rf OpenBLAS-0.3.10 && rm v0.3.10.tar.gz

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"

# Install tbb
source /etc/profile.d/devtoolset-7.sh && \
    git clone https://github.com/wjakob/tbb.git && \
    cd tbb/build && \
    cmake .. && make -j && make install && \
    cd ../../ && rm -rf tbb/

# Install boost
source /etc/profile.d/devtoolset-7.sh && \
    wget -q https://boostorg.jfrog.io/artifactory/main/release/1.65.1/source/boost_1_65_1.tar.gz && \
    tar zxf boost_1_65_1.tar.gz && cd boost_1_65_1 && \
    ./bootstrap.sh --prefix=/usr/local --with-toolset=gcc --without-libraries=python && \
    ./b2 -j2 --prefix=/usr/local --without-python toolset=gcc install && \
    cd ../ && rm -rf ./boost_1_65_1*

export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

# Install Go
export GOPATH="/go"
export GOROOT="/usr/local/go"
export GO111MODULE="on"
export PATH="$GOPATH/bin:$GOROOT/bin:$PATH"
mkdir -p /usr/local/go && wget -qO- "https://golang.org/dl/go1.15.2.linux-amd64.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go && \
    mkdir -p "$GOPATH/src" "$GOPATH/bin" && \
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ${GOPATH}/bin v1.43.0 && \
    export GO111MODULE=on && go get github.com/quasilyte/go-ruleguard/cmd/ruleguard@v0.2.1 && \
    go get -v github.com/ramya-rao-a/go-outline && \
    go get -v golang.org/x/tools/gopls && \
    go get -v github.com/uudashr/gopkgs/v2/cmd/gopkgs && \
    go get -v github.com/go-delve/delve/cmd/dlv && \
    go get -v honnef.co/go/tools/cmd/staticcheck && \
    go clean --modcache && \
    chmod -R 777 "$GOPATH" && chmod -R a+w $(go env GOTOOLDIR)

ln -s /go/bin/dlv /go/bin/dlv-dap

