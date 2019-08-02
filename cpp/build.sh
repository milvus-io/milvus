#!/bin/bash

BUILD_TYPE="Debug"
BUILD_UNITTEST="OFF"
LICENSE_CHECK="OFF"
INSTALL_PREFIX=$(pwd)/milvus
MAKE_CLEAN="OFF"
BUILD_COVERAGE="OFF"
DB_PATH="/opt/milvus"
PROFILING="OFF"
BUILD_FAISS_WITH_MKL="OFF"
USE_JFROG_CACHE="OFF"
KNOWHERE_OPTS=""

while getopts "p:d:t:uhlrcgmj" arg
do
        case $arg in
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             u)
                echo "Build and run unittest cases" ;
                BUILD_UNITTEST="ON";
                ;;
             p)
                INSTALL_PREFIX=$OPTARG
                ;;
             d)
                DB_PATH=$OPTARG
                ;;
             l)
                LICENSE_CHECK="ON"
                ;;
             r)
                if [[ -d cmake_build ]]; then
                    rm ./cmake_build -r
                    MAKE_CLEAN="ON"
                fi
                ;;
             c)
                BUILD_COVERAGE="ON"
                ;;
             g)
                PROFILING="ON"
                ;;
             m)
                BUILD_FAISS_WITH_MKL="ON"
                ;;
             j)
                USE_JFROG_CACHE="ON"
                KNOWHERE_OPTS="${KNOWHERE_OPTS} -j"
                ;;
             h) # help
                echo "

parameter:
-t: build type(default: Debug)
-u: building unit test options(default: OFF)
-p: install prefix(default: $(pwd)/milvus)
-d: db path(default: /opt/milvus)
-l: build license version(default: OFF)
-r: remove previous build directory(default: OFF)
-c: code coverage(default: OFF)
-g: profiling(default: OFF)
-m: build faiss with MKL(default: OFF)
-j: use jfrog cache build directory

usage:
./build.sh -t \${BUILD_TYPE} [-u] [-h] [-g] [-r] [-c] [-m] [-j]
                "
                exit 0
                ;;
             ?)
                echo "unknown argument"
        exit 1
        ;;
        esac
done

if [[ ! -d cmake_build ]]; then
	mkdir cmake_build
	MAKE_CLEAN="ON"
fi

# Build Knowhere
KNOWHERE_BUILD_DIR="`pwd`/thirdparty/knowhere_build"
pushd `pwd`/thirdparty/knowhere
./build.sh -t Release -p ${KNOWHERE_BUILD_DIR} ${KNOWHERE_OPTS}
popd

cd cmake_build

CUDA_COMPILER=/usr/local/cuda/bin/nvcc

if [[ ${MAKE_CLEAN} == "ON" ]]; then
    CMAKE_CMD="cmake -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
    -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
    -DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
    -DCMAKE_LICENSE_CHECK=${LICENSE_CHECK} \
    -DBUILD_COVERAGE=${BUILD_COVERAGE} \
    -DMILVUS_DB_PATH=${DB_PATH} \
    -DMILVUS_ENABLE_PROFILING=${PROFILING} \
    -DBUILD_FAISS_WITH_MKL=${BUILD_FAISS_WITH_MKL} \
    -DMILVUS_ENABLE_THRIFT=${MILVUS_ENABLE_THRIFT} \
    -DKNOWHERE_BUILD_DIR=${KNOWHERE_BUILD_DIR} \
    -DUSE_JFROG_CACHE=${USE_JFROG_CACHE} \
    $@ ../"
    echo ${CMAKE_CMD}

    ${CMAKE_CMD}
    make clean
fi

make -j 4 || exit 1

if [[ ${BUILD_TYPE} != "Debug" ]]; then
    strip src/milvus_server
fi

make install || exit 1

if [[ ${BUILD_COVERAGE} == "ON" ]]; then
    cd -
    bash `pwd`/coverage.sh
    cd -
fi
