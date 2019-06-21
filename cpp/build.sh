#!/bin/bash

BUILD_TYPE="Debug"
BUILD_UNITTEST="off"
LICENSE_CHECK="OFF"
INSTALL_PREFIX=$(pwd)/milvus
MAKE_CLEAN="OFF"
BUILD_COVERAGE="OFF"

while getopts "p:t:uhlrc" arg
do
        case $arg in
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             u)
                echo "Build and run unittest cases" ;
                BUILD_UNITTEST="on";
                ;;
             p)
                INSTALL_PREFIX=$OPTARG
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
             h) # help
                echo "

parameter:
-t: build type
-u: building unit test options
-p: install prefix
-l: build license version
-r: remove previous build directory
-c: code coverage

usage:
./build.sh -t \${BUILD_TYPE} [-u] [-h] [-g] [-r] [-c]
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

cd cmake_build

CUDA_COMPILER=/usr/local/cuda/bin/nvcc

if [[ ${MAKE_CLEAN} == "ON" ]]; then
    CMAKE_CMD="cmake -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
    -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
    -DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
    -DCMAKE_LICENSE_CHECK=${LICENSE_CHECK} \
    -DBUILD_COVERAGE=${BUILD_COVERAGE} \
    $@ ../"
    echo ${CMAKE_CMD}

    ${CMAKE_CMD}
    make clean
fi

make -j 4 || exit 1

if [[ ${BUILD_TYPE} != "Debug" ]]; then
    strip src/milvus_server
fi

if [[ ${BUILD_COVERAGE} == "ON" ]]; then
    cd -
    bash `pwd`/coverage.sh
    cd -
fi

make install
