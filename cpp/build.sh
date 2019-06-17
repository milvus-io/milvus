#!/bin/bash

BUILD_TYPE="Debug"
BUILD_UNITTEST="off"
LICENSE_CHECK="OFF"
INSTALL_PREFIX=$(pwd)/milvus
MAKE_CLEAN="OFF"

while getopts "p:t:uhgr" arg
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
             h) # help
                echo "

parameter:
-t: build type
-u: building unit test options
-p: install prefix
-l: build license version
-r: remove previous build directory

usage:
./build.sh -t \${BUILD_TYPE} [-u] [-h] [-g] [-r]
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

if [[ ${MAKE_CLEAN} = "ON" ]]; then
    CMAKE_CMD="cmake -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
    -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
    -DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
    -DCMAKE_LICENSE_CHECK=${LICENSE_CHECK} \
    $@ ../"
    echo ${CMAKE_CMD}

    ${CMAKE_CMD}
    make clean
fi

make -j 4 || exit 1

if [[ ${BUILD_TYPE} != "Debug" ]]; then
    strip src/milvus_server
fi

make install

