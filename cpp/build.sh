#!/bin/bash

BUILD_TYPE="Debug"
BUILD_UNITTEST="off"
INSTALL_PREFIX=$(pwd)/megasearch

while getopts "p:t:uh" arg
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
             h) # help
                echo "

parameter:
-t: build type
-u: building unit test options
-p: install prefix

usage:
./build.sh -t \${BUILD_TYPE} [-u] [-h]
                "
                exit 0
                ;;
             ?)
                echo "unknown argument"
        exit 1
        ;;
        esac
done

if [[ -d cmake_build ]]; then
	rm cmake_build -r
fi

rm -rf ./cmake_build
mkdir cmake_build
cd cmake_build

CUDA_COMPILER=/usr/local/cuda/bin/nvcc

CMAKE_CMD="cmake -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
$@ ../"
echo ${CMAKE_CMD}

${CMAKE_CMD}

make clean && make -j || exit 1

if [[ ${BUILD_TYPE} != "Debug" ]]; then
    strip src/vecwise_server
fi

make install

