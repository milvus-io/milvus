#!/bin/bash

BUILD_TYPE="Debug"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX=$(pwd)/cmake_build
MAKE_CLEAN="OFF"
PROFILING="OFF"

while getopts "p:d:t:uhrcgm" arg
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
             r)
                if [[ -d cmake_build ]]; then
                    cd cmake_build
                    make clean
                fi
                ;;
             g)
                PROFILING="ON"
                ;;
             h) # help
                echo "

parameter:
-t: build type(default: Debug)
-u: building unit test options(default: OFF)
-p: install prefix(default: $(pwd)/knowhere)
-r: remove previous build directory(default: OFF)
-g: profiling(default: OFF)

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
fi

cd cmake_build

CUDA_COMPILER=/usr/local/cuda/bin/nvcc

CMAKE_CMD="cmake -DKNOWHERE_BUILD_TESTS=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DMILVUS_ENABLE_PROFILING=${PROFILING} \
../"
echo ${CMAKE_CMD}

${CMAKE_CMD}

make -j 8 ||exit 1

make install || exit 1


