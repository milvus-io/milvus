#!/bin/bash

BUILD_TYPE="Debug"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX=$(pwd)/milvus
MAKE_CLEAN="OFF"
BUILD_COVERAGE="OFF"
DB_PATH="/opt/milvus"
PROFILING="OFF"
USE_JFROG_CACHE="OFF"
RUN_CPPLINT="OFF"
CUDA_COMPILER=/usr/local/cuda/bin/nvcc

while getopts "p:d:t:ulrcgjh" arg
do
        case $arg in
             p)
                INSTALL_PREFIX=$OPTARG
                ;;
             d)
                DB_PATH=$OPTARG
                ;;
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             u)
                echo "Build and run unittest cases" ;
                BUILD_UNITTEST="ON";
                ;;
             l)
                RUN_CPPLINT="ON"
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
             j)
                USE_JFROG_CACHE="ON"
                ;;
             h) # help
                echo "

parameter:
-p: install prefix(default: $(pwd)/milvus)
-d: db path(default: /opt/milvus)
-t: build type(default: Debug)
-u: building unit test options(default: OFF)
-l: run cpplint, clang-format and clang-tidy(default: OFF)
-r: remove previous build directory(default: OFF)
-c: code coverage(default: OFF)
-g: profiling(default: OFF)
-j: use jfrog cache build directory(default: OFF)
-h: help

usage:
./build.sh -p \${INSTALL_PREFIX} -t \${BUILD_TYPE} [-u] [-l] [-r] [-c] [-g] [-j] [-h]
                "
                exit 0
                ;;
             ?)
                echo "ERROR! unknown argument"
        exit 1
        ;;
        esac
done

if [[ ! -d cmake_build ]]; then
    mkdir cmake_build
fi

cd cmake_build

CMAKE_CMD="cmake \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DMILVUS_DB_PATH=${DB_PATH} \
-DMILVUS_ENABLE_PROFILING=${PROFILING} \
-DUSE_JFROG_CACHE=${USE_JFROG_CACHE} \
../"
echo ${CMAKE_CMD}
${CMAKE_CMD}

if [[ ${MAKE_CLEAN} == "ON" ]]; then
    make clean
fi

if [[ ${RUN_CPPLINT} == "ON" ]]; then
    # cpplint check
    make lint
    if [ $? -ne 0 ]; then
        echo "ERROR! cpplint check failed"
        exit 1
    fi
    echo "cpplint check passed!"

    # clang-format check
    make check-clang-format
    if [ $? -ne 0 ]; then
        echo "ERROR! clang-format check failed"
        exit 1
    fi
    echo "clang-format check passed!"

    # clang-tidy check
    make check-clang-tidy
    if [ $? -ne 0 ]; then
        echo "ERROR! clang-tidy check failed"
        exit 1
    fi
    echo "clang-tidy check passed!"
else
    # compile and build
    make -j 4 || exit 1

    # strip binary symbol
    if [[ ${BUILD_TYPE} != "Debug" ]]; then
        strip src/milvus_server
    fi

    make install || exit 1

    # evaluate code coverage
    if [[ ${BUILD_COVERAGE} == "ON" ]]; then
        cd -
        bash `pwd`/coverage.sh
        cd -
    fi
fi