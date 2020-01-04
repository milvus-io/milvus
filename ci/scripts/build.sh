#!/bin/bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

MILVUS_CORE_DIR="${SCRIPTS_DIR}/../../core"
CORE_BUILD_DIR="${MILVUS_CORE_DIR}/cmake_build"
BUILD_TYPE="Debug"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX="/var/lib/milvus"
FAISS_ROOT=""
PRIVILEGES="OFF"
BUILD_COVERAGE="OFF"
RUN_CPPLINT="OFF"
GPU_VERSION="OFF"
WITH_MKL="OFF"
CUDA_COMPILER=/usr/local/cuda/bin/nvcc

while getopts "o:t:b:f:pgulcmh" arg
do
        case $arg in
             o)
                INSTALL_PREFIX=$OPTARG
                ;;
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             b)
                CORE_BUILD_DIR=$OPTARG # CORE_BUILD_DIR
                ;;
             f)
                FAISS_ROOT=$OPTARG # FAISS ROOT PATH
                ;;
             p)
                PRIVILEGES="ON" # ELEVATED PRIVILEGES
                ;;
             g)
                GPU_VERSION="ON";
                ;;
             u)
                echo "Build and run unittest cases" ;
                BUILD_UNITTEST="ON";
                ;;
             l)
                RUN_CPPLINT="ON"
                ;;
             c)
                BUILD_COVERAGE="ON"
                ;;
             m)
                WITH_MKL="ON"
                ;;
             h) # help
                echo "

parameter:
-o: install prefix(default: /var/lib/milvus)
-t: build type(default: Debug)
-b: core code build directory
-f: faiss root path
-p: install command with elevated privileges
-g: gpu version
-u: building unit test options(default: OFF)
-l: run cpplint, clang-format and clang-tidy(default: OFF)
-c: code coverage(default: OFF)
-m: build with MKL(default: OFF)
-h: help

usage:
./build.sh -o \${INSTALL_PREFIX} -t \${BUILD_TYPE} -b \${CORE_BUILD_DIR} -f \${FAISS_ROOT} [-p] [-g] [-u] [-l] [-c] [-m] [-h]
                "
                exit 0
                ;;
             ?)
                echo "ERROR! unknown argument"
        exit 1
        ;;
        esac
done

echo -e "===\n=== ccache statistics before build\n==="
ccache --show-stats

if [[ ! -d ${CORE_BUILD_DIR} ]]; then
    mkdir ${CORE_BUILD_DIR}
fi

cd ${CORE_BUILD_DIR}

CMAKE_CMD="cmake \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DMILVUS_GPU_VERSION=${GPU_VERSION} \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DFAISS_ROOT=${FAISS_ROOT} \
-DFAISS_WITH_MKL=${WITH_MKL} \
-DArrow_SOURCE=AUTO \
-DFAISS_SOURCE=AUTO \
${MILVUS_CORE_DIR}"
echo ${CMAKE_CMD}
${CMAKE_CMD}


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

#    # clang-tidy check
#    make check-clang-tidy
#    if [ $? -ne 0 ]; then
#        echo "ERROR! clang-tidy check failed"
#        rm -f CMakeCache.txt
#        exit 1
#    fi
#    echo "clang-tidy check passed!"
fi

# compile and build
make -j8 || exit 1

if [[ ${PRIVILEGES} == "ON" ]];then
    sudo make install || exit 1
else
    make install || exit 1
fi
