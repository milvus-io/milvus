#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

CMAKE_BUILD_DIR="${SCRIPTS_DIR}/../../../core/cmake_build"
BUILD_TYPE="Debug"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX="/opt/milvus"
BUILD_COVERAGE="OFF"
DB_PATH="/opt/milvus"
PROFILING="OFF"
USE_JFROG_CACHE="OFF"
RUN_CPPLINT="OFF"
CUSTOMIZATION="OFF" # default use ori faiss
CUDA_COMPILER=/usr/local/cuda/bin/nvcc

CUSTOMIZED_FAISS_URL="${FAISS_URL:-NONE}"
wget -q --method HEAD ${CUSTOMIZED_FAISS_URL}
if [ $? -eq 0 ]; then
  CUSTOMIZATION="ON"
else
  CUSTOMIZATION="OFF"
fi

while getopts "o:d:t:ulcgjhx" arg
do
        case $arg in
             o)
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
             c)
                BUILD_COVERAGE="ON"
                ;;
             g)
                PROFILING="ON"
                ;;
             j)
                USE_JFROG_CACHE="ON"
                ;;
             x)
                CUSTOMIZATION="OFF" # force use ori faiss
                ;;
             h) # help
                echo "

parameter:
-o: install prefix(default: /opt/milvus)
-d: db data path(default: /opt/milvus)
-t: build type(default: Debug)
-u: building unit test options(default: OFF)
-l: run cpplint, clang-format and clang-tidy(default: OFF)
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

if [[ ! -d ${CMAKE_BUILD_DIR} ]]; then
    mkdir ${CMAKE_BUILD_DIR}
fi

cd ${CMAKE_BUILD_DIR}

# remove make cache since build.sh -l use default variables
# force update the variables each time 
make rebuild_cache

CMAKE_CMD="cmake \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DMILVUS_DB_PATH=${DB_PATH} \
-DMILVUS_ENABLE_PROFILING=${PROFILING} \
-DUSE_JFROG_CACHE=${USE_JFROG_CACHE} \
-DCUSTOMIZATION=${CUSTOMIZATION} \
-DFAISS_URL=${CUSTOMIZED_FAISS_URL} \
.."
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
else
    # compile and build
    make -j8 || exit 1
    make install || exit 1
fi
