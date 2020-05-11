#!/bin/bash

BUILD_OUTPUT_DIR="cmake_build"
BUILD_TYPE="Debug"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX=$(pwd)/milvus
MAKE_CLEAN="OFF"
BUILD_COVERAGE="OFF"
DB_PATH="/tmp/milvus"
PROFILING="OFF"
RUN_CPPLINT="OFF"
CUDA_COMPILER=/usr/local/cuda/bin/nvcc
GPU_VERSION="OFF" #defaults to CPU version
WITH_MKL="OFF"
FAISS_ROOT="" #FAISS root path
FAISS_SOURCE="BUNDLED"
WITH_PROMETHEUS="ON"
FIU_ENABLE="OFF"
BUILD_OPENBLAS="ON"

while getopts "p:d:t:f:ulrcghzmei" arg; do
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
  f)
    FAISS_ROOT=$OPTARG
    FAISS_SOURCE="AUTO"
    ;;
  u)
    echo "Build and run unittest cases"
    BUILD_UNITTEST="ON"
    ;;
  l)
    RUN_CPPLINT="ON"
    ;;
  r)
    if [[ -d ${BUILD_OUTPUT_DIR} ]]; then
      rm ./${BUILD_OUTPUT_DIR} -r
      MAKE_CLEAN="ON"
    fi
    ;;
  c)
    BUILD_COVERAGE="ON"
    ;;
  z)
    PROFILING="ON"
    ;;
  g)
    GPU_VERSION="ON"
    ;;
  m)
    WITH_MKL="ON"
    ;;
  e)
    WITH_PROMETHEUS="OFF"
    ;;
  i)
    FIU_ENABLE="ON"
    ;;
  h) # help
    echo "

parameter:
-p: install prefix(default: $(pwd)/milvus)
-d: db data path(default: /tmp/milvus)
-t: build type(default: Debug)
-f: FAISS root path(default: empty). The path should be an absolute path
    containing the pre-installed lib/ and include/ directory of FAISS. If they can't be found,
    we will build the original FAISS from source instead.
-u: building unit test options(default: OFF)
-l: run cpplint, clang-format and clang-tidy(default: OFF)
-r: remove previous build directory(default: OFF)
-c: code coverage(default: OFF)
-z: profiling(default: OFF)
-g: build GPU version(default: OFF)
-m: build with MKL(default: OFF)
-e: build without prometheus(default: OFF)
-i: build FIU_ENABLE(default: OFF)
-h: help

usage:
./build.sh -p \${INSTALL_PREFIX} -t \${BUILD_TYPE} -f \${FAISS_ROOT} [-u] [-l] [-r] [-c] [-z] [-g] [-m] [-e] [-h]
                "
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done

if [[ ! -d ${BUILD_OUTPUT_DIR} ]]; then
  mkdir ${BUILD_OUTPUT_DIR}
fi

cd ${BUILD_OUTPUT_DIR}

# remove make cache since build.sh -l use default variables
# force update the variables each time
make rebuild_cache >/dev/null 2>&1

CMAKE_CMD="cmake \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DFAISS_ROOT=${FAISS_ROOT} \
-DFAISS_SOURCE=${FAISS_SOURCE} \
-DOpenBLAS_SOURCE=AUTO \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DMILVUS_DB_PATH=${DB_PATH} \
-DENABLE_CPU_PROFILING=${PROFILING} \
-DMILVUS_GPU_VERSION=${GPU_VERSION} \
-DFAISS_WITH_MKL=${WITH_MKL} \
-DMILVUS_WITH_PROMETHEUS=${WITH_PROMETHEUS} \
-DMILVUS_WITH_FIU=${FIU_ENABLE} \
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

#    # clang-tidy check
#    make check-clang-tidy
#    if [ $? -ne 0 ]; then
#        echo "ERROR! clang-tidy check failed"
#        exit 1
#    fi
#    echo "clang-tidy check passed!"
else

  # compile and build
  make -j 8 install || exit 1
fi
