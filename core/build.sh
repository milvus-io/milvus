#!/bin/bash

# Compile jobs variable; Usage: $ jobs=12 ./build.sh ...
if [[ ! ${jobs+1} ]]; then
    jobs=$(nproc)
fi

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
WITH_PROMETHEUS="ON"
FIU_ENABLE="OFF"
CUDA_ARCH="DEFAULT"

while getopts "p:d:t:f:s:ulrcghzmei" arg; do
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
  e)
    WITH_PROMETHEUS="OFF"
    ;;
  i)
    FIU_ENABLE="ON"
    ;;
  s)
    CUDA_ARCH=$OPTARG
    ;;
  h) # help
    echo "

parameter:
-p: install prefix(default: $(pwd)/milvus)
-d: db data path(default: /tmp/milvus)
-t: build type(default: Debug)
-u: building unit test options(default: OFF)
-l: run cpplint, clang-format and clang-tidy(default: OFF)
-r: remove previous build directory(default: OFF)
-c: code coverage(default: OFF)
-z: profiling(default: OFF)
-g: build GPU version(default: OFF)
-e: build without prometheus(default: OFF)
-i: build FIU_ENABLE(default: OFF)
-s: build with CUDA arch(default:DEFAULT), for example '-gencode=compute_61,code=sm_61;-gencode=compute_75,code=sm_75'
-h: help

usage:
./build.sh -p \${INSTALL_PREFIX} -t \${BUILD_TYPE} -s \${CUDA_ARCH}[-u] [-l] [-r] [-c] [-z] [-g] [-m] [-e] [-h]
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
-DOpenBLAS_SOURCE=AUTO \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DMILVUS_DB_PATH=${DB_PATH} \
-DENABLE_CPU_PROFILING=${PROFILING} \
-DMILVUS_GPU_VERSION=${GPU_VERSION} \
-DMILVUS_WITH_PROMETHEUS=${WITH_PROMETHEUS} \
-DMILVUS_WITH_FIU=${FIU_ENABLE} \
-DMILVUS_CUDA_ARCH=${CUDA_ARCH} \
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

  clang-format check
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
  make -j ${jobs} install || exit 1
fi
