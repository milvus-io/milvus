#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Compile jobs variable; Usage: $ jobs=12 ./core_build.sh ...
if [[ ! ${jobs+1} ]]; then
    if command -v nproc &> /dev/null
    # For linux
    then
        jobs=$(nproc)
    elif command -v sysctl &> /dev/null
    # For macOS
    then
        jobs=$(sysctl -n hw.logicalcpu)
    else
        jobs=4
    fi
fi

function get_cpu_arch {
  local CPU_ARCH=$1

  local OS
  OS=$(uname)
  local MACHINE
  MACHINE=$(uname -m)
  ADDITIONAL_FLAGS=""

  if [ -z "$CPU_ARCH" ]; then

    if [ "$OS" = "Darwin" ]; then

      if [ "$MACHINE" = "x86_64" ]; then
        local CPU_CAPABILITIES
        CPU_CAPABILITIES=$(sysctl -a | grep machdep.cpu.features | awk '{print tolower($0)}')

        if [[ $CPU_CAPABILITIES =~ "avx" ]]; then
          CPU_ARCH="avx"
        else
          CPU_ARCH="sse"
        fi

      elif [[ $(sysctl -a | grep machdep.cpu.brand_string) =~ "Apple" ]]; then
        # Apple silicon.
        CPU_ARCH="arm64"
      fi

    else [ "$OS" = "Linux" ];

      local CPU_CAPABILITIES
      CPU_CAPABILITIES=$(cat /proc/cpuinfo | grep flags | head -n 1| awk '{print tolower($0)}')

      if [[ "$CPU_CAPABILITIES" =~ "avx" ]]; then
            CPU_ARCH="avx"
      elif [[ "$CPU_CAPABILITIES" =~ "sse" ]]; then
            CPU_ARCH="sse"
      elif [ "$MACHINE" = "aarch64" ]; then
            CPU_ARCH="aarch64"
      fi
    fi
  fi
  echo -n $CPU_ARCH
}

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

CPP_SRC_DIR="${ROOT_DIR}/internal/core"

BUILD_OUTPUT_DIR="${ROOT_DIR}/cmake_build"
BUILD_TYPE="Release"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX="${CPP_SRC_DIR}/output"
MAKE_CLEAN="OFF"
BUILD_COVERAGE="OFF"
DB_PATH="/tmp/milvus"
PROFILING="OFF"
RUN_CPPLINT="OFF"
CUDA_COMPILER=/usr/local/cuda/bin/nvcc
GPU_VERSION="OFF" #defaults to CPU version
WITH_PROMETHEUS="ON"
CUDA_ARCH="DEFAULT"
CUSTOM_THIRDPARTY_PATH=""
EMBEDDED_MILVUS="OFF"
BUILD_DISK_ANN="OFF"
USE_ASAN="OFF"
OPEN_SIMD="OFF"
USE_DYNAMIC_SIMD="OFF"

while getopts "p:d:t:s:f:n:i:y:a:ulrcghzmeb" arg; do
  case $arg in
  f)
    CUSTOM_THIRDPARTY_PATH=$OPTARG
    ;;
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
  s)
    CUDA_ARCH=$OPTARG
    ;;
  b)
    EMBEDDED_MILVUS="ON"
    ;;
  n)
    BUILD_DISK_ANN=$OPTARG
    ;;
  a)
    ENV_VAL=$OPTARG
    if [[ ${ENV_VAL} == 'true' ]]; then
        echo "Set USE_ASAN to ON"
        USE_ASAN="ON"
        BUILD_TYPE=Debug
    fi
    ;;
  i)
    OPEN_SIMD=$OPTARG
    ;;
  y)
    USE_DYNAMIC_SIMD=$OPTARG
    ;;
  h) # help
    echo "

parameter:
-f: custom paths of thirdparty downloaded files(default: NULL)
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
-s: build with CUDA arch(default:DEFAULT), for example '-gencode=compute_61,code=sm_61;-gencode=compute_75,code=sm_75'
-b: build embedded milvus(default: OFF)
-a: build milvus with AddressSanitizer(default: false)
-h: help

usage:
./core_build.sh -p \${INSTALL_PREFIX} -t \${BUILD_TYPE} -s \${CUDA_ARCH} -f\${CUSTOM_THIRDPARTY_PATH} [-u] [-l] [-r] [-c] [-z] [-g] [-m] [-e] [-h] [-b]
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
source ${ROOT_DIR}/scripts/setenv.sh

CMAKE_GENERATOR="Unix Makefiles"

# MSYS system
if [ "$MSYSTEM" == "MINGW64" ] ; then
  BUILD_COVERAGE=OFF
  PROFILING=OFF
  GPU_VERSION=OFF
  WITH_PROMETHEUS=OFF
  CUDA_ARCH=OFF

  # extra default cmake args for msys
  CMAKE_GENERATOR="MSYS Makefiles"

  # clang tools path
  export CLANG_TOOLS_PATH=/mingw64/bin

  # using system blas
  export OpenBLAS_HOME="$(cygpath -w /mingw64)"
fi

# UBUNTU system build diskann index
if [ "$OS_NAME" == "ubuntu20.04" ] ; then
  BUILD_DISK_ANN=ON
fi

pushd ${BUILD_OUTPUT_DIR}

# Remove make cache since build.sh -l use default variables
# Force update the variables each time
make rebuild_cache >/dev/null 2>&1


if [[ ${MAKE_CLEAN} == "ON" ]]; then
  echo "Runing make clean in ${BUILD_OUTPUT_DIR} ..."
  make clean
  exit 0
fi

CPU_ARCH=$(get_cpu_arch $CPU_TARGET)

arch=$(uname -m)
CMAKE_CMD="cmake \
${CMAKE_EXTRA_ARGS} \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DOpenBLAS_SOURCE=AUTO \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DCMAKE_LIBRARY_ARCHITECTURE=${arch} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DMILVUS_DB_PATH=${DB_PATH} \
-DENABLE_CPU_PROFILING=${PROFILING} \
-DMILVUS_GPU_VERSION=${GPU_VERSION} \
-DMILVUS_WITH_PROMETHEUS=${WITH_PROMETHEUS} \
-DMILVUS_CUDA_ARCH=${CUDA_ARCH} \
-DCUSTOM_THIRDPARTY_DOWNLOAD_PATH=${CUSTOM_THIRDPARTY_PATH} \
-DEMBEDDED_MILVUS=${EMBEDDED_MILVUS} \
-DBUILD_DISK_ANN=${BUILD_DISK_ANN} \
-DUSE_ASAN=${USE_ASAN} \
-DOPEN_SIMD=${OPEN_SIMD} \
-DUSE_DYNAMIC_SIMD=${USE_DYNAMIC_SIMD}
-DCPU_ARCH=${CPU_ARCH} \
${CPP_SRC_DIR}"

echo "CC $CC"
echo ${CMAKE_CMD}
${CMAKE_CMD} -G "${CMAKE_GENERATOR}"

set

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
else
  # compile and build
  make -j ${jobs} install || exit 1
fi

if command -v ccache &> /dev/null
then
	ccache -s
fi

popd
