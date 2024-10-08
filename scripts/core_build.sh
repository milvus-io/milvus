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
BUILD_COVERAGE="OFF"
RUN_CPPLINT="OFF"
CUDA_COMPILER=/usr/local/cuda/bin/nvcc
GPU_VERSION="OFF" #defaults to CPU version
CUDA_ARCH="DEFAULT"
EMBEDDED_MILVUS="OFF"
BUILD_DISK_ANN="OFF"
USE_ASAN="OFF"
USE_DYNAMIC_SIMD="ON"
USE_OPENDAL="OFF"
INDEX_ENGINE="KNOWHERE"
: "${ENABLE_GCP_NATIVE:="OFF"}"

while getopts "p:d:t:s:f:n:i:y:a:x:o:ulrcghzmebZ" arg; do
  case $arg in
  p)
    INSTALL_PREFIX=$OPTARG
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
  c)
    BUILD_COVERAGE="ON"
    ;;
  g)
    GPU_VERSION="ON"
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
    if [[ ${ENV_VAL} == 'ON' ]]; then
        echo "Set USE_ASAN to ON"
        USE_ASAN="ON"
        BUILD_TYPE=Debug
    fi
    ;;
  y)
    USE_DYNAMIC_SIMD=$OPTARG
    ;;
  Z)
    BUILD_WITHOUT_AZURE="on"
    ;;
  x)
    INDEX_ENGINE=$OPTARG
    ;;
  o)
    USE_OPENDAL=$OPTARG
    ;;
  h) # help
    echo "

parameter:
-p: install prefix(default: $(pwd)/milvus)
-d: db data path(default: /tmp/milvus)
-t: build type(default: Debug)
-u: building unit test options(default: OFF)
-l: run cpplint, clang-format and clang-tidy(default: OFF)
-c: code coverage(default: OFF)
-g: build GPU version(default: OFF)
-e: build without prometheus(default: OFF)
-s: build with CUDA arch(default:DEFAULT), for example '-gencode=compute_61,code=sm_61;-gencode=compute_75,code=sm_75'
-b: build embedded milvus(default: OFF)
-a: build milvus with AddressSanitizer(default: false)
-Z: build milvus without azure-sdk-for-cpp, so cannot use azure blob
-o: build milvus with opendal(default: false)
-h: help

usage:
./core_build.sh -p \${INSTALL_PREFIX} -t \${BUILD_TYPE} -s \${CUDA_ARCH} [-u] [-l] [-r] [-c] [-z] [-g] [-m] [-e] [-h] [-b] [-o]
                "
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done

if [ -z "$BUILD_WITHOUT_AZURE" ]; then
  AZURE_BUILD_DIR="${ROOT_DIR}/cmake_build/azure"
  if [ ! -d ${AZURE_BUILD_DIR} ]; then
    mkdir -p ${AZURE_BUILD_DIR}
  fi
  pushd ${AZURE_BUILD_DIR}
  env bash ${ROOT_DIR}/scripts/azure_build.sh -p ${INSTALL_PREFIX} -s ${ROOT_DIR}/internal/core/src/storage/azure-blob-storage -t ${BUILD_UNITTEST}
  if [ ! -e libblob-chunk-manager* ]; then
    echo "build blob-chunk-manager fail..."
    cat vcpkg-bootstrap.log
    exit 1
  fi
  if [ ! -e ${INSTALL_PREFIX}/lib/libblob-chunk-manager* ]; then
    echo "install blob-chunk-manager fail..."
    exit 1
  fi
  popd
  SYSTEM_NAME=$(uname -s)
  if [[ ${SYSTEM_NAME} == "Darwin" ]]; then
    SYSTEM_NAME="osx"
  elif [[ ${SYSTEM_NAME} == "Linux" ]]; then
    SYSTEM_NAME="linux"
  fi
  ARCHITECTURE=$(uname -m)
  if [[ ${ARCHITECTURE} == "x86_64" ]]; then
    ARCHITECTURE="x64"
  elif [[ ${ARCHITECTURE} == "aarch64" ]]; then
    ARCHITECTURE="arm64"
  fi
  VCPKG_TARGET_TRIPLET=${ARCHITECTURE}-${SYSTEM_NAME}
fi

if [[ ! -d ${BUILD_OUTPUT_DIR} ]]; then
  mkdir ${BUILD_OUTPUT_DIR}
fi
source ${ROOT_DIR}/scripts/setenv.sh

CMAKE_GENERATOR="Unix Makefiles"

# build with diskann index if OS is ubuntu or rocky or amzn
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
fi
if [ "$OS" = "ubuntu" ] || [ "$OS" = "rocky" ] || [ "$OS" = "amzn" ]; then
  BUILD_DISK_ANN=ON
fi

pushd ${BUILD_OUTPUT_DIR}

# Remove make cache since build.sh -l use default variables
# Force update the variables each time
make rebuild_cache >/dev/null 2>&1

CPU_ARCH=$(get_cpu_arch $CPU_TARGET)

arch=$(uname -m)
CMAKE_CMD="cmake \
${CMAKE_EXTRA_ARGS} \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DCMAKE_LIBRARY_ARCHITECTURE=${arch} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DMILVUS_GPU_VERSION=${GPU_VERSION} \
-DMILVUS_CUDA_ARCH=${CUDA_ARCH} \
-DEMBEDDED_MILVUS=${EMBEDDED_MILVUS} \
-DBUILD_DISK_ANN=${BUILD_DISK_ANN} \
-DUSE_ASAN=${USE_ASAN} \
-DUSE_DYNAMIC_SIMD=${USE_DYNAMIC_SIMD} \
-DCPU_ARCH=${CPU_ARCH} \
-DUSE_OPENDAL=${USE_OPENDAL} \
-DINDEX_ENGINE=${INDEX_ENGINE} \
-DENABLE_GCP_NATIVE=${ENABLE_GCP_NATIVE} "
if [ -z "$BUILD_WITHOUT_AZURE" ]; then
CMAKE_CMD=${CMAKE_CMD}"-DAZURE_BUILD_DIR=${AZURE_BUILD_DIR} \
-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET} "
fi
CMAKE_CMD=${CMAKE_CMD}"${CPP_SRC_DIR}"

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
