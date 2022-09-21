#!/bin/bash

# Compile jobs variable; Usage: $ jobs=12 ./build.sh ...
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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

BUILD_OUTPUT_DIR="./cmake_build"
BUILD_TYPE="Release"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX="${SCRIPTS_DIR}/output"
MAKE_CLEAN="OFF"
BUILD_COVERAGE="OFF"
PROFILING="OFF"
RUN_CPPLINT="OFF"
CUDA_COMPILER=/usr/local/cuda/bin/nvcc
GPU_VERSION="OFF" #defaults to CPU version
WITH_PROMETHEUS="ON"
CUDA_ARCH="DEFAULT"
CUSTOM_THIRDPARTY_PATH=""
BUILD_DISK_ANN="OFF"

while getopts "p:t:s:f:o:ulrcghzme" arg; do
  case $arg in
  f)
    CUSTOM_THIRDPARTY_PATH=$OPTARG
    ;;
  p)
    INSTALL_PREFIX=$OPTARG
    ;;
  o)
    BUILD_OUTPUT_DIR=$OPTARG
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
  n)
    BUILD_DISK_ANN="OFF"
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
-h: help
usage:
./build.sh -p \${INSTALL_PREFIX} -t \${BUILD_TYPE} -s \${CUDA_ARCH} -f\${CUSTOM_THIRDPARTY_PATH} [-u] [-l] [-r] [-c] [-z] [-g] [-m] [-e] [-h]
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


if [[ ${MAKE_CLEAN} == "ON" ]]; then
  echo "Runing make clean in ${BUILD_OUTPUT_DIR} ..."
  make clean
  exit 0
fi

unameOut="$(uname -s)"
case "${unameOut}" in
    Darwin*)
        llvm_prefix="$(brew --prefix llvm)"
        export CLANG_TOOLS_PATH="${llvm_prefix}/bin"
        export CC="${llvm_prefix}/bin/clang"
        export CXX="${llvm_prefix}/bin/clang++"
        export LDFLAGS="-L${llvm_prefix}/lib -L/usr/local/opt/libomp/lib"
        export CXXFLAGS="-I${llvm_prefix}/include -I/usr/local/include -I/usr/local/opt/libomp/include"
        ;;
          *)   echo "==System:${unameOut}";
esac

CMAKE_CMD="cmake \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DOpenBLAS_SOURCE=AUTO \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DENABLE_CPU_PROFILING=${PROFILING} \
-DMILVUS_GPU_VERSION=${GPU_VERSION} \
-DMILVUS_WITH_PROMETHEUS=${WITH_PROMETHEUS} \
-DMILVUS_CUDA_ARCH=${CUDA_ARCH} \
-DCUSTOM_THIRDPARTY_DOWNLOAD_PATH=${CUSTOM_THIRDPARTY_PATH} \
-DKNOWHERE_GPU_VERSION=${SUPPORT_GPU} \
-DBUILD_DISK_ANN=${BUILD_DISK_ANN} \
${SCRIPTS_DIR}"
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

  # clang-tidy check
  make check-clang-tidy
  if [ $? -ne 0 ]; then
      echo "ERROR! clang-tidy check failed"
      exit 1
  fi
  echo "clang-tidy check passed!"
else
  # compile and build
  make -j ${jobs} install || exit 1
fi
