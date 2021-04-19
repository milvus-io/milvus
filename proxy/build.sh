#!/bin/bash

# Compile jobs variable; Usage: $ jobs=12 ./build.sh ...
if [[ ! ${jobs+1} ]]; then
    jobs=$(nproc)
fi

BUILD_OUTPUT_DIR="cmake_build_release"
BUILD_TYPE="Release"
BUILD_UNITTEST="OFF"
INSTALL_PREFIX=$(pwd)/milvus
MAKE_CLEAN="OFF"
DB_PATH="/tmp/milvus"
RUN_CPPLINT="OFF"

while getopts "p:d:t:s:ulrcghzme" arg; do
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
      MAKE_CLEAN="ON"
    fi
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
-h: help

usage:
./build.sh -p \${INSTALL_PREFIX} -t \${BUILD_TYPE} [-u] [-l] [-r] [-h]
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

CMAKE_CMD="cmake \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DOpenBLAS_SOURCE=AUTO \
-DMILVUS_DB_PATH=${DB_PATH} \
../"
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
#  make check-clang-tidy
#  if [ $? -ne 0 ]; then
#      echo "ERROR! clang-tidy check failed"
#      exit 1
#  fi
#  echo "clang-tidy check passed!"
else
  # compile and build
  make -j ${jobs} install || exit 1
fi
