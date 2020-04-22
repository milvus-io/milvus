#!/bin/bash

BUILD_OUTPUT_DIR="cmake_build"
BUILD_TYPE="Debug"
MAKE_CLEAN="OFF"
RUN_CPPLINT="OFF"

while getopts "p:d:t:f:ulrcgjhxzme" arg; do
  case $arg in
  t)
    BUILD_TYPE=$OPTARG # BUILD_TYPE
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
  h) # help
    echo "

parameter:
-t: build type(default: Debug)
-l: run cpplint, clang-format and clang-tidy(default: OFF)
-h: help

usage:
./build.sh -t \${BUILD_TYPE} -f \${FAISS_ROOT} [-l] [-r] [-h]
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
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
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
  make -j 8  || exit 1
fi
