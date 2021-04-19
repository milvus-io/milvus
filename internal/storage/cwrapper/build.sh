#!/bin/bash

SOURCE=${BASH_SOURCE[0]}
while [ -h $SOURCE ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR=$( cd -P $( dirname $SOURCE ) && pwd )
  SOURCE=$(readlink $SOURCE)
  [[ $SOURCE != /* ]] && SOURCE=$DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR=$( cd -P $( dirname $SOURCE ) && pwd )
# echo $DIR

CMAKE_BUILD=${DIR}/cmake_build
OUTPUT_LIB=${DIR}/output

if [ ! -d ${CMAKE_BUILD} ];then
    mkdir ${CMAKE_BUILD}
fi

if [ -d ${OUTPUT_LIB} ];then
    rm -rf ${OUTPUT_LIB}
fi
mkdir ${OUTPUT_LIB}

BUILD_TYPE="Debug"
GIT_ARROW_REPO="https://github.com/apache/arrow.git"
GIT_ARROW_TAG="apache-arrow-2.0.0"

while getopts "a:b:t:h" arg; do
  case $arg in
  t)
    BUILD_TYPE=$OPTARG # BUILD_TYPE
    ;;
  a)
    GIT_ARROW_REPO=$OPTARG
    ;;
  b)
    GIT_ARROW_TAG=$OPTARG
    ;;
  h) # help
    echo "-t: build type(default: Debug)
-a: arrow repo(default: https://github.com/apache/arrow.git)
-b: arrow tag(default: apache-arrow-2.0.0)
-h: help
                "
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done
echo "BUILD_TYPE: " $BUILD_TYPE
echo "GIT_ARROW_REPO: " $GIT_ARROW_REPO
echo "GIT_ARROW_TAG: " $GIT_ARROW_TAG

pushd ${CMAKE_BUILD}
cmake -DCMAKE_INSTALL_PREFIX=${OUTPUT_LIB} -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DGIT_ARROW_REPO=${GIT_ARROW_REPO} -DGIT_ARROW_TAG=${GIT_ARROW_TAG} .. && make && make install
