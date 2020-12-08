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
CUSTOM_THIRDPARTY_PATH=""

while getopts "a:b:t:h:f:" arg; do
  case $arg in
  f)
    CUSTOM_THIRDPARTY_PATH=$OPTARG
    ;;
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
-f: custom thirdparty path(default:)
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
echo "CUSTOM_THIRDPARTY_PATH: " $CUSTOM_THIRDPARTY_PATH

pushd ${CMAKE_BUILD}
CMAKE_CMD="cmake \
-DCMAKE_INSTALL_PREFIX=${OUTPUT_LIB} \
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCUSTOM_THIRDPARTY_DOWNLOAD_PATH=${CUSTOM_THIRDPARTY_PATH} .."

${CMAKE_CMD}
echo ${CMAKE_CMD}

if [[ ! ${jobs+1} ]]; then
    jobs=$(nproc)
fi
make -j ${jobs} && make install
