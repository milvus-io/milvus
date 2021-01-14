#!/bin/bash

SOURCE=${BASH_SOURCE[0]}
while [ -h $SOURCE ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR=$( cd -P $( dirname $SOURCE ) && pwd )
  SOURCE=$(readlink $SOURCE)
  [[ $SOURCE != /* ]] && SOURCE=$DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR=$( cd -P $( dirname $SOURCE ) && pwd )
# echo $DIR


CMAKE_BUILD=${DIR}/../cwrapper_rocksdb_build
OUTPUT_LIB=${DIR}/../internal/kv/rocksdb/cwrapper/output
SRC_DIR=${DIR}/../internal/kv/rocksdb/cwrapper

if [ ! -d ${CMAKE_BUILD} ];then
    mkdir ${CMAKE_BUILD}
fi

if [ ! -d ${OUTPUT_LIB} ];then
    mkdir ${OUTPUT_LIB}
fi

BUILD_TYPE="Debug"
CUSTOM_THIRDPARTY_PATH=""

while getopts "t:h:f:" arg; do
  case $arg in
  f)
    CUSTOM_THIRDPARTY_PATH=$OPTARG
    ;;
  t)
    BUILD_TYPE=$OPTARG # BUILD_TYPE
    ;;
  h) # help
    echo "-t: build type(default: Debug)
-f: custom thirdparty path(default: "")
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
-DCMAKE_BUILD_TYPE=${BUILD_TYPE}
-DCMAKE_INSTALL_PREFIX=${OUTPUT_LIB} \
-DCUSTOM_THIRDPARTY_DOWNLOAD_PATH=${CUSTOM_THIRDPARTY_PATH} ${SRC_DIR}"

${CMAKE_CMD}
echo ${CMAKE_CMD}

if [[ ! ${jobs+1} ]]; then
    jobs=$(nproc)
fi
make -j ${jobs}
