#!/bin/bash

SOURCE=${BASH_SOURCE[0]}
while [ -h $SOURCE ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR=$( cd -P $( dirname $SOURCE ) && pwd )
  SOURCE=$(readlink $SOURCE)
  [[ $SOURCE != /* ]] && SOURCE=$DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR=$( cd -P $( dirname $SOURCE ) && pwd )
# echo $DIR

CGO_CFLAGS="-I$(pwd)/output/include"
CGO_LDFLAGS="-L$(pwd)/output/lib -l:librocksdb.a -lstdc++ -lm -lz"

OUTPUT_LIB=${DIR}/output

if [ -d ${OUTPUT_LIB} ];then
    rm -rf ${OUTPUT_LIB}
fi
mkdir ${OUTPUT_LIB}

BUILD_TYPE="Debug"

while getopts "t:h:" arg; do
  case $arg in
  t)
    BUILD_TYPE=$OPTARG # BUILD_TYPE
    ;;
  h) # help
    echo "-t: build type(default: Debug)
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

pushd ${OUTPUT_LIB}
CMAKE_CMD="cmake \
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} .."

${CMAKE_CMD}
echo ${CMAKE_CMD}

if [[ ! ${jobs+1} ]]; then
    jobs=$(nproc)
fi
make -j ${jobs} VERBOSE=0
