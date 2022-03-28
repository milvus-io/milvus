#!/bin/bash

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

SOURCE=${BASH_SOURCE[0]}
while [ -h $SOURCE ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR=$( cd -P $( dirname $SOURCE ) && pwd )
  SOURCE=$(readlink $SOURCE)
  [[ $SOURCE != /* ]] && SOURCE=$DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR=$( cd -P $( dirname $SOURCE ) && pwd )
# DIR=${DIR}/../internal/storage/cwrapper

CMAKE_BUILD=${DIR}/../cwrapper_build
OUTPUT_LIB=${DIR}/../internal/storage/cwrapper/output
SRC_DIR=${DIR}/../internal/storage/cwrapper

if [ ! -d ${CMAKE_BUILD} ];then
    mkdir ${CMAKE_BUILD}
fi

mkdir -p ${OUTPUT_LIB}

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


# MSYS system
CMAKE_GENERATOR="Unix Makefiles"
if [ "$MSYSTEM" == "MINGW64" ] ; then
  CMAKE_GENERATOR="MSYS Makefiles"
fi

pushd ${CMAKE_BUILD}
CMAKE_CMD="cmake \
${CMAKE_EXTRA_ARGS} \
-DCMAKE_INSTALL_PREFIX=${OUTPUT_LIB} \
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCUSTOM_THIRDPARTY_DOWNLOAD_PATH=${CUSTOM_THIRDPARTY_PATH} ${SRC_DIR}"

${CMAKE_CMD} -G "${CMAKE_GENERATOR}"
echo ${CMAKE_CMD}

if [[ ! ${jobs+1} ]]; then
  unameOut="$(uname -s)"
  case "${unameOut}" in
      Linux*)     jobs=$(nproc);;
      MINGW64*)   jobs=$(nproc);;
      Darwin*)    jobs=$(sysctl -n hw.physicalcpu);;
      *)          echo "UNKNOWN:${unameOut}" ; exit 0;
  esac
fi
make -j ${jobs} && make install
