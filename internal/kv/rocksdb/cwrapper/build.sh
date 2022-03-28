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
# echo $DIR

CGO_CFLAGS="-I$(pwd)/output/include"
if [ -f "$(pwd)/output/lib/librocksdb.a" ];then
    CGO_LDFLAGS="-L$(pwd)/output/lib -l:librocksdb.a -lstdc++ -lm -lz"
elif [ -f "$(pwd)/output/lib64/librocksdb.a" ];then
    CGO_LDFLAGS="-L$(pwd)/output/lib64 -l:librocksdb.a -lstdc++ -lm -lz"
else
  echo "No found 'librocksdb.a'."
  exit 1
fi

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

unameOut="$(uname -s)"
if [[ ! ${jobs+1} ]]; then
  case "${unameOut}" in
      Linux*)     jobs=$(nproc);;
      Darwin*)
        llvm_prefix="$(brew --prefix llvm)"
        export CLANG_TOOLS_PATH="${llvm_prefix}/bin"
        export CC="${llvm_prefix}/bin/clang"
        export CXX="${llvm_prefix}/bin/clang++"
        export LDFLAGS="-L${llvm_prefix}/lib -L/usr/local/opt/libomp/lib"
        export CXXFLAGS="-I${llvm_prefix}/include -I/usr/local/include -I/usr/local/opt/libomp/include"
        jobs=$(sysctl -n hw.physicalcpu);;
       *)
          echo "Exit 0, System:${unameOut}";
          exit 0;
  esac
fi

pushd ${OUTPUT_LIB}
CMAKE_CMD="cmake \
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} .."

${CMAKE_CMD}
echo ${CMAKE_CMD}

make -j ${jobs} VERBOSE=0
