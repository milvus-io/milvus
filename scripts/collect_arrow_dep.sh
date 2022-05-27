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

# assume we are in milvus directory

CUSTOM_THIRDPARTY_PATH=

while getopts "f:h" arg; do
  case $arg in
  f)
    CUSTOM_THIRDPARTY_PATH=$OPTARG
    ;;

  h) # help
    echo "

parameter:
-f: custom paths of thirdparty downloaded files(default: NULL)
-h: help

usage:
./collect_arrow_dep.sh -f\${CUSTOM_THIRDPARTY_PATH} [-h]
                "
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done


if [ -z "$CUSTOM_THIRDPARTY_PATH" ]
then
	echo "
	parameter:
	-f: custom paths of thirdparty downloaded files(default: NULL)
	-h: help

	usage:
	./core_build.sh -f\${CUSTOM_THIRDPARTY_PATH} [-h]
			"
    	exit 0
fi

if [ ! -d "$CUSTOM_THIRDPARTY_PATH" ]
then
	echo "${CUSTOM_THIRDPARTY_PATH} is not a directory"
	exit 0
fi

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

BUILD_OUTPUT_DIR="${SCRIPTS_DIR}/../cmake_build"
ARROW_DIR=${BUILD_OUTPUT_DIR}/thirdparty/arrow 
ARROWBIN_DIR=${BUILD_OUTPUT_DIR}/thirdparty/arrow/arrow-bin

echo ${ARROWBIN_DIR}

pushd ${ARROWBIN_DIR}
targetNames=(
"jemalloc-5.2.1.tar.bz2"
"thrift-0.13.0.tar.gz"
"utf8proc-v2.7.0.tar.gz"
"xsimd-7d1778c3b38d63db7cec7145d939f40bc5d859d1.tar.gz"
"zstd-v1.5.1.tar.gz"
)

srcNames=(
"jemalloc_ep-prefix/src/jemalloc-5.2.1.tar.bz2"
"thrift_ep-prefix/src/thrift-0.13.0.tar.gz"
"utf8proc_ep-prefix/src/v2.7.0.tar.gz"
"src/7d1778c3b38d63db7cec7145d939f40bc5d859d1.tar.gz"
"zstd_ep-prefix/src/v1.5.1.tar.gz"
)

for i in "${!srcNames[@]}"; do
   if test -f "${srcNames[i]}"; then
	echo "${srcNames[i]} exists. start to copy to ${CUSTOM_THIRDPARTY_PATH}/${targetNames[i]}"
	echo cp -f "${srcNames[i]}"  "${CUSTOM_THIRDPARTY_PATH}/${targetNames[i]}"
	cp  -f "${srcNames[i]}" "${CUSTOM_THIRDPARTY_PATH}/${targetNames[i]}"
   fi
done
popd
