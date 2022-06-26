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

# Exit immediately for non zero status
set -e

SOURCE="${BASH_SOURCE[0]}"
# fix on zsh environment
if [[ "$SOURCE" == "" ]]; then 
  SOURCE="$0"
fi

while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

export PKG_CONFIG_PATH="${PKG_CONFIG_PATH}:$ROOT_DIR/internal/core/output/lib/pkgconfig:$ROOT_DIR/internal/core/output/lib64/pkgconfig"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$ROOT_DIR/internal/core/output/lib:$ROOT_DIR/internal/core/output/lib64"

# only used on MacOS
export DYLD_LIBRARY_PATH=$ROOT_DIR/internal/core/output/lib
unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     export RPATH=$LD_LIBRARY_PATH;;
    Darwin*)    export RPATH=$DYLD_LIBRARY_PATH;;
    *)          export RPATH=$LD_LIBRARY_PATH;;
esac




