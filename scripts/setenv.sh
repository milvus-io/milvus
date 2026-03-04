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

# Exit immediately for non zero status
set +e

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
export MILVUS_WORK_DIR=$ROOT_DIR

unameOut="$(uname -s)"

case "${unameOut}" in
    Linux*)
      # check if use asan.
      MILVUS_ENABLE_ASAN_LIB=$(ldd $ROOT_DIR/internal/core/output/lib/libmilvus_core.so | grep asan | awk '{print $3}')
      if [ -n "$MILVUS_ENABLE_ASAN_LIB" ]; then
          echo "Enable ASAN With ${MILVUS_ENABLE_ASAN_LIB}"
          export MILVUS_ENABLE_ASAN_LIB="$MILVUS_ENABLE_ASAN_LIB"
      fi

      LIBJEMALLOC=$PWD/internal/core/output/lib/libjemalloc.so
      if test -f "$LIBJEMALLOC"; then
        export LD_PRELOAD="$LIBJEMALLOC"
      else
        echo "WARN: Cannot find $LIBJEMALLOC"
      fi
      export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:+$PKG_CONFIG_PATH:}$ROOT_DIR/internal/core/output/lib/pkgconfig:$ROOT_DIR/internal/core/output/lib64/pkgconfig"
      export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}$ROOT_DIR/internal/core/output/lib:$ROOT_DIR/internal/core/output/lib64"
      export RPATH=$LD_LIBRARY_PATH;;
    Darwin*)
      # detect llvm version by valid list (supports LLVM 14-17)
      # Note: LLVM 18 is NOT supported because Conan 1.x cannot handle the newer
      # compiler profiles/settings that LLVM 18 requires. Until we migrate to Conan 2,
      # please use LLVM 17 or earlier.
      for llvm_version in 17 16 15 14 NOT_FOUND ; do
        if brew ls --versions llvm@${llvm_version} > /dev/null 2>&1; then
          break
        fi
      done
      if [ "${llvm_version}" = "NOT_FOUND" ] ; then
        echo "ERROR: Valid LLVM (14-17) not installed. Run: brew install llvm@17"
        echo "NOTE: LLVM 18 is not supported due to Conan 1.x incompatibility."
        exit 1
      fi
      llvm_prefix="$(brew --prefix llvm@${llvm_version})"
      export CLANG_TOOLS_PATH="${llvm_prefix}/bin"
      export CC=${llvm_prefix}/bin/clang
      export CXX=${llvm_prefix}/bin/clang++
      export ASM=${llvm_prefix}/bin/clang
      macos_sdk_path="$(xcrun --show-sdk-path)"
      export CFLAGS="-Wno-deprecated-declarations -I$(brew --prefix libomp)/include -isysroot ${macos_sdk_path}"
      export CXXFLAGS=${CFLAGS}
      export LDFLAGS="-L$(brew --prefix libomp)/lib"
      export CGO_CFLAGS="${CFLAGS}"
      export CGO_LDFLAGS="${LDFLAGS} -framework Security -framework CoreFoundation"

      export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:+$PKG_CONFIG_PATH:}$ROOT_DIR/internal/core/output/lib/pkgconfig"
      export DYLD_LIBRARY_PATH=$ROOT_DIR/internal/core/output/lib
      export RPATH=$DYLD_LIBRARY_PATH;;
    MINGW*)
      extra_path=$(cygpath -w "$ROOT_DIR/internal/core/output/lib")
      export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:+$PKG_CONFIG_PATH;}${extra_path}\pkgconfig"
      export LD_LIBRARY_PATH=$extra_path
      export RPATH=$LD_LIBRARY_PATH;;
    *)
      echo "does not supported"
esac

