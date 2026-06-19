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
      MILVUS_LIB_DIRS="$ROOT_DIR/internal/core/output/lib:$ROOT_DIR/internal/core/output/lib64:$ROOT_DIR/cmake_build/lib"
      # LIBRARY_PATH: build-time linker search path (gcc/ld)
      export LIBRARY_PATH="${LIBRARY_PATH:+$LIBRARY_PATH:}$MILVUS_LIB_DIRS"
      # LD_LIBRARY_PATH: runtime linker search path - needed for transitive
      # shared library dependencies (e.g. libfolly_exception_tracer.so has no
      # RPATH and depends on libfolly_exception_tracer_base.so)
      export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}$MILVUS_LIB_DIRS"
      export RPATH=$MILVUS_LIB_DIRS;;
    Darwin*)
      # detect llvm version by valid list (supports LLVM 14-18)
      for llvm_version in 18 17 16 15 14 NOT_FOUND ; do
        if brew ls --versions llvm@${llvm_version} > /dev/null 2>&1; then
          break
        fi
      done
      if [ "${llvm_version}" = "NOT_FOUND" ] ; then
        echo "ERROR: Valid LLVM (14-18) not installed. Run: brew install llvm@17"
        exit 1
      fi
      llvm_prefix="$(brew --prefix llvm@${llvm_version})"
      export CLANG_TOOLS_PATH="${llvm_prefix}/bin"
      export CC=${llvm_prefix}/bin/clang
      export CXX=${llvm_prefix}/bin/clang++
      export ASM=${llvm_prefix}/bin/clang
      macos_sdk_path="$(xcrun --show-sdk-path)"
      # LLVM from Homebrew doesn't set TARGET_OS_OSX=1 (unlike Apple's clang), causing
      # macOS SDK headers to exclude macOS-specific APIs. Define it explicitly.
      export CFLAGS="-Wno-deprecated-declarations -I$(brew --prefix libomp)/include -isysroot ${macos_sdk_path} -DTARGET_OS_OSX=1"
      export CXXFLAGS=${CFLAGS}
      # Include LLVM's own libc++/libc++abi so Conan packages that reference
      # Apple-extension symbols (e.g. ___cxa_decrement_exception_refcount in
      # folly) are resolved against the same runtime they were built with.
      # -lc++abi is explicit because Homebrew LLVM clang++ does not inject it
      # automatically (unlike Apple clang), but folly's static archive references it.
      export LDFLAGS="-L$(brew --prefix libomp)/lib -L${llvm_prefix}/lib/c++ -lc++abi"
      # Rust cc-rs crate uses the Xcode SDK version as deployment target if
      # MACOSX_DEPLOYMENT_TARGET is unset. When the SDK version (e.g. 26.2) is
      # newer than what this LLVM version understands, compilation fails.
      # Cap it at the current macOS major version to stay compatible.
      sdk_ver="$(xcrun --show-sdk-version 2>/dev/null)"
      os_major="$(sw_vers -productVersion | cut -d. -f1)"
      if [ -n "${sdk_ver}" ] && [ -n "${os_major}" ] && \
         [ "$(echo "${sdk_ver}" | cut -d. -f1)" -gt "${os_major}" ]; then
        export MACOSX_DEPLOYMENT_TARGET="${os_major}.0"
      fi
      # Rust cc-rs needs TARGET_OS_OSX for correct platform detection in aws-lc-sys.
      # Set for both ARM and Intel targets so cross-compilation also works.
      export CFLAGS_aarch64_apple_darwin="-DTARGET_OS_OSX=1"
      export CFLAGS_x86_64_apple_darwin="-DTARGET_OS_OSX=1"
      export CGO_CFLAGS="${CFLAGS}"
      export CGO_LDFLAGS="${LDFLAGS} -framework Security -framework CoreFoundation"

      export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:-}:$ROOT_DIR/internal/core/output/lib/pkgconfig"
      export DYLD_LIBRARY_PATH=$ROOT_DIR/cmake_build/lib:$ROOT_DIR/internal/core/output/lib
      export RPATH=$(echo "$DYLD_LIBRARY_PATH" | sed 's/:/ -r /g');;
    MINGW*)
      extra_path=$(cygpath -w "$ROOT_DIR/internal/core/output/lib")
      export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:+$PKG_CONFIG_PATH;}${extra_path}\pkgconfig"
      export LD_LIBRARY_PATH=$extra_path
      export RPATH=$LD_LIBRARY_PATH;;
    *)
      echo "does not supported"
esac
