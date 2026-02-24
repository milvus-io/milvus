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

# Skip the installation and compilation of third-party code, 
# if the developer is certain that it has already been done.
if [[ ${SKIP_3RDPARTY} -eq 1 ]]; then
  exit 0
fi

usage() {
  echo "Usage: $0 [-o BUILD_OPENDAL] [-t BUILD_TYPE] [-h]"
  echo "  -o BUILD_OPENDAL  Enable/disable OpenDAL build (ON/OFF, default: OFF)"
  echo "  -t BUILD_TYPE     Set build type (Debug/Release/RelWithDebInfo/MinSizeRel, default: Release)"
  echo "  -h                Show this help message"
  echo ""
  echo "Examples:"
  echo "  $0                          # Build with default settings (Release, OpenDAL OFF)"
  echo "  $0 -t Debug                 # Build in Debug mode"
  echo "  $0 -o ON -t RelWithDebInfo  # Build with OpenDAL enabled and RelWithDebInfo"
}

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done

BUILD_OPENDAL="OFF"
BUILD_TYPE="Release"
while getopts "o:t:h" arg; do
  case $arg in
  o)
    BUILD_OPENDAL=$OPTARG
    ;;
  t)
    BUILD_TYPE=$OPTARG
    ;;
  h)
    usage
    exit 0
    ;;
  *)
    usage
    exit 1
    ;;
 esac
done

# Validate build type
case "${BUILD_TYPE}" in
  Debug|Release)
    echo "Build type: ${BUILD_TYPE}"
    ;;
  *)
    echo "Invalid build type: ${BUILD_TYPE}. Valid options are: Debug, Release"
    exit 1
    ;;
esac

ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"
CPP_SRC_DIR="${ROOT_DIR}/internal/core"
BUILD_OUTPUT_DIR="${ROOT_DIR}/cmake_build"

if [[ ! -d ${BUILD_OUTPUT_DIR} ]]; then
  mkdir ${BUILD_OUTPUT_DIR}
fi

source ${ROOT_DIR}/scripts/setenv.sh

# Add conan to PATH if installed in user's local bin directory
if [[ -f "$HOME/.local/bin/conan" ]]; then
    export PATH="$HOME/.local/bin:$PATH"
fi

# Ensure conan version matches the required version (1.66.0)
# CI Docker images may have an older version pre-installed
REQUIRED_CONAN_VERSION="1.66.0"
CURRENT_CONAN_VERSION=$(conan --version 2>/dev/null | grep -oP '\d+\.\d+\.\d+' || echo "0.0.0")
if [[ "${CURRENT_CONAN_VERSION}" != "${REQUIRED_CONAN_VERSION}" ]]; then
    echo "Conan version mismatch: ${CURRENT_CONAN_VERSION} != ${REQUIRED_CONAN_VERSION}, upgrading..."
    pip3 install conan==${REQUIRED_CONAN_VERSION} 2>/dev/null || pip install conan==${REQUIRED_CONAN_VERSION}
fi

pushd ${BUILD_OUTPUT_DIR}

export CONAN_REVISIONS_ENABLED=1
export CXXFLAGS="-Wno-error=address -Wno-error=deprecated-declarations -include cstdint"
export CFLAGS="-Wno-error=address -Wno-error=deprecated-declarations"
# LLVM from Homebrew doesn't set TARGET_OS_OSX=1 (unlike Apple's clang), which causes
# macOS SDK headers to exclude macOS-specific APIs (SecImportExport, SCPreferences, etc.).
# This fixes aws-c-cal, c-ares, and other packages that depend on macOS Security/SystemConfig APIs.
if [[ "$(uname -s)" == "Darwin" ]]; then
    export CFLAGS="${CFLAGS} -DTARGET_OS_OSX=1"
    export CXXFLAGS="${CXXFLAGS} -DTARGET_OS_OSX=1"
fi
# Allow CMake 4.x to build packages with old cmake_minimum_required versions (< 3.5)
export CMAKE_POLICY_VERSION_MINIMUM=3.5

# Determine the Conan remote URL, using the environment variable if set, otherwise defaulting
CONAN_ARTIFACTORY_URL="${CONAN_ARTIFACTORY_URL:-https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local}"

if [[ ! `conan remote list` == *default-conan-local* ]]; then
    conan remote add default-conan-local $CONAN_ARTIFACTORY_URL
fi

unameOut="$(uname -s)"
case "${unameOut}" in
  Darwin*)
    # Use ccache as compiler launcher
    export CMAKE_C_COMPILER_LAUNCHER=ccache
    export CMAKE_CXX_COMPILER_LAUNCHER=ccache
    echo "Using CXX: $CXX"
    echo "Using CC: $CC"
    CONAN_ARGS="--install-folder conan --build=missing -s build_type=${BUILD_TYPE} -s compiler=clang -s compiler.version=${llvm_version} -s compiler.libcxx=libc++ -s compiler.cppstd=17 -u"

    # On macOS, Conan packages with shared libraries (protobuf, grpc) produce
    # binaries (protoc, grpc_cpp_plugin) whose install rpaths don't include
    # dependency lib dirs (e.g. abseil). This causes dyld failures when
    # downstream packages (e.g. opentelemetry-cpp) invoke them during build.
    #
    # Strategy: run conan install once (may fail on downstream packages),
    # fix rpaths on the already-built binaries, then retry.
    conan install ${CPP_SRC_DIR} ${CONAN_ARGS} || {
        echo "First conan install attempt failed, fixing shared library rpaths and retrying..."

        ABSEIL_LIB=$(find ~/.conan/data/abseil/ -path "*/package/*/lib/libabsl_base.*.dylib" 2>/dev/null | head -1 | xargs dirname 2>/dev/null)
        PROTOBUF_LIB=$(find ~/.conan/data/protobuf/ -path "*/package/*/lib/libprotoc.*.dylib" 2>/dev/null | head -1 | xargs dirname 2>/dev/null)

        # Fix protoc: needs rpath to abseil
        PROTOC=$(find ~/.conan/data/protobuf/ -path "*/package/*/bin/protoc" 2>/dev/null | head -1)
        if [[ -n "$PROTOC" && -n "$ABSEIL_LIB" ]]; then
            install_name_tool -add_rpath "$ABSEIL_LIB" "$PROTOC" 2>/dev/null
            echo "Fixed protoc rpath -> abseil"
        fi

        # Fix grpc plugins: need rpaths to protobuf and abseil
        GRPC_CPP_PLUGIN=$(find ~/.conan/data/grpc/ -path "*/package/*/bin/grpc_cpp_plugin" 2>/dev/null | head -1)
        if [[ -n "$GRPC_CPP_PLUGIN" ]]; then
            GRPC_BIN_DIR=$(dirname "$GRPC_CPP_PLUGIN")
            for plugin in "$GRPC_BIN_DIR"/grpc_*_plugin; do
                [ -n "$PROTOBUF_LIB" ] && install_name_tool -add_rpath "$PROTOBUF_LIB" "$plugin" 2>/dev/null
                [ -n "$ABSEIL_LIB" ] && install_name_tool -add_rpath "$ABSEIL_LIB" "$plugin" 2>/dev/null
            done
            echo "Fixed grpc plugin rpaths -> protobuf, abseil"
        fi

        conan install ${CPP_SRC_DIR} ${CONAN_ARGS} || { echo 'conan install failed'; exit 1; }
    }
    ;;
  Linux*)
    if [ -f /etc/os-release ]; then
        OS_NAME=$(grep '^PRETTY_NAME=' /etc/os-release | cut -d '=' -f2 | tr -d '"')
    else
        OS_NAME="Linux"
    fi
    echo "Running on ${OS_NAME}"
    export CPU_TARGET=avx
    GCC_VERSION=`gcc -dumpversion`
    if [[ `gcc -v 2>&1 | sed -n 's/.*\(--with-default-libstdcxx-abi\)=\(\w*\).*/\2/p'` == "gcc4" ]]; then
      conan install ${CPP_SRC_DIR} --install-folder conan --build=missing -s build_type=${BUILD_TYPE} -s compiler.version=${GCC_VERSION} -s compiler.cppstd=17 -u || { echo 'conan install failed'; exit 1; }
    else
      conan install ${CPP_SRC_DIR} --install-folder conan --build=missing -s build_type=${BUILD_TYPE} -s compiler.version=${GCC_VERSION} -s compiler.libcxx=libstdc++11 -s compiler.cppstd=17 -u || { echo 'conan install failed'; exit 1; }
    fi
    ;;
  *)
    echo "Cannot build on windows"
    ;;
esac

popd

mkdir -p ${ROOT_DIR}/internal/core/output/lib
mkdir -p ${ROOT_DIR}/internal/core/output/include

pushd ${ROOT_DIR}/cmake_build/thirdparty
if command -v cargo >/dev/null 2>&1; then
    echo "cargo exists"
    unameOut="$(uname -s)"
    case "${unameOut}" in
        Darwin*)
          echo "running on mac os, reinstall rust 1.89"
          # github will install rust 1.74 by default.
          # https://github.com/actions/runner-images/blob/main/images/macos/macos-12-Readme.md
          rustup install 1.89
          rustup default 1.89;;
        *)
          echo "not running on mac os, no need to reinstall rust";;
    esac
else
    bash -c "curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain=1.89 -y" || { echo 'rustup install failed'; exit 1;}
    source $HOME/.cargo/env
fi
