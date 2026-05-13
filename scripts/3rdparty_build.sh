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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done

BUILD_OPENDAL="OFF"
while getopts "o:" arg; do
  case $arg in
  o)
    BUILD_OPENDAL=$OPTARG
    ;;
 esac
done

ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"
CPP_SRC_DIR="${ROOT_DIR}/internal/core"
BUILD_OUTPUT_DIR="${ROOT_DIR}/cmake_build"

if [[ ! -d ${BUILD_OUTPUT_DIR} ]]; then
  mkdir ${BUILD_OUTPUT_DIR}
fi

source ${ROOT_DIR}/scripts/setenv.sh

# Allow overriding the Conan binary via CONAN_CMD, e.g. for developers who
# keep Conan 2.x as their default `conan` (for master) but need 1.x here:
#   CONAN_CMD=conan-1 make
CONAN="${CONAN_CMD:-conan}"

# This branch uses Conan 1.x. Verify the selected binary is 1.x up-front so
# we fail with a clear message instead of letting confusing 2.x argparse
# errors propagate up from `conan install`.
_major=$("$CONAN" --version 2>/dev/null | grep -oE '[0-9]+' | head -1)
if [[ "${_major}" != "1" ]]; then
    if [[ -n "${CONAN_CMD:-}" ]]; then
        echo "ERROR: CONAN_CMD=${CONAN_CMD} reports major version '${_major:-unknown}', but this branch requires Conan 1.x." >&2
        echo "If your default 'conan' is already 1.x, unset CONAN_CMD and retry." >&2
    else
        echo "ERROR: 'conan' reports major version '${_major:-unknown}', but this branch requires Conan 1.x." >&2
        echo "If you already have a Conan 1.x binary (e.g. 'conan-1'), set CONAN_CMD, e.g.: CONAN_CMD=conan-1 make" >&2
    fi
    echo "Otherwise install Conan 1.x, e.g.: pipx install conan==1.66.0 --suffix=-1" >&2
    exit 1
fi
unset _major

pushd ${BUILD_OUTPUT_DIR}

export CONAN_REVISIONS_ENABLED=1
export CXXFLAGS="-Wno-error=address -Wno-error=deprecated-declarations"
export CFLAGS="-Wno-error=address -Wno-error=deprecated-declarations"

# Determine the Conan remote URL, using the environment variable if set, otherwise defaulting
CONAN_ARTIFACTORY_URL="${CONAN_ARTIFACTORY_URL:-https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local}"

if [[ ! `"$CONAN" remote list` == *default-conan-local* ]]; then
    "$CONAN" remote add default-conan-local $CONAN_ARTIFACTORY_URL
fi

unameOut="$(uname -s)"
case "${unameOut}" in
  Darwin*)
    "$CONAN" install ${CPP_SRC_DIR} --install-folder conan --build=missing -s compiler=clang -s compiler.version=${llvm_version} -s compiler.libcxx=libc++ -s compiler.cppstd=17 -r default-conan-local -u || { echo 'conan install failed'; exit 1; }
    ;;
  Linux*)
    echo "Running on ${OS_NAME}"
    export CPU_TARGET=avx
    GCC_VERSION=`gcc -dumpversion`
    if [[ `gcc -v 2>&1 | sed -n 's/.*\(--with-default-libstdcxx-abi\)=\(\w*\).*/\2/p'` == "gcc4" ]]; then
      "$CONAN" install ${CPP_SRC_DIR} --install-folder conan --build=missing -s compiler.version=${GCC_VERSION} -r default-conan-local -u || { echo 'conan install failed'; exit 1; }
    else
      "$CONAN" install ${CPP_SRC_DIR} --install-folder conan --build=missing -s compiler.version=${GCC_VERSION} -s compiler.libcxx=libstdc++11 -r default-conan-local -u || { echo 'conan install failed'; exit 1; }
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
          echo "running on mac os, reinstall rust 1.83"
          # github will install rust 1.74 by default.
          # https://github.com/actions/runner-images/blob/main/images/macos/macos-12-Readme.md
          rustup install 1.83
          rustup default 1.83;;
        *)
          echo "not running on mac os, no need to reinstall rust";;
    esac
else
    bash -c "curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain=1.83 -y" || { echo 'rustup install failed'; exit 1;}
    source $HOME/.cargo/env
fi
