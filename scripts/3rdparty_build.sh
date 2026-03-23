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
  Debug|Release|RelWithDebInfo|MinSizeRel)
    echo "Build type: ${BUILD_TYPE}"
    ;;
  *)
    echo "Invalid build type: ${BUILD_TYPE}. Valid options are: Debug, Release, RelWithDebInfo, MinSizeRel"
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
CURRENT_CONAN_VERSION=$(conan --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "0.0.0")
if [[ "${CURRENT_CONAN_VERSION}" != "${REQUIRED_CONAN_VERSION}" ]]; then
    echo "Conan version mismatch: ${CURRENT_CONAN_VERSION} != ${REQUIRED_CONAN_VERSION}, upgrading..."
    pip3 install conan==${REQUIRED_CONAN_VERSION} 2>/dev/null || pip install conan==${REQUIRED_CONAN_VERSION}
fi

pushd ${BUILD_OUTPUT_DIR}

# Unset LD_PRELOAD and LD_LIBRARY_PATH to prevent jemalloc and other shared
# libraries (set by setenv.sh) from being injected into conan's Python process.
# - LD_PRELOAD with jemalloc causes immediate segfaults in Python.
# - LD_LIBRARY_PATH with cmake_build/lib/ (populated by conan imports on
#   previous runs) causes the Python ssl module to load a mismatched
#   libssl.so during the conan update check, also triggering segfaults.
# The pre_build hook (below) handles setting LD_LIBRARY_PATH per-build
# for tools like grpc_cpp_plugin that need shared libs at build time.
SAVED_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
SAVED_LD_PRELOAD="${LD_PRELOAD:-}"
SAVED_DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-}"
SAVED_DYLD_INSERT_LIBRARIES="${DYLD_INSERT_LIBRARIES:-}"
unset LD_PRELOAD
unset LD_LIBRARY_PATH
unset DYLD_LIBRARY_PATH
unset DYLD_INSERT_LIBRARIES

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

# Install a conan pre_build hook that sets LD_LIBRARY_PATH (Linux) or
# DYLD_LIBRARY_PATH (macOS) before each package build. This ensures that
# tools like grpc_cpp_plugin can find shared libraries (e.g. libprotoc.so)
# when invoked during downstream package builds (opentelemetry-cpp, googleapis).
# Use "conan config home" to get the correct conan home directory, as ~ may
# differ from the conan home in CI containers.
CONAN_HOME_DIR=$(conan config home 2>/dev/null || echo "${CONAN_USER_HOME:-$HOME}/.conan")
mkdir -p "$CONAN_HOME_DIR/hooks"
cat > "$CONAN_HOME_DIR/hooks/fix_shared_lib_env.py" << 'HOOK_EOF'
import os, platform

_saved_env = {}

def pre_build(output, conanfile, **kwargs):
    """Set library path before build so tools like grpc_cpp_plugin can find
    shared libs (e.g. libprotoc.so). Saved state is restored in post_build
    to avoid polluting the conan process environment."""
    dep_lib_dirs = []
    try:
        for _, dep in conanfile.deps_cpp_info.dependencies:
            for p in dep.lib_paths:
                if os.path.isdir(p):
                    dep_lib_dirs.append(p)
    except Exception:
        return
    if not dep_lib_dirs:
        return
    env_var = "DYLD_LIBRARY_PATH" if platform.system() == "Darwin" else "LD_LIBRARY_PATH"
    _saved_env[env_var] = os.environ.get(env_var)
    existing = os.environ.get(env_var, "")
    new_val = ":".join(dep_lib_dirs) + (":" + existing if existing else "")
    os.environ[env_var] = new_val
    output.info("Set %s for %d dependency lib dirs" % (env_var, len(dep_lib_dirs)))

def post_build(output, conanfile, **kwargs):
    """Restore library path after build to prevent conan process corruption."""
    for env_var, old_val in _saved_env.items():
        if old_val is None:
            os.environ.pop(env_var, None)
        else:
            os.environ[env_var] = old_val
    _saved_env.clear()
HOOK_EOF
conan config set hooks.fix_shared_lib_env 2>/dev/null

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
    # downstream packages (opentelemetry-cpp, googleapis) invoke them.
    #
    # Fix: install a Conan post_package hook that adds dependency rpaths to
    # each package's binaries right after it is built (before the manifest is
    # sealed), so every tool binary works the first time it is invoked.
    mkdir -p "$CONAN_HOME_DIR/hooks"
    conan config set hooks.fix_macos_rpaths 2>/dev/null
    cat > "$CONAN_HOME_DIR/hooks/fix_macos_rpaths.py" << 'HOOK_EOF'
import os, platform, subprocess, stat

def post_package(output, conanfile, conanfile_path, **kwargs):
    if platform.system() != "Darwin":
        return
    pkg = conanfile.package_folder
    if not pkg:
        return
    bin_dir = os.path.join(pkg, "bin")
    if not os.path.isdir(bin_dir):
        return
    dep_lib_dirs = []
    try:
        for _, dep in conanfile.deps_cpp_info.dependencies:
            for p in dep.lib_paths:
                if os.path.isdir(p):
                    dep_lib_dirs.append(p)
    except Exception:
        return
    if not dep_lib_dirs:
        return
    fixed = []
    for name in os.listdir(bin_dir):
        fp = os.path.join(bin_dir, name)
        if not os.path.isfile(fp) or not (os.stat(fp).st_mode & stat.S_IXUSR):
            continue
        r = subprocess.run(["file", fp], capture_output=True, text=True)
        if "Mach-O" not in r.stdout:
            continue
        for lib_path in dep_lib_dirs:
            subprocess.run(["install_name_tool", "-add_rpath", lib_path, fp],
                           capture_output=True)
        fixed.append(name)
    if fixed:
        output.info("Fixed macOS rpaths for: %s" % ", ".join(fixed))
HOOK_EOF

    conan install ${CPP_SRC_DIR} ${CONAN_ARGS} || { echo 'conan install failed'; exit 1; }
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

# Restore LD_LIBRARY_PATH/LD_PRELOAD/DYLD vars so downstream build steps can find shared libs
if [[ -n "${SAVED_LD_LIBRARY_PATH}" ]]; then
    export LD_LIBRARY_PATH="${SAVED_LD_LIBRARY_PATH}"
fi
if [[ -n "${SAVED_LD_PRELOAD}" ]]; then
    export LD_PRELOAD="${SAVED_LD_PRELOAD}"
fi
if [[ -n "${SAVED_DYLD_LIBRARY_PATH}" ]]; then
    export DYLD_LIBRARY_PATH="${SAVED_DYLD_LIBRARY_PATH}"
fi
if [[ -n "${SAVED_DYLD_INSERT_LIBRARIES}" ]]; then
    export DYLD_INSERT_LIBRARIES="${SAVED_DYLD_INSERT_LIBRARIES}"
fi

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
