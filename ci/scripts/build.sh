#!/bin/bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

MILVUS_CORE_DIR="${SCRIPTS_DIR}/../../core"
CORE_BUILD_DIR="${MILVUS_CORE_DIR}/cmake_build"

HELP="
Usage:
  $0 [flags] [Arguments]

    clean                     Remove all existing build artifacts and configuration (start over)
    -i [INSTALL_PREFIX] or --install_prefix=[INSTALL_PREFIX]
                              Install directory used by install.
    -t [BUILD_TYPE] or --build_type=[BUILD_TYPE]
                              Build type (default: Release)
    -j[N] or --jobs=[N]       Allow N jobs at once; infinite jobs with no arg.
    -l                        Run cpplint & check clang-format
    -n                        No make and make install step
    -g                        Building for the architecture of the GPU in the system
    --with_mkl                Build with MKL (default: OFF)
    --with_fiu                Build with FIU (default: OFF)
    -c or --coverage          Build Code Coverage
    -u or --tests             Build unittest case
    -p or --privileges        Install command with elevated privileges
    -v or --verbose           A level above ‘basic’; includes messages about which makefiles were parsed, prerequisites that did not need to be rebuilt
    -h or --help              Print help information


Use \"$0  --help\" for more information about a given command.
"

ARGS=`getopt -o "i:t:j::lngcupvh" -l "install_prefix::,build_type::,jobs::,with_mkl,with_fiu,coverage,tests,privileges,help" -n "$0" -- "$@"`

eval set -- "${ARGS}"

while true ; do
        case "$1" in
                -i|--install_prefix)
                        # o has an optional argument. As we are in quoted mode,
                        # an empty parameter will be generated if its optional
                        # argument is not found.
                        case "$2" in
                                "") echo "Option install_prefix, no argument"; exit 1 ;;
                                *)  INSTALL_PREFIX=$2 ; shift 2 ;;
                        esac ;;
                -t|--build_type)
                        case "$2" in
                                "") echo "Option build_type, no argument"; exit 1 ;;
                                *)  BUILD_TYPE=$2 ; shift 2 ;;
                        esac ;;
                -j|--jobs)
                        case "$2" in
                                "") PARALLEL_LEVEL=""; shift 2 ;;
                                *)  PARALLEL_LEVEL=$2 ; shift 2 ;;
                        esac ;;
                -g) echo "Building for the architecture of the GPU in the system..." ; GPU_VERSION="ON" ; shift ;;
                --with_mkl) echo "Build with MKL" ; WITH_MKL="ON" ; shift ;;
                --with_fiu) echo "Build with FIU" ; FIU_ENABLE="ON" ; shift ;;
                --coverage) echo "Build code coverage" ; BUILD_COVERAGE="ON" ; shift ;;
                -u|--tests) echo "Build unittest cases" ; BUILD_UNITTEST="ON" ; shift ;;
                -n) echo "No build and install step" ; COMPILE_BUILD="OFF" ; shift ;;
                -l) RUN_CPPLINT="ON" ; shift ;;
                -p|--privileges) PRIVILEGES="ON" ; shift ;;
                -v|--verbose) VERBOSE="1" ; shift ;;
                -h|--help) echo -e "${HELP}" ; exit 0 ;;
                --) shift ; break ;;
                *) echo "Internal error!" ; exit 1 ;;
        esac
done

# Set defaults for vars modified by flags to this script
CUDA_COMPILER=/usr/local/cuda/bin/nvcc
INSTALL_PREFIX=${INSTALL_PREFIX:="/var/lib/milvus"}
VERBOSE=${VERBOSE:=""}
BUILD_TYPE=${BUILD_TYPE:="Release"}
BUILD_UNITTEST=${BUILD_UNITTEST:="OFF"}
BUILD_COVERAGE=${BUILD_COVERAGE:="OFF"}
COMPILE_BUILD=${COMPILE_BUILD:="ON"}
GPU_VERSION=${GPU_VERSION:="OFF"}
RUN_CPPLINT=${RUN_CPPLINT:="OFF"}
WITH_MKL=${WITH_MKL:="OFF"}
FIU_ENABLE=${FIU_ENABLE:="OFF"}
PRIVILEGES=${PRIVILEGES:="OFF"}
CLEANUP=${CLEANUP:="OFF"}
PARALLEL_LEVEL=${PARALLEL_LEVEL:="8"}

for arg do
if [[ $arg == "clean" ]];then
    echo "Remove all existing build artifacts and configuration..."
    if [ -d ${CORE_BUILD_DIR} ]; then
        find ${CORE_BUILD_DIR} -mindepth 1 -delete
        rmdir ${CORE_BUILD_DIR} || true
    fi
    exit 0
fi
done

if [[ ! -d ${CORE_BUILD_DIR} ]]; then
    mkdir ${CORE_BUILD_DIR}
fi

echo -e "===\n=== ccache statistics before build\n==="
ccache --show-stats

pushd ${CORE_BUILD_DIR}

CMAKE_CMD="cmake \
-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
-DCMAKE_CUDA_COMPILER=${CUDA_COMPILER} \
-DMILVUS_GPU_VERSION=${GPU_VERSION} \
-DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DBUILD_COVERAGE=${BUILD_COVERAGE} \
-DFAISS_WITH_MKL=${WITH_MKL} \
-DArrow_SOURCE=AUTO \
-DFAISS_SOURCE=AUTO \
-DOpenBLAS_SOURCE=AUTO \
-DMILVUS_WITH_FIU=${FIU_ENABLE} \
${MILVUS_CORE_DIR}"
echo ${CMAKE_CMD}
${CMAKE_CMD}

if [[ ${RUN_CPPLINT} == "ON" ]]; then
    # cpplint check
    make lint
    if [ $? -ne 0 ]; then
        echo "ERROR! cpplint check failed"
        exit 1
    fi
    echo "cpplint check passed!"

    # clang-format check
    make check-clang-format
    if [ $? -ne 0 ]; then
        echo "ERROR! clang-format check failed"
        exit 1
    fi
    echo "clang-format check passed!"

#    # clang-tidy check
#    make check-clang-tidy
#    if [ $? -ne 0 ]; then
#        echo "ERROR! clang-tidy check failed"
#        rm -f CMakeCache.txt
#        exit 1
#    fi
#    echo "clang-tidy check passed!"
fi

if [[ ${COMPILE_BUILD} == "ON" ]];then
    # compile and build
    make -j${PARALLEL_LEVEL} VERBOSE=${VERBOSE} || exit 1

    if [[ ${PRIVILEGES} == "ON" ]];then
        sudo make install || exit 1
    else
        make install || exit 1
    fi
fi

popd
