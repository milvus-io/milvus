#!/bin/bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

FAISS_SOURCE_DIR="${SCRIPTS_DIR}/../.."

FAISS_WITH_MKL="False"
FAISS_GPU_VERSION="False"
FAISS_COMMON_CONFIGURE_ARGS="CXXFLAGS=\"-mavx2 -mf16c\" --without-python"
FAISS_CONFIGURE_ARGS="${FAISS_COMMON_CONFIGURE_ARGS}"
CUDA_TOOLKIT_ROOT_DIR="/usr/local/cuda"
FAISS_CUDA_ARCH="-gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61 -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_75,code=sm_75"
MKL_ROOT_DIR="/opt/intel/compilers_and_libraries_2019.5.281/linux/mkl"

while getopts "o:s:m:b:l:c:a:igh" arg
do
        case $arg in
             o)
                FAISS_INSTALL_PREFIX=$OPTARG
                ;;
             s)
                FAISS_SOURCE_DIR=$OPTARG
                ;;
             m)
                MKL_ROOT_DIR=$OPTARG
                ;;
             b)
                OPENBLAS_PREFIX=$OPTARG
                ;;
             l)
                LAPACK_PREFIX=$OPTARG
                ;;
             c)
                CUDA_TOOLKIT_ROOT_DIR=$OPTARG
                ;;
             a)
                FAISS_CUDA_ARCH=$OPTARG
                ;;
             i)
                FAISS_WITH_MKL="True"
                ;;
             g)
                FAISS_GPU_VERSION="True"
                ;;
             h) # help
                echo "

parameter:
-o: faiss install prefix path
-s: faiss source directory
-m: mkl root directory
-b: openblas install prefix path
-l: lapack install prefix path
-c: CUDA toolkit root directory
-a: faiss CUDA compute architecture
-i: faiss with mkl
-g: faiss gpu version
-h: help

usage:
./build.sh -o \${FAISS_INSTALL_PREFIX} -s \${FAISS_SOURCE_DIR} -m \${MKL_ROOT_DIR} -b \${OPENBLAS_PREFIX} -l \${LAPACK_PREFIX} -c \${CUDA_TOOLKIT_ROOT_DIR} -a \${FAISS_CUDA_ARCH} [-i] [-g] [-h]
                "
                exit 0
                ;;
             ?)
                echo "ERROR! unknown argument"
        exit 1
        ;;
        esac
done

if [[ -n "${FAISS_INSTALL_PREFIX}" ]];then
        FAISS_CONFIGURE_ARGS="${FAISS_CONFIGURE_ARGS} --prefix=${FAISS_INSTALL_PREFIX}"
fi

if [[ "${FAISS_GPU_VERSION}" == "True" ]];then
    if [[ ! -n "${FAISS_CUDA_ARCH}" ]];then
        echo "FAISS_CUDA_ARCH: \"${FAISS_CUDA_ARCH}\" is empty!"
        exit 1
    fi
    if [[ ! -d "${CUDA_TOOLKIT_ROOT_DIR}" ]];then
        echo "CUDA_TOOLKIT_ROOT_DIR: \"${CUDA_TOOLKIT_ROOT_DIR}\" directory doesn't exist!"
        exit 1
    fi
    FAISS_CONFIGURE_ARGS="${FAISS_CONFIGURE_ARGS} --with-cuda=${CUDA_TOOLKIT_ROOT_DIR} --with-cuda-arch='${FAISS_CUDA_ARCH}'"
else
    FAISS_CONFIGURE_ARGS="${FAISS_CONFIGURE_ARGS} --without-cuda"
fi

if [[ "${FAISS_WITH_MKL}" == "True" ]];then
    if [[ ! -d "${MKL_ROOT_DIR}" ]];then
        echo "MKL_ROOT_DIR: \"${MKL_ROOT_DIR}\" directory doesn't exist!"
        exit 1
    fi
    FAISS_CONFIGURE_ARGS="${FAISS_CONFIGURE_ARGS} CPPFLAGS='-DFINTEGER=long -DMKL_ILP64 -m64 -I${MKL_ROOT_DIR}/include' LDFLAGS='-L${MKL_ROOT_DIR}/lib/intel64'"
else
    if [[ -n "${LAPACK_PREFIX}" ]];then
        if [[ ! -d "${LAPACK_PREFIX}" ]];then
            echo "LAPACK_PREFIX: \"${LAPACK_PREFIX}\" directory doesn't exist!"
            exit 1
        fi
        FAISS_CONFIGURE_ARGS="${FAISS_CONFIGURE_ARGS} LDFLAGS=-L${LAPACK_PREFIX}/lib"
    fi
    if [[ -n "${OPENBLAS_PREFIX}" ]];then
        if [[ ! -d "${OPENBLAS_PREFIX}" ]];then
            echo "OPENBLAS_PREFIX: \"${OPENBLAS_PREFIX}\" directory doesn't exist!"
            exit 1
        fi
        FAISS_CONFIGURE_ARGS="${FAISS_CONFIGURE_ARGS} LDFLAGS=-L${OPENBLAS_PREFIX}/lib"
    fi
fi

cd ${FAISS_SOURCE_DIR}

sh -c "./configure ${FAISS_CONFIGURE_ARGS}"

# compile and build
make -j8 || exit 1
make install || exit 1
