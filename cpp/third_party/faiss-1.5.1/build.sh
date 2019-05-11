#!/bin/bash -x

cd ..
INSTALL_PREFIX=$(pwd)/build
cd -

CUDA_PATH="--with-cuda=/usr/local/cuda"
CUDA_CONFIG=--with-cuda-arch="-gencode=arch=compute_61,code=sm_61"

BUILD_TYPE=Release
GPU_VERSION=OFF

while getopts "p:t:uhg" arg
do
        case $arg in
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             g)
                GPU_VERSION=ON
                ;;
             ?)
                echo "unkonw argument"
        exit 1
        ;;
        esac
done

CXXFLAGS='-O3'
CFLAGS='-O3'
if [ "$BUILD_TYPE" == "Debug" ];then
CXXFLAGS='-g -O0'
CFLAGS='-g -O0'
fi

mkdir -p INSTALL_PREFIX
if [ "$GPU_VERSION" == "ON" ];then
./configure --prefix=${INSTALL_PREFIX} CXXFLAGS="$CXXFLAGS" CFLAGS="$CFLAGS" LDFLAGS=-L${INSTALL_PREFIX}/lib ${CUDA_PATH} ${CUDA_CONFIG}
else
./configure --prefix=${INSTALL_PREFIX} CXXFLAGS="$CXXFLAGS" CFLAGS="$CFLAGS" LDFLAGS=-L${INSTALL_PREFIX}/lib --without-cuda
fi

 
make clean
make -j
make install
