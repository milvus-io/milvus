#!/bin/bash -x

cd ..
INSTALL_PREFIX=$(pwd)/build
cd -

CUDA_PATH="--with-cuda=/usr/local/cuda"
CUDA_CONFIG=--with-cuda-arch="-gencode=arch=compute_61,code=sm_61"

mkdir -p INSTALL_PREFIX
./configure --prefix=${INSTALL_PREFIX} ${CUDA_PATH} ${CUDA_CONFIG}
 
make -j
make install