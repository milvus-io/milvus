#!/bin/bash -x

cd ..
INSTALL_PREFIX=$(pwd)/build
cd -

mkdir -p INSTALL_PREFIX
./configure --prefix=${INSTALL_PREFIX}
 
make -j
make PREFIX=${INSTALL_PREFIX} install