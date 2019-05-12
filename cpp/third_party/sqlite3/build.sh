#!/bin/bash -x

cd ..
INSTALL_PREFIX=$(pwd)/build
cd -

mkdir -p INSTALL_PREFIX
autoreconf -f -i
./configure --prefix=${INSTALL_PREFIX} CFLAGS="-g -O0" CXXFLAGS="-g -O0"
 
make -j
make PREFIX=${INSTALL_PREFIX} install