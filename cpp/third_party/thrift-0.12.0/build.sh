#!/bin/bash -x

cd ..
INSTALL_PREFIX=$(pwd)/build
cd -


mkdir -p INSTALL_PREFIX
./configure --prefix=${INSTALL_PREFIX} CXXFLAGS='-g -O0' CFLAGS='-g -O0' --without-java --without-python
 
make -j
make install
