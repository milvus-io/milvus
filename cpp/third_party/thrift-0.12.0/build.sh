#!/bin/bash -x

cd ..
INSTALL_PREFIX=$(pwd)/build
cd -


mkdir -p INSTALL_PREFIX
./configure --prefix=${INSTALL_PREFIX} --without-java --without-python
 
make -j
make install
