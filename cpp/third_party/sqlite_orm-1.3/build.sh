#!/bin/bash -x

if [[ -d cmake_build ]]; then
	rm cmake_build -rf
fi

mkdir cmake_build
cd cmake_build

INSTALL_PREFIX=$(pwd)/../../build/

CMAKE_CMD="cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} $@ ../"

${CMAKE_CMD}

make -j

make install
