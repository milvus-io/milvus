#!/bin/bash -x

if [[ -d cmake_build ]]; then
	rm cmake_build -rf
fi

mkdir cmake_build
cd cmake_build

INSTALL_PREFIX=$(pwd)/../../build/

CMAKE_CMD="cmake -Dbuild_static_lib=ON -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} $@ ../"

${CMAKE_CMD}

make -j

make install
