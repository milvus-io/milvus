#!/bin/bash -x

if [[ -d cmake_build ]]; then
	rm cmake_build -rf
fi

mkdir cmake_build
cd cmake_build

INSTALL_PREFIX=$(pwd)/../../build

CMAKE_CMD="cmake -DLAPACK_BINARY_DIR=${INSTALL_PREFIX} -DCMAKE_INSTALL_LIBDIR=${INSTALL_PREFIX}/lib $@ ../"

${CMAKE_CMD}

cmake --build -j . --target install
