#!/bin/bash -x

if [[ -d cmake_build ]]; then
	rm cmake_build -rf
fi

mkdir cmake_build
cd cmake_build

INSTALL_PREFIX=$(pwd)/../../build

BOOST_PATH="/home/zilliz/opt/app/boost"
TBB_PATH="/home/zilliz/opt/app/tbb/tbb"
OPTION="-DBOOST_ROOT=$BOOST_PATH -DTBB_DIR=${TBB_PATH}"

# CMAKE_CMD="cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ${OPTION} ../"

CMAKE_CMD="cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ../"

${CMAKE_CMD}

make -j8 && make install
