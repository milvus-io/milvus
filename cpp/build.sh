#!/bin/bash

BUILD_TYPE="Debug"
BUILD_UNITTEST="off"

while getopts "p:t:uh" arg
do
        case $arg in
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             u)
                echo "Build and run unittest cases" ;
                BUILD_UNITTEST="on";
                ;;
             h) # help
                echo "

parameter:
-t: build type
-u: building unit test options

usage:
./build.sh -t \${BUILD_TYPE} [-u] [-h]
                "
                exit 0
                ;;
             ?)
                echo "unkonw argument"
        exit 1
        ;;
        esac
done

if [[ -d cmake_build ]]; then
	rm cmake_build -r
fi

rm -rf ./cmake_build
mkdir cmake_build
cd cmake_build

CMAKE_CMD="cmake -DBUILD_UNIT_TEST=${BUILD_UNITTEST} \
-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
$@ ../"
echo ${CMAKE_CMD}

${CMAKE_CMD}

make clean && make -j || exit 1

