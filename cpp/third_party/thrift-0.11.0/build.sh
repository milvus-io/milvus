#!/bin/bash -x

cd ..
INSTALL_PREFIX=$(pwd)/build
cd -

BUILD_TYPE=Release

while getopts "p:t:uh" arg
do
        case $arg in
             t)
                BUILD_TYPE=$OPTARG # BUILD_TYPE
                ;;
             ?)
                echo "unkonw argument"
        exit 1
        ;;
        esac
done

CXXFLAGS='-O3'
CFLAGS='-O3'
if [ "$BUILD_TYPE" == "Debug" ];then
CXXFLAGS='-g -O0'
CFLAGS='-g -O0'
fi

./bootstrap.sh

mkdir -p INSTALL_PREFIX
./configure --prefix=${INSTALL_PREFIX} CXXFLAGS="$CXXFLAGS" CFLAGS="$CFLAGS" --without-java --without-python --without-php --without-go --without-nodejs

make -j
make install
