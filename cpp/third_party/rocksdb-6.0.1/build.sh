#!/bin/bash -x

make clean
make static_lib prefix=../build -j4

make install
