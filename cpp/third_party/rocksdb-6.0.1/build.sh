#!/bin/bash -x

make static_lib prefix=../build -j4

make install
