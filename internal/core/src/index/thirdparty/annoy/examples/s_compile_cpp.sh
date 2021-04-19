#!/bin/bash


echo "compiling precision example..."
cmd="g++ precision_test.cpp -o precision_test -std=c++11"
eval $cmd
echo "Done"
