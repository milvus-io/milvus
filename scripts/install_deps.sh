#!/usr/bin/env bash

if [[ -x "$(command -v apt)" ]]; then
    sudo apt install -y g++ gcc make libssl-dev zlib1g-dev libboost-regex-dev \
    libboost-program-options-dev libboost-system-dev libboost-filesystem-dev \
    libboost-serialization-dev python3-dev libboost-python-dev libcurl4-openssl-dev gfortran libtbb-dev
elif [[ -x "$(command -v yum)" ]]; then
    sudo yum install -y epel-release centos-release-scl-rh \
      && sudo yum install -y make automake openssl-devel zlib-devel tbb-devel \
      libcurl-devel python3-devel boost-devel boost-python \
      devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran \
      llvm-toolset-7.0-clang llvm-toolset-7.0-clang-tools-extra

    echo "source scl_source enable devtoolset-7" | sudo tee -a /etc/profile.d/devtoolset-7.sh
    echo "source scl_source enable llvm-toolset-7.0" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
    echo "export CLANG_TOOLS_PATH=/opt/rh/llvm-toolset-7.0/root/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
else
    echo "Error Install Dependencies ..."
    exit 1
fi
