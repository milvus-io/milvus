#!/bin/bash

sudo yum install -y epel-release centos-release-scl-rh && sudo yum install -y wget curl which && \
sudo wget -qO- "https://cmake.org/files/v3.14/cmake-3.14.3-Linux-x86_64.tar.gz" | sudo tar --strip-components=1 -xz -C /usr/local && \
sudo yum install -y ccache make automake git python3-pip libcurl-devel python3-devel boost-static mysql-devel \
devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran llvm-toolset-7.0-clang llvm-toolset-7.0-clang-tools-extra lcov \
lapack-devel openssl-devel

echo "source scl_source enable devtoolset-7" | sudo tee -a /etc/profile.d/devtoolset-7.sh
echo "source scl_source enable llvm-toolset-7.0" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
echo "export CLANG_TOOLS_PATH=/opt/rh/llvm-toolset-7.0/root/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
