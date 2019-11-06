#!/usr/bin/env bash

set -ex

wget -P /tmp https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB

sudo apt-key add /tmp/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB

echo "deb https://apt.repos.intel.com/mkl all main" | \
  sudo tee /etc/apt/sources.list.d/intel-mkl.list

sudo apt-get update -qq

sudo apt-get install -y -q --no-install-recommends \
  flex \
  bison \
  gfortran \
  lsb-core \
  libtool \
  automake \
  pkg-config \
  libboost-filesystem-dev \
  libboost-system-dev \
  libboost-regex-dev \
  intel-mkl-gnu-2019.4-243 \
  intel-mkl-core-2019.4-243  \
  libmysqlclient-dev \
  clang-format-6.0 \
  clang-tidy-6.0 \
  lcov

sudo ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so \
  /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/intel/compilers_and_libraries_2019.4.243/linux/mkl/lib/intel64