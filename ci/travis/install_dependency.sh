#!/usr/bin/env bash

set -e

wget -P /tmp https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB

sudo apt-key add /tmp/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB

echo "deb https://apt.repos.intel.com/mkl all main" | \
  sudo tee /etc/apt/sources.list.d/intel-mkl.list

sudo wget -O /usr/share/keyrings/apache-arrow-keyring.gpg https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-keyring.gpg

sudo tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
deb [arch=amd64 signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
deb-src [signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
APT_LINE

sudo apt-get update -qq

sudo apt-get install -y -q --no-install-recommends \
  gfortran \
  lsb-core \
  libtool \
  automake \
  ccache \
  pkg-config \
  libgtest-dev \
  libarrow-dev \
  libjemalloc-dev \
  libboost-filesystem-dev \
  libboost-system-dev \
  libboost-regex-dev \
  intel-mkl-gnu-2019.5-281 \
  intel-mkl-core-2019.5-281 \
  libmysqlclient-dev \
  clang-format-6.0 \
  clang-tidy-6.0 \
  lcov

sudo ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so \
  /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/intel/compilers_and_libraries_2019.5.281/linux/mkl/lib/intel64
