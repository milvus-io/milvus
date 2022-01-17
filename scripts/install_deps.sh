#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function install_linux_deps() {
  if [[ -x "$(command -v apt)" ]]; then
      # for Ubuntu 18.04
      sudo apt update
      sudo apt install -y build-essential g++ gcc make ccache lcov libssl-dev zlib1g-dev libboost-regex-dev \
          libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libboost-serialization-dev \
          python3-dev libboost-python-dev libcurl4-openssl-dev gfortran libtbb-dev
  elif [[ -x "$(command -v yum)" ]]; then
      # for CentOS 7
      sudo yum install -y epel-release centos-release-scl-rh && \
      sudo yum install -y git make automake ccache openssl-devel zlib-devel \
          lcov libcurl-devel python3-devel \
          devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran \
          llvm-toolset-7.0-clang llvm-toolset-7.0-clang-tools-extra

      echo "source scl_source enable devtoolset-7" | sudo tee -a /etc/profile.d/devtoolset-7.sh
      echo "source scl_source enable llvm-toolset-7.0" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
      echo "export CLANG_TOOLS_PATH=/opt/rh/llvm-toolset-7.0/root/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset-7.sh
      source "/etc/profile.d/llvm-toolset-7.sh"

      # Install tbb
      git clone https://github.com/wjakob/tbb.git && \
      cd tbb/build && \
      cmake .. && make -j && \
      sudo make install && \
      cd ../../ && rm -rf tbb/

      # Install boost
      wget -q https://boostorg.jfrog.io/artifactory/main/release/1.65.1/source/boost_1_65_1.tar.gz && \
          tar zxf boost_1_65_1.tar.gz && cd boost_1_65_1 && \
          ./bootstrap.sh --prefix=/usr/local --with-toolset=gcc --without-libraries=python && \
          sudo ./b2 -j2 --prefix=/usr/local --without-python toolset=gcc install && \
          cd ../ && rm -rf ./boost_1_65_1*
  else
      echo "Error Install Dependencies ..."
      exit 1
  fi
}

function install_mac_deps() {
  if [ ! -f ~/.gvm/scripts/gvm ]; then
     sudo bash < <(curl -s -S -L https://raw.githubusercontent.com/moovweb/gvm/master/binscripts/gvm-installer)
  fi
  source ~/.gvm/scripts/gvm
  gvm install go1.17.2
  gvm  use  go1.17.2

  sudo xcode-select --install  > /dev/null 2>&1
  brew install boost cmake lcov libomp llvm ninja tbb
  brew uninstall grep
  brew install grep
  export PATH="/usr/local/opt/grep/libexec/gnubin:$PATH"
  brew update && brew upgrade
}

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     install_linux_deps;;
    Darwin*)    install_mac_deps;;
    *)          echo "Unsupported OS:${unameOut}" ; exit 0;
esac
