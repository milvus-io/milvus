#!/bin/bash -ex

CMAKE_VERSION=3.30.5
PYTHON_VERSION=3.10.2
GO_VERSION=1.22.6
SCRIPT_PATH=$(dirname $(realpath $0))
wdir=`pwd`

create_cmake_conanfile()
{
touch /usr/local/cmake/conanfile.py
cat <<EOT >> /usr/local/cmake/conanfile.py
from conans import ConanFile, tools
class CmakeConan(ConanFile):
  name = "cmake"
  package_type = "application"
  version = "${CMAKE_VERSION}"
  description = "CMake, the cross-platform, open-source build system."
  homepage = "https://github.com/Kitware/CMake"
  license = "BSD-3-Clause"
  topics = ("build", "installer")
  settings = "os", "arch"
  def package(self):
    self.copy("*")
  def package_info(self):
    self.cpp_info.libs = tools.collect_libs(self)
EOT
}

#Install centos and epel repos
yum config-manager --add-repo https://mirror.stream.centos.org/9-stream/CRB/ppc64le/os
yum config-manager --add-repo https://mirror.stream.centos.org/9-stream/AppStream//ppc64le/os
yum config-manager --add-repo https://mirror.stream.centos.org/9-stream/BaseOS/ppc64le/os
#rpm --import http://mirror.centos.org/centos/RPM-GPG-KEY-CentOS-Official
rpm --import https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official
dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm

#Install and setup deps
yum install -y --allowerasing make wget git sudo curl zip unzip tar pkg-config perl-IPC-Cmd perl-Digest-SHA perl-FindBin perl-File-Compare openssl-devel scl-utils openblas-devel rust cargo gcc gcc-c++ libstdc++-static which libaio libuuid-devel ncurses-devel ccache libtool m4 autoconf automake ninja-build zlib-devel libffi-devel java-11-openjdk-devel gfortran yum-utils patchelf  hdf5-devel sqlite-devel bzip2-devel xz-devel perl-open.noarch diffutils texinfo
export JAVA_HOME=$(compgen -G '/usr/lib/jvm/java-11-openjdk-*')
export JRE_HOME=${JAVA_HOME}/jre
export PATH=${JAVA_HOME}/bin:$PATH

#Install cmake
cd $wdir
if [ -z "$(ls -A $wdir/cmake-${CMAKE_VERSION})" ]; then
        wget -c https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.tar.gz
        tar -zxvf cmake-${CMAKE_VERSION}.tar.gz
        rm -rf cmake-${CMAKE_VERSION}.tar.gz
        cd cmake-${CMAKE_VERSION}
        ./bootstrap --prefix=/usr/local/cmake --parallel=2 -- -DBUILD_TESTING:BOOL=OFF -DCMAKE_BUILD_TYPE:STRING=Release -DCMAKE_USE_OPENSSL:BOOL=ON
else
        cd cmake-${CMAKE_VERSION}
fi
make install -j$(nproc)
export PATH=/usr/local/cmake/bin:$PATH
cmake --version

#Install Python from source
cd $wdir
if [ -z "$(ls -A $wdir/Python-${PYTHON_VERSION})" ]; then
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
    tar xzf Python-${PYTHON_VERSION}.tgz
    rm -rf Python-${PYTHON_VERSION}.tgz
    cd Python-${PYTHON_VERSION}
    ./configure --enable-loadable-sqlite-extensions
    make -j ${nproc}
else
    cd Python-${PYTHON_VERSION}
fi
make altinstall
ln -sf $(which python3.10) /usr/bin/python3
ln -sf $(which python3.10) /usr/bin/python
ln -sf $(which pip3.10) /usr/bin/pip3
python3 -V && pip3 -V

#Install Python dependencies
pip3 install conan==1.64.1 setuptools==59.5.0

#Install Golang
cd $wdir
wget https://go.dev/dl/go${GO_VERSION}.linux-ppc64le.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go${GO_VERSION}.linux-ppc64le.tar.gz
rm -rf go${GO_VERSION}.linux-ppc64le.tar.gz
export PATH=/usr/local/go/bin:$PATH
go version
export PATH=$PATH:$HOME/go/bin

#Build
cd $wdir
pushd /usr/local/cmake
create_cmake_conanfile
conan export-pkg . cmake/${CMAKE_REQUIRED_VERSION}@ -s os="Linux" -s arch="ppc64le"
conan profile update settings.compiler.libcxx=libstdc++11 default
popd
export VCPKG_FORCE_SYSTEM_BINARIES=1
export ENABLE_AZURE=false
mkdir -p $HOME/.cargo/bin/
ret=0
make -j$(nproc) || ret=$?
if [ "$ret" -ne 0 ]
then
        echo "FAIL: Build failed."
        exit 1
fi
export MILVUS_BIN=$wdir/${PACKAGE_NAME}/bin/milvus

#Disable Azure tests
sed -i "s#test_azure_chunk_manager.cpp##g" ./internal/core/unittest/CMakeLists.txt

#Cpp unit tests
cd $wdir/${PACKAGE_NAME}/
make test-cpp -j$(nproc) || ret=$?
if [ "$ret" -ne 0 ]
then
        echo "Cpp Tests fail."
        exit 2
fi

# Conclude
set +ex
echo "Complete: Build and Test successful! Milvus binary available at [$MILVUS_BIN]"
echo "10 Azure related tests were disabled."
