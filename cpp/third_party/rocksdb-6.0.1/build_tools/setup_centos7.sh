#!/bin/bash
set -e

ROCKSDB_VERSION="5.10.3"
ZSTD_VERSION="1.1.3"

echo "This script configures CentOS with everything needed to build and run RocksDB"

yum update -y && yum install epel-release -y

yum install -y \
  wget \
  gcc-c++ \
  snappy snappy-devel \
  zlib zlib-devel \
  bzip2 bzip2-devel \
  lz4-devel \
  libasan \
  gflags

mkdir -pv /usr/local/rocksdb-${ROCKSDB_VERSION}
ln -sfT /usr/local/rocksdb-${ROCKSDB_VERSION} /usr/local/rocksdb

wget -qO /tmp/zstd-${ZSTD_VERSION}.tar.gz https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz
wget -qO /tmp/rocksdb-${ROCKSDB_VERSION}.tar.gz https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz

cd /tmp

tar xzvf zstd-${ZSTD_VERSION}.tar.gz
tar xzvf rocksdb-${ROCKSDB_VERSION}.tar.gz -C /usr/local/

echo "Installing ZSTD..."
pushd zstd-${ZSTD_VERSION}
make && make install
popd

echo "Compiling RocksDB..."
cd /usr/local/rocksdb
chown -R vagrant:vagrant /usr/local/rocksdb/
sudo -u vagrant make static_lib
cd examples/
sudo -u vagrant make all
sudo -u vagrant ./c_simple_example
