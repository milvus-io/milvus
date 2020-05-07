#!/bin/bash

wget -P /tmp https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
sudo apt-key add /tmp/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB

sudo sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list'
sudo apt-get -y update && sudo apt-get -y install intel-mkl-gnu-2019.5-281 intel-mkl-core-2019.5-281

sudo apt-get install -y gfortran libmysqlclient-dev mysql-client libcurl4-openssl-dev libboost-system-dev \
libboost-filesystem-dev libboost-serialization-dev libboost-regex-dev liblapack3 libssl-dev

if [ ! -f "/usr/lib/x86_64-linux-gnu/libmysqlclient_r.so" ]; then
    sudo ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so
fi
