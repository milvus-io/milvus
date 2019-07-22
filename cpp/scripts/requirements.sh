#!/usr/bin/env bash

wget -P /tmp https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
apt-key add /tmp/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB

sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list'
apt -y update && apt-get -y install intel-mkl-gnu-2019.4-243 intel-mkl-core-2019.4-243

#sh -c 'echo export LD_LIBRARY_PATH=/opt/intel/compilers_and_libraries_2019.4.243/linux/mkl/lib/intel64:\$LD_LIBRARY_PATH > /etc/profile.d/mkl.sh'
#source /etc/profile
