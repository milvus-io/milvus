FROM centos:centos7

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN yum install -y epel-release centos-release-scl-rh && yum install -y wget make automake \
    devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran && \
    rm -rf /var/cache/yum/* && \
    echo "source scl_source enable devtoolset-7" >> /etc/profile.d/devtoolset-7.sh

RUN source /etc/profile.d/devtoolset-7.sh && \
    wget https://github.com/xianyi/OpenBLAS/archive/v0.3.9.tar.gz && \
    tar zxvf v0.3.9.tar.gz && cd OpenBLAS-0.3.9 && \
    make TARGET=CORE2 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 USE_THREAD=0 USE_OPENMP=0 FC=gfortran CC=gcc COMMON_OPT="-O3 -g -fPIC" FCOMMON_OPT="-O3 -g -fPIC -frecursive" NMAX="NUM_THREADS=128" LIBPREFIX="libopenblas" LAPACKE="NO_LAPACKE=1" INTERFACE64=0 NO_STATIC=1 && \
    make PREFIX=/usr NO_STATIC=1 install && \
    cd .. && rm -rf OpenBLAS-0.3.9 && rm v0.3.9.tar.gz

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
