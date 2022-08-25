FROM ubuntu:focal-20220426

# pipefail is enabled for proper error detection in the `wget | apt-key add`
# step
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates gnupg2 && \
    apt-get update && apt-get install -y --no-install-recommends g++ gcc gfortran git make && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/* && \
    wget https://github.com/xianyi/OpenBLAS/archive/v0.3.9.tar.gz && \
    tar zxvf v0.3.9.tar.gz && cd OpenBLAS-0.3.9 && \
    make TARGET=CORE2 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 USE_THREAD=0 USE_OPENMP=0 FC=gfortran CC=gcc COMMON_OPT="-O3 -g -fPIC" FCOMMON_OPT="-O3 -g -fPIC -frecursive" NMAX="NUM_THREADS=128" LIBPREFIX="libopenblas" INTERFACE64=0 NO_STATIC=1 && \
    make -j4 PREFIX=/usr NO_STATIC=1 install && \
    cd .. && rm -rf OpenBLAS-0.3.9 && rm v0.3.9.tar.gz

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
