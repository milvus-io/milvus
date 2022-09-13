FROM ubuntu:focal-20220426

# pipefail is enabled for proper error detection in the `wget | apt-key add`
# step
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates gnupg2 && \
    apt-get update && apt-get install -y --no-install-recommends g++ gcc gfortran git make && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/* && \
    wget https://github.com/xianyi/OpenBLAS/archive/v0.3.21.tar.gz && \
    tar zxvf v0.3.21.tar.gz && cd OpenBLAS-0.3.21 && \
    make NO_STATIC=1 NO_LAPACK=1 NO_LAPACKE=1 NO_CBLAS=1 NO_AFFINITY=1 USE_OPENMP=1 \
       CFLAGS="-O3 -fPIC" TARGET=CORE2 DYNAMIC_ARCH=1 \
       NUM_THREADS=64 MAJOR_VERSION=3 libs shared &&\
    make -j4 PREFIX=/usr NO_STATIC=1 install && \
    cd .. && rm -rf OpenBLAS-0.3.21 && rm v0.3.21.tar.gz

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
