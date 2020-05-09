ARG arch=amd64
FROM ${arch}/ubuntu:18.04

# pipefail is enabled for proper error detection in the `wget | apt-key add`
# step
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates gnupg2 && \
    wget -P /tmp https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
    apt-key add /tmp/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
    sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list' && \
    wget -qO- "https://cmake.org/files/v3.14/cmake-3.14.3-Linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
    apt-get update && apt-get install -y --no-install-recommends \
    g++ git gfortran lsb-core \
    libboost-serialization-dev libboost-filesystem-dev libboost-system-dev libboost-regex-dev \
    curl libtool automake libssl-dev pkg-config libcurl4-openssl-dev python3-pip \
    clang-format-6.0 clang-tidy-6.0 \
    lcov mysql-client libmysqlclient-dev intel-mkl-gnu-2019.5-281 intel-mkl-core-2019.5-281 && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so \
  /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so

RUN sh -c 'echo export LD_LIBRARY_PATH=/opt/intel/compilers_and_libraries_2019.5.281/linux/mkl/lib/intel64:\$LD_LIBRARY_PATH > /etc/profile.d/mkl.sh'

RUN wget https://github.com/xianyi/OpenBLAS/archive/v0.3.9.tar.gz && \
    tar zxvf v0.3.9.tar.gz && cd OpenBLAS-0.3.9 && \
    make TARGET=CORE2 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 USE_THREAD=0 USE_OPENMP=0 FC=gfortran CC=gcc COMMON_OPT="-O3 -g -fPIC" FCOMMON_OPT="-O3 -g -fPIC -frecursive" NMAX="NUM_THREADS=128" LIBPREFIX="libopenblas" LAPACKE="NO_LAPACKE=1" INTERFACE64=0 NO_STATIC=1 && \
    make PREFIX=/usr install && \
    cd .. && rm -rf OpenBLAS-0.3.9 && rm v0.3.9.tar.gz

RUN apt-get update && apt-get install -y --no-install-recommends ccache && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

# use login shell to activate environment un the RUN commands
SHELL [ "/bin/bash", "-c", "-l" ]

# use login shell when running the container
ENTRYPOINT [ "/bin/bash", "-c", "-l" ]
