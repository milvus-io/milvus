FROM nvidia/cuda:10.1-devel-ubuntu16.04

ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    wget -qO- "https://cmake.org/files/v3.14/cmake-3.14.3-Linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
    wget -P /tmp https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
    apt-key add /tmp/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
    sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list' && \
    apt-get update && apt-get install -y --no-install-recommends \
    git flex bison gfortran lsb-core ccache \
    curl libtool automake libboost1.58-all-dev libssl-dev pkg-config libcurl4-openssl-dev python3-pip \
    clang-format-6.0 clang-tidy-6.0 \
    lcov mysql-client libmysqlclient-dev intel-mkl-gnu-2019.5-281 intel-mkl-core-2019.5-281 libopenblas-dev liblapack3 && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so

RUN sh -c 'echo export LD_LIBRARY_PATH=/opt/intel/compilers_and_libraries_2019.5.281/linux/mkl/lib/intel64:\$LD_LIBRARY_PATH > /etc/profile.d/mkl.sh'

COPY docker-entrypoint.sh /app/docker-entrypoint.sh

WORKDIR /root

ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
CMD [ "start" ]
