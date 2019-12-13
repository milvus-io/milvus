FROM nvidia/cuda:10.1-devel-centos7

ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

RUN yum install -y epel-release centos-release-scl-rh && yum install -y wget curl && \
    wget -qO- "https://cmake.org/files/v3.14/cmake-3.14.3-Linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
    yum install -y ccache make automake git python3-pip libcurl-devel python3-devel boost-static devtoolset-7-gcc llvm-toolset-7.0-clang lcov \
    && \
    rm -rf /var/cache/yum/*

RUN scl enable devtoolset-7 bash

ENV CLANG_TOOLS_PATH="/opt/rh/llvm-toolset-7.0/root/usr/bin"
ENV PATH="/opt/rh/llvm-toolset-7.0/root/usr/bin:$PATH"
ENV LD_LIBRARY_PATH="/opt/rh/llvm-toolset-7.0/root/usr/lib64:$LD_LIBRARY_PATH"

COPY docker-entrypoint.sh /app/docker-entrypoint.sh

WORKDIR /root

ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
CMD [ "start" ]
