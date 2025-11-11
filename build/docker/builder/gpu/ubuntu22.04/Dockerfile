FROM nvidia/cuda:11.8.0-devel-ubuntu22.04 as builder

ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends wget curl ca-certificates gnupg2 ninja-build && \
    wget -qO- "https://cmake.org/files/v3.31/cmake-3.31.8-linux-`uname -m`.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
    apt-get update && apt-get install -y --no-install-recommends \
    g++ gcc gfortran git make ccache libssl-dev zlib1g-dev zip unzip \
    clang-format-12 clang-tidy-12  lcov libtool m4 autoconf automake python3 python3-pip \
    pkg-config uuid-dev libaio-dev libgoogle-perftools-dev libopenblas-dev && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata && \
        ln -sf /usr/share/zoneinfo/Etc/UTC /etc/localtime && \
        echo "Etc/UTC" > /etc/timezone && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*


# Install go
RUN mkdir -p /usr/local/go && wget -qO- "https://go.dev/dl/go1.24.9.linux-$TARGETARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go
# Install conan
RUN pip3 install conan==1.64.1
# Install rust
RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=1.89 -y
ENV PATH=/root/.cargo/bin:/usr/local/bin:/usr/local/go/bin:$PATH

# refer: https://code.visualstudio.com/docs/remote/containers-advanced#_avoiding-extension-reinstalls-on-container-rebuild
RUN mkdir -p /home/milvus/.vscode-server/extensions \
    /home/milvus/.vscode-server-insiders/extensions \
    && chmod -R 777 /home/milvus



RUN wget -O /tini https://github.com/krallin/tini/releases/download/v0.19.0/tini-$TARGETARCH && \
    chmod +x /tini



