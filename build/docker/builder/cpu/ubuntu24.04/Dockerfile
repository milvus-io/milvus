# Copyright (C) 2019-2022 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

FROM ubuntu:noble-20240530

ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends wget curl ca-certificates gnupg2 \
    g++ gcc gdb gdbserver ninja-build git make ccache libssl-dev zlib1g-dev zip unzip \
    clang-format-18 clang-tidy-18 lcov libtool m4 autoconf automake python3 python3-pip pipx \
    pkg-config uuid-dev libaio-dev libopenblas-dev && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata && \
        ln -sf /usr/share/zoneinfo/Etc/UTC /etc/localtime && \
        echo "Etc/UTC" > /etc/timezone && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

# Ubuntu 24.04 ships GCC 13 by default, no upgrade needed

RUN pip3 install --break-system-packages conan==2.25.1

RUN echo "target arch $TARGETARCH"
RUN wget -qO- "https://cmake.org/files/v3.31/cmake-3.31.8-linux-`uname -m`.tar.gz" | tar --strip-components=1 -xz -C /usr/local

# Install Go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV GO111MODULE on
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
RUN mkdir -p /usr/local/go && wget -qO- "https://go.dev/dl/go1.25.9.linux-$TARGETARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go && \
    mkdir -p "$GOPATH/src" "$GOPATH/bin" && \
    go clean --modcache && \
    chmod -R 777 "$GOPATH" && chmod -R a+w $(go env GOTOOLDIR)

# refer: https://code.visualstudio.com/docs/remote/containers-advanced#_avoiding-extension-reinstalls-on-container-rebuild
RUN mkdir -p /home/milvus/.vscode-server/extensions \
    /home/milvus/.vscode-server-insiders/extensions \
    && chmod -R 777 /home/milvus

COPY --chown=0:0 build/docker/builder/entrypoint.sh /

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=1.92 -y

ENV PATH=/root/.cargo/bin:$PATH

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
