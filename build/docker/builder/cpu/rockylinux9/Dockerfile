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

FROM rockylinux/rockylinux:9

ARG TARGETARCH

# Rocky 9 ships with GCC 11 and Python 3.9 by default.
# Enable CRB (CodeReady Builder) repo for openblas-devel, texinfo, etc.
RUN dnf install -y dnf-plugins-core epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y \
        wget g++ gcc gdb libatomic libstdc++-static ninja-build git make zip unzip tar which \
        autoconf automake python3 python3-pip perl-FindBin texinfo ccache \
        pkg-config libuuid-devel libaio libaio-devel perl-IPC-Cmd perl-Digest-SHA \
        libasan openblas-devel tzdata libtool && \
    rm -rf /var/cache/dnf/* && \
    for f in /usr/include/openblas/*.h; do ln -sf "$f" /usr/include/$(basename "$f"); done

ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV GO111MODULE on
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
RUN mkdir -p /usr/local/go && wget -qO- "https://go.dev/dl/go1.24.12.linux-$TARGETARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go && \
    mkdir -p "$GOPATH/src" "$GOPATH/bin" && \
    go clean --modcache && \
    chmod -R 777 "$GOPATH" && chmod -R a+w $(go env GOTOOLDIR)

RUN pip3 install conan==1.66.0

RUN echo "target arch $TARGETARCH"
RUN wget -qO- "https://cmake.org/files/v3.27/cmake-3.27.5-linux-`uname -m`.tar.gz" | tar --strip-components=1 -xz -C /usr/local

# refer: https://code.visualstudio.com/docs/remote/containers-advanced#_avoiding-extension-reinstalls-on-container-rebuild
RUN mkdir -p /home/milvus/.vscode-server/extensions \
    /home/milvus/.vscode-server-insiders/extensions \
    && chmod -R 777 /home/milvus

COPY --chown=0:0 build/docker/builder/entrypoint.sh /

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=1.89 -y

ENV PATH=/root/.cargo/bin:$PATH

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
