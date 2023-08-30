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

FROM amazonlinux:2023

ARG TARGETARCH

RUN yum install -y wget g++ gcc gdb libstdc++-static git make zip unzip tar which \
    autoconf automake golang python3 python3-pip perl-FindBin texinfo \
    pkg-config libuuid-devel libaio && \
    rm -rf /var/cache/yum/*

RUN pip3 install conan==1.58.0

RUN echo "target arch $TARGETARCH"
RUN if [ "$TARGETARCH" = "amd64" ]; then CMAKE_SUFFIX=x86_64; else CMAKE_SUFFIX=aarch64; fi &&\
    wget -qO- "https://cmake.org/files/v3.24/cmake-3.24.4-linux-$CMAKE_SUFFIX.tar.gz" | tar --strip-components=1 -xz -C /usr/local

RUN mkdir /tmp/ccache && wget -qO- https://github.com/ccache/ccache/releases/download/v4.8.2/ccache-4.8.2.tar.gz | tar --strip-components=1 -xz -C /tmp/ccache &&\
    cd /tmp/ccache && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr .. && make && make install &&\
    rm -rf /tmp/ccache

# https://github.com/golang/go/issues/22040 Due to this issue, ld.gold cannot be used in the ARM environment, so the official golang package cannot be used. You need to use the golang package that comes with yum.
# Install Go
# ENV GOPATH /go
# ENV GOROOT /usr/local/go
# ENV GO111MODULE on
# ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
# RUN mkdir -p /usr/local/go && wget -qO- "https://go.dev/dl/go1.20.7.linux-$TARGETARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go && \
#     mkdir -p "$GOPATH/src" "$GOPATH/bin" && \
#     go clean --modcache && \
#     chmod -R 777 "$GOPATH" && chmod -R a+w $(go env GOTOOLDIR)

# refer: https://code.visualstudio.com/docs/remote/containers-advanced#_avoiding-extension-reinstalls-on-container-rebuild
RUN mkdir -p /home/milvus/.vscode-server/extensions \
        /home/milvus/.vscode-server-insiders/extensions \
    && chmod -R 777 /home/milvus

COPY --chown=0:0 build/docker/builder/entrypoint.sh /

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
