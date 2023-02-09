# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

FROM milvusdb/openblas:centos7-20230209

RUN yum install -y epel-release centos-release-scl-rh && yum install -y wget curl which && \
    wget -qO- "https://cmake.org/files/v3.24/cmake-3.24.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
    yum install -y git make automake python3-devel \
    devtoolset-8-gcc devtoolset-8-gcc-c++ devtoolset-8-gcc-gfortran \
    llvm-toolset-8.0-clang llvm-toolset-8.0-clang-tools-extra \
    libaio libuuid-devel unzip \
    ccache lcov libtool m4 autoconf automake && \
    rm -rf /var/cache/yum/* && \
    echo "source scl_source enable devtoolset-8" >> /etc/profile.d/devtoolset-8.sh && \
    echo "source scl_source enable llvm-toolset-8.0" >> /etc/profile.d/llvm-toolset-8.sh

RUN pip3 install conan==1.58.0

RUN mkdir /tmp/stdlib && cd /tmp/stdlib && \
    wget https://repo.anaconda.com/archive/Anaconda3-2019.07-Linux-x86_64.sh && \
    sh Anaconda3-2019.07-Linux-x86_64.sh -b -p conda && \
    cp conda/lib/libstdc++.so.6.0.26 /usr/lib64 && \
    rm /usr/lib64/libstdc++.so.6 && \
    ln -s /usr/lib64/libstdc++.so.6.0.26 /usr/lib64/libstdc++.so.6 && \
    cp conda/lib/libatomic.so.1.2.0 /usr/lib64 && \
    ln -s /usr/lib64/libatomic.so.1.2.0 /usr/lib64/libatomic.so && \
    ln -s /usr/lib64/libatomic.so.1.2.0 /usr/lib64/libatomic.so.1 && \
    rm -rf /tmp/stdlib

ENV CLANG_TOOLS_PATH="/opt/rh/llvm-toolset-8.0/root/usr/bin"
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

# Install Go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV GO111MODULE on
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
RUN mkdir -p /usr/local/go && wget -qO- "https://golang.org/dl/go1.18.3.linux-amd64.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go && \
    mkdir -p "$GOPATH/src" "$GOPATH/bin" && \
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ${GOPATH}/bin v1.46.2 && \
    go install github.com/ramya-rao-a/go-outline@latest && \
    go install golang.org/x/tools/gopls@latest && \
    go install github.com/uudashr/gopkgs/v2/cmd/gopkgs@latest && \
    go install github.com/go-delve/delve/cmd/dlv@latest && \
    go install honnef.co/go/tools/cmd/staticcheck@2022.1 && \
    go clean --modcache && \
    chmod -R 777 "$GOPATH" && chmod -R a+w $(go env GOTOOLDIR)

RUN ln -s /go/bin/dlv /go/bin/dlv-dap

RUN echo 'root:root' | chpasswd

# refer: https://code.visualstudio.com/docs/remote/containers-advanced#_avoiding-extension-reinstalls-on-container-rebuild
RUN mkdir -p /home/milvus/.vscode-server/extensions \
        /home/milvus/.vscode-server-insiders/extensions \
    && chmod -R 777 /home/milvus

COPY --chown=0:0 build/docker/builder/entrypoint.sh /

RUN wget -qO- "https://github.com/jeffoverflow/autouseradd/releases/download/1.2.0/autouseradd-1.2.0-amd64.tar.gz" | tar xz -C / --strip-components 1

RUN wget -O /tini https://github.com/krallin/tini/releases/download/v0.19.0/tini && \
    chmod +x /tini

ENTRYPOINT [ "/tini", "--", "autouseradd", "--user", "milvus", "--", "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
