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

RUN dnf install -y wget g++ gcc gdb libatomic libstdc++-static ninja-build git make zip unzip tar which \
    autoconf automake golang python3 python3-pip perl-FindBin texinfo \
    pkg-config libuuid-devel libaio perl-IPC-Cmd && \
    rm -rf /var/cache/yum/*

RUN pip3 install conan==1.61.0

RUN echo "target arch $TARGETARCH"
RUN wget -qO- "https://cmake.org/files/v3.27/cmake-3.27.5-linux-`uname -m`.tar.gz" | tar --strip-components=1 -xz -C /usr/local

RUN mkdir /opt/vcpkg &&  \
    wget -qO- vcpkg.tar.gz https://github.com/microsoft/vcpkg/archive/master.tar.gz | tar --strip-components=1 -xz -C /opt/vcpkg && \
    rm -rf vcpkg.tar.gz

ENV VCPKG_FORCE_SYSTEM_BINARIES 1

RUN /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics && ln -s /opt/vcpkg/vcpkg /usr/local/bin/vcpkg && vcpkg version

RUN vcpkg install azure-identity-cpp azure-storage-blobs-cpp gtest --only-downloads

RUN mkdir /tmp/ccache && cd /tmp/ccache &&\
    wget https://dl.fedoraproject.org/pub/epel/9/Everything/`uname -m`/Packages/h/hiredis-1.0.2-1.el9.`uname -m`.rpm &&\
    wget https://dl.fedoraproject.org/pub/epel/9/Everything/`uname -m`/Packages/c/ccache-4.5.1-2.el9.`uname -m`.rpm &&\
    rpm -i hiredis-1.0.2-1.el9.`uname -m`.rpm ccache-4.5.1-2.el9.`uname -m`.rpm &&\
    rm -rf /tmp/ccache


# refer: https://code.visualstudio.com/docs/remote/containers-advanced#_avoiding-extension-reinstalls-on-container-rebuild
RUN mkdir -p /home/milvus/.vscode-server/extensions \
        /home/milvus/.vscode-server-insiders/extensions \
    && chmod -R 777 /home/milvus

COPY --chown=0:0 build/docker/builder/entrypoint.sh /

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain stable -y

ENV PATH=/root/.cargo/bin:$PATH

RUN rustup install 1.73 && rustup default 1.73

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
