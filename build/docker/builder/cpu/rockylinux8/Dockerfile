FROM rockylinux/rockylinux:8 as vcpkg-installer

RUN dnf -y install curl wget tar zip unzip git \
    gcc gcc-c++ make cmake \
    perl-IPC-Cmd perl-Digest-SHA

# install ninjia
RUN dnf -y update && \
    dnf -y install dnf-plugins-core && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install ninja-build

ENV VCPKG_FORCE_SYSTEM_BINARIES 1

# install vcpkg
RUN mkdir /opt/vcpkg &&  \
    wget -qO- vcpkg.tar.gz https://github.com/microsoft/vcpkg/archive/master.tar.gz | tar --strip-components=1 -xz -C /opt/vcpkg && \
    rm -rf vcpkg.tar.gz

# empty the vscpkg toolchains linux.cmake file to avoid the error
RUN echo "" > /opt/vcpkg/scripts/toolchains/linux.cmake

# install azure-identity-cpp azure-storage-blobs-cpp gtest via vcpkg
RUN /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics && \
    ln -s /opt/vcpkg/vcpkg /usr/local/bin/vcpkg && \
    vcpkg version && \
    vcpkg install azure-identity-cpp azure-storage-blobs-cpp gtest 

########################################################################################
FROM rockylinux/rockylinux:8

ARG TARGETARCH

RUN dnf install -y make cmake automake gcc gcc-c++ curl zip unzip tar git which \
    libaio libuuid-devel wget python3 python3-pip \
    pkg-config perl-IPC-Cmd perl-Digest-SHA libatomic libtool

# install openblas-devel texinfo ninja
RUN dnf -y update && \
    dnf -y install dnf-plugins-core && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install texinfo openblas-devel ninja-build


RUN pip3 install conan==1.64.1
RUN mkdir -p /usr/local/go && wget -qO- "https://go.dev/dl/go1.21.11.linux-$TARGETARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go 
RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=1.83 -y

ENV PATH=/root/.cargo/bin:/usr/local/bin:/usr/local/go/bin:$PATH

ENV VCPKG_FORCE_SYSTEM_BINARIES 1

# install vcpkg
RUN mkdir /opt/vcpkg &&  \
    wget -qO- vcpkg.tar.gz https://github.com/microsoft/vcpkg/archive/master.tar.gz | tar --strip-components=1 -xz -C /opt/vcpkg && \
    rm -rf vcpkg.tar.gz
# Copy the vcpkg installed libraries
COPY --from=vcpkg-installer /root/.cache/vcpkg /root/.cache/vcpkg


COPY --chown=0:0 build/docker/builder/entrypoint.sh /

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
