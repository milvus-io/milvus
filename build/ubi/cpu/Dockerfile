FROM registry.access.redhat.com/ubi9:9.5-1732804088 as vcpkg-installer

RUN dnf install -y wget zip git gcc gcc-c++ cmake \
        dnf-plugins-core ninja-build \
        perl-IPC-Cmd perl-Digest-SHA \
        perl-FindBin perl-File-Compare perl-File-Copy

ENV VCPKG_FORCE_SYSTEM_BINARIES 1

RUN mkdir /opt/vcpkg &&  \
    wget -qO- vcpkg.tar.gz https://github.com/microsoft/vcpkg/archive/refs/tags/2023.11.20.tar.gz | tar --strip-components=1 -xz -C /opt/vcpkg && \
    rm -rf vcpkg.tar.gz

RUN echo "" > /opt/vcpkg/scripts/toolchains/linux.cmake

RUN /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics && \
    ln -s /opt/vcpkg/vcpkg /usr/local/bin/vcpkg && \
    vcpkg version && \
    vcpkg install azure-identity-cpp azure-storage-blobs-cpp gtest


FROM registry.access.redhat.com/ubi9:9.5-1732804088 as builder

ARG TARGETARCH

COPY ./build/ubi/arch_vars.sh /opt/set_arch_vars.sh
RUN chmod +x /opt/set_arch_vars.sh && /opt/set_arch_vars.sh "$TARGETARCH" > /opt/arch_vars

RUN . /opt/arch_vars && dnf update -y && dnf install -y \
        https://www.rpmfind.net/linux/centos-stream/9-stream/AppStream/$SYSTEM_ARCH/os/Packages/perl-Unicode-EastAsianWidth-12.0-7.el9.noarch.rpm \
        https://www.rpmfind.net/linux/centos-stream/9-stream/AppStream/$SYSTEM_ARCH/os/Packages/perl-Text-Unidecode-1.30-16.el9.noarch.rpm \
        https://www.rpmfind.net/linux/centos-stream/9-stream/AppStream/$SYSTEM_ARCH/os/Packages/perl-libintl-perl-1.32-4.el9.$SYSTEM_ARCH.rpm \
        https://www.rpmfind.net/linux/centos-stream/9-stream/CRB/$SYSTEM_ARCH/os/Packages/texinfo-6.7-15.el9.$SYSTEM_ARCH.rpm \
        https://www.rpmfind.net/linux/centos-stream/9-stream/AppStream/$SYSTEM_ARCH/os/Packages/libaio-devel-0.3.111-13.el9.$SYSTEM_ARCH.rpm \
        https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm

RUN dnf install -y \
    git \
    gcc \
    gcc-c++ \
    glibc-devel \
    gdb \
    make \
    #cmake \
    #golang \
    automake \
    libffi-devel \
    libtool \
    pkgconfig \
    wget \
    unzip \
    tar \
    bzip2 \
    openssl-devel \
    libaio \
    libstdc++-static \
    llvm-toolset \
    ninja-build \
    openblas \
    openblas-devel \
    zip unzip \
    zlib-devel \
    libcurl-devel \
    libuuid-devel \
    perl-IPC-Cmd perl-Digest-SHA perl-FindBin \
    python3-pip && \
    pip3 install --upgrade pip && \
    pip3 install conan==1.64.1 && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Install cmake version 3.27 in line with other distro builds of Milvus, 'dnf install cmake' version fails
# Go version with 'dnf install golang' is 1.22, want 1.23
RUN . /opt/arch_vars && \
    wget -qO- "https://cmake.org/files/v3.27/cmake-3.27.5-linux-$SYSTEM_ARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local && \
    mkdir -p /usr/local/go && \
    wget -qO- "https://go.dev/dl/go1.23.3.linux-$TARGETARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go

ENV PATH=$PATH:/usr/local/go/bin

WORKDIR /home/milvus

#RUN git clone --depth 1 --branch master https://github.com/milvus-io/milvus.git .
COPY . .

COPY --chown=1001:1001 build/docker/builder/entrypoint.sh /home/milvus/

COPY --from=vcpkg-installer /root/.cache/vcpkg /home/milvus/.cache/vcpkg


# Install rust
RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=1.73 -y
ENV PATH=/home/milvus/.cargo/bin:/usr/local/bin:/usr/local/go/bin:$PATH

# Option - Add Include Path instead of symlinks below?
#ENV CMAKE_INCLUDE_PATH=/usr/include/openblas:$CMAKE_INCLUDE_PATH
RUN for file in /usr/include/openblas/*; do \
      ln -s "$file" /usr/include/"$(basename "$file")"; \
    done

ENV HOME=/home/milvus
RUN useradd -m milvus && mkdir -p /home/milvus/conan && chown -R milvus:milvus /home/milvus
RUN chown -R milvus:milvus /home/milvus
USER milvus
RUN /home/milvus/entrypoint.sh

ENV CONAN_USER_HOME=/home/milvus/conan
RUN make install

FROM registry.access.redhat.com/ubi9:9.5-1732804088

WORKDIR /milvus
COPY --chown=1001:1001 --from=builder /opt/arch_vars /opt/arch_vars

RUN dnf update -y && dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm
RUN . /opt/arch_vars && dnf install -y \
    https://www.rpmfind.net/linux/centos-stream/9-stream/AppStream/$SYSTEM_ARCH/os/Packages/libaio-devel-0.3.111-13.el9.$SYSTEM_ARCH.rpm \
    ca-certificates \
    libaio \
    libgomp \
    openblas \
    openblas-devel && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN . /opt/arch_vars && curl -L -o /milvus/tini https://github.com/krallin/tini/releases/download/v0.19.0/tini-$TARGETARCH && \
    chmod +x /milvus/tini && rm /opt/arch_vars

COPY --chmod=774 --from=builder /home/milvus/bin/ /milvus/bin/
COPY --chmod=774 --from=builder /home/milvus/configs/ /etc/milvus/configs/
COPY --chmod=774 --from=builder /home/milvus/lib/ /milvus/lib/

ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV LD_PRELOAD=/milvus/lib/libjemalloc.so
ENV MALLOC_CONF=background_thread:true

ENTRYPOINT ["/milvus/tini", "--"]
