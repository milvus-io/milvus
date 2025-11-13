FROM rockylinux/rockylinux:8 as vcpkg-installer

RUN dnf -y install curl wget tar zip unzip git \
    gcc gcc-c++ make cmake \
    perl-IPC-Cmd perl-Digest-SHA

# install ninjia
RUN dnf -y update && \
    dnf -y install dnf-plugins-core && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install ninja-build

########################################################################################
FROM rockylinux/rockylinux:8

ARG TARGETARCH

RUN dnf install -y make cmake automake gcc gcc-c++ curl zip unzip tar git which \
    libaio libuuid-devel wget python3 python3-pip \
    pkg-config perl-IPC-Cmd perl-Digest-SHA libatomic libtool tzdata

# install openblas-devel texinfo ninja
RUN dnf -y update && \
    dnf -y install dnf-plugins-core && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install texinfo openblas-devel ninja-build


RUN pip3 install conan==1.64.1
RUN mkdir -p /usr/local/go && wget -qO- "https://go.dev/dl/go1.24.9.linux-$TARGETARCH.tar.gz" | tar --strip-components=1 -xz -C /usr/local/go 
RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=1.89 -y

ENV PATH=/root/.cargo/bin:/usr/local/bin:/usr/local/go/bin:$PATH

COPY --chown=0:0 build/docker/builder/entrypoint.sh /

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
