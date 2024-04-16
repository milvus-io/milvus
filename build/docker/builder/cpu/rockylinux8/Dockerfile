FROM rockylinux/rockylinux:8

RUN dnf install -y make automake gcc gcc-c++ curl zip unzip tar git which \
        libaio libuuid-devel wget python3 python3-pip \
        pkg-config perl-IPC-Cmd perl-Digest-SHA libatomic libtool 

# install openblas-devel and texinfo
RUN dnf -y install dnf-plugins-core && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install texinfo openblas-devel

RUN pip3 install conan==1.61.0
RUN wget https://go.dev/dl/go1.21.6.linux-amd64.tar.gz && tar -xvf go1.21.6.linux-amd64.tar.gz -C /usr/local
RUN wget -qO- "https://cmake.org/files/v3.27/cmake-3.27.5-linux-`uname -m`.tar.gz" | tar --strip-components=1 -xz -C /usr/local
RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=1.73 -y

ENV PATH=/root/.cargo/bin:/usr/local/bin:/usr/local/go/bin:$PATH

COPY --chown=0:0 build/docker/builder/entrypoint.sh /

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["tail", "-f", "/dev/null"]
