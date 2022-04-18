FROM debian:buster

# arg that specifies the image name (for debugging)
ARG IMAGE_ARG

# arg that specifies the go version to install
ARG GO_VERSION

# add envs:
# - so we can debug with the image name:tag
# - adding gsutil etc. to path (where we will install them)
# - disabling prompts when installing gsutil etc.
# - hinting that we are in a docker container
ENV KRTE_IMAGE=${IMAGE_ARG} \
    GOPATH=/home/go \
    PATH=/home/go/bin:/usr/local/go/bin:${PATH} \
    KIND_VERSION=0.11.1 \
    DOCKER_COMPOSE_VERSION=1.29.1 \
    CONTAINER=docker \
    YQ_VERSION=v4.24.5 \
    YQ_BINARY=yq_linux_amd64

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install tools needed to:
# - install docker
# - build kind (dockerized)
# - build kubernetes (dockerized, or with bazel)
#
# TODO: the `sed` is a bit of a hack, look into alternatives.
# Why this exists: `docker service start` on debian runs a `cgroupfs_mount` method,
# We're already inside docker though so we can be sure these are already mounted.
# Trying to remount these makes for a very noisy error block in the beginning of
# the pod logs, so we just comment out the call to it... :shrug:
RUN echo "Installing Packages ..." \
        && apt-get update \
        && apt-get install -y --no-install-recommends \
            apt-transport-https \
            build-essential \
            ca-certificates \
            curl \
            file \
            git \
            gnupg2 \
            kmod \
            lsb-release \
            mercurial \
            pkg-config \
            procps \
            python3 \
            python3-dev \
            python3-pip \
            python3-setuptools \
            rsync \
            software-properties-common \
            unzip \
            jq \
            wget \
            vim \
    && python3 -m pip install --no-cache-dir --upgrade pip \
        && python3 -m pip install --no-cache-dir -r https://raw.githubusercontent.com/milvus-io/milvus/master/tests/python_client/requirements.txt \
        && rm -rf /var/lib/apt/lists/* \
    && echo "Installing Go ..." \
        && export GO_TARBALL="go${GO_VERSION}.linux-amd64.tar.gz"\
        && curl -fsSL "https://golang.org/dl/${GO_TARBALL}" --output "${GO_TARBALL}" \
        && tar xzf "${GO_TARBALL}" -C /usr/local \
        && rm "${GO_TARBALL}"\
        && mkdir -p "${GOPATH}/bin" \
    && echo "Installing kubectl, helm ..." \
        && curl -fsSL "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" --output /usr/local/bin/kubectl \
        && chmod 755 /usr/local/bin/kubectl \
        && ln -s /usr/local/bin/kubectl /usr/bin/kubectl \
        && curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 \
        && chmod 700 get_helm.sh \
        && ./get_helm.sh \
        && rm ./get_helm.sh \
    && echo "Installing Docker ..." \
        && curl -fsSL "https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg" | apt-key add - \
        && add-apt-repository \
            "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
            $(lsb_release -cs) stable" \
        && apt-get update \
        && apt-get install -y --no-install-recommends docker-ce \
        && rm -rf /var/lib/apt/lists/* \
        && sed -i 's/cgroupfs_mount$/#cgroupfs_mount\n/' /etc/init.d/docker \
    && echo "Installing Docker Compose ..." \
        && curl -fsSL "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
        && chmod +x /usr/local/bin/docker-compose \
        && ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose \
    && echo "Installing KinD ..." \
        && curl -fsSL -o /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/v${KIND_VERSION}/kind-linux-amd64" \
        && chmod +x /usr/local/bin/kind \
    && echo "Installing yq ..." \
        && wget https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/${YQ_BINARY} -O /usr/bin/yq \ 
        && chmod +x /usr/bin/yq \
    && echo "Ensuring Legacy Iptables ..." \
        && update-alternatives --set iptables /usr/sbin/iptables-legacy \
        && update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

# copy in image utility scripts
COPY wrapper.sh /usr/local/bin/

# entrypoint is our wrapper script, in Prow you will need to explicitly re-specify this
ENTRYPOINT ["wrapper.sh"]
# volume for docker in docker, use an emptyDir in Prow
VOLUME ["/var/lib/docker"]
