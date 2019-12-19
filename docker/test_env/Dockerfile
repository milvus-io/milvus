FROM python:3.6.8-jessie

RUN apt-get update && apt-get install -y --no-install-recommends wget apt-transport-https && \
    wget -qO- "https://get.helm.sh/helm-v3.0.2-linux-amd64.tar.gz" | tar --strip-components=1 -xz -C /usr/local/bin linux-amd64/helm && \
    wget -P /tmp https://mirrors.aliyun.com/kubernetes/apt/doc/apt-key.gpg && \
    apt-key add /tmp/apt-key.gpg && \
    sh -c 'echo deb https://mirrors.aliyun.com/kubernetes/apt/ kubernetes-xenial main > /etc/apt/sources.list.d/kubernetes.list' && \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential kubectl && \
    apt-get remove --purge -y

COPY docker-entrypoint.sh /app/docker-entrypoint.sh
WORKDIR /root

ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
CMD [ "start" ]
