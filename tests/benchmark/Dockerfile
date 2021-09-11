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

FROM python:3.6.8-jessie

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get install -y --no-install-recommends wget apt-transport-https && \
    wget -qO- "https://get.helm.sh/helm-v3.6.1-linux-amd64.tar.gz" | tar --strip-components=1 -xz -C /usr/local/bin linux-amd64/helm && \
    wget -P /tmp https://mirrors.aliyun.com/kubernetes/apt/doc/apt-key.gpg && \
    apt-key add /tmp/apt-key.gpg && \
    sh -c 'echo deb https://mirrors.aliyun.com/kubernetes/apt/ kubernetes-xenial main > /etc/apt/sources.list.d/kubernetes.list' && \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential kubectl && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

RUN python3 -m pip install --no-cache-dir -r /requirements.txt

WORKDIR /root
