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

FROM centos:centos7

RUN yum install -y epel-release && \
    yum install -y wget libgomp libgfortran4 mysql-devel openblas-devel lapack-devel && \
    rm -rf /var/cache/yum/*

COPY ./milvus /var/lib/milvus
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/var/lib/milvus/lib"

RUN wget -O /tini https://github.com/krallin/tini/releases/download/v0.19.0/tini && \
    chmod +x /tini

ENTRYPOINT ["/tini", "--"]

WORKDIR /var/lib/milvus

CMD [ "/var/lib/milvus/bin/milvus_server", "-c", "/var/lib/milvus/conf/server_config.yaml" ]

EXPOSE 19530
