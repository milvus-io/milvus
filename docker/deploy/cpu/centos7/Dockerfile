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

FROM milvusdb/milvus-dev:amd64-centos-7-core AS openblas

FROM centos:centos7

RUN yum install -y wget && \
    wget -P /etc/yum.repos.d/ http://mirrors.aliyun.com/repo/epel-7.repo && \
    yum clean all && yum makecache && \
    yum install -y libgomp libgfortran4 mysql-devel && \
    rm -rf /var/cache/yum/*

COPY ./milvus /var/lib/milvus
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/var/lib/milvus/lib"

COPY --from=openblas /usr/lib/libopenblas-r0.3.9.so /var/lib/milvus/lib/
RUN ln -s /var/lib/milvus/lib/libopenblas-r0.3.9.so /var/lib/milvus/lib/libopenblas.so.0 && \
    ln -s /var/lib/milvus/lib/libopenblas.so.0 /var/lib/milvus/lib/libopenblas.so

WORKDIR /var/lib/milvus

CMD [ "/var/lib/milvus/bin/milvus_server", "-c", "/var/lib/milvus/conf/milvus.yaml" ]

EXPOSE 19530
