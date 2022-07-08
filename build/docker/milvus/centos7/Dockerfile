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

FROM milvusdb/openblas:centos7-20210706 AS openblas

FROM centos:centos7

RUN yum install -y wget && \
    wget -P /etc/yum.repos.d/ http://mirrors.aliyun.com/repo/epel-7.repo && \
    yum clean all && yum makecache && \
    yum install -y libgomp libgfortran4 tbb-devel && \
    rm -rf /var/cache/yum/*

COPY --from=openblas /usr/lib/libopenblas-r0.3.10.so /usr/lib/

RUN ln -s /usr/lib/libopenblas-r0.3.10.so /usr/lib/libopenblas.so.0 && \
    ln -s /usr/lib/libopenblas.so.0 /usr/lib/libopenblas.so

COPY ./bin/ /milvus/bin/

COPY ./configs/ /milvus/configs/

COPY ./lib/ /milvus/lib/

ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib

# Add Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

WORKDIR /milvus
