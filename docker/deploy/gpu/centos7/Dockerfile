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

FROM nvidia/cuda:10.1-devel-centos7

ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

RUN yum --disablerepo=cuda install -y epel-release && \
    yum --disablerepo=cuda install -y libgomp libgfortran4 mysql-devel && \
    rm -rf /var/cache/yum/*

COPY ./milvus /var/lib/milvus
COPY ./docker-entrypoint.sh /var/lib/milvus/
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/var/lib/milvus/lib"

COPY --from=openblas /usr/lib/libopenblas*.so* /var/lib/milvus/lib/

WORKDIR /var/lib/milvus

ENTRYPOINT [ "/var/lib/milvus/docker-entrypoint.sh" ]

CMD [ "start" ]

EXPOSE 19530
