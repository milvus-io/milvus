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

FROM hectormolinero/tini:v18 AS tini
FROM nvidia/cuda:11.6.2-runtime-centos7 AS runtime

FROM nvidia/cuda:11.6.2-base-centos7

ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

RUN yum --disablerepo=cuda install -y epel-release && \
    yum --disablerepo=cuda install -y wget libgomp libgfortran4 mysql-devel openblas-devel lapack-devel && \
    rm -rf /var/cache/yum/*

COPY ./milvus /var/lib/milvus
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/var/lib/milvus/lib"

COPY --from=tini /usr/bin/tini /tini

# copy cublas library from runtime
COPY --from=runtime /usr/local/cuda/targets/x86_64-linux/lib/libcublasLt.so.11 /var/lib/milvus/lib/libcublasLt.so.11
COPY --from=runtime /usr/local/cuda/targets/x86_64-linux/lib/libcublas.so.11 /var/lib/milvus/lib/libcublas.so.11

ENTRYPOINT ["/tini", "--"]

WORKDIR /var/lib/milvus

CMD [ "/var/lib/milvus/bin/milvus_server", "-c", "/var/lib/milvus/conf/server_config.yaml" ]

EXPOSE 19530
