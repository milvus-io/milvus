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

FROM milvusdb/milvus-distributed-dev:amd64-ubuntu18.04-latest AS openblas

#FROM alpine
FROM ubuntu:bionic-20200921

RUN apt-get update && apt-get install -y --no-install-recommends libtbb-dev gfortran && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

# Add Tini
ENV TINI_VERSION v0.19.0

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini

RUN chmod +x /tini

COPY --from=openblas /usr/lib/libopenblas-r0.3.9.so /usr/lib/

RUN ln -s /usr/lib/libopenblas-r0.3.9.so /usr/lib/libopenblas.so.0 && \
    ln -s /usr/lib/libopenblas.so.0 /usr/lib/libopenblas.so

COPY ./bin/ /milvus-distributed/bin/

COPY ./configs/ /milvus-distributed/configs/

COPY ./lib/ /milvus-distributed/lib/

ENV LD_LIBRARY_PATH=/milvus-distributed/lib:$LD_LIBRARY_PATH:/usr/lib

ENTRYPOINT ["/tini", "--"]

WORKDIR /milvus-distributed/
