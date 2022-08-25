# Copyright (C) 2019-2022 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

FROM milvusdb/openblas:ubuntu20.04-20220825-4feedf1 AS openblas

#FROM alpine
FROM ubuntu:ubuntu:focal-20220426

RUN apt-get update && \
    apt-get install -y --no-install-recommends libtbb-dev gfortran netcat iputils-ping ca-certificates && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=openblas /usr/lib/libopenblas-r0.3.9.so /usr/lib/

RUN ln -s /usr/lib/libopenblas-r0.3.9.so /usr/lib/libopenblas.so.0 && \
    ln -s /usr/lib/libopenblas.so.0 /usr/lib/libopenblas.so

COPY ./bin/ /milvus/bin/

COPY ./configs/ /milvus/configs/

COPY ./lib/ /milvus/lib/

ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV LD_PRELOAD=/milvus/lib/libjemalloc.so

# Add Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

WORKDIR /milvus/
