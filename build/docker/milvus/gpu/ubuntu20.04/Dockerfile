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
ARG MILVUS_BASE_IMAGE_REPO="milvusdb/milvus-base"
ARG MILVUS_BASE_IMAGE_TAG="gpu-20230822-34f9067"
FROM ${MILVUS_BASE_IMAGE_REPO}:${MILVUS_BASE_IMAGE_TAG}

RUN mkdir -p /milvus/bin
COPY --chown=root:root --chmod=774 ./bin/milvus /milvus/bin/milvus

COPY --chown=root:root --chmod=774 ./configs/ /milvus/configs/

COPY --chown=root:root --chmod=774 ./lib/ /milvus/lib/


ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV LD_PRELOAD=/milvus/lib/libjemalloc.so
ENV MALLOC_CONF=background_thread:true
