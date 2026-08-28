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
FROM ghcr.io/astral-sh/uv:0.11.23 AS uv
FROM ${MILVUS_BASE_IMAGE_REPO}:${MILVUS_BASE_IMAGE_TAG}

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python
ENV UV_PYTHON_BIN_DIR=/usr/local/bin
ENV PYTHON=python3.12

# Upgrade openssl to fix CVE-2026-45447 (base image is a pinned/legacy ubuntu20.04 build)
RUN apt-get update && \
    apt-get upgrade -y openssl libssl1.1 && \
    rm -rf /var/lib/apt/lists/*

RUN uv python install 3.12.13 && \
    python_lib="$(python3.12 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')" && \
    printf '%s\n' "${python_lib}" > /etc/ld.so.conf.d/milvus-python.conf && \
    ldconfig

RUN mkdir -p /milvus/bin
COPY --chown=root:root --chmod=774 ./bin/milvus /milvus/bin/milvus

COPY --chown=root:root --chmod=774 ./configs/ /milvus/configs/

COPY --chown=root:root --chmod=774 ./lib/ /milvus/lib/

RUN uv pip install --python python3.12 --system --break-system-packages \
        /milvus/lib/pyudf/milvus_pyudf_runtime-*.whl && \
    rm -rf /milvus/lib/pyudf


ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV LD_PRELOAD=/milvus/lib/libjemalloc.so
ENV MALLOC_CONF=background_thread:true

# Change user to milvus
RUN groupadd -r milvus && useradd -r -g milvus milvus && \
    mkdir -p /var/lib/milvus && \
    chown -R milvus:milvus /milvus /var/lib/milvus
USER milvus:milvus
