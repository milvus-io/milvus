#!/bin/bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Exit immediately for non zero status
set -e
# Print commands
set -x

# Absolute path to the toplevel milvus directory.
toplevel=$(dirname "$(cd "$(dirname "${0}")"; pwd)")

OS_NAME="${OS_NAME:-ubuntu20.04}"
MILVUS_IMAGE_REPO="${MILVUS_IMAGE_REPO:-milvusdb/milvus}"
MILVUS_IMAGE_TAG="${MILVUS_IMAGE_TAG:-latest}"

if [ -z "$IMAGE_ARCH" ]; then
    MACHINE=$(uname -m)
    if [ "$MACHINE" = "x86_64" ]; then
        IMAGE_ARCH="amd64"
    else
        IMAGE_ARCH="arm64"
    fi
fi

echo ${IMAGE_ARCH}


BUILD_ARGS="${BUILD_ARGS:---build-arg TARGETARCH=${IMAGE_ARCH}}"

pushd "${toplevel}"

docker build ${BUILD_ARGS} --platform linux/${IMAGE_ARCH} -f "./build/docker/milvus/${OS_NAME}/Dockerfile" -t "${MILVUS_IMAGE_REPO}:${MILVUS_IMAGE_TAG}" .

image_size=$(docker inspect ${MILVUS_IMAGE_REPO}:${MILVUS_IMAGE_TAG}  -f '{{.Size}}'| awk '{ byte =$1 /1024/1024/1024; print byte " GB" }')

echo "Image Size for  ${MILVUS_IMAGE_REPO}:${MILVUS_IMAGE_TAG} is ${image_size}"

popd
