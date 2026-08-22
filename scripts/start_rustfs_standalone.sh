#!/usr/bin/env bash
set -e

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

# Start Milvus with RustFS as the object storage backend.
# RustFS is a high-performance S3-compatible object storage system.
# Visit https://github.com/rustfs/rustfs for more information.

echo "Starting Milvus with RustFS backend..."

# Set RustFS as the storage backend
export MINIO_ADDRESS=${MINIO_ADDRESS:-localhost:9000}
export MINIO_ACCESS_KEY_ID=${MINIO_ACCESS_KEY_ID:-rustfsadmin}
export MINIO_SECRET_ACCESS_KEY=${MINIO_SECRET_ACCESS_KEY:-rustfsadmin}

# Optional: Set cloud provider to rustfs for specialized optimizations
# export COMMON_STORAGETYPE=remote
# Note: cloudProvider can be set in milvus.yaml to "rustfs"

# Start Milvus Standalone
nohup ./bin/milvus run standalone --run-with-subprocess >/tmp/milvus_rustfs.log 2>&1 &

echo "Milvus with RustFS started. Check logs at /tmp/milvus_rustfs.log"
echo "RustFS Console: http://localhost:9001 (default credentials: rustfsadmin/rustfsadmin)"
