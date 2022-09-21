// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/init_c.h"

#include <string>
#include "config/ConfigChunkManager.h"

void
MinioAddressInit(const char* address) {
    std::string minio_address(address);
    milvus::ChunkMangerConfig::SetAddress(address);
}

void
MinioAccessKeyInit(const char* key) {
    std::string minio_access_key(key);
    milvus::ChunkMangerConfig::SetAccessKey(minio_access_key);
}

void
MinioAccessValueInit(const char* value) {
    std::string minio_access_value(value);
    milvus::ChunkMangerConfig::SetAccessValue(value);
}

void
MinioSSLInit(bool use_ssl) {
    milvus::ChunkMangerConfig::SetUseSSL(use_ssl);
}

void
MinioUseIamInit(bool use_iam) {
    milvus::ChunkMangerConfig::SetUseIAM(use_iam);
}

void
MinioBucketNameInit(const char* name) {
    std::string bucket_name(name);
    milvus::ChunkMangerConfig::SetBucketName(name);
}

void
MinioRootPathInit(const char* name) {
    std::string root_path(name);
    milvus::ChunkMangerConfig::SetRemoteRootPath(name);
}

void
LocalRootPathInit(const char* root_path) {
    std::string local_path_root(root_path);
    milvus::ChunkMangerConfig::SetLocalRootPath(local_path_root);
}
