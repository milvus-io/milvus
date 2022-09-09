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

#include "config/ConfigChunkManager.h"

namespace milvus::ChunkMangerConfig {

std::string MINIO_ADDRESS = "localhost:9000";   // NOLINT
std::string MINIO_ACCESS_KEY = "minioadmin";    // NOLINT
std::string MINIO_ACCESS_VALUE = "minioadmin";  // NOLINT
std::string MINIO_BUCKET_NAME = "a-bucket";     // NOLINT
std::string LOCAL_BUCKET_NAME = "/tmp/milvus";  // NOLINT
bool MINIO_USE_SSL = false;

void
SetAddress(const std::string& address) {
    MINIO_ADDRESS = address.c_str();
}

std::string
GetAddress() {
    return MINIO_ADDRESS;
}

void
SetAccessKey(const std::string& access_key) {
    MINIO_ACCESS_KEY = access_key.c_str();
}

std::string
GetAccessKey() {
    return MINIO_ACCESS_KEY;
}

void
SetAccessValue(const std::string& access_value) {
    MINIO_ACCESS_VALUE = access_value.c_str();
}

std::string
GetAccessValue() {
    return MINIO_ACCESS_VALUE;
}

void
SetBucketName(const std::string& bucket_name) {
    MINIO_BUCKET_NAME = bucket_name.c_str();
}

std::string
GetBucketName() {
    return MINIO_BUCKET_NAME;
}

void
SetUseSSL(bool use_ssl) {
    MINIO_USE_SSL = use_ssl;
}

bool
GetUseSSL() {
    return MINIO_USE_SSL;
}

void
SetLocalBucketName(const std::string& path_prefix) {
    LOCAL_BUCKET_NAME = path_prefix.c_str();
}

std::string
GetLocalBucketName() {
    return LOCAL_BUCKET_NAME;
}

}  // namespace milvus::ChunkMangerConfig
