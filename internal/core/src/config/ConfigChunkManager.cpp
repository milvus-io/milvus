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

std::string REMOTE_ADDRESS = "localhost:9000";   // NOLINT
std::string REMOTE_ACCESS_KEY = "minioadmin";    // NOLINT
std::string REMOTE_ACCESS_VALUE = "minioadmin";  // NOLINT
std::string REMOTE_BUCKET_NAME = "a-bucket";     // NOLINT
std::string REMOTE_ROOT_PATH = "files";          // NOLINT
std::string LOCAL_ROOT_PATH = "/tmp/milvus";     // NOLINT
bool MINIO_USE_SSL = false;
bool MINIO_USE_IAM = false;

void
SetAddress(const std::string& address) {
    REMOTE_ADDRESS = address;
}

std::string
GetAddress() {
    return REMOTE_ADDRESS;
}

void
SetAccessKey(const std::string& access_key) {
    REMOTE_ACCESS_KEY = access_key;
}

std::string
GetAccessKey() {
    return REMOTE_ACCESS_KEY;
}

void
SetAccessValue(const std::string& access_value) {
    REMOTE_ACCESS_VALUE = access_value;
}

std::string
GetAccessValue() {
    return REMOTE_ACCESS_VALUE;
}

void
SetBucketName(const std::string& bucket_name) {
    REMOTE_BUCKET_NAME = bucket_name;
}

std::string
GetBucketName() {
    return REMOTE_BUCKET_NAME;
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
SetUseIAM(bool use_iam) {
    MINIO_USE_IAM = use_iam;
}

bool
GetUseIAM() {
    return MINIO_USE_IAM;
}

void
SetRemoteRootPath(const std::string& root_path) {
    REMOTE_ROOT_PATH = root_path;
}

std::string
GetRemoteRootPath() {
    return REMOTE_ROOT_PATH;
}

void
SetLocalRootPath(const std::string& path_prefix) {
    LOCAL_ROOT_PATH = path_prefix;
}

std::string
GetLocalRootPath() {
    return LOCAL_ROOT_PATH;
}

}  // namespace milvus::ChunkMangerConfig
