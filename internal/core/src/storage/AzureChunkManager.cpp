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

#include <iostream>
#include <sstream>
#include "common/EasyAssert.h"
#include "storage/AzureChunkManager.h"

namespace milvus {
namespace storage {

AzureChunkManager::AzureChunkManager(const StorageConfig& storage_config)
    : default_bucket_name_(storage_config.bucket_name),
      path_prefix_(storage_config.root_path) {
    client_ = std::make_shared<azure::AzureBlobChunkManager>(
        storage_config.access_key_id,
        storage_config.access_key_value,
        storage_config.address,
        storage_config.useIAM);
}

AzureChunkManager::~AzureChunkManager() {
}

uint64_t
AzureChunkManager::Size(const std::string& filepath) {
    return GetObjectSize(default_bucket_name_, filepath);
}

bool
AzureChunkManager::Exist(const std::string& filepath) {
    return ObjectExists(default_bucket_name_, filepath);
}

void
AzureChunkManager::Remove(const std::string& filepath) {
    DeleteObject(default_bucket_name_, filepath);
}

std::vector<std::string>
AzureChunkManager::ListWithPrefix(const std::string& filepath) {
    return ListObjects(default_bucket_name_.c_str(), filepath.c_str());
}

uint64_t
AzureChunkManager::Read(const std::string& filepath, void* buf, uint64_t size) {
    if (!ObjectExists(default_bucket_name_, filepath)) {
        std::stringstream err_msg;
        err_msg << "object('" << default_bucket_name_ << "', '" << filepath
                << "') not exists";
        throw SegcoreError(ObjectNotExist, err_msg.str());
    }
    return GetObjectBuffer(default_bucket_name_, filepath, buf, size);
}

void
AzureChunkManager::Write(const std::string& filepath,
                         void* buf,
                         uint64_t size) {
    PutObjectBuffer(default_bucket_name_, filepath, buf, size);
}

bool
AzureChunkManager::BucketExists(const std::string& bucket_name) {
    return client_->BucketExists(bucket_name);
}

std::vector<std::string>
AzureChunkManager::ListBuckets() {
    return client_->ListBuckets();
}

bool
AzureChunkManager::CreateBucket(const std::string& bucket_name) {
    try {
        client_->CreateBucket(bucket_name);
    } catch (std::exception& e) {
        throw SegcoreError(BucketInvalid, e.what());
    }
    return true;
}

bool
AzureChunkManager::DeleteBucket(const std::string& bucket_name) {
    try {
        client_->DeleteBucket(bucket_name);
    } catch (std::exception& e) {
        throw SegcoreError(BucketInvalid, e.what());
    }
    return true;
}

bool
AzureChunkManager::ObjectExists(const std::string& bucket_name,
                                const std::string& object_name) {
    return client_->ObjectExists(bucket_name, object_name);
}

int64_t
AzureChunkManager::GetObjectSize(const std::string& bucket_name,
                                 const std::string& object_name) {
    try {
        return client_->GetObjectSize(bucket_name, object_name);
    } catch (std::exception& e) {
        throw SegcoreError(ObjectNotExist, e.what());
    }
}

bool
AzureChunkManager::DeleteObject(const std::string& bucket_name,
                                const std::string& object_name) {
    try {
        client_->DeleteObject(bucket_name, object_name);
    } catch (std::exception& e) {
        throw SegcoreError(ObjectNotExist, e.what());
    }
    return true;
}

bool
AzureChunkManager::PutObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    return client_->PutObjectBuffer(bucket_name, object_name, buf, size);
}

uint64_t
AzureChunkManager::GetObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    return client_->GetObjectBuffer(bucket_name, object_name, buf, size);
}

std::vector<std::string>
AzureChunkManager::ListObjects(const char* bucket_name, const char* prefix) {
    return client_->ListObjects(bucket_name, prefix);
}

}  // namespace storage
}  // namespace milvus
