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
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "storage/AzureChunkManager.h"

namespace milvus {
namespace storage {

std::atomic<size_t> AzureChunkManager::init_count_(0);
std::mutex AzureChunkManager::client_mutex_;

std::string const ErrorText = "ERROR";
std::string const WarningText = "WARN ";
std::string const InformationalText = "INFO ";
std::string const VerboseText = "DEBUG";
std::string const UnknownText = "?????";

inline std::string const&
LogLevelToConsoleString(Azure::Core::Diagnostics::Logger::Level logLevel) {
    switch (logLevel) {
        case Azure::Core::Diagnostics::Logger::Level::Error:
            return ErrorText;

        case Azure::Core::Diagnostics::Logger::Level::Warning:
            return WarningText;

        case Azure::Core::Diagnostics::Logger::Level::Informational:
            return InformationalText;

        case Azure::Core::Diagnostics::Logger::Level::Verbose:
            return VerboseText;

        default:
            return UnknownText;
    }
}

void
AzureLogger(Azure::Core::Diagnostics::Logger::Level level,
            std::string const& msg) {
    LOG_SEGCORE_INFO_ << "[AZURE LOG] [" << LogLevelToConsoleString(level)
                      << "] " << msg;
}

AzureChunkManager::AzureChunkManager(const StorageConfig& storage_config)
    : default_bucket_name_(storage_config.bucket_name),
      path_prefix_(storage_config.root_path) {
    std::scoped_lock lock{client_mutex_};
    const size_t initCount = init_count_++;
    if (initCount == 0) {
        azure::AzureBlobChunkManager::InitLog(storage_config.log_level,
                                              AzureLogger);
    }
    client_ = std::make_shared<azure::AzureBlobChunkManager>(
        storage_config.access_key_id,
        storage_config.access_key_value,
        storage_config.address,
        storage_config.requestTimeoutMs == 0
            ? DEFAULT_CHUNK_MANAGER_REQUEST_TIMEOUT_MS
            : storage_config.requestTimeoutMs,
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
    return ListObjects(default_bucket_name_, filepath);
}

uint64_t
AzureChunkManager::Read(const std::string& filepath, void* buf, uint64_t size) {
    try {
        return GetObjectBuffer(default_bucket_name_, filepath, buf, size);
    } catch (const std::exception& e) {
        std::stringstream err_msg;
        err_msg << "read object('" << default_bucket_name_ << "', '" << filepath
                << "' fail: " << e.what();
        throw SegcoreError(ObjectNotExist, err_msg.str());
    }
}

void
AzureChunkManager::Write(const std::string& filepath,
                         void* buf,
                         uint64_t size) {
    PutObjectBuffer(default_bucket_name_, filepath, buf, size);
}

bool
AzureChunkManager::BucketExists(const std::string& bucket_name) {
    bool res;
    try {
        res = client_->BucketExists(bucket_name);
    } catch (std::exception& err) {
        ThrowAzureError("BucketExists", err, "params, bucket={}", bucket_name);
    }
    return res;
}

std::vector<std::string>
AzureChunkManager::ListBuckets() {
    std::vector<std::string> res;
    try {
        res = client_->ListBuckets();
    } catch (std::exception& err) {
        ThrowAzureError("ListBuckets", err, "params");
    }
    return res;
}

bool
AzureChunkManager::CreateBucket(const std::string& bucket_name) {
    bool res;
    try {
        res = client_->CreateBucket(bucket_name);
    } catch (std::exception& err) {
        ThrowAzureError("CreateBucket", err, "params, bucket={}", bucket_name);
    }
    return res;
}

bool
AzureChunkManager::DeleteBucket(const std::string& bucket_name) {
    bool res;
    try {
        res = client_->DeleteBucket(bucket_name);
    } catch (std::exception& err) {
        ThrowAzureError("DeleteBucket", err, "params, bucket={}", bucket_name);
    }
    return res;
}

bool
AzureChunkManager::ObjectExists(const std::string& bucket_name,
                                const std::string& object_name) {
    bool res;
    try {
        res = client_->ObjectExists(bucket_name, object_name);
    } catch (std::exception& err) {
        ThrowAzureError("ObjectExists",
                        err,
                        "params, bucket={}, object={}",
                        bucket_name,
                        object_name);
    }
    return res;
}

uint64_t
AzureChunkManager::GetObjectSize(const std::string& bucket_name,
                                 const std::string& object_name) {
    uint64_t res;
    try {
        res = client_->GetObjectSize(bucket_name, object_name);
    } catch (std::exception& err) {
        ThrowAzureError("GetObjectSize",
                        err,
                        "params, bucket={}, object={}",
                        bucket_name,
                        object_name);
    }
    return res;
}

bool
AzureChunkManager::DeleteObject(const std::string& bucket_name,
                                const std::string& object_name) {
    bool res;
    try {
        res = client_->DeleteObject(bucket_name, object_name);
    } catch (std::exception& err) {
        ThrowAzureError("DeleteObject",
                        err,
                        "params, bucket={}, object={}",
                        bucket_name,
                        object_name);
    }
    return res;
}

bool
AzureChunkManager::PutObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    bool res;
    try {
        res = client_->PutObjectBuffer(bucket_name, object_name, buf, size);
    } catch (std::exception& err) {
        ThrowAzureError("PutObjectBuffer",
                        err,
                        "params, bucket={}, object={}",
                        bucket_name,
                        object_name);
    }
    return res;
}

uint64_t
AzureChunkManager::GetObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    uint64_t res;
    try {
        res = client_->GetObjectBuffer(bucket_name, object_name, buf, size);
    } catch (std::exception& err) {
        ThrowAzureError("GetObjectBuffer",
                        err,
                        "params, bucket={}, object={}",
                        bucket_name,
                        object_name);
    }
    return res;
}

std::vector<std::string>
AzureChunkManager::ListObjects(const std::string& bucket_name,
                               const std::string& prefix) {
    std::vector<std::string> res;
    try {
        res = client_->ListObjects(bucket_name, prefix);
    } catch (std::exception& err) {
        ThrowAzureError("ListObjects",
                        err,
                        "params, bucket={}, prefix={}",
                        bucket_name,
                        prefix);
    }
    return res;
}

}  // namespace storage
}  // namespace milvus
