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

#include <chrono>
#include <iostream>
#include <sstream>
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "monitor/prometheus_client.h"
#include "storage/azure/AzureChunkManager.h"

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
    LOG_INFO("[AZURE LOG] [{}] {}", LogLevelToConsoleString(level), msg);
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
    try {
        client_ = std::make_shared<azure::AzureBlobChunkManager>(
            storage_config.access_key_id,
            storage_config.access_key_value,
            storage_config.address,
            storage_config.requestTimeoutMs == 0
                ? DEFAULT_CHUNK_MANAGER_REQUEST_TIMEOUT_MS
                : storage_config.requestTimeoutMs,
            storage_config.useIAM);
    } catch (std::exception& err) {
        ThrowAzureError(
            "PreCheck",
            err,
            "precheck chunk manager client failed, error:{}, configuration:{}",
            err.what(),
            storage_config.ToString());
    }
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
        auto start = std::chrono::system_clock::now();
        res = client_->ObjectExists(bucket_name, object_name);
        monitor::internal_storage_request_latency_stat.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start)
                .count());
        monitor::internal_storage_op_count_stat_suc.Increment();
    } catch (std::exception& err) {
        monitor::internal_storage_op_count_stat_fail.Increment();
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
        auto start = std::chrono::system_clock::now();
        res = client_->GetObjectSize(bucket_name, object_name);
        monitor::internal_storage_request_latency_stat.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start)
                .count());
        monitor::internal_storage_op_count_stat_suc.Increment();
    } catch (std::exception& err) {
        monitor::internal_storage_op_count_stat_fail.Increment();
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
        auto start = std::chrono::system_clock::now();
        res = client_->DeleteObject(bucket_name, object_name);
        monitor::internal_storage_request_latency_remove.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start)
                .count());
        monitor::internal_storage_op_count_remove_suc.Increment();
    } catch (std::exception& err) {
        monitor::internal_storage_op_count_remove_fail.Increment();
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
        auto start = std::chrono::system_clock::now();
        res = client_->PutObjectBuffer(bucket_name, object_name, buf, size);
        monitor::internal_storage_request_latency_put.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start)
                .count());
        monitor::internal_storage_op_count_put_suc.Increment();
        monitor::internal_storage_kv_size_put.Observe(size);
    } catch (std::exception& err) {
        monitor::internal_storage_op_count_put_fail.Increment();
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
        auto start = std::chrono::system_clock::now();
        res = client_->GetObjectBuffer(bucket_name, object_name, buf, size);
        monitor::internal_storage_request_latency_get.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start)
                .count());
        monitor::internal_storage_op_count_get_suc.Increment();
        monitor::internal_storage_kv_size_get.Observe(size);
    } catch (std::exception& err) {
        monitor::internal_storage_op_count_get_fail.Increment();
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
        auto start = std::chrono::system_clock::now();
        res = client_->ListObjects(bucket_name, prefix);
        monitor::internal_storage_request_latency_list.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start)
                .count());
        monitor::internal_storage_op_count_list_suc.Increment();
    } catch (std::exception& err) {
        monitor::internal_storage_op_count_list_fail.Increment();
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
