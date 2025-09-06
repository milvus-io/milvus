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
#include "monitor/Monitor.h"
#include "storage/azure/AzureChunkManager.h"
#include "storage/azure-blob-storage/AzureBlobChunkManager.h"

namespace milvus {
namespace storage {

std::atomic<size_t> init_count_(0);
std::mutex client_mutex_;

std::string const ErrorText = "ERROR";
std::string const WarningText = "WARN ";
std::string const InformationalText = "INFO ";
std::string const VerboseText = "DEBUG";
std::string const UnknownText = "?????";

void
AzureLogger(Azure::Core::Diagnostics::Logger::Level level,
            std::string const& msg);

class AzureChunkManager::Impl {
 public:
    explicit Impl(const StorageConfig& storage_config)
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
            ThrowAzureError("PreCheck",
                            err,
                            "precheck chunk manager client failed, error:{}, "
                            "configuration:{}",
                            err.what(),
                            storage_config.ToString());
        }
    }
    ~Impl() = default;

    uint64_t
    Size(const std::string& filepath) {
        return GetObjectSize(default_bucket_name_, filepath);
    }

    bool
    Exist(const std::string& filepath) {
        return ObjectExists(default_bucket_name_, filepath);
    }

    void
    Remove(const std::string& filepath) {
        DeleteObject(default_bucket_name_, filepath);
    }

    std::vector<std::string>
    ListWithPrefix(const std::string& filepath) {
        return ListObjects(default_bucket_name_, filepath);
    }

    uint64_t
    Read(const std::string& filepath, void* buf, uint64_t size) {
        return GetObjectBuffer(default_bucket_name_, filepath, buf, size);
    }

    void
    Write(const std::string& filepath, void* buf, uint64_t size) {
        PutObjectBuffer(default_bucket_name_, filepath, buf, size);
    }

    bool
    BucketExists(const std::string& bucket_name) {
        bool res;
        try {
            res = client_->BucketExists(bucket_name);
        } catch (std::exception& err) {
            ThrowAzureError(
                "BucketExists", err, "params, bucket={}", bucket_name);
        }
        return res;
    }

    std::vector<std::string>
    ListBuckets() {
        std::vector<std::string> res;
        try {
            res = client_->ListBuckets();
        } catch (std::exception& err) {
            ThrowAzureError("ListBuckets", err, "params");
        }
        return res;
    }

    bool
    CreateBucket(const std::string& bucket_name) {
        bool res;
        try {
            res = client_->CreateBucket(bucket_name);
        } catch (std::exception& err) {
            ThrowAzureError(
                "CreateBucket", err, "params, bucket={}", bucket_name);
        }
        return res;
    }

    bool
    DeleteBucket(const std::string& bucket_name) {
        bool res;
        try {
            res = client_->DeleteBucket(bucket_name);
        } catch (std::exception& err) {
            ThrowAzureError(
                "DeleteBucket", err, "params, bucket={}", bucket_name);
        }
        return res;
    }

    bool
    ObjectExists(const std::string& bucket_name,
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
    GetObjectSize(const std::string& bucket_name,
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
    DeleteObject(const std::string& bucket_name,
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
    PutObjectBuffer(const std::string& bucket_name,
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
    GetObjectBuffer(const std::string& bucket_name,
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
    ListObjects(const std::string& bucket_name, const std::string& prefix) {
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

    std::string
    GetRootPath() const {
        return path_prefix_;
    }

    std::string
    GetBucketName() const {
        return default_bucket_name_;
    }

    void
    SetBucketName(const std::string& bucket_name) {
        default_bucket_name_ = bucket_name;
    }

 private:
    std::string default_bucket_name_;
    std::string path_prefix_;
    std::shared_ptr<azure::AzureBlobChunkManager> client_;
};

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

//------------------------------------------------------------------------------
// AzureChunkManager methods
//------------------------------------------------------------------------------

AzureChunkManager::AzureChunkManager(const StorageConfig& storage_config)
    : pImpl(std::make_unique<Impl>(storage_config)) {
}

AzureChunkManager::~AzureChunkManager() = default;

AzureChunkManager::AzureChunkManager(const AzureChunkManager& other)
    : pImpl(std::make_unique<Impl>(*other.pImpl)) {
}

AzureChunkManager&
AzureChunkManager::operator=(const AzureChunkManager& other) {
    if (this != &other) {
        pImpl = std::make_unique<Impl>(*other.pImpl);
    }
    return *this;
}

bool
AzureChunkManager::Exist(const std::string& filepath) {
    return pImpl->Exist(filepath);
}

uint64_t
AzureChunkManager::Size(const std::string& filepath) {
    return pImpl->Size(filepath);
}

uint64_t
AzureChunkManager::Read(const std::string& filepath, void* buf, uint64_t len) {
    return pImpl->Read(filepath, buf, len);
}

void
AzureChunkManager::Write(const std::string& filepath, void* buf, uint64_t len) {
    pImpl->Write(filepath, buf, len);
}

std::vector<std::string>
AzureChunkManager::ListWithPrefix(const std::string& filepath) {
    return pImpl->ListWithPrefix(filepath);
}

void
AzureChunkManager::Remove(const std::string& filepath) {
    pImpl->Remove(filepath);
}

bool
AzureChunkManager::BucketExists(const std::string& bucket_name) {
    return pImpl->BucketExists(bucket_name);
}

bool
AzureChunkManager::CreateBucket(const std::string& bucket_name) {
    return pImpl->CreateBucket(bucket_name);
}

bool
AzureChunkManager::DeleteBucket(const std::string& bucket_name) {
    return pImpl->DeleteBucket(bucket_name);
}

std::vector<std::string>
AzureChunkManager::ListBuckets() {
    return pImpl->ListBuckets();
}

bool
AzureChunkManager::ObjectExists(const std::string& bucket_name,
                                const std::string& object_name) {
    return pImpl->ObjectExists(bucket_name, object_name);
}

uint64_t
AzureChunkManager::GetObjectSize(const std::string& bucket_name,
                                 const std::string& object_name) {
    return pImpl->GetObjectSize(bucket_name, object_name);
}

bool
AzureChunkManager::DeleteObject(const std::string& bucket_name,
                                const std::string& object_name) {
    return pImpl->DeleteObject(bucket_name, object_name);
}

bool
AzureChunkManager::PutObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    return pImpl->PutObjectBuffer(bucket_name, object_name, buf, size);
}

uint64_t
AzureChunkManager::GetObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    return pImpl->GetObjectBuffer(bucket_name, object_name, buf, size);
}

std::vector<std::string>
AzureChunkManager::ListObjects(const std::string& bucket_name,
                               const std::string& prefix) {
    return pImpl->ListObjects(bucket_name, prefix);
}

std::string
AzureChunkManager::GetRootPath() const {
    return pImpl->GetRootPath();
}

std::string
AzureChunkManager::GetBucketName() const {
    return pImpl->GetBucketName();
}

void
AzureChunkManager::SetBucketName(const std::string& bucket_name) {
    pImpl->SetBucketName(bucket_name);
}

}  // namespace storage
}  // namespace milvus
