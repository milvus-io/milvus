// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <chrono>
#include <iostream>
#include <sstream>
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "monitor/prometheus_client.h"
#include "storage/GcpNativeChunkManager.h"

namespace milvus {
namespace storage {

GcpNativeChunkManager::GcpNativeChunkManager(
    const StorageConfig& storage_config)
    : default_bucket_name_(storage_config.bucket_name),
      path_prefix_(storage_config.root_path) {
    try {
        client_ = std::make_unique<gcpnative::GcpNativeClientManager>(
            storage_config.address,
            storage_config.gcp_credential_json,
            storage_config.useSSL,
            storage_config.gcp_native_without_auth,
            storage_config.requestTimeoutMs == 0
                ? DEFAULT_CHUNK_MANAGER_REQUEST_TIMEOUT_MS
                : storage_config.requestTimeoutMs);
    } catch (std::exception& err) {
        ThrowGcpNativeError("GcpNativeChunkManager",
                            err,
                            "gcp native chunk manager client creation failed, "
                            "error: {}, configuration: {}",
                            err.what(),
                            storage_config.ToString());
    }
}

GcpNativeChunkManager::~GcpNativeChunkManager() {
}

uint64_t
GcpNativeChunkManager::Size(const std::string& filepath) {
    return GetObjectSize(default_bucket_name_, filepath);
}

bool
GcpNativeChunkManager::Exist(const std::string& filepath) {
    return ObjectExists(default_bucket_name_, filepath);
}

void
GcpNativeChunkManager::Remove(const std::string& filepath) {
    DeleteObject(default_bucket_name_, filepath);
}

std::vector<std::string>
GcpNativeChunkManager::ListWithPrefix(const std::string& filepath) {
    return ListObjects(default_bucket_name_, filepath);
}

uint64_t
GcpNativeChunkManager::Read(const std::string& filepath,
                            void* buf,
                            uint64_t size) {
    return GetObjectBuffer(default_bucket_name_, filepath, buf, size);
}

void
GcpNativeChunkManager::Write(const std::string& filepath,
                             void* buf,
                             uint64_t size) {
    PutObjectBuffer(default_bucket_name_, filepath, buf, size);
}

bool
GcpNativeChunkManager::BucketExists(const std::string& bucket_name) {
    bool res;
    try {
        res = client_->BucketExists(bucket_name);
    } catch (std::exception& err) {
        ThrowGcpNativeError(
            "BucketExists", err, "params, bucket={}", bucket_name);
    }
    return res;
}

std::vector<std::string>
GcpNativeChunkManager::ListBuckets() {
    std::vector<std::string> res;
    try {
        res = client_->ListBuckets();
    } catch (std::exception& err) {
        ThrowGcpNativeError("ListBuckets", err, "params");
    }
    return res;
}

bool
GcpNativeChunkManager::CreateBucket(const std::string& bucket_name) {
    bool res;
    try {
        res = client_->CreateBucket(bucket_name);
    } catch (std::exception& err) {
        ThrowGcpNativeError(
            "CreateBucket", err, "params, bucket={}", bucket_name);
    }
    return res;
}

bool
GcpNativeChunkManager::DeleteBucket(const std::string& bucket_name) {
    bool res;
    try {
        res = client_->DeleteBucket(bucket_name);
    } catch (std::exception& err) {
        ThrowGcpNativeError(
            "DeleteBucket", err, "params, bucket={}", bucket_name);
    }
    return res;
}

bool
GcpNativeChunkManager::ObjectExists(const std::string& bucket_name,
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
        ThrowGcpNativeError("ObjectExists",
                            err,
                            "params, bucket={}, object={}",
                            bucket_name,
                            object_name);
    }
    return res;
}

uint64_t
GcpNativeChunkManager::GetObjectSize(const std::string& bucket_name,
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
        ThrowGcpNativeError("GetObjectSize",
                            err,
                            "params, bucket={}, object={}",
                            bucket_name,
                            object_name);
    }
    return res;
}

bool
GcpNativeChunkManager::DeleteObject(const std::string& bucket_name,
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
        ThrowGcpNativeError("DeleteObject",
                            err,
                            "params, bucket={}, object={}",
                            bucket_name,
                            object_name);
    }
    return res;
}

bool
GcpNativeChunkManager::PutObjectBuffer(const std::string& bucket_name,
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
        ThrowGcpNativeError("PutObjectBuffer",
                            err,
                            "params, bucket={}, object={}",
                            bucket_name,
                            object_name);
    }
    return res;
}

uint64_t
GcpNativeChunkManager::GetObjectBuffer(const std::string& bucket_name,
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
        ThrowGcpNativeError("GetObjectBuffer",
                            err,
                            "params, bucket={}, object={}",
                            bucket_name,
                            object_name);
    }
    return res;
}

std::vector<std::string>
GcpNativeChunkManager::ListObjects(const std::string& bucket_name,
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
        ThrowGcpNativeError("ListObjects",
                            err,
                            "params, bucket={}, prefix={}",
                            bucket_name,
                            prefix);
    }
    return res;
}

}  // namespace storage
}  // namespace milvus
