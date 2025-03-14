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

#pragma once

#include <iostream>
#include <stdlib.h>
#include <string>
#include <vector>
#include "storage/gcp-native-storage/GcpNativeClientManager.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"

namespace milvus {
namespace storage {

template <typename... Args>
static std::string
GcpErrorMessage(const std::string& func,
                const std::exception& err,
                const std::string& fmt_string,
                Args&&... args) {
    std::ostringstream oss;
    const auto& message = fmt::format(fmt_string, std::forward<Args>(args)...);
    oss << "Error in " << func << "[exception:" << err.what()
        << ", params:" << message << "]";
    return oss.str();
}

template <typename... Args>
static SegcoreError
ThrowGcpNativeError(const std::string& func,
                    const std::exception& err,
                    const std::string& fmt_string,
                    Args&&... args) {
    std::string error_message = GcpErrorMessage(func, err, fmtString, args...);
    LOG_WARN(error_message);
    throw SegcoreError(GcpNativeError, error_message);
}

/**
 * @brief This GcpNativeChunkManager is used to interact with
 * Google Cloud Storage (GCS) services using GcpNativeClientManager class.
   */
class GcpNativeChunkManager : public ChunkManager {
 public:
    explicit GcpNativeChunkManager(const StorageConfig& storage_config);

    GcpNativeChunkManager(const GcpNativeChunkManager&);
    GcpNativeChunkManager&
    operator=(const GcpNativeChunkManager&);

    virtual ~GcpNativeChunkManager();

 public:
    bool
    Exist(const std::string& filepath) override;

    uint64_t
    Size(const std::string& filepath) override;

    uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) override {
        PanicInfo(NotImplemented, GetName() + "Read with offset not implement");
    }

    void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) override {
        PanicInfo(NotImplemented,
                  GetName() + "Write with offset not implement");
    }

    uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len) override;

    void
    Write(const std::string& filepath, void* buf, uint64_t len) override;

    std::vector<std::string>
    ListWithPrefix(const std::string& filepath) override;

    void
    Remove(const std::string& filepath) override;

    std::string
    GetName() const override {
        return "GcpNativeChunkManager";
    }

    std::string
    GetRootPath() const override {
        return path_prefix_;
    }

 public:
    inline std::string
    GetBucketName() {
        return default_bucket_name_;
    }

    inline void
    SetBucketName(const std::string& bucket_name) {
        default_bucket_name_ = bucket_name;
    }

    bool
    BucketExists(const std::string& bucket_name);

    bool
    CreateBucket(const std::string& bucket_name);

    bool
    DeleteBucket(const std::string& bucket_name);

    std::vector<std::string>
    ListBuckets();

    bool
    ObjectExists(const std::string& bucket_name,
                 const std::string& object_name);
    uint64_t
    GetObjectSize(const std::string& bucket_name,
                  const std::string& object_name);
    bool
    DeleteObject(const std::string& bucket_name,
                 const std::string& object_name);
    bool
    PutObjectBuffer(const std::string& bucket_name,
                    const std::string& object_name,
                    void* buf,
                    uint64_t size);
    uint64_t
    GetObjectBuffer(const std::string& bucket_name,
                    const std::string& object_name,
                    void* buf,
                    uint64_t size);
    std::vector<std::string>
    ListObjects(const std::string& bucket_name,
                const std::string& prefix = nullptr);

 private:
    std::unique_ptr<gcpnative::GcpNativeClientManager> client_;
    std::string default_bucket_name_;
    std::string path_prefix_;
};

}  // namespace storage
}  // namespace milvus
