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

#pragma once

#include <iostream>
#include <string>
#include <vector>
#include "storage/ChunkManager.h"
#include "storage/Types.h"
#include "log/Log.h"

namespace milvus {
namespace storage {

template <typename... Args>
static std::string
AzureErrorMessage(const std::string& func,
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
ThrowAzureError(const std::string& func,
                const std::exception& err,
                const std::string& fmt_string,
                Args&&... args) {
    std::string error_message =
        AzureErrorMessage(func, err, fmt_string, args...);
    LOG_WARN(error_message);
    throw SegcoreError(S3Error, error_message);
}

/**
 * @brief This AzureChunkManager is responsible for read and write file in blob.
   */
class AzureChunkManager : public ChunkManager {
 public:
    explicit AzureChunkManager(const StorageConfig& storage_config);

    AzureChunkManager(const AzureChunkManager&);
    AzureChunkManager&
    operator=(const AzureChunkManager&);

 public:
    virtual ~AzureChunkManager();

    bool
    Exist(const std::string& filepath) override;

    uint64_t
    Size(const std::string& filepath) override;

    uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) override {
        ThrowInfo(NotImplemented, GetName() + "Read with offset not implement");
    }

    void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) override {
        ThrowInfo(NotImplemented,
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
        return "AzureChunkManager";
    }

    std::string
    GetRootPath() const override;

    std::string
    GetBucketName() const override;

    inline void
    SetBucketName(const std::string& bucket_name);

    bool
    BucketExists(const std::string& bucket_name);

    bool
    CreateBucket(const std::string& bucket_name);

    bool
    DeleteBucket(const std::string& bucket_name);

    std::vector<std::string>
    ListBuckets();

 public:
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
    ListObjects(const std::string& bucket_name, const std::string& prefix = "");

 private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

using AzureChunkManagerPtr = std::unique_ptr<AzureChunkManager>;

}  // namespace storage
}  // namespace milvus
