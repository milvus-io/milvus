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
// export CPLUS_INCLUDE_PATH=/opt/homebrew/Cellar/boost/1.81.0_1/include/

#pragma once

#include <iostream>
#include <stdlib.h>
#include <string>
#include <vector>
#include "storage/azure-blob-storage/AzureBlobChunkManager.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"

namespace milvus {
namespace storage {

template <typename... Args>

static SegcoreError
ThrowAzureError(const std::string& func,
                const std::exception& err,
                const std::string& fmtString,
                Args&&... args) {
    std::ostringstream oss;
    const auto& message = fmt::format(fmtString, std::forward<Args>(args)...);
    oss << "Error in " << func << "[exception:" << err.what()
        << ", params:" << message << "]";
    throw SegcoreError(S3Error, oss.str());
}

void
AzureLogger(Azure::Core::Diagnostics::Logger::Level level,
            std::string const& msg);

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

    virtual bool
    Exist(const std::string& filepath);

    virtual uint64_t
    Size(const std::string& filepath);

    virtual uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) {
        throw SegcoreError(NotImplemented,
                           GetName() + "Read with offset not implement");
    }

    virtual void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) {
        throw SegcoreError(NotImplemented,
                           GetName() + "Write with offset not implement");
    }

    virtual uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len);

    virtual void
    Write(const std::string& filepath, void* buf, uint64_t len);

    virtual std::vector<std::string>
    ListWithPrefix(const std::string& filepath);

    virtual void
    Remove(const std::string& filepath);

    virtual std::string
    GetName() const {
        return "AzureChunkManager";
    }

    virtual std::string
    GetRootPath() const {
        return path_prefix_;
    }

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
    ListObjects(const std::string& bucket_name,
                const std::string& prefix = nullptr);

 private:
    static std::atomic<size_t> init_count_;
    static std::mutex client_mutex_;
    std::shared_ptr<azure::AzureBlobChunkManager> client_;
    std::string default_bucket_name_;
    std::string path_prefix_;
};

using AzureChunkManagerPtr = std::unique_ptr<AzureChunkManager>;

}  // namespace storage
}  // namespace milvus
