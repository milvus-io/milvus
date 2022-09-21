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

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "ChunkManager.h"
#include "Exception.h"
#include "config/ConfigChunkManager.h"

namespace milvus::storage {

/**
 * @brief This MinioChunkManager is responsible for read and write file in S3.
 */
class MinioChunkManager : public RemoteChunkManager {
 private:
    explicit MinioChunkManager(const std::string& endpoint,
                               const std::string& access_key,
                               const std::string& access_value,
                               const std::string& default_bucket_name,
                               bool serure = false,
                               bool use_iam = false);

    MinioChunkManager(const MinioChunkManager&);
    MinioChunkManager&
    operator=(const MinioChunkManager&);

 public:
    virtual ~MinioChunkManager();

    static MinioChunkManager&
    GetInstance() {
        // thread-safe enough after c++ 11
        static MinioChunkManager instance(ChunkMangerConfig::GetAddress(), ChunkMangerConfig::GetAccessKey(),
                                          ChunkMangerConfig::GetAccessValue(), ChunkMangerConfig::GetBucketName(),
                                          ChunkMangerConfig::GetUseSSL(), ChunkMangerConfig::GetUseIAM());
        return instance;
    }

    virtual bool
    Exist(const std::string& filepath);

    virtual uint64_t
    Size(const std::string& filepath);

    virtual uint64_t
    Read(const std::string& filepath, uint64_t offset, void* buf, uint64_t len) {
        throw NotImplementedException(GetName() + "Read with offset not implement");
    }

    virtual void
    Write(const std::string& filepath, uint64_t offset, void* buf, uint64_t len) {
        throw NotImplementedException(GetName() + "Write with offset not implement");
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
        return "MinioChunkManager";
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

 private:
    bool
    ObjectExists(const std::string& bucket_name, const std::string& object_name);
    int64_t
    GetObjectSize(const std::string& bucket_name, const std::string& object_name);
    bool
    DeleteObject(const std::string& bucket_name, const std::string& object_name);
    bool
    PutObjectBuffer(const std::string& bucket_name, const std::string& object_name, void* buf, uint64_t size);
    uint64_t
    GetObjectBuffer(const std::string& bucket_name, const std::string& object_name, void* buf, uint64_t size);
    std::vector<std::string>
    ListObjects(const char* bucket_name, const char* prefix = NULL);

 private:
    Aws::SDKOptions sdk_options_;
    std::shared_ptr<Aws::S3::S3Client> client_;
    std::string default_bucket_name_;
};

using MinioChunkManagerSPtr = std::shared_ptr<MinioChunkManager>;

}  // namespace milvus::storage
