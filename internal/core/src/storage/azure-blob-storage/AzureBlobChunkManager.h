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

#include <azure/storage/blobs.hpp>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <vector>
#include "azure/storage/common/storage_exception.hpp"

namespace azure {
/**
 * @brief This AzureBlobChunkManager is responsible for read and write file in blob.
   */
class AzureBlobChunkManager {
 public:
    explicit AzureBlobChunkManager(const std::string& access_key_id,
                                   const std::string& access_key_value,
                                   const std::string& address,
                                   bool useIAM = false);

    AzureBlobChunkManager(const AzureBlobChunkManager&);
    AzureBlobChunkManager&
    operator=(const AzureBlobChunkManager&);

 public:
    virtual ~AzureBlobChunkManager();

    bool
    BucketExists(const std::string& bucket_name);
    void
    CreateBucket(const std::string& bucket_name);
    void
    DeleteBucket(const std::string& bucket_name);
    std::vector<std::string>
    ListBuckets();
    bool
    ObjectExists(const std::string& bucket_name,
                 const std::string& object_name);
    int64_t
    GetObjectSize(const std::string& bucket_name,
                  const std::string& object_name);
    void
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
    ListObjects(const char* bucket_name, const char* prefix = nullptr);

 private:
    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> client_;
};

}  // namespace azure
