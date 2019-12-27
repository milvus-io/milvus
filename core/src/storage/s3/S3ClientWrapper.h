// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <string>
#include "storage/IStorage.h"

namespace milvus {
namespace storage {

class S3ClientWrapper : public IStorage {
 public:
    S3ClientWrapper() = default;
    ~S3ClientWrapper() = default;

    Status
    Create(const std::string& ip_address, const std::string& port, const std::string& access_key,
           const std::string& secret_key) override;
    Status
    Close() override;

    Status
    CreateBucket(const std::string& bucket_name) override;
    Status
    DeleteBucket(const std::string& bucket_name) override;
    Status
    PutObjectFile(const std::string& bucket_name, const std::string& object_key, const std::string& path_key) override;
    Status
    PutObjectStr(const std::string& bucket_name, const std::string& object_key, const std::string& content) override;
    Status
    GetObjectFile(const std::string& bucket_name, const std::string& object_key, const std::string& path_key) override;
    Status
    DeleteObject(const std::string& bucket_name, const std::string& object_key) override;

 private:
    Aws::S3::S3Client* client_ = nullptr;
    Aws::SDKOptions options_;
};

}  // namespace storage
}  // namespace milvus
