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
#include <vector>
#include "storage/IStorage.h"

namespace milvus {
namespace storage {

class S3ClientWrapper : public IStorage {
 public:
    S3ClientWrapper() = default;
    ~S3ClientWrapper() = default;

    static S3ClientWrapper&
    GetInstance() {
        static S3ClientWrapper wrapper;
        return wrapper;
    }

    Status
    StartService();
    Status
    StopService();

    Status
    CreateBucket() override;
    Status
    DeleteBucket() override;
    Status
    PutObjectFile(const std::string& object_key, const std::string& file_path) override;
    Status
    PutObjectStr(const std::string& object_key, const std::string& content) override;
    Status
    GetObjectFile(const std::string& object_key, const std::string& file_path) override;
    Status
    GetObjectStr(const std::string& object_key, std::string& content) override;
    Status
    ListObjects(std::vector<std::string>& object_list, const std::string& marker = "") override;
    Status
    DeleteObject(const std::string& object_key) override;
    Status
    DeleteObjects(const std::string& marker) override;

 private:
    Aws::S3::S3Client* client_ptr_ = nullptr;
    Aws::SDKOptions options_;

    std::string minio_address_;
    std::string minio_port_;
    std::string minio_access_key_;
    std::string minio_secret_key_;
    std::string minio_bucket_;
};

}  // namespace storage
}  // namespace milvus
