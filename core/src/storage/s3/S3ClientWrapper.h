// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <memory>
#include <string>
#include <vector>

#include "utils/Status.h"

namespace milvus {
namespace storage {

class S3ClientWrapper {
 public:
    static S3ClientWrapper&
    GetInstance() {
        static S3ClientWrapper wrapper;
        return wrapper;
    }

    Status
    StartService();
    void
    StopService();

    Status
    CreateBucket();
    Status
    DeleteBucket();
    Status
    PutObjectFile(const std::string& object_key, const std::string& file_path);
    Status
    PutObjectStr(const std::string& object_key, const std::string& content);
    Status
    GetObjectFile(const std::string& object_key, const std::string& file_path);
    Status
    GetObjectStr(const std::string& object_key, std::string& content);
    Status
    ListObjects(std::vector<std::string>& object_list, const std::string& marker = "");
    Status
    DeleteObject(const std::string& object_key);
    Status
    DeleteObjects(const std::string& marker);

 private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr_;
    Aws::SDKOptions options_;

    std::string s3_address_;
    std::string s3_port_;
    std::string s3_access_key_;
    std::string s3_secret_key_;
    std::string s3_bucket_;
};

}  // namespace storage
}  // namespace milvus
