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


//#pragma once
//
//#include "storage/IStorage.h"
//
//
//#include <aws/s3/S3Client.h>
//#include <aws/core/Aws.h>
//#include <aws/core/auth/AWSCredentialsProvider.h>
//
//
//using namespace Aws::S3;
//using namespace Aws::S3::Model;
//
//namespace zilliz {
//namespace milvus {
//namespace engine {
//namespace storage {
//
//class S3ClientWrapper : public IStorage {
// public:
//
//    S3ClientWrapper() = default;
//    ~S3ClientWrapper() = default;
//
//    Status Create(const std::string &ip_address,
//                const std::string &port,
//                const std::string &access_key,
//                const std::string &secret_key) override;
//    Status Close() override;
//
//    Status CreateBucket(std::string& bucket_name) override;
//    Status DeleteBucket(std::string& bucket_name) override;
//    Status UploadFile(std::string &BucketName, std::string &objectKey, std::string &pathkey) override;
//    Status DownloadFile(std::string &BucketName, std::string &objectKey, std::string &pathkey) override;
//    Status DeleteFile(std::string &bucket_name, std::string &object_key) override;
//
// private:
//    S3Client *client_ = nullptr;
//    Aws::SDKOptions options_;
//};
//
//}
//}
//}
//}