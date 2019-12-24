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

#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fstream>
#include <iostream>

#include "storage/s3/S3ClientWrapper.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace milvus {
namespace storage {

Status
S3ClientWrapper::Create(const std::string& ip_address, const std::string& port, const std::string& access_key,
                        const std::string& secret_key) {
    Aws::InitAPI(options_);
    Aws::Client::ClientConfiguration cfg;

    cfg.endpointOverride = ip_address + ":" + port;
    cfg.scheme = Aws::Http::Scheme::HTTP;
    cfg.verifySSL = false;
    client_ = new Aws::S3::S3Client(Aws::Auth::AWSCredentials(access_key, secret_key), cfg,
                                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always, false);
    if (client_ == nullptr) {
        std::string str = "Cannot connect S3 server.";
        return milvus::Status(SERVER_UNEXPECTED_ERROR, str);
    } else {
        return Status::OK();
    }
}

Status
S3ClientWrapper::Close() {
    if (client_ != nullptr) {
        delete client_;
        client_ = nullptr;
    }
    Aws::ShutdownAPI(options_);
    return Status::OK();
}

Status
S3ClientWrapper::CreateBucket(std::string& bucket_name) {
    Aws::S3::Model::CreateBucketRequest request;
    request.SetBucket(bucket_name);

    auto ret = client_->CreateBucket(request);

    if (ret.IsSuccess()) {
        return Status::OK();
    } else {
        STORAGE_LOG_ERROR << "CreateBucket error: " << ret.GetError().GetExceptionName() << ": "
                          << ret.GetError().GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, ret.GetError().GetMessage());
    }
}

Status
S3ClientWrapper::DeleteBucket(std::string& bucket_name) {
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(bucket_name);

    auto ret = client_->DeleteBucket(request);

    if (ret.IsSuccess()) {
        return Status::OK();
    } else {
        STORAGE_LOG_ERROR << "DeleteBucket error: " << ret.GetError().GetExceptionName() << ": "
                          << ret.GetError().GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, ret.GetError().GetMessage());
    }
}

Status
S3ClientWrapper::UploadFile(std::string& bucket_name, std::string& object_key, std::string& path_key) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucket_name.c_str()).WithKey(object_key.c_str());

    auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", path_key.c_str(),
                                                    std::ios_base::in | std::ios_base::binary);
    request.SetBody(input_data);
    auto ret = client_->PutObject(request);
    if (ret.IsSuccess()) {
        return Status::OK();
    } else {
        STORAGE_LOG_ERROR << "PutObject error: " << ret.GetError().GetExceptionName() << ": "
                          << ret.GetError().GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, ret.GetError().GetMessage());
    }
}

Status
S3ClientWrapper::DownloadFile(std::string& bucket_name, std::string& object_key, std::string& path_key) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name.c_str()).WithKey(object_key.c_str());
    auto ret = client_->GetObject(request);
    if (ret.IsSuccess()) {
        Aws::OFStream local_file(path_key.c_str(), std::ios::out | std::ios::binary);
        local_file << ret.GetResult().GetBody().rdbuf();
        return Status::OK();
    } else {
        STORAGE_LOG_ERROR << "GetObject error: " << ret.GetError().GetExceptionName() << ": "
                          << ret.GetError().GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, ret.GetError().GetMessage());
    }
}

Status
S3ClientWrapper::DeleteFile(std::string& bucket_name, std::string& object_key) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(bucket_name).WithKey(object_key);

    auto ret = client_->DeleteObject(request);

    if (ret.IsSuccess()) {
        return Status::OK();
    } else {
        STORAGE_LOG_ERROR << "DeleteObject error: " << ret.GetError().GetExceptionName() << ": "
                          << ret.GetError().GetMessage();

        return Status(SERVER_UNEXPECTED_ERROR, ret.GetError().GetMessage());
    }
}

}  // namespace storage
}  // namespace milvus
