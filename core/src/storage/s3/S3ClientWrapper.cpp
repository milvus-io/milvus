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

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <utility>

#include "server/Config.h"
#include "storage/s3/S3ClientWrapper.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace milvus {
namespace storage {

Status
S3ClientWrapper::StartService() {
    server::Config& config = server::Config::GetInstance();
    bool minio_enable = false;
    CONFIG_CHECK(config.GetStorageConfigMinioEnable(minio_enable));
    if (!minio_enable) {
        STORAGE_LOG_INFO << "MinIO not enabled!";
        return Status::OK();
    }

    CONFIG_CHECK(config.GetStorageConfigMinioAddress(minio_address_));
    CONFIG_CHECK(config.GetStorageConfigMinioPort(minio_port_));
    CONFIG_CHECK(config.GetStorageConfigMinioAccessKey(minio_access_key_));
    CONFIG_CHECK(config.GetStorageConfigMinioSecretKey(minio_secret_key_));
    CONFIG_CHECK(config.GetStorageConfigMinioBucket(minio_bucket_));

    Aws::InitAPI(options_);
    Aws::Client::ClientConfiguration cfg;

    cfg.endpointOverride = minio_address_ + ":" + minio_port_;
    cfg.scheme = Aws::Http::Scheme::HTTP;
    cfg.verifySSL = false;
    client_ptr_ = new Aws::S3::S3Client(Aws::Auth::AWSCredentials(minio_access_key_, minio_secret_key_), cfg,
                                        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always, false);
    if (client_ptr_ == nullptr) {
        std::string str = "Cannot connect S3 server.";
        return milvus::Status(SERVER_UNEXPECTED_ERROR, str);
    }

    return CreateBucket();
}

Status
S3ClientWrapper::StopService() {
    if (client_ptr_ != nullptr) {
        delete client_ptr_;
        client_ptr_ = nullptr;
    }
    Aws::ShutdownAPI(options_);
    return Status::OK();
}

Status
S3ClientWrapper::CreateBucket() {
    Aws::S3::Model::CreateBucketRequest request;
    request.WithBucket(minio_bucket_);

    auto outcome = client_ptr_->CreateBucket(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        if (err.GetErrorType() != Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU) {
            STORAGE_LOG_ERROR << "ERROR: CreateBucket: " << err.GetExceptionName() << ": " << err.GetMessage();
            return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
        }
    }

    STORAGE_LOG_DEBUG << "CreateBucket '" << minio_bucket_ << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::DeleteBucket() {
    Aws::S3::Model::DeleteBucketRequest request;
    request.WithBucket(minio_bucket_);

    auto outcome = client_ptr_->DeleteBucket(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        STORAGE_LOG_ERROR << "ERROR: DeleteBucket: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    STORAGE_LOG_DEBUG << "DeleteBucket '" << minio_bucket_ << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::PutObjectFile(const std::string& object_name, const std::string& file_path) {
    struct stat buffer;
    if (stat(file_path.c_str(), &buffer) != 0) {
        std::string str = "File '" + file_path + "' not exist!";
        STORAGE_LOG_ERROR << "ERROR: " << str;
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }

    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(minio_bucket_).WithKey(object_name);

    auto input_data =
        Aws::MakeShared<Aws::FStream>("PutObjectFile", file_path.c_str(), std::ios_base::in | std::ios_base::binary);
    request.SetBody(input_data);

    auto outcome = client_ptr_->PutObject(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        STORAGE_LOG_ERROR << "ERROR: PutObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    STORAGE_LOG_DEBUG << "PutObjectFile '" << file_path << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::PutObjectStr(const std::string& object_name, const std::string& content) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(minio_bucket_).WithKey(object_name);

    const std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::StringStream>("");
    input_data->write(content.data(), content.length());
    request.SetBody(input_data);

    auto outcome = client_ptr_->PutObject(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        STORAGE_LOG_ERROR << "ERROR: PutObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    STORAGE_LOG_DEBUG << "PutObjectStr successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::GetObjectFile(const std::string& object_name, const std::string& file_path) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(minio_bucket_).WithKey(object_name);

    auto outcome = client_ptr_->GetObject(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        STORAGE_LOG_ERROR << "ERROR: GetObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    auto& retrieved_file = outcome.GetResultWithOwnership().GetBody();
    std::ofstream output_file(file_path, std::ios::binary);
    output_file << retrieved_file.rdbuf();
    output_file.close();

    STORAGE_LOG_DEBUG << "GetObjectFile '" << file_path << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::GetObjectStr(const std::string& object_name, std::string& content) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(minio_bucket_).WithKey(object_name);

    auto outcome = client_ptr_->GetObject(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        STORAGE_LOG_ERROR << "ERROR: GetObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    auto& retrieved_file = outcome.GetResultWithOwnership().GetBody();
    std::stringstream ss;
    ss << retrieved_file.rdbuf();
    content = std::move(ss.str());

    STORAGE_LOG_DEBUG << "GetObjectStr successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::ListObjects(std::vector<std::string>& object_list, const std::string& marker) {
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(minio_bucket_);

    if (!marker.empty()) {
        request.WithMarker(marker);
    }

    auto outcome = client_ptr_->ListObjects(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        STORAGE_LOG_ERROR << "ERROR: ListObjects: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    Aws::Vector<Aws::S3::Model::Object> result_list = outcome.GetResult().GetContents();

    for (auto const& s3_object : result_list) {
        object_list.push_back(s3_object.GetKey());
    }

    if (marker.empty()) {
        STORAGE_LOG_DEBUG << "ListObjects '" << minio_bucket_ << "' successfully!";
    } else {
        STORAGE_LOG_DEBUG << "ListObjects '" << minio_bucket_ << ":" << marker << "' successfully!";
    }
    return Status::OK();
}

Status
S3ClientWrapper::DeleteObject(const std::string& object_name) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(minio_bucket_).WithKey(object_name);

    auto outcome = client_ptr_->DeleteObject(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        STORAGE_LOG_ERROR << "ERROR: DeleteObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    STORAGE_LOG_DEBUG << "DeleteObject '" << object_name << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::DeleteObjects(const std::string& marker) {
    std::vector<std::string> object_list;

    Status stat = ListObjects(object_list, marker);
    if (!stat.ok()) {
        return stat;
    }

    for (std::string& obj_name : object_list) {
        stat = DeleteObject(obj_name);
        if (!stat.ok()) {
            return stat;
        }
    }

    return Status::OK();
}

}  // namespace storage
}  // namespace milvus
