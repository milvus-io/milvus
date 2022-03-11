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

#include "storage/s3/S3ClientWrapper.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fiu-local.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <utility>

#include "config/Config.h"
#include "storage/s3/S3ClientMock.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace milvus {
namespace storage {

Status
S3ClientWrapper::StartService() {
    server::Config& config = server::Config::GetInstance();
    bool s3_enable = false;
    auto s3_use_https = false;
    config.GetStorageConfigS3Enable(s3_enable);
    config.GetStorageConfigS3UseHttps(s3_use_https);
    fiu_do_on("S3ClientWrapper.StartService.s3_disable", s3_enable = false);
    if (!s3_enable) {
        LOG_STORAGE_INFO_ << "S3 not enabled!";
        return Status::OK();
    }

    config.GetStorageConfigS3Address(s3_address_);
    config.GetStorageConfigS3Port(s3_port_);
    config.GetStorageConfigS3AccessKey(s3_access_key_);
    config.GetStorageConfigS3SecretKey(s3_secret_key_);
    config.GetStorageConfigS3Bucket(s3_bucket_);
    config.GetStorageConfigS3Region(s3_region_);

    Aws::InitAPI(options_);

    Aws::Client::ClientConfiguration cfg;
    cfg.endpointOverride = s3_address_ + ":" + s3_port_;
    if (s3_use_https) {
        cfg.scheme = Aws::Http::Scheme::HTTPS;
    } else {
        cfg.scheme = Aws::Http::Scheme::HTTP;
    }
    cfg.verifySSL = false;
    if (!s3_region_.empty()) {
        cfg.region = s3_region_.c_str();
    }
    client_ptr_ =
        std::make_shared<Aws::S3::S3Client>(Aws::Auth::AWSCredentials(s3_access_key_, s3_secret_key_), cfg,
                                            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always, false);

    bool mock_enable = false;
    fiu_do_on("S3ClientWrapper.StartService.mock_enable", mock_enable = true);
    if (mock_enable) {
        client_ptr_ = std::make_shared<S3ClientMock>();
    }

    std::cout << "S3 service connection check ...... " << std::flush;
    Status stat = CreateBucket();
    std::cout << (stat.ok() ? "OK" : "FAIL") << std::endl;
    return stat;
}

void
S3ClientWrapper::StopService() {
    client_ptr_ = nullptr;
    Aws::ShutdownAPI(options_);
}

Status
S3ClientWrapper::CreateBucket() {
    Aws::S3::Model::CreateBucketRequest request;
    auto constraint = Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(s3_region_);
    Aws::S3::Model::CreateBucketConfiguration createBucketConfig;
    createBucketConfig.WithLocationConstraint(constraint);

    request.WithBucket(s3_bucket_);
    if (!s3_region_.empty()) {
        request.WithCreateBucketConfiguration(createBucketConfig);
    }

    auto outcome = client_ptr_->CreateBucket(request);

    fiu_do_on("S3ClientWrapper.CreateBucket.outcome.fail", outcome = Aws::S3::Model::CreateBucketOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        if (err.GetErrorType() != Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU) {
            LOG_STORAGE_ERROR_ << "ERROR: CreateBucket: " << err.GetExceptionName() << ": " << err.GetMessage();
            return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
        }
    }

    LOG_STORAGE_DEBUG_ << "CreateBucket '" << s3_bucket_ << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::DeleteBucket() {
    Aws::S3::Model::DeleteBucketRequest request;
    request.WithBucket(s3_bucket_);

    auto outcome = client_ptr_->DeleteBucket(request);

    fiu_do_on("S3ClientWrapper.DeleteBucket.outcome.fail", outcome = Aws::S3::Model::DeleteBucketOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        LOG_STORAGE_ERROR_ << "ERROR: DeleteBucket: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    LOG_STORAGE_DEBUG_ << "DeleteBucket '" << s3_bucket_ << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::PutObjectFile(const std::string& object_name, const std::string& file_path) {
    struct stat buffer;
    if (stat(file_path.c_str(), &buffer) != 0) {
        std::string str = "File '" + file_path + "' not exist!";
        LOG_STORAGE_ERROR_ << "ERROR: " << str;
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }

    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(s3_bucket_).WithKey(object_name);

    auto input_data =
        Aws::MakeShared<Aws::FStream>("PutObjectFile", file_path.c_str(), std::ios_base::in | std::ios_base::binary);
    request.SetBody(input_data);

    auto outcome = client_ptr_->PutObject(request);

    fiu_do_on("S3ClientWrapper.PutObjectFile.outcome.fail", outcome = Aws::S3::Model::PutObjectOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        LOG_STORAGE_ERROR_ << "ERROR: PutObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    LOG_STORAGE_DEBUG_ << "PutObjectFile '" << file_path << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::PutObjectStr(const std::string& object_name, const std::string& content) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(s3_bucket_).WithKey(object_name);

    const std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::StringStream>("");
    input_data->write(content.data(), content.length());
    request.SetBody(input_data);

    auto outcome = client_ptr_->PutObject(request);

    fiu_do_on("S3ClientWrapper.PutObjectStr.outcome.fail", outcome = Aws::S3::Model::PutObjectOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        LOG_STORAGE_ERROR_ << "ERROR: PutObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    LOG_STORAGE_DEBUG_ << "PutObjectStr successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::GetObjectFile(const std::string& object_name, const std::string& file_path) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(s3_bucket_).WithKey(object_name);

    auto outcome = client_ptr_->GetObject(request);

    fiu_do_on("S3ClientWrapper.GetObjectFile.outcome.fail", outcome = Aws::S3::Model::GetObjectOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        LOG_STORAGE_ERROR_ << "ERROR: GetObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    auto& retrieved_file = outcome.GetResultWithOwnership().GetBody();
    std::ofstream output_file(file_path, std::ios::binary);
    output_file << retrieved_file.rdbuf();
    output_file.close();

    LOG_STORAGE_DEBUG_ << "GetObjectFile '" << file_path << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::GetObjectStr(const std::string& object_name, std::string& content) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(s3_bucket_).WithKey(object_name);

    auto outcome = client_ptr_->GetObject(request);

    fiu_do_on("S3ClientWrapper.GetObjectStr.outcome.fail", outcome = Aws::S3::Model::GetObjectOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        LOG_STORAGE_ERROR_ << "ERROR: GetObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    auto& retrieved_file = outcome.GetResultWithOwnership().GetBody();
    std::stringstream ss;
    ss << retrieved_file.rdbuf();
    content = std::move(ss.str());

    LOG_STORAGE_DEBUG_ << "GetObjectStr successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::ListObjects(std::vector<std::string>& object_list, const std::string& prefix) {
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(s3_bucket_);

    if (!prefix.empty()) {
        const char* prefix_str = prefix.c_str();
        if (*prefix_str == '/') {
            prefix_str++;
        }
        request.WithPrefix(prefix_str);
    }

    auto outcome = client_ptr_->ListObjects(request);

    fiu_do_on("S3ClientWrapper.ListObjects.outcome.fail", outcome = Aws::S3::Model::ListObjectsOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        LOG_STORAGE_ERROR_ << "ERROR: ListObjects: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    Aws::Vector<Aws::S3::Model::Object> result_list = outcome.GetResult().GetContents();

    for (auto const& s3_object : result_list) {
        object_list.push_back(s3_object.GetKey());
    }

    if (prefix.empty()) {
        LOG_STORAGE_DEBUG_ << "ListObjects '" << s3_bucket_ << "' successfully!";
    } else {
        LOG_STORAGE_DEBUG_ << "ListObjects '" << s3_bucket_ << ":" << prefix << "' successfully!";
    }
    for (const auto& path : object_list) {
        LOG_STORAGE_DEBUG_ << "  object: '" << path;
    }
    return Status::OK();
}

Status
S3ClientWrapper::DeleteObject(const std::string& object_name) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(s3_bucket_).WithKey(object_name);

    auto outcome = client_ptr_->DeleteObject(request);

    fiu_do_on("S3ClientWrapper.DeleteObject.outcome.fail", outcome = Aws::S3::Model::DeleteObjectOutcome());
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        LOG_STORAGE_ERROR_ << "ERROR: DeleteObject: " << err.GetExceptionName() << ": " << err.GetMessage();
        return Status(SERVER_UNEXPECTED_ERROR, err.GetMessage());
    }

    LOG_STORAGE_DEBUG_ << "DeleteObject '" << object_name << "' successfully!";
    return Status::OK();
}

Status
S3ClientWrapper::DeleteObjects(const std::string& prefix) {
    std::vector<std::string> object_list;

    Status stat = ListObjects(object_list, prefix);
    if (!stat.ok()) {
        return stat;
    }

    LOG_STORAGE_DEBUG_ << "DeleteObjects '" << prefix << "' started.";
    for (std::string& obj_name : object_list) {
        stat = DeleteObject(obj_name);
        if (!stat.ok()) {
            return stat;
        }
    }
    LOG_STORAGE_DEBUG_ << "DeleteObjects '" << prefix << "' end.";

    return Status::OK();
}

}  // namespace storage
}  // namespace milvus
