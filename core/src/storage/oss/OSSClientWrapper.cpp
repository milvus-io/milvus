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

#include "storage/oss/OSSClientWrapper.h"

#include <alibabacloud/oss/OssClient.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <utility>

#include "config/Config.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace milvus {
namespace storage {

Status
OSSClientWrapper::StartService() {
    server::Config& config = server::Config::GetInstance();
    AlibabaCloud::OSS::InitializeSdk();
    AlibabaCloud::OSS::ClientConfiguration conf;

    config.GetStorageConfigOSSEndpoint(oss_endpoint_);
    config.GetStorageConfigOSSAccessKey(oss_access_key_);
    config.GetStorageConfigOSSSecretKey(oss_secret_key_);
    config.GetStorageConfigOSSBucket(oss_bucket_);

    client_ptr_ = std::make_shared<AlibabaCloud::OSS::OssClient>(oss_endpoint_, oss_access_key_, oss_secret_key_, conf);

    auto status = CreateBucket();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

void
OSSClientWrapper::StopService() {
    client_ptr_ = nullptr;
    AlibabaCloud::OSS::ShutdownSdk();
}

Status
OSSClientWrapper::CreateBucket() {
    AlibabaCloud::OSS::CreateBucketRequest request(oss_bucket_, AlibabaCloud::OSS::StorageClass::IA,
                                                   AlibabaCloud::OSS::CannedAccessControlList::Private);
    const auto outcome = client_ptr_->CreateBucket(request);
    if (!outcome.isSuccess()) {
        const auto& err = outcome.error();
        if (err.Code() != "BucketAlreadyExists") {
            LOG_STORAGE_ERROR_ << "ERROR: CreateBucket: " << err.Code() << ": " << err.Message();
            return Status(SERVER_UNEXPECTED_ERROR, err.Message());
        }
    }

    LOG_STORAGE_DEBUG_ << "CreateBucket '" << oss_bucket_ << "' successfully!";
    return Status::OK();
}

Status
OSSClientWrapper::DeleteBucket() {
    const auto outcome = client_ptr_->DeleteBucket(oss_bucket_);
    if (!outcome.isSuccess()) {
        const auto& err = outcome.error();
        LOG_STORAGE_WARNING_ << "ERROR: DeleteBucket: " << err.Code() << ": " << err.Message();
        return Status(SERVER_UNEXPECTED_ERROR, err.Message());
    }

    LOG_STORAGE_DEBUG_ << "DeleteBucket '" << oss_bucket_ << "' successfully!";
    return Status::OK();
}

Status
OSSClientWrapper::PutObjectFile(const std::string& object_name, const std::string& file_path) {
    struct stat buffer;
    if (stat(file_path.c_str(), &buffer) != 0) {
        std::string str = "File '" + file_path + "' not exist!";
        LOG_STORAGE_WARNING_ << "ERROR: " << str;
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }

    std::shared_ptr<std::iostream> content = std::make_shared<std::fstream>(file_path, std::ios::in | std::ios::binary);
    AlibabaCloud::OSS::PutObjectRequest request(oss_bucket_, normalize_object_name(object_name), content);
    const auto outcome = client_ptr_->PutObject(request);
    if (!outcome.isSuccess()) {
        const auto& err = outcome.error();
        LOG_STORAGE_WARNING_ << "ERROR: PutObject: " << object_name << "," << err.Code() << ": " << err.Message();
        return Status(SERVER_UNEXPECTED_ERROR, err.Message());
    }

    LOG_STORAGE_DEBUG_ << "PutObjectFile '" << object_name << "' successfully!";
    return Status::OK();
}

Status
OSSClientWrapper::PutObjectStr(const std::string& object_name, const std::string& content) {
    std::shared_ptr<std::iostream> content_stream = std::make_shared<std::stringstream>();
    *content_stream << content;
    AlibabaCloud::OSS::PutObjectRequest request(oss_bucket_, normalize_object_name(object_name), content_stream);
    const auto outcome = client_ptr_->PutObject(request);
    if (!outcome.isSuccess()) {
        const auto& err = outcome.error();
        LOG_STORAGE_WARNING_ << "ERROR: PutObjectStr: " << err.Code() << ": " << err.Message();
        return Status(SERVER_UNEXPECTED_ERROR, err.Message());
    }

    LOG_STORAGE_DEBUG_ << "PutObjectStr '" << object_name << "' successfully!";
    return Status::OK();
}

Status
OSSClientWrapper::GetObjectFile(const std::string& object_name, const std::string& file_path) {
    AlibabaCloud::OSS::GetObjectRequest request(oss_bucket_, normalize_object_name(object_name));
    request.setResponseStreamFactory([=]() {
        return std::make_shared<std::fstream>(
            file_path, std::ios_base::out | std::ios_base::in | std::ios_base::trunc | std::ios_base::binary);
    });

    const auto outcome = client_ptr_->GetObject(request);
    if (!outcome.isSuccess()) {
        const auto& err = outcome.error();
        LOG_STORAGE_WARNING_ << "ERROR: GetObjectFile: " << object_name << ", " << err.Code() << ": " << err.Message();
        return Status(SERVER_UNEXPECTED_ERROR, err.Message());
    }

    LOG_STORAGE_DEBUG_ << "GetObjectFile '" << object_name << "' successfully!";
    return Status::OK();
}

Status
OSSClientWrapper::GetObjectStr(const std::string& object_name, std::string& content) {
    AlibabaCloud::OSS::GetObjectRequest request(oss_bucket_, normalize_object_name(object_name));
    const auto outcome = client_ptr_->GetObject(request);
    if (!outcome.isSuccess()) {
        const auto& err = outcome.error();
        LOG_STORAGE_WARNING_ << "ERROR: GetObjectStr: " << err.Code() << ": " << err.Message();
        return Status(SERVER_UNEXPECTED_ERROR, err.Message());
    }

    auto content_length = outcome.result().Metadata().ContentLength();
    LOG_STORAGE_DEBUG_ << "GetObjectStr"
                       << " success, Content-Length:" << content_length << std::endl;
    auto& stream = outcome.result().Content();
    content.resize(content_length);
    stream->read(content.data(), content_length);

    return Status::OK();
}

Status
OSSClientWrapper::ListObjects(std::vector<std::string>& object_list, const std::string& prefix) {
    AlibabaCloud::OSS::ListObjectsRequest request(oss_bucket_);
    request.setPrefix(normalize_object_name(prefix) + '/');
    request.setDelimiter("/");
    request.setMaxKeys(1000);

    std::string next_marker = "";
    bool is_truncated = false;
    do {
        request.setMarker(next_marker);
        const auto outcome = client_ptr_->ListObjects(request);

        if (!outcome.isSuccess()) {
            const auto& err = outcome.error();
            LOG_STORAGE_WARNING_ << "ERROR: ListObjects: " << err.Code() << ": " << err.Message();
            return Status(SERVER_UNEXPECTED_ERROR, err.Message());
        }
        for (const auto& object : outcome.result().ObjectSummarys()) {
            object_list.emplace_back(object.Key());
        }
        next_marker = outcome.result().NextMarker();
        is_truncated = outcome.result().IsTruncated();
    } while (is_truncated);

    if (prefix.empty()) {
        LOG_STORAGE_DEBUG_ << "ListObjects '" << oss_bucket_ << "' successfully!";
    } else {
        LOG_STORAGE_DEBUG_ << "ListObjects '" << oss_bucket_ << ":" << prefix << "' successfully!";
    }
    for (const auto& path : object_list) {
        LOG_STORAGE_DEBUG_ << "  object: '" << path;
    }
    return Status::OK();
}

Status
OSSClientWrapper::DeleteObject(const std::string& object_name) {
    AlibabaCloud::OSS::DeleteObjectRequest request(oss_bucket_, normalize_object_name(object_name));
    const auto outcome = client_ptr_->DeleteObject(request);
    if (!outcome.isSuccess()) {
        const auto& err = outcome.error();
        LOG_STORAGE_WARNING_ << "ERROR: DeleteObject: " << err.Code() << ": " << err.Message();
        return Status(SERVER_UNEXPECTED_ERROR, err.Message());
    }
    LOG_STORAGE_DEBUG_ << "DeleteObject '" << object_name << "' successfully!";
    return Status::OK();
}

Status
OSSClientWrapper::DeleteObjects(const std::string& prefix) {
    std::vector<std::string> object_list;
    ListObjects(object_list, prefix);
    for (const auto& object : object_list) {
        auto status = DeleteObject(object);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

std::string
OSSClientWrapper::normalize_object_name(const std::string& object_key) {
    std::string object_name = object_key;
    if (object_name.empty()) {
        return object_name;
    }
    if (object_name[0] == '/') {
        object_name = object_name.substr(1);
    }
    if (object_name[object_name.size() - 1] == '/') {
        object_name = object_name.substr(0, object_name.size() - 1);
    }
    return object_name;
}

}  // namespace storage
}  // namespace milvus
