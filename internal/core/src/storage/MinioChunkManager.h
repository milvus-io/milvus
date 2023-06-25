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
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/http/URI.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/s3/S3Client.h>
#include <google/cloud/credentials.h>
#include <google/cloud/internal/oauth2_credentials.h>
#include <google/cloud/internal/oauth2_google_credentials.h>
#include <google/cloud/storage/oauth2/compute_engine_credentials.h>
#include <google/cloud/storage/oauth2/google_credentials.h>
#include <google/cloud/status_or.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "storage/ChunkManager.h"
#include "storage/Exception.h"
#include "storage/Types.h"

namespace milvus::storage {

enum class RemoteStorageType { S3 = 0, GOOGLE_CLOUD = 1, ALIYUN_CLOUD = 2 };

/**
 * @brief This MinioChunkManager is responsible for read and write file in S3.
 */
class MinioChunkManager : public ChunkManager {
 public:
    explicit MinioChunkManager(const StorageConfig& storage_config);

    MinioChunkManager(const MinioChunkManager&);
    MinioChunkManager&
    operator=(const MinioChunkManager&);

 public:
    virtual ~MinioChunkManager();

    virtual bool
    Exist(const std::string& filepath);

    virtual uint64_t
    Size(const std::string& filepath);

    virtual uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) {
        throw NotImplementedException(GetName() +
                                      "Read with offset not implement");
    }

    virtual void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) {
        throw NotImplementedException(GetName() +
                                      "Write with offset not implement");
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

    virtual std::string
    GetRootPath() const {
        return remote_root_path_;
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

 public:
    bool
    ObjectExists(const std::string& bucket_name,
                 const std::string& object_name);
    int64_t
    GetObjectSize(const std::string& bucket_name,
                  const std::string& object_name);
    bool
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
    void
    InitSDKAPI(RemoteStorageType type);
    void
    ShutdownSDKAPI();
    void
    BuildS3Client(const StorageConfig& storage_config,
                  const Aws::Client::ClientConfiguration& config);
    void
    BuildAliyunCloudClient(const StorageConfig& storage_config,
                           const Aws::Client::ClientConfiguration& config);
    void
    BuildGoogleCloudClient(const StorageConfig& storage_config,
                           const Aws::Client::ClientConfiguration& config);

 private:
    Aws::SDKOptions sdk_options_;
    static std::atomic<size_t> init_count_;
    static std::mutex client_mutex_;
    std::shared_ptr<Aws::S3::S3Client> client_;
    std::string default_bucket_name_;
    std::string remote_root_path_;
};

using MinioChunkManagerPtr = std::unique_ptr<MinioChunkManager>;

static const char* GOOGLE_CLIENT_FACTORY_ALLOCATION_TAG =
    "GoogleHttpClientFactory";

class GoogleHttpClientFactory : public Aws::Http::HttpClientFactory {
 public:
    explicit GoogleHttpClientFactory(
        std::shared_ptr<google::cloud::oauth2_internal::Credentials>
            credentials) {
        credentials_ = credentials;
    }

    void
    SetCredentials(std::shared_ptr<google::cloud::oauth2_internal::Credentials>
                       credentials) {
        credentials_ = credentials;
    }

    std::shared_ptr<Aws::Http::HttpClient>
    CreateHttpClient(const Aws::Client::ClientConfiguration&
                         clientConfiguration) const override {
        return Aws::MakeShared<Aws::Http::CurlHttpClient>(
            GOOGLE_CLIENT_FACTORY_ALLOCATION_TAG, clientConfiguration);
    }

    std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(
        const Aws::String& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override {
        return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
    }

    std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(
        const Aws::Http::URI& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override {
        auto request =
            Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
                GOOGLE_CLIENT_FACTORY_ALLOCATION_TAG, uri, method);
        request->SetResponseStreamFactory(streamFactory);
        auto auth_header = credentials_->AuthorizationHeader();
        if (!auth_header.ok()) {
            throw std::runtime_error(
                "get authorization failed, errcode:" +
                StatusCodeToString(auth_header.status().code()));
        }
        request->SetHeaderValue(auth_header->first.c_str(),
                                auth_header->second.c_str());

        return request;
    }

 private:
    std::shared_ptr<google::cloud::oauth2_internal::Credentials> credentials_;
};

}  // namespace milvus::storage
