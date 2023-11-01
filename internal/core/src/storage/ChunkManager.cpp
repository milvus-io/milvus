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

#include <fstream>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "storage/MinioChunkManager.h"
#include "storage/AliyunSTSClient.h"
#include "storage/AliyunCredentialsProvider.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "signal.h"

namespace milvus::storage {

Aws::String
ConvertToAwsString(const std::string& str) {
    return Aws::String(str.c_str(), str.size());
}

Aws::Client::ClientConfiguration
generateConfig(const StorageConfig& storage_config) {
    // The ClientConfiguration default constructor will take a long time.
    // For more details, please refer to https://github.com/aws/aws-sdk-cpp/issues/1440
    static Aws::Client::ClientConfiguration g_config;
    Aws::Client::ClientConfiguration config = g_config;
    config.endpointOverride = ConvertToAwsString(storage_config.address);

    if (storage_config.useSSL) {
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true;
    } else {
        config.scheme = Aws::Http::Scheme::HTTP;
        config.verifySSL = false;
    }

    if (!storage_config.region.empty()) {
        config.region = ConvertToAwsString(storage_config.region);
    }

    config.requestTimeoutMs = storage_config.requestTimeoutMs == 0
                                  ? DEFAULT_CHUNK_MANAGER_REQUEST_TIMEOUT_MS
                                  : storage_config.requestTimeoutMs;

    return config;
}

AwsChunkManager::AwsChunkManager(const StorageConfig& storage_config) {
    default_bucket_name_ = storage_config.bucket_name;
    remote_root_path_ = storage_config.root_path;

    InitSDKAPIDefault(storage_config.log_level);

    Aws::Client::ClientConfiguration config = generateConfig(storage_config);
    if (storage_config.useIAM) {
        auto provider =
            std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
        auto aws_credentials = provider->GetAWSCredentials();
        AssertInfo(!aws_credentials.GetAWSAccessKeyId().empty(),
                   "if use iam, access key id should not be empty");
        AssertInfo(!aws_credentials.GetAWSSecretKey().empty(),
                   "if use iam, secret key should not be empty");
        AssertInfo(!aws_credentials.GetSessionToken().empty(),
                   "if use iam, token should not be empty");

        client_ = std::make_shared<Aws::S3::S3Client>(
            provider,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            storage_config.useVirtualHost);
    } else {
        BuildAccessKeyClient(storage_config, config);
    }

    LOG_SEGCORE_INFO_ << "init AwsChunkManager with parameter[endpoint: '"
                      << storage_config.address << "', default_bucket_name:'"
                      << storage_config.bucket_name << "', root_path:'"
                      << storage_config.root_path << "', use_secure:'"
                      << std::boolalpha << storage_config.useSSL << "']";
}

GcpChunkManager::GcpChunkManager(const StorageConfig& storage_config) {
    default_bucket_name_ = storage_config.bucket_name;
    remote_root_path_ = storage_config.root_path;

    if (storage_config.useIAM) {
        sdk_options_.httpOptions.httpClientFactory_create_fn = []() {
            auto credentials = std::make_shared<
                google::cloud::oauth2_internal::GOOGLE_CLOUD_CPP_NS::
                    ComputeEngineCredentials>();
            return Aws::MakeShared<GoogleHttpClientFactory>(
                GOOGLE_CLIENT_FACTORY_ALLOCATION_TAG, credentials);
        };
    }

    InitSDKAPIDefault(storage_config.log_level);

    Aws::Client::ClientConfiguration config = generateConfig(storage_config);
    if (storage_config.useIAM) {
        // Using S3 client instead of google client because of compatible protocol
        client_ = std::make_shared<Aws::S3::S3Client>(
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            storage_config.useVirtualHost);
    } else {
        BuildAccessKeyClient(storage_config, config);
    }

    LOG_SEGCORE_INFO_ << "init GcpChunkManager with parameter[endpoint: '"
                      << storage_config.address << "', default_bucket_name:'"
                      << storage_config.bucket_name << "', root_path:'"
                      << storage_config.root_path << "', use_secure:'"
                      << std::boolalpha << storage_config.useSSL << "']";
}

AliyunChunkManager::AliyunChunkManager(const StorageConfig& storage_config) {
    default_bucket_name_ = storage_config.bucket_name;
    remote_root_path_ = storage_config.root_path;

    InitSDKAPIDefault(storage_config.log_level);

    Aws::Client::ClientConfiguration config = generateConfig(storage_config);
    if (storage_config.useIAM) {
        auto aliyun_provider = Aws::MakeShared<
            Aws::Auth::AliyunSTSAssumeRoleWebIdentityCredentialsProvider>(
            "AliyunSTSAssumeRoleWebIdentityCredentialsProvider");
        auto aliyun_credentials = aliyun_provider->GetAWSCredentials();
        AssertInfo(!aliyun_credentials.GetAWSAccessKeyId().empty(),
                   "if use iam, access key id should not be empty");
        AssertInfo(!aliyun_credentials.GetAWSSecretKey().empty(),
                   "if use iam, secret key should not be empty");
        AssertInfo(!aliyun_credentials.GetSessionToken().empty(),
                   "if use iam, token should not be empty");
        client_ = std::make_shared<Aws::S3::S3Client>(
            aliyun_provider,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            storage_config.useVirtualHost);
    } else {
        BuildAccessKeyClient(storage_config, config);
    }

    LOG_SEGCORE_INFO_ << "init AliyunChunkManager with parameter[endpoint: '"
                      << storage_config.address << "', default_bucket_name:'"
                      << storage_config.bucket_name << "', root_path:'"
                      << storage_config.root_path << "', use_secure:'"
                      << std::boolalpha << storage_config.useSSL << "']";
}

}  // namespace milvus::storage
