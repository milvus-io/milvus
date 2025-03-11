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

#include "storage/MinioChunkManager.h"

#include <fstream>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3-crt/model/HeadBucketRequest.h>
#include <aws/s3-crt/model/CreateBucketRequest.h>
#include <aws/s3-crt/model/DeleteBucketRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsRequest.h>
#include "storage/AliyunSTSClient.h"
#include "storage/AliyunCredentialsProvider.h"
#include "storage/TencentCloudCredentialsProvider.h"
#include "monitor/prometheus_client.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "signal.h"
#include <aws/core/Globals.h>

namespace milvus::storage {

std::atomic<size_t> MinioChunkManager::init_count_(0);
std::mutex MinioChunkManager::client_mutex_;

static void
SwallowHandler(int signal) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    switch (signal) {
        case SIGPIPE:
            // cannot use log or stdio
            write(1, "SIGPIPE Swallowed\n", 18);
            break;
        default:
            // cannot use log or stdio
            write(2, "Unexpected signal\n", 18);
    }
#pragma GCC diagnostic pop
}

/**
 * @brief convert std::string to Aws::String
 * because Aws has String type internally
 * but has a copy of string content unfortunately
 * TODO: remove this convert
 * @param str
 * @return Aws::String
 */
inline Aws::String
ConvertToAwsString(const std::string& str) {
    return Aws::String(str.c_str(), str.size());
}

Aws::S3Crt::ClientConfiguration
generateConfig(const StorageConfig& storage_config) {
    // The ClientConfiguration default constructor will take a long time.
    // For more details, please refer to https://github.com/aws/aws-sdk-cpp/issues/1440
    static Aws::S3Crt::ClientConfiguration g_config;

    Aws::S3Crt::ClientConfiguration s3CrtConfig = g_config;
    s3CrtConfig.endpointOverride = ConvertToAwsString(storage_config.address);

    if (storage_config.useSSL) {
        s3CrtConfig.scheme = Aws::Http::Scheme::HTTPS;
    } else {
        s3CrtConfig.scheme = Aws::Http::Scheme::HTTP;
        s3CrtConfig.tlsConnectionOptions = nullptr;
    }

    if (!storage_config.region.empty()) {
        s3CrtConfig.region = ConvertToAwsString(storage_config.region);
    }
    s3CrtConfig.throughputTargetGbps = 10;
    s3CrtConfig.partSize = 8 * 1024 * 1024;
    return s3CrtConfig;
}

/**
 * @brief convert Aws::string to std::string
 * @param aws_str
 * @return std::string
 */
inline std::string
ConvertFromAwsString(const Aws::String& aws_str) {
    return std::string(aws_str.c_str(), aws_str.size());
}

void
AwsLogger::ProcessFormattedStatement(Aws::String&& statement) {
    LOG_INFO("[AWS LOG] {}", statement);
}

void
MinioChunkManager::InitSDKAPI(RemoteStorageType type,
                              bool useIAM,
                              const std::string& log_level_str) {
    std::scoped_lock lock{client_mutex_};
    const size_t initCount = init_count_++;
    if (initCount == 0) {
        // sdk_options_.httpOptions.installSigPipeHandler = true;
        struct sigaction psa;
        memset(&psa, 0, sizeof psa);
        psa.sa_handler = SwallowHandler;
        psa.sa_flags = psa.sa_flags | SA_ONSTACK;
        sigaction(SIGPIPE, &psa, 0);
        // block multiple SIGPIPE concurrently processing
        sigemptyset(&psa.sa_mask);
        sigaddset(&psa.sa_mask, SIGPIPE);
        sigaction(SIGPIPE, &psa, 0);
        if (type == RemoteStorageType::GOOGLE_CLOUD && useIAM) {
            sdk_options_.httpOptions.httpClientFactory_create_fn = []() {
                auto credentials = std::make_shared<
                    google::cloud::oauth2_internal::GOOGLE_CLOUD_CPP_NS::
                        ComputeEngineCredentials>();
                return Aws::MakeShared<GoogleHttpClientFactory>(
                    GOOGLE_CLIENT_FACTORY_ALLOCATION_TAG, credentials);
            };
        }
        LOG_INFO("init aws with log level:{}", log_level_str);
        auto get_aws_log_level = [](const std::string& level_str) {
            Aws::Utils::Logging::LogLevel level =
                Aws::Utils::Logging::LogLevel::Off;
            if (level_str == "fatal") {
                level = Aws::Utils::Logging::LogLevel::Fatal;
            } else if (level_str == "error") {
                level = Aws::Utils::Logging::LogLevel::Error;
            } else if (level_str == "warn") {
                level = Aws::Utils::Logging::LogLevel::Warn;
            } else if (level_str == "info") {
                level = Aws::Utils::Logging::LogLevel::Info;
            } else if (level_str == "debug") {
                level = Aws::Utils::Logging::LogLevel::Debug;
            } else if (level_str == "trace") {
                level = Aws::Utils::Logging::LogLevel::Trace;
            }
            return level;
        };
        auto log_level = get_aws_log_level(log_level_str);
        sdk_options_.loggingOptions.logLevel = log_level;
        sdk_options_.loggingOptions.logger_create_fn = [log_level]() {
            return std::make_shared<AwsLogger>(log_level);
        };
        Aws::InitAPI(sdk_options_);
    }
}

void
MinioChunkManager::InitSDKAPIDefault(const std::string& log_level_str) {
    std::scoped_lock lock{client_mutex_};
    const size_t initCount = init_count_++;
    if (initCount == 0) {
        // sdk_options_.httpOptions.installSigPipeHandler = true;
        struct sigaction psa;
        memset(&psa, 0, sizeof psa);
        psa.sa_handler = SwallowHandler;
        psa.sa_flags = psa.sa_flags | SA_ONSTACK;
        sigaction(SIGPIPE, &psa, 0);
        // block multiple SIGPIPE concurrently processing
        sigemptyset(&psa.sa_mask);
        sigaddset(&psa.sa_mask, SIGPIPE);
        sigaction(SIGPIPE, &psa, 0);
        LOG_INFO("init aws with log level:{}", log_level_str);
        auto get_aws_log_level = [](const std::string& level_str) {
            Aws::Utils::Logging::LogLevel level =
                Aws::Utils::Logging::LogLevel::Off;
            if (level_str == "fatal") {
                level = Aws::Utils::Logging::LogLevel::Fatal;
            } else if (level_str == "error") {
                level = Aws::Utils::Logging::LogLevel::Error;
            } else if (level_str == "warn") {
                level = Aws::Utils::Logging::LogLevel::Warn;
            } else if (level_str == "info") {
                level = Aws::Utils::Logging::LogLevel::Info;
            } else if (level_str == "debug") {
                level = Aws::Utils::Logging::LogLevel::Debug;
            } else if (level_str == "trace") {
                level = Aws::Utils::Logging::LogLevel::Trace;
            }
            return level;
        };
        auto log_level = get_aws_log_level(log_level_str);
        sdk_options_.loggingOptions.logLevel = log_level;
        sdk_options_.loggingOptions.logger_create_fn = [log_level]() {
            return std::make_shared<AwsLogger>(log_level);
        };
        Aws::InitAPI(sdk_options_);
    }
}

void
MinioChunkManager::ShutdownSDKAPI() {
    std::scoped_lock lock{client_mutex_};
    const size_t initCount = --init_count_;
    if (initCount == 0) {
        Aws::ShutdownAPI(sdk_options_);
    }
}

void
MinioChunkManager::BuildS3Client(
    const StorageConfig& storage_config,
    const Aws::S3Crt::ClientConfiguration& config) {
    if (storage_config.useIAM) {
        BuildIAMClient<Aws::Auth::DefaultAWSCredentialsProviderChain>(storage_config, config, "");
    } else {
        BuildAccessKeyClient(storage_config, config);
    }
}

void
MinioChunkManager::PreCheck(const StorageConfig& config) {
    LOG_INFO("start to precheck chunk manager with configuration: {}",
             config.ToString());
    try {
        // Just test connection not check real list, avoid cost resource.
        ListWithPrefix("justforconnectioncheck");
    } catch (SegcoreError& e) {
        auto err_message = fmt::format(
            "precheck chunk manager client failed, "
            "error:{}, "
            "configuration:{}",
            e.what(),
            config.ToString());
        LOG_ERROR(err_message);
        throw SegcoreError(S3Error, err_message);
    } catch (std::exception& e) {
        throw e;
    }
};

void
MinioChunkManager::BuildAccessKeyClient(
    const StorageConfig& storage_config,
    const Aws::S3Crt::ClientConfiguration& config) {
    AssertInfo(!storage_config.access_key_id.empty(),
               "if not use iam, access key should not be empty");
    AssertInfo(!storage_config.access_key_value.empty(),
               "if not use iam, access value should not be empty");

    // hc--init crt client
    Aws::Auth::AWSCredentials credentials(ConvertToAwsString(storage_config.access_key_id),
                                          ConvertToAwsString(storage_config.access_key_value));
    LOG_INFO("hc===start to create s3_crt_client_1");
    crt_client_ = Aws::MakeShared<Aws::S3Crt::S3CrtClient>("S3-Crt-client", credentials, config,
                                                           Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                                           storage_config.useVirtualHost);
    LOG_INFO("hc===finished created s3_crt_client");
}

void
MinioChunkManager::BuildAliyunCloudClient(
    const StorageConfig& storage_config,
    const Aws::S3Crt::ClientConfiguration& config) {
    // For aliyun oss, support use virtual host mode
    StorageConfig mutable_config = storage_config;
    mutable_config.useVirtualHost = true;
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
        crt_client_ = std::make_shared<Aws::S3Crt::S3CrtClient>(
            aliyun_provider,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            mutable_config.useVirtualHost);
    } else {
        BuildAccessKeyClient(mutable_config, config);
    }
}

void
MinioChunkManager::BuildGoogleCloudClient(
    const StorageConfig& storage_config,
    const Aws::S3Crt::ClientConfiguration& config) {
    if (storage_config.useIAM) {
        // Using S3 client instead of google client because of compatible protocol
        crt_client_ = std::make_shared<Aws::S3Crt::S3CrtClient>(
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            storage_config.useVirtualHost);
    } else {
        BuildAccessKeyClient(storage_config, config);
    }
}

MinioChunkManager::MinioChunkManager(const StorageConfig& storage_config)
    : default_bucket_name_(storage_config.bucket_name) {
    remote_root_path_ = storage_config.root_path;
    RemoteStorageType storageType;
    if (storage_config.address.find("google") != std::string::npos) {
        storageType = RemoteStorageType::GOOGLE_CLOUD;
    } else if (storage_config.address.find("aliyun") != std::string::npos) {
        storageType = RemoteStorageType::ALIYUN_CLOUD;
    } else {
        storageType = RemoteStorageType::S3;
    }

    InitSDKAPI(storageType, storage_config.useIAM, storage_config.log_level);

    // The ClientConfiguration default constructor will take a long time.
    // For more details, please refer to https://github.com/aws/aws-sdk-cpp/issues/1440
    Aws::S3Crt::ClientConfiguration s3CrtConfig = generateConfig(storage_config);
    if(!storage_config.useSSL){
        Aws::SetDefaultTlsConnectionOptions(nullptr);
    }

    if (storageType == RemoteStorageType::S3) {
        BuildS3Client(storage_config, s3CrtConfig);
    } else if (storageType == RemoteStorageType::ALIYUN_CLOUD) {
        BuildAliyunCloudClient(storage_config, s3CrtConfig);
    } else if (storageType == RemoteStorageType::GOOGLE_CLOUD) {
        BuildGoogleCloudClient(storage_config, s3CrtConfig);
    }

    PreCheck(storage_config);

    LOG_INFO(
        "init MinioChunkManager with "
        "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
        storage_config.address,
        storage_config.bucket_name,
        storage_config.root_path,
        storage_config.useSSL);
}

MinioChunkManager::~MinioChunkManager() {
    crt_client_.reset();
    ShutdownSDKAPI();
}

uint64_t
MinioChunkManager::Size(const std::string& filepath) {
    return GetObjectSize(default_bucket_name_, filepath);
}

bool
MinioChunkManager::Exist(const std::string& filepath) {
    return ObjectExists(default_bucket_name_, filepath);
}

void
MinioChunkManager::Remove(const std::string& filepath) {
    DeleteObject(default_bucket_name_, filepath);
}

std::vector<std::string>
MinioChunkManager::ListWithPrefix(const std::string& filepath) {
    return ListObjects(default_bucket_name_, filepath);
}

uint64_t
MinioChunkManager::Read(const std::string& filepath, void* buf, uint64_t size) {
    return GetObjectBuffer(default_bucket_name_, filepath, buf, size);
}

void
MinioChunkManager::Write(const std::string& filepath,
                         void* buf,
                         uint64_t size) {
    PutObjectBuffer(default_bucket_name_, filepath, buf, size);
}

bool
MinioChunkManager::BucketExists(const std::string& bucket_name) {
    Aws::S3Crt::Model::HeadBucketRequest request;
    request.SetBucket(bucket_name.c_str());

    auto outcome = crt_client_->HeadBucket(request);

    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        auto error_type = err.GetErrorType();
        // only throw if the error is not nosuchbucket
        // if bucket not exist, HeadBucket return errorType RESOURCE_NOT_FOUND
        if (error_type != Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET &&
            error_type != Aws::S3Crt::S3CrtErrors::RESOURCE_NOT_FOUND) {
            ThrowS3CrtError("BucketExists", err, "params, bucket={}", bucket_name);
        }
        return false;
    }
    return true;
}

std::vector<std::string>
MinioChunkManager::ListBuckets() {
    std::vector<std::string> buckets;
    auto outcome = crt_client_->ListBuckets();

    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        ThrowS3CrtError("ListBuckets", err, "params");
    }
    for (auto&& b : outcome.GetResult().GetBuckets()) {
        buckets.emplace_back(b.GetName());
    }
    return buckets;
}

bool
MinioChunkManager::CreateBucket(const std::string& bucket_name) {
    Aws::S3Crt::Model::CreateBucketRequest request;
    request.SetBucket(bucket_name.c_str());

    auto outcome = crt_client_->CreateBucket(request);

    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        if (err.GetErrorType() !=
            Aws::S3Crt::S3CrtErrors::BUCKET_ALREADY_OWNED_BY_YOU) {
            ThrowS3CrtError("CreateBucket", err, "params, bucket={}", bucket_name);
        }
        return false;
    }
    return true;
}

bool
MinioChunkManager::DeleteBucket(const std::string& bucket_name) {
    Aws::S3Crt::Model::DeleteBucketRequest request;
    request.SetBucket(bucket_name.c_str());

    auto outcome = crt_client_->DeleteBucket(request);

    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        auto error_type = err.GetErrorType();
        if (error_type != Aws::S3Crt::S3CrtErrors::NO_SUCH_BUCKET &&
            error_type != Aws::S3Crt::S3CrtErrors::RESOURCE_NOT_FOUND) {
            ThrowS3CrtError("DeleteBucket", err, "params, bucket={}", bucket_name);
        }
        return false;
    }
    return true;
}

bool
MinioChunkManager::ObjectExists(const std::string& bucket_name,
                                const std::string& object_name) {
    Aws::S3Crt::Model::HeadObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_name.c_str());

    auto start = std::chrono::system_clock::now();
    auto outcome = crt_client_->HeadObject(request);
    monitor::internal_storage_request_latency_stat.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - start)
            .count());

    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        if (!IsNotFound(err.GetErrorType())) {
            monitor::internal_storage_op_count_stat_fail.Increment();
            ThrowS3CrtError("ObjectExists",
                         err,
                         "params, bucket={}, object={}",
                         bucket_name,
                         object_name);
        }
        monitor::internal_storage_op_count_stat_suc.Increment();
        return false;
    }
    monitor::internal_storage_op_count_stat_suc.Increment();
    return true;
}

uint64_t
MinioChunkManager::GetObjectSize(const std::string& bucket_name,
                                 const std::string& object_name) {
    Aws::S3Crt::Model::HeadObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_name.c_str());

    auto start = std::chrono::system_clock::now();
    auto outcome = crt_client_->HeadObject(request);
    monitor::internal_storage_request_latency_stat.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - start)
            .count());
    if (!outcome.IsSuccess()) {
        monitor::internal_storage_op_count_stat_fail.Increment();
        const auto& err = outcome.GetError();
        ThrowS3CrtError("GetObjectSize",
                     err,
                     "params, bucket={}, object={}",
                     bucket_name,
                     object_name);
    }
    monitor::internal_storage_op_count_stat_suc.Increment();
    return outcome.GetResult().GetContentLength();
}

bool
MinioChunkManager::DeleteObject(const std::string& bucket_name,
                                const std::string& object_name) {
    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_name.c_str());

    auto start = std::chrono::system_clock::now();
    auto outcome = crt_client_->DeleteObject(request);
    monitor::internal_storage_request_latency_remove.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - start)
            .count());

    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        if (!IsNotFound(err.GetErrorType())) {
            monitor::internal_storage_op_count_remove_fail.Increment();
            ThrowS3CrtError("DeleteObject",
                         err,
                         "params, bucket={}, object={}",
                         bucket_name,
                         object_name);
        }
        monitor::internal_storage_op_count_remove_suc.Increment();
        return false;
    }
    monitor::internal_storage_op_count_remove_suc.Increment();
    return true;
}

bool
MinioChunkManager::PutObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_name.c_str());

    const std::shared_ptr<Aws::IOStream> input_data =
        Aws::MakeShared<Aws::StringStream>("");

    input_data->write(reinterpret_cast<char*>(buf), size);
    request.SetBody(input_data);

    auto start = std::chrono::system_clock::now();
    auto outcome = crt_client_->PutObject(request);
    monitor::internal_storage_request_latency_put.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - start)
            .count());
    monitor::internal_storage_kv_size_put.Observe(size);

    if (!outcome.IsSuccess()) {
        monitor::internal_storage_op_count_put_fail.Increment();
        const auto& err = outcome.GetError();
        ThrowS3CrtError("PutObjectBuffer",
                     err,
                     "params, bucket={}, object={}",
                     bucket_name,
                     object_name);
    }
    monitor::internal_storage_op_count_put_suc.Increment();
    return true;
}

class AwsStreambuf : public std::streambuf {
 public:
    AwsStreambuf(char* buffer, std::streamsize buffer_size) {
        setp(buffer, buffer + buffer_size - 1);
    }

 protected:
    int_type
    overflow(int_type ch) override {
        if (ch != traits_type::eof()) {
            *pptr() = ch;
            pbump(1);
        }
        return ch;
    }
};

class AwsResponseStream : public Aws::IOStream {
 public:
    /**
     * Creates a stream for get response from server
     * @param buffer the buffer address from user space
     * @param size length of the underlying buffer.
     */
    AwsResponseStream(char* buffer, int64_t size)
        : Aws::IOStream(&aws_streambuf), aws_streambuf(buffer, size) {
    }

 private:
    AwsResponseStream(const AwsResponseStream&) = delete;
    AwsResponseStream(AwsResponseStream&&) = delete;
    AwsResponseStream&
    operator=(const AwsResponseStream&) = delete;
    AwsResponseStream&
    operator=(AwsResponseStream&&) = delete;

    AwsStreambuf aws_streambuf;
};

uint64_t
MinioChunkManager::GetObjectBuffer(const std::string& bucket_name,
                                   const std::string& object_name,
                                   void* buf,
                                   uint64_t size) {
    Aws::S3Crt::Model::GetObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_name.c_str());

    /*
    request.SetResponseStreamFactory([buf, size]() {
    // For macOs, pubsetbuf interface not implemented
#ifdef __linux__
        std::unique_ptr<Aws::StringStream> stream(
            Aws::New<Aws::StringStream>(""));
        stream->rdbuf()->pubsetbuf(static_cast<char*>(buf), size);
#else
        std::unique_ptr<Aws::IOStream> stream(Aws::New<AwsResponseStream>(
            "AwsResponseStream", static_cast<char*>(buf), size));
#endif
        return stream.release();
    });*/
    auto start = std::chrono::system_clock::now();
    auto outcome = crt_client_->GetObject(request);
    monitor::internal_storage_request_latency_get.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - start)
            .count());
    monitor::internal_storage_kv_size_get.Observe(size);

    if (!outcome.IsSuccess()) {
        monitor::internal_storage_op_count_get_fail.Increment();
        const auto& err = outcome.GetError();
        ThrowS3CrtError("GetObjectBuffer",
                     err,
                     "params, bucket={}, object={}",
                     bucket_name,
                     object_name);
    }
    monitor::internal_storage_op_count_get_suc.Increment();
    return size;
}

std::vector<std::string>
MinioChunkManager::ListObjects(const std::string& bucket_name,
                               const std::string& prefix) {
    std::vector<std::string> objects_vec;
    Aws::S3Crt::Model::ListObjectsRequest request;
    request.WithBucket(bucket_name);
    if (prefix != "") {
        request.SetPrefix(prefix);
    }

    auto start = std::chrono::system_clock::now();
    auto outcome = crt_client_->ListObjects(request);
    monitor::internal_storage_request_latency_list.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - start)
            .count());

    if (!outcome.IsSuccess()) {
        monitor::internal_storage_op_count_list_fail.Increment();
        const auto& err = outcome.GetError();
        ThrowS3CrtError("ListObjects",
                     err,
                     "params, bucket={}, prefix={}",
                     bucket_name,
                     prefix);
    }
    monitor::internal_storage_op_count_list_suc.Increment();
    auto objects = outcome.GetResult().GetContents();
    for (auto& obj : objects) {
        objects_vec.emplace_back(obj.GetKey());
    }
    return objects_vec;
}

AwsChunkManager::AwsChunkManager(const StorageConfig& storage_config) {
    default_bucket_name_ = storage_config.bucket_name;
    remote_root_path_ = storage_config.root_path;

    InitSDKAPIDefault(storage_config.log_level);
    if(!storage_config.useSSL){
        Aws::SetDefaultTlsConnectionOptions(nullptr);
    }

    Aws::S3Crt::ClientConfiguration config = generateConfig(storage_config);
    if (storage_config.useIAM) {
        BuildIAMClient<Aws::Auth::DefaultAWSCredentialsProviderChain>(storage_config, config, "");
    } else {
        BuildAccessKeyClient(storage_config, config);
    }

    PreCheck(storage_config);

    LOG_INFO(
            "init AwsChunkManager with "
            "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
            storage_config.address,
            storage_config.bucket_name,
            storage_config.root_path,
            storage_config.useSSL);
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

    Aws::S3Crt::ClientConfiguration config = generateConfig(storage_config);
    if (storage_config.useIAM) {
        // Using S3 client instead of google client because of compatible protocol
        crt_client_ = std::make_shared<Aws::S3Crt::S3CrtClient>(
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                storage_config.useVirtualHost);
    } else {
        BuildAccessKeyClient(storage_config, config);
    }

    PreCheck(storage_config);

    LOG_INFO(
            "init GcpChunkManager with "
            "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
            storage_config.address,
            storage_config.bucket_name,
            storage_config.root_path,
            storage_config.useSSL);
}

AliyunChunkManager::AliyunChunkManager(const StorageConfig& storage_config) {
    default_bucket_name_ = storage_config.bucket_name;
    remote_root_path_ = storage_config.root_path;

    InitSDKAPIDefault(storage_config.log_level);

    Aws::S3Crt::ClientConfiguration config = generateConfig(storage_config);

    // For aliyun oss, support use virtual host mode
    StorageConfig mutable_config = storage_config;
    mutable_config.useVirtualHost = true;
    if (storage_config.useIAM) {
        BuildIAMClient<Aws::Auth::AliyunSTSAssumeRoleWebIdentityCredentialsProvider>(storage_config, config, "AliyunSTSAssumeRoleWebIdentityCredentialsProvider");
    } else {
        BuildAccessKeyClient(mutable_config, config);
    }

    PreCheck(storage_config);

    LOG_INFO(
            "init AliyunChunkManager with "
            "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
            storage_config.address,
            storage_config.bucket_name,
            storage_config.root_path,
            storage_config.useSSL);
}

TencentCloudChunkManager::TencentCloudChunkManager(
        const StorageConfig& storage_config) {
    default_bucket_name_ = storage_config.bucket_name;
    remote_root_path_ = storage_config.root_path;

    InitSDKAPIDefault(storage_config.log_level);

    Aws::S3Crt::ClientConfiguration config = generateConfig(storage_config);

    StorageConfig mutable_config = storage_config;
    mutable_config.useVirtualHost = true;
    if (storage_config.useIAM) {
        BuildIAMClient<Aws::Auth::TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider>
                (storage_config, config, "TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider");

    } else {
        BuildAccessKeyClient(mutable_config, config);
    }

    PreCheck(storage_config);

    LOG_INFO(
            "init TencentCloudChunkManager with "
            "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
            storage_config.address,
            storage_config.bucket_name,
            storage_config.root_path,
            storage_config.useSSL);
}

}  // namespace milvus::storage
