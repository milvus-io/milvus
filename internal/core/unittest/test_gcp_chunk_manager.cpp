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

#include <gtest/gtest.h>

#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

#include "milvus-storage/filesystem/gcp/gcp_credential_provider.h"
#include "milvus-storage/filesystem/gcp/gcp_filesystem_producer.h"
#include "milvus-storage/filesystem/s3/s3_options.h"
#include "storage/minio/MinioChunkManager.h"

namespace milvus::storage {
namespace {

[[noreturn]] static void
ExitWithTestResult() {
    const auto* result =
        ::testing::UnitTest::GetInstance()->current_test_info()->result();
    for (int i = 0; i < result->total_part_count(); ++i) {
        const auto& part = result->GetTestPartResult(i);
        if (part.failed()) {
            std::fprintf(stderr,
                         "%s:%d: %s\n",
                         part.file_name(),
                         part.line_number(),
                         part.message());
        }
    }
    std::_Exit(result->Failed() ? 1 : 0);
}

class RecordingHttpClient final : public Aws::Http::HttpClient {
 public:
    std::shared_ptr<Aws::Http::HttpResponse>
    MakeRequest(const std::shared_ptr<Aws::Http::HttpRequest>& request,
                Aws::Utils::RateLimits::RateLimiterInterface*,
                Aws::Utils::RateLimits::RateLimiterInterface*) const override {
        authorization = request->HasHeader("Authorization")
                            ? request->GetAuthorization()
                            : Aws::String{};
        ++requests;
        auto response =
            std::make_shared<Aws::Http::Standard::StandardHttpResponse>(
                request);
        response->SetResponseCode(Aws::Http::HttpResponseCode::OK);
        return response;
    }

    mutable Aws::String authorization;
    mutable int requests = 0;
};

class RecordingHttpFactory final : public TlsHttpClientFactory {
 public:
    explicit RecordingHttpFactory(std::shared_ptr<RecordingHttpClient> client)
        : TlsHttpClientFactory(""), client_(std::move(client)) {
    }

    std::shared_ptr<Aws::Http::HttpClient>
    CreateHttpClient(const Aws::Client::ClientConfiguration&) const override {
        return client_;
    }

 private:
    const std::shared_ptr<RecordingHttpClient> client_;
};

class TestChunkManager : public MinioChunkManager {
 public:
    using MinioChunkManager::client_;
};

class TestBearer : public milvus_storage::GcpCredentialProvider {
 public:
    explicit TestBearer(const milvus_storage::ArrowFileSystemConfig& config)
        : GcpCredentialProvider(config) {
    }

 private:
    arrow::Result<bool>
    AuthorizeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request) override {
        request->SetAuthorization("Bearer external-table");
        return true;
    }
};

static void
CheckGcpClients(bool legacy_first, bool use_iam) {
    setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
    setenv("AWS_CONFIG_FILE", "/dev/null", 1);
    setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null", 1);
    // Fail primary ADC locally so the test needs no cloud credential.
    setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null", 1);
    setenv("GCE_METADATA_ROOT", "127.0.0.1:1", 1);

    const auto http = std::make_shared<RecordingHttpClient>();
    Aws::SDKOptions sdk;
    sdk.httpOptions.httpClientFactory_create_fn = [http] {
        return std::make_shared<RecordingHttpFactory>(http);
    };
    Aws::InitAPI(sdk);

    StorageConfig primary;
    primary.cloud_provider = "gcp";
    primary.address =
        legacy_first ? "primary.example:8443" : "storage.googleapis.com";
    primary.bucket_name = "shared-bucket";
    primary.region = "us-east-1";
    primary.useSSL = true;
    primary.useIAM = use_iam;
    primary.access_key_id = "primary-key";
    primary.access_key_value = "primary-secret";
    Aws::Client::ClientConfiguration config;
    config.endpointOverride = primary.address.c_str();
    config.region = primary.region.c_str();

    TestChunkManager manager;
    auto open_primary = [&] {
        manager.InitSDKAPIDefault("off");
        manager.BuildGoogleCloudClient(primary, config);
    };
    if (legacy_first) {
        open_primary();
    }

    milvus_storage::ArrowFileSystemConfig external;
    external.storage_type = "remote";
    external.cloud_provider = "gcp";
    external.alias = "external";
    external.address = primary.address;
    external.bucket_name = primary.bucket_name;
    external.region = primary.region;
    external.use_ssl = true;
    external.access_key_id = "storage-key";
    external.access_key_value = "storage-secret";
    auto fs = milvus_storage::GcpFileSystemProducer(external).Make();
    ASSERT_TRUE(fs.ok()) << fs.status();

    const auto bearer = std::make_shared<TestBearer>(external);
    Aws::S3::S3Client external_client(
        bearer,
        config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        false);
    milvus_storage::S3Options options;
    options.cloud_provider = "gcp";
    options.credentials_provider = bearer;
    ASSERT_TRUE(milvus_storage::GcpCredentialProvider::ConfigS3HttpClient(
        options, external_client));
    if (!legacy_first) {
        open_primary();
    }

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(primary.bucket_name.c_str());
    request.SetKey("object");
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(external_client.HeadObject(request).IsSuccess());
        EXPECT_EQ(http->authorization, "Bearer external-table");
        ASSERT_TRUE(manager.client_->HeadObject(request).IsSuccess());
        if (use_iam) {
            EXPECT_TRUE(http->authorization.empty());
        } else {
            EXPECT_NE(http->authorization.find("Credential=primary-key/"),
                      Aws::String::npos);
        }
    }

    const int before = http->requests;
    request.SetBucket("different-bucket");
    EXPECT_FALSE(external_client.HeadObject(request).IsSuccess());
    EXPECT_EQ(http->requests, before);

    // Broker exchanges use their own HTTP client. Creating an unbound request
    // to the same endpoint and bucket must not attach either GCS identity.
    const auto broker_request = Aws::Http::CreateHttpRequest(
        Aws::String(("https://" + primary.address + "/" + primary.bucket_name +
                     "/broker")
                        .c_str()),
        Aws::Http::HttpMethod::HTTP_POST,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    EXPECT_FALSE(broker_request->HasHeader("Authorization"));
}

}  // namespace

// Each case execs a fresh process: neither AWS InitAPI nor storage's call_once
// can be reset reliably inside a shared all_tests process.
TEST(GcpChunkManagerDeathTest, StorageFirstIAM) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpClients(false, true);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(GcpChunkManagerDeathTest, StorageFirstAccessKey) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpClients(false, false);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(GcpChunkManagerDeathTest, LegacyFirstIAM) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpClients(true, true);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(GcpChunkManagerDeathTest, LegacyFirstAccessKey) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpClients(true, false);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

}  // namespace milvus::storage
