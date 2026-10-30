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

#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/status.h>

#include "milvus-storage/filesystem/gcp/gcp_credential_registry.h"
#include "milvus-storage/filesystem/gcp/gcp_filesystem_producer.h"
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

static void
CheckGcpRegistration(const bool legacy_first, const bool use_iam) {
    // Disable metadata discovery while checking bucket registration only.
    setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
    setenv("AWS_CONFIG_FILE", "/dev/null", 1);
    setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null", 1);

    MinioChunkManager manager;
    if (legacy_first) {
        manager.InitSDKAPI(RemoteStorageType::GOOGLE_CLOUD, use_iam, "off");
    }

    milvus_storage::ArrowFileSystemConfig fs_config;
    fs_config.address = "storage.googleapis.com:443";
    fs_config.bucket_name = "registered-storage-bucket";
    fs_config.cloud_provider = "gcp";
    fs_config.region = "europe-west3";
    fs_config.use_ssl = true;
    fs_config.use_iam = use_iam;
    fs_config.access_key_id = "storage-access-key";
    fs_config.access_key_value = "storage-secret-key";
    auto fs = milvus_storage::GcpFileSystemProducer(fs_config).Make();
    ASSERT_TRUE(fs.ok()) << fs.status();

    if (!legacy_first) {
        manager.InitSDKAPI(RemoteStorageType::GOOGLE_CLOUD, use_iam, "off");
    }

    StorageConfig storage_config;
    storage_config.address = fs_config.address;
    storage_config.bucket_name = "legacy-only-bucket";
    storage_config.cloud_provider = "gcp";
    storage_config.useSSL = true;
    storage_config.useIAM = use_iam;
    storage_config.access_key_id = "legacy-access-key";
    storage_config.access_key_value = "legacy-secret-key";
    Aws::Client::ClientConfiguration client_config;
    client_config.region = "europe-west3";
    client_config.endpointOverride = "storage.googleapis.com:443";

    auto& registry = milvus_storage::GcpCredentialRegistry::Instance();
    const Aws::Http::URI uri(
        "https://storage.googleapis.com/legacy-only-bucket?prefix=check");
    ASSERT_EQ(registry.Lookup(uri), nullptr);
    if (!legacy_first) {
        // Prove storage's factory won InitAPI: an unregistered URI is rejected
        // locally even with an AWS signature. Use loopback so a factory
        // regression cannot send this probe to GCP.
        client_config.connectTimeoutMs = 100;
        client_config.requestTimeoutMs = 100;
        const auto http_client = Aws::Http::CreateHttpClient(client_config);
        const auto request = Aws::Http::CreateHttpRequest(
            Aws::Http::URI("http://127.0.0.1:1/unregistered-bucket"),
            Aws::Http::HttpMethod::HTTP_GET,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
        request->SetHeaderValue("Authorization", "AWS4-HMAC-SHA256 test");
        const auto response = http_client->MakeRequest(request);
        ASSERT_EQ(response->GetResponseCode(),
                  Aws::Http::HttpResponseCode::FORBIDDEN);
        std::ostringstream body;
        body << response->GetResponseBody().rdbuf();
        ASSERT_NE(body.str().find("No GcpCredentialProvider registered"),
                  std::string::npos);
    }

    manager.BuildGoogleCloudClient(storage_config, client_config);
    const auto provider = registry.Lookup(uri);
    ASSERT_NE(provider, nullptr);
    EXPECT_EQ(registry.Lookup(Aws::Http::URI(
                  "https://legacy-only-bucket.storage.googleapis.com/key")),
              provider);
    EXPECT_EQ(registry.Lookup(Aws::Http::URI(
                  "https://storage.googleapis.com:443/legacy-only-bucket/key")),
              provider);
    EXPECT_EQ(
        registry.Lookup(Aws::Http::URI(
            "https://storage.googleapis.com:8443/legacy-only-bucket/key")),
        nullptr);
    EXPECT_EQ(registry.Lookup(
                  Aws::Http::URI("http://169.254.169.254/latest/api/token")),
              nullptr);

    const auto request =
        std::make_shared<Aws::Http::Standard::StandardHttpRequest>(
            uri, Aws::Http::HttpMethod::HTTP_PUT);
    request->SetHeaderValue("Authorization", "existing-authorization");
    ASSERT_TRUE(provider->MaybeSignConditionalWrite(request).ok());
    EXPECT_EQ(request->GetHeaderValue("Authorization"),
              "existing-authorization");
    request->SetHeaderValue("x-goog-if-generation-match", "0");
    ASSERT_TRUE(provider->MaybeSignConditionalWrite(request).ok());
    if (use_iam) {
        // IAM takes precedence even when AK/SK are also configured, and must
        // leave Authorization to the OAuth2 path without fetching a real token.
        EXPECT_EQ(request->GetHeaderValue("Authorization"),
                  "existing-authorization");
    } else {
        EXPECT_FALSE(provider->AuthorizationHeader().has_value());
        EXPECT_EQ(request->GetHeaderValue("Authorization")
                      .find("GOOG4-HMAC-SHA256 Credential=legacy-access-key/"),
                  0);
    }
}

class FakeGcpCredentialProvider : public milvus_storage::GcpCredentialProvider {
 public:
    enum class Mode { kToken, kNoToken, kEmptyHeader, kThrow };

    explicit FakeGcpCredentialProvider(Mode mode) : mode_(mode) {
    }

    std::optional<std::pair<std::string, std::string>>
    AuthorizationHeader() override {
        ++calls_;
        switch (mode_) {
            case Mode::kToken:
                return std::make_pair(std::string("Authorization"),
                                      std::string("Bearer fake-token"));
            case Mode::kNoToken:
                return std::nullopt;
            case Mode::kEmptyHeader:
                return std::make_pair(std::string(), std::string());
            case Mode::kThrow:
                throw std::runtime_error("malformed token response");
        }
        return std::nullopt;
    }

    arrow::Status
    MaybeSignConditionalWrite(
        const std::shared_ptr<Aws::Http::HttpRequest>&) override {
        return arrow::Status::OK();
    }

    int
    calls() const {
        return calls_;
    }

 private:
    Mode mode_;
    int calls_ = 0;
};

// Registers a provider for a bucket on `endpoint`, as milvus-storage does for
// every filesystem it opens.
static std::shared_ptr<FakeGcpCredentialProvider>
RegisterFakeProvider(const std::string& endpoint,
                     const std::string& bucket,
                     FakeGcpCredentialProvider::Mode mode) {
    auto provider = std::make_shared<FakeGcpCredentialProvider>(mode);
    milvus_storage::GcpCredentialRegistry::Instance().Register(
        {milvus_storage::NormalizeGcpEndpoint(endpoint, true), bucket},
        provider);
    return provider;
}

// Registers a provider the way MinioChunkManager::BuildGoogleCloudClient does
// for the GCS endpoint Milvus is configured with.
static std::shared_ptr<FakeGcpCredentialProvider>
RegisterMilvusProvider(const std::string& bucket,
                       FakeGcpCredentialProvider::Mode mode) {
    AddGcsEndpointForCredentials("gcs-factory-test.invalid", true);
    return RegisterFakeProvider("gcs-factory-test.invalid", bucket, mode);
}

static std::shared_ptr<Aws::Http::HttpRequest>
CreateGoogleRequest(const std::string& uri) {
    GoogleHttpClientFactory factory;
    return factory.CreateHttpRequest(
        Aws::Http::URI(uri.c_str()),
        Aws::Http::HttpMethod::HTTP_GET,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
}

static void
CheckMilvusBucketKeepsItsProvider() {
    setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
    setenv("AWS_CONFIG_FILE", "/dev/null", 1);
    setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null", 1);
    // Milvus' own IAM provider asks the metadata server for a token; point it
    // at a closed port so the fetch fails at once instead of reaching GCP.
    setenv("GCE_METADATA_ROOT", "127.0.0.1:1", 1);
    unsetenv("GOOGLE_APPLICATION_CREDENTIALS");
    setenv("GOOGLE_GCLOUD_ADC_PATH_OVERRIDE", "/nonexistent/adc.json", 1);

    MinioChunkManager manager;
    manager.InitSDKAPI(RemoteStorageType::GOOGLE_CLOUD, true, "off");

    StorageConfig storage_config;
    storage_config.address = "storage.googleapis.com:443";
    storage_config.bucket_name = "milvus-own-bucket";
    storage_config.cloud_provider = "gcp";
    storage_config.useSSL = true;
    storage_config.useIAM = true;
    Aws::Client::ClientConfiguration client_config;
    client_config.region = "europe-west3";
    client_config.endpointOverride = "storage.googleapis.com:443";
    manager.BuildGoogleCloudClient(storage_config, client_config);

    auto& registry = milvus_storage::GcpCredentialRegistry::Instance();
    const Aws::Http::URI own_uri(
        "https://storage.googleapis.com/milvus-own-bucket/key");
    const auto milvus_provider = registry.Lookup(own_uri);
    ASSERT_NE(milvus_provider, nullptr);

    // milvus-storage registers another identity for the same bucket, as for
    // an external table on it. Milvus' own requests keep Milvus' provider.
    const milvus_storage::GcpBucketKey own_key{
        milvus_storage::NormalizeGcpEndpoint(storage_config.address, true),
        storage_config.bucket_name};
    auto replacement = std::make_shared<FakeGcpCredentialProvider>(
        FakeGcpCredentialProvider::Mode::kToken);
    registry.Register(own_key, replacement);
    for (const auto* uri :
         {"https://storage.googleapis.com/milvus-own-bucket/key",
          "https://milvus-own-bucket.storage.googleapis.com/key"}) {
        const auto request = Aws::Http::CreateHttpRequest(
            Aws::Http::URI(uri),
            Aws::Http::HttpMethod::HTTP_GET,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
        EXPECT_NE(request->GetHeaderValue("Authorization"), "Bearer fake-token")
            << uri;
    }
    EXPECT_EQ(replacement->calls(), 0);

    // Another bucket still uses the provider milvus-storage registered.
    auto external =
        RegisterFakeProvider("storage.googleapis.com",
                             "external-table-bucket",
                             FakeGcpCredentialProvider::Mode::kToken);
    EXPECT_EQ(Aws::Http::CreateHttpRequest(
                  Aws::Http::URI("https://storage.googleapis.com/"
                                 "external-table-bucket/key"),
                  Aws::Http::HttpMethod::HTTP_GET,
                  Aws::Utils::Stream::DefaultResponseStreamFactoryMethod)
                  ->GetHeaderValue("Authorization"),
              "Bearer fake-token");
    EXPECT_EQ(external->calls(), 1);

    // A later chunk manager on the bucket reuses Milvus' provider and its
    // cached token.
    manager.BuildGoogleCloudClient(storage_config, client_config);
    EXPECT_EQ(registry.Lookup(own_uri), milvus_provider);
}

}  // namespace

TEST(GoogleHttpClientFactoryTest, AuthenticatesRegisteredBucketOnly) {
    auto provider = RegisterMilvusProvider(
        "factory-token-bucket", FakeGcpCredentialProvider::Mode::kToken);

    for (const auto* uri :
         {"https://factory-token-bucket.gcs-factory-test.invalid/key",
          "https://gcs-factory-test.invalid/factory-token-bucket/key"}) {
        auto request = CreateGoogleRequest(uri);
        EXPECT_EQ(request->GetHeaderValue("Authorization"), "Bearer fake-token")
            << uri;
    }
    EXPECT_EQ(provider->calls(), 2);

    // The GCP token must not reach any other host: the instance metadata
    // endpoint, an external table endpoint, or another bucket or port.
    for (const auto* uri :
         {"http://169.254.169.254/latest/meta-data/iam/security-credentials",
          "http://external-endpoint.example:9000/factory-token-bucket/key",
          "https://gcs-factory-test.invalid/other-bucket/key",
          "https://gcs-factory-test.invalid:8443/factory-token-bucket/key"}) {
        auto request = CreateGoogleRequest(uri);
        EXPECT_FALSE(request->HasHeader("Authorization")) << uri;
    }
    EXPECT_EQ(provider->calls(), 2);
}

TEST(GoogleHttpClientFactoryTest, AuthenticatesGcsEndpointsOnly) {
    // A provider registered for a host that is neither Google's nor the GCS
    // endpoint Milvus is configured with gets no token.
    auto other = RegisterFakeProvider("other-endpoint.example",
                                      "factory-other-bucket",
                                      FakeGcpCredentialProvider::Mode::kToken);
    for (const auto* uri :
         {"https://other-endpoint.example/factory-other-bucket/key",
          "https://factory-other-bucket.other-endpoint.example/key"}) {
        auto request = CreateGoogleRequest(uri);
        EXPECT_FALSE(request->HasHeader("Authorization")) << uri;
    }
    EXPECT_EQ(other->calls(), 0);

    // Look-alike hosts are not Google's.
    for (const auto* host :
         {"storage.googleapis.com.example", "evilgoogleapis.com"}) {
        auto lookalike =
            RegisterFakeProvider(host,
                                 "factory-lookalike-bucket",
                                 FakeGcpCredentialProvider::Mode::kToken);
        auto request = CreateGoogleRequest(std::string("https://") + host +
                                           "/factory-lookalike-bucket/key");
        EXPECT_FALSE(request->HasHeader("Authorization")) << host;
        EXPECT_EQ(lookalike->calls(), 0) << host;
    }

    // A configured endpoint is matched by host, whatever its port.
    AddGcsEndpointForCredentials("gcs-port-test.invalid:443", true);
    auto configured =
        RegisterFakeProvider("gcs-port-test.invalid",
                             "factory-port-bucket",
                             FakeGcpCredentialProvider::Mode::kToken);
    EXPECT_EQ(CreateGoogleRequest(
                  "https://gcs-port-test.invalid/factory-port-bucket/k")
                  ->GetHeaderValue("Authorization"),
              "Bearer fake-token");
    EXPECT_EQ(configured->calls(), 1);

    // Google's own endpoints need no configuration.
    auto google = RegisterFakeProvider("storage.googleapis.com",
                                       "factory-google-bucket",
                                       FakeGcpCredentialProvider::Mode::kToken);
    for (const auto* uri :
         {"https://storage.googleapis.com/factory-google-bucket/key",
          "https://factory-google-bucket.storage.googleapis.com/key"}) {
        auto request = CreateGoogleRequest(uri);
        EXPECT_EQ(request->GetHeaderValue("Authorization"), "Bearer fake-token")
            << uri;
    }
    EXPECT_EQ(google->calls(), 2);
}

TEST(GoogleHttpClientFactoryTest, TokenFailureDoesNotThrow) {
    // The AWS SDK calls the factory and is built with -fno-exceptions, so a
    // failing or throwing provider must give an unauthenticated request.
    for (auto mode : {FakeGcpCredentialProvider::Mode::kNoToken,
                      FakeGcpCredentialProvider::Mode::kEmptyHeader,
                      FakeGcpCredentialProvider::Mode::kThrow}) {
        const auto bucket =
            "factory-failure-bucket-" + std::to_string(static_cast<int>(mode));
        auto provider = RegisterMilvusProvider(bucket, mode);
        std::shared_ptr<Aws::Http::HttpRequest> request;
        ASSERT_NO_THROW(
            request = CreateGoogleRequest("https://gcs-factory-test.invalid/" +
                                          bucket + "/k"));
        EXPECT_FALSE(request->HasHeader("Authorization"));
        // An empty header name would go out as a bare ":" header line.
        EXPECT_FALSE(request->HasHeader(""));
        EXPECT_EQ(provider->calls(), 1);
    }
}

// Each case execs a fresh process: neither AWS InitAPI nor storage's call_once
// can be reset reliably inside a shared all_tests process.
TEST(GcpChunkManagerRegistrationDeathTest, StorageFirstIAM) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpRegistration(false, true);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(GcpChunkManagerRegistrationDeathTest, StorageFirstAccessKey) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpRegistration(false, false);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(GcpChunkManagerRegistrationDeathTest, LegacyFirstIAM) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpRegistration(true, true);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(GcpChunkManagerRegistrationDeathTest, LegacyFirstAccessKey) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckGcpRegistration(true, false);
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(GcpChunkManagerRegistrationDeathTest, MilvusBucketKeepsItsProvider) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            CheckMilvusBucketKeepsItsProvider();
            ExitWithTestResult();
        },
        ::testing::ExitedWithCode(0),
        "");
}

}  // namespace milvus::storage
