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
#include <sstream>

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

}  // namespace

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

}  // namespace milvus::storage
