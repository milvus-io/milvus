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

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/s3/model/ListObjectsRequest.h>

#include <cstdio>
#include <cstdlib>
#include <functional>

#include "storage/minio/GuardedS3EndpointProvider.h"

namespace milvus::storage {
namespace {

using Aws::Client::CoreErrors;
using Aws::Endpoint::AWSEndpoint;
using Aws::Endpoint::EndpointParameters;
using Aws::Endpoint::ResolveEndpointOutcome;
using Aws::S3::Endpoint::S3EndpointProvider;

// No network is used, even if the guard fails to intercept a request.
class RecordingHttpClient : public Aws::Http::HttpClient {
 public:
    mutable int requests = 0;
    mutable Aws::String authorization;
    Aws::Http::HttpResponseCode status = Aws::Http::HttpResponseCode::OK;
    bool network_error = false;

    std::shared_ptr<Aws::Http::HttpResponse>
    MakeRequest(const std::shared_ptr<Aws::Http::HttpRequest>& request,
                Aws::Utils::RateLimits::RateLimiterInterface*,
                Aws::Utils::RateLimits::RateLimiterInterface*) const override {
        ++requests;
        authorization = request->GetHeaderValue("authorization");
        auto response =
            std::make_shared<Aws::Http::Standard::StandardHttpResponse>(
                request);
        response->SetResponseCode(status);
        if (network_error) {
            response->SetClientErrorType(CoreErrors::NETWORK_CONNECTION);
            response->SetClientErrorMessage("test connection failure");
            return response;
        }
        if (status == Aws::Http::HttpResponseCode::OK) {
            response->GetResponseBody()
                << "<ListBucketResult><IsTruncated>false</IsTruncated>"
                   "</ListBucketResult>";
        } else if (status == Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE) {
            response->GetResponseBody()
                << "<Error><Code>SlowDown</Code><Message>test "
                   "throttle</Message></Error>";
        } else {
            response->GetResponseBody()
                << "<Error><Code>AccessDenied</Code>"
                   "<Message>test denial</Message></Error>";
        }
        return response;
    }
};

class RecordingHttpFactory : public Aws::Http::HttpClientFactory {
 public:
    std::shared_ptr<RecordingHttpClient> http =
        std::make_shared<RecordingHttpClient>();

    std::shared_ptr<Aws::Http::HttpClient>
    CreateHttpClient(const Aws::Client::ClientConfiguration&) const override {
        return http;
    }

    std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(
        const Aws::String& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& stream_factory) const override {
        return CreateHttpRequest(Aws::Http::URI(uri), method, stream_factory);
    }

    std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(
        const Aws::Http::URI& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& stream_factory) const override {
        auto request =
            std::make_shared<Aws::Http::Standard::StandardHttpRequest>(uri,
                                                                       method);
        request->SetResponseStreamFactory(stream_factory);
        return request;
    }
};

class FixedEndpointProvider : public S3EndpointProvider {
 public:
    explicit FixedEndpointProvider(ResolveEndpointOutcome outcome)
        : outcome_(std::move(outcome)) {
    }

    ResolveEndpointOutcome
    ResolveEndpoint(const EndpointParameters&) const override {
        return outcome_;
    }

 private:
    ResolveEndpointOutcome outcome_;
};

// Each test runs in a fresh process because SDK initialization and default
// region configuration are cached globally (also by other Milvus test suites).
void
RunWithSdk(const std::function<void(RecordingHttpClient&)>& test) {
    setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
    setenv("AWS_CONFIG_FILE", "/dev/null", 1);
    setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null", 1);
    unsetenv("AWS_REGION");
    unsetenv("AWS_DEFAULT_REGION");
    auto factory = std::make_shared<RecordingHttpFactory>();
    Aws::SDKOptions options;
    options.httpOptions.httpClientFactory_create_fn = [factory] {
        return factory;
    };
    Aws::InitAPI(options);
    test(*factory->http);
    Aws::ShutdownAPI(options);
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

class S3EndpointAuthGuardDeathTest : public ::testing::Test {
    void
    SetUp() override {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
};

Aws::Client::ClientConfiguration
ClientConfig() {
    Aws::Client::ClientConfiguration config;
    config.region = "cn-north-1";
    config.endpointOverride = "http://s3.example.test";
    config.retryStrategy =
        std::make_shared<Aws::Client::DefaultRetryStrategy>(0);
    return config;
}

AWSEndpoint
EndpointWithSigner(const Aws::String& signer) {
    AWSEndpoint endpoint;
    endpoint.SetURL("http://s3.example.test/test-bucket");
    AWSEndpoint::EndpointAttributes attributes{};
    attributes.authScheme.SetName(signer);
    attributes.authScheme.SetSigningRegion("cn-north-1");
    attributes.authScheme.SetSigningName("s3");
    endpoint.SetAttributes(std::move(attributes));
    return endpoint;
}

TEST_F(S3EndpointAuthGuardDeathTest, MalformedPropertiesStopBeforeSigning) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient& http) {
            auto endpoint = EndpointWithSigner("SignatureV4");
            // Use the actual SDK parser, not a mock of the empty-name check.
            endpoint.SetAttributes(AWSEndpoint::EndpointAttributes::
                                       BuildEndpointAttributesFromJson(
                                           "{invalid-auth-properties"));
            ASSERT_TRUE(endpoint.GetAttributes());
            ASSERT_TRUE(endpoint.GetAttributes()->authScheme.GetName().empty());
            Aws::S3::S3Client client(
                Aws::Auth::AWSCredentials("test-key", "test-secret"),
                ClientConfig(),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false);
            client.accessEndpointProvider() =
                std::make_shared<FixedEndpointProvider>(endpoint);
            InstallS3EndpointAuthGuard(client);
            auto guarded = client.accessEndpointProvider();
            InstallS3EndpointAuthGuard(client);
            EXPECT_EQ(guarded, client.accessEndpointProvider());
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket("test-bucket");
            auto result = client.ListObjects(request);
            ASSERT_FALSE(result.IsSuccess());
            EXPECT_EQ(static_cast<CoreErrors>(result.GetError().GetErrorType()),
                      CoreErrors::ENDPOINT_RESOLUTION_FAILURE);
            EXPECT_FALSE(result.GetError().ShouldRetry());
            EXPECT_NE(result.GetError().GetMessage().find("empty signer"),
                      Aws::String::npos);
            EXPECT_EQ(
                result.GetError().GetMessage().find("invalid-auth-properties"),
                Aws::String::npos);
            EXPECT_EQ(http.requests, 0);
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(S3EndpointAuthGuardDeathTest,
       CredentialsProviderClientStopsBeforeSigning) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient& http) {
            auto credentials =
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
                    "test-key", "test-secret", "test-session");
            Aws::S3::S3Client client(
                credentials,
                ClientConfig(),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false);
            client.accessEndpointProvider() =
                std::make_shared<FixedEndpointProvider>(EndpointWithSigner(""));
            InstallS3EndpointAuthGuard(client);
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket("test-bucket");
            auto result = client.ListObjects(request);
            ASSERT_FALSE(result.IsSuccess());
            EXPECT_EQ(static_cast<CoreErrors>(result.GetError().GetErrorType()),
                      CoreErrors::ENDPOINT_RESOLUTION_FAILURE);
            EXPECT_FALSE(result.GetError().ShouldRetry());
            EXPECT_EQ(http.requests, 0);
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(S3EndpointAuthGuardDeathTest, EffectiveRegionMalformedJson) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient& http) {
            auto config = ClientConfig();
            // Exercise the suspect effective value without assuming it came from
            // IMDS. Newer SDK endpoint rules may already reject it before the
            // guard; this is a compatibility test, not proof of the incident
            // trigger or of the guard itself.
            config.region = R"({"code":-1)";
            Aws::S3::S3Client client(
                Aws::Auth::AWSCredentials("test-key", "test-secret"),
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false);
            InstallS3EndpointAuthGuard(client);
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket("test-bucket");
            auto result = client.ListObjects(request);
            ASSERT_FALSE(result.IsSuccess());
            EXPECT_EQ(static_cast<CoreErrors>(result.GetError().GetErrorType()),
                      CoreErrors::ENDPOINT_RESOLUTION_FAILURE);
            EXPECT_FALSE(result.GetError().ShouldRetry());
            EXPECT_EQ(http.requests, 0);
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(S3EndpointAuthGuardDeathTest,
       ValidAuthSchemesAndMissingAttributesPreserved) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient&) {
            for (const auto* signer : {"SignatureV4",
                                       "AsymmetricSignatureV4",
                                       "S3ExpressSigner",
                                       "NullSigner"}) {
                auto endpoint = EndpointWithSigner(signer);
                auto delegate =
                    std::make_shared<FixedEndpointProvider>(endpoint);
                GuardedS3EndpointProvider guard(delegate);
                auto result = guard.ResolveEndpoint({});
                ASSERT_TRUE(result.IsSuccess());
                EXPECT_EQ(
                    result.GetResult().GetAttributes()->authScheme.GetName(),
                    signer);
                EXPECT_EQ(result.GetResult().GetURL(), endpoint.GetURL());
            }
            AWSEndpoint endpoint;
            endpoint.SetURL("http://s3.example.test");
            GuardedS3EndpointProvider guard(
                std::make_shared<FixedEndpointProvider>(endpoint));
            auto result = guard.ResolveEndpoint({});
            ASSERT_TRUE(result.IsSuccess());
            EXPECT_FALSE(result.GetResult().GetAttributes());
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(S3EndpointAuthGuardDeathTest, ResolverErrorPreserved) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient&) {
            Aws::Client::AWSError<CoreErrors> error(
                CoreErrors::INTERNAL_FAILURE,
                "test-error",
                "resolver failed",
                true);
            GuardedS3EndpointProvider guard(
                std::make_shared<FixedEndpointProvider>(error));
            auto result = guard.ResolveEndpoint({});
            ASSERT_FALSE(result.IsSuccess());
            EXPECT_EQ(result.GetError().GetErrorType(), error.GetErrorType());
            EXPECT_EQ(result.GetError().GetExceptionName(),
                      error.GetExceptionName());
            EXPECT_EQ(result.GetError().GetMessage(), error.GetMessage());
            EXPECT_TRUE(result.GetError().ShouldRetry());
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(S3EndpointAuthGuardDeathTest,
       NormalListObjectsAndAccessDeniedPreserved) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient& http) {
            Aws::S3::S3Client client(
                Aws::Auth::AWSCredentials("test-key", "test-secret"),
                ClientConfig(),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false);
            InstallS3EndpointAuthGuard(client);
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket("test-bucket");
            auto result = client.ListObjects(request);
            ASSERT_TRUE(result.IsSuccess()) << result.GetError().GetMessage();
            EXPECT_EQ(http.requests, 1);
            EXPECT_FALSE(http.authorization.empty());
            http.status = Aws::Http::HttpResponseCode::FORBIDDEN;
            auto denied = client.ListObjects(request);
            EXPECT_FALSE(denied.IsSuccess());
            EXPECT_EQ(denied.GetError().GetResponseCode(),
                      Aws::Http::HttpResponseCode::FORBIDDEN);
            EXPECT_FALSE(denied.GetError().ShouldRetry());
            EXPECT_EQ(http.requests, 2);
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(S3EndpointAuthGuardDeathTest, TransientHttpFailuresPreserved) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient& http) {
            Aws::S3::S3Client client(
                Aws::Auth::AWSCredentials("test-key", "test-secret"),
                ClientConfig(),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false);
            InstallS3EndpointAuthGuard(client);
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket("test-bucket");
            http.status = Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE;
            auto throttled = client.ListObjects(request);
            ASSERT_FALSE(throttled.IsSuccess());
            EXPECT_EQ(throttled.GetError().GetResponseCode(), http.status);
            EXPECT_TRUE(throttled.GetError().ShouldRetry());
            EXPECT_EQ(http.requests, 1);
            http.network_error = true;
            auto disconnected = client.ListObjects(request);
            ASSERT_FALSE(disconnected.IsSuccess());
            EXPECT_EQ(
                static_cast<CoreErrors>(disconnected.GetError().GetErrorType()),
                CoreErrors::NETWORK_CONNECTION);
            EXPECT_TRUE(disconnected.GetError().ShouldRetry());
            EXPECT_EQ(http.requests, 2);
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(S3EndpointAuthGuardDeathTest, DefaultsAndEndpointOverridesPreserved) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient&) {
            Aws::S3::S3ClientConfiguration config;
            // No explicit region: leave the SDK's default resolution intact.
            config.endpointOverride = "http://s3.example.test";
            auto delegate = std::make_shared<S3EndpointProvider>();
            GuardedS3EndpointProvider guard(delegate);
            guard.InitBuiltInParameters(config);
            guard.AccessClientContextParameters().SetForcePathStyle(true);
            EXPECT_EQ(&guard.GetClientContextParameters(),
                      &delegate->GetClientContextParameters());
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket("test-bucket");
            auto params = request.GetEndpointContextParams();
            auto result = guard.ResolveEndpoint(params);
            ASSERT_TRUE(result.IsSuccess()) << result.GetError().GetMessage();
            EXPECT_NE(
                result.GetResult().GetURL().find("s3.example.test/test-bucket"),
                Aws::String::npos);
            EXPECT_EQ(result.GetResult().GetAttributes()->authScheme.GetName(),
                      "SignatureV4");
            EXPECT_EQ(*result.GetResult()
                           .GetAttributes()
                           ->authScheme.GetSigningRegion(),
                      config.region);
            guard.OverrideEndpoint("http://other.example.test");
            result = guard.ResolveEndpoint(params);
            ASSERT_TRUE(result.IsSuccess());
            EXPECT_NE(result.GetResult().GetURL().find(
                          "other.example.test/test-bucket"),
                      Aws::String::npos);
            guard.InitBuiltInParameters(config, "s3");
            guard.AccessClientContextParameters().SetForcePathStyle(false);
            result = guard.ResolveEndpoint(params);
            ASSERT_TRUE(result.IsSuccess());
            EXPECT_NE(
                result.GetResult().GetURL().find("test-bucket.s3.example.test"),
                Aws::String::npos);
        }),
        ::testing::ExitedWithCode(0),
        "");
}

}  // namespace
}  // namespace milvus::storage
