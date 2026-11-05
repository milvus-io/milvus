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
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/endpoint/AWSPartitions.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/crt/endpoints/RuleEngine.h>

#include <cstdio>
#include <cstdlib>
#include <functional>

#include "milvus-storage/filesystem/s3/s3_filesystem_producer.h"

namespace milvus::storage {
namespace {

// No network is used, even if the guard fails to intercept a request.
class RecordingHttpClient : public Aws::Http::HttpClient {
 public:
    mutable int requests = 0;
    mutable int metadata_requests = 0;

    std::shared_ptr<Aws::Http::HttpResponse>
    MakeRequest(const std::shared_ptr<Aws::Http::HttpRequest>& request,
                Aws::Utils::RateLimits::RateLimiterInterface*,
                Aws::Utils::RateLimits::RateLimiterInterface*) const override {
        ++requests;
        auto response =
            std::make_shared<Aws::Http::Standard::StandardHttpResponse>(
                request);
        response->SetResponseCode(Aws::Http::HttpResponseCode::OK);
        if (request->GetUri().GetAuthority() == "169.254.169.254") {
            ++metadata_requests;
            const auto& path = request->GetUri().GetPath();
            if (path == "/latest/api/token") {
                response->GetResponseBody() << "test-imds-token";
            } else if (path ==
                       "/latest/meta-data/placement/availability-zone") {
                response->GetResponseBody() << "us-east-1a";
            } else if (path == "/latest/meta-data/iam/security-credentials") {
                response->GetResponseBody() << "test-node-role";
            } else if (path ==
                       "/latest/meta-data/iam/security-credentials/"
                       "test-node-role") {
                response->GetResponseBody()
                    << R"({"Code":"Success","AccessKeyId":"NODE_AK",)"
                       R"("SecretAccessKey":"NODE_SK","Token":"NODE_TOKEN",)"
                       R"("Expiration":"2099-01-01T00:00:00Z"})";
            } else {
                ADD_FAILURE() << "Unexpected IMDS path: " << path;
            }
            return response;
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

// Each test runs in a fresh process because SDK initialization and default
// region configuration are cached globally (also by other Milvus test suites).
void
RunWithSdk(const std::function<void(RecordingHttpClient&)>& test) {
    setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
    setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://169.254.169.254", 1);
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

class AwsCredentialsDeathTest : public ::testing::Test {
    void
    SetUp() override {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
};

TEST_F(AwsCredentialsDeathTest, CrtRuleEngineUsesSdkInitialization) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient&) {
            // Call CRT directly from the executable after InitAPI in SDK core.
            // Static CRT copies in this binary and libmilvus-storage.so leave
            // this copy's JSON allocator uninitialized, even when compiler
            // inlining happens to make the endpoint-provider tests pass.
            // S3 now uses internal binary rules. Use a small JSON ruleset to
            // keep exercising CRT's JSON allocator through its public API.
            constexpr char rules[] = R"({
                "version": "1.0",
                "parameters": {},
                "rules": [{
                    "type": "endpoint", "conditions": [],
                    "endpoint": {
                        "url": "https://s3.example.test",
                        "properties": {}, "headers": {}
                    }
                }]
            })";
            Aws::Crt::Endpoints::RuleEngine engine(
                Aws::Crt::ByteCursorFromCString(rules),
                Aws::Crt::ByteCursorFromArray(
                    reinterpret_cast<const uint8_t*>(
                        Aws::Endpoint::AWSPartitions::GetPartitionsBlob()),
                    Aws::Endpoint::AWSPartitions::PartitionsBlobSize));
            ASSERT_TRUE(engine);
            Aws::Crt::Endpoints::RequestContext context;
            auto endpoint = engine.Resolve(context);
            ASSERT_TRUE(endpoint);
            ASSERT_TRUE(endpoint->IsEndpoint());
            auto url = endpoint->GetUrl();
            ASSERT_TRUE(url);
            EXPECT_EQ(url->compare("https://s3.example.test"), 0);
            // engine is destroyed before RunWithSdk shuts down the SDK.
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(AwsCredentialsDeathTest, AwsCredentialsRequireOptInForNodeRole) {
    ASSERT_EXIT(
        RunWithSdk([](RecordingHttpClient& http) {
            unsetenv("AWS_ACCESS_KEY_ID");
            unsetenv("AWS_SECRET_ACCESS_KEY");
            unsetenv("AWS_SESSION_TOKEN");
            unsetenv("AWS_WEB_IDENTITY_TOKEN_FILE");
            unsetenv("AWS_ROLE_ARN");
            unsetenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
            unsetenv("AWS_CONTAINER_CREDENTIALS_FULL_URI");
            setenv("AWS_DEFAULT_REGION", "us-east-1", 1);
            setenv("AWS_EC2_METADATA_DISABLED", "false", 1);

            const char* const disabled_values[] = {nullptr, "", "false", "1"};
            for (const auto* const value : disabled_values) {
                SCOPED_TRACE(value == nullptr ? "unset" : value);
                if (value == nullptr) {
                    unsetenv("AWS_EC2_METADATA_ENABLE");
                } else {
                    setenv("AWS_EC2_METADATA_ENABLE", value, 1);
                }
                milvus_storage::AwsCredentialsProviderChain chain;
                EXPECT_TRUE(chain.GetAWSCredentials().IsEmpty());
                EXPECT_EQ(http.metadata_requests, 0);
                // These are the only allowed credential sources by default.
                const auto& providers = chain.GetProviders();
                ASSERT_EQ(providers.size(), 2);
                EXPECT_NE(std::dynamic_pointer_cast<
                              Aws::Auth::EnvironmentAWSCredentialsProvider>(
                              providers[0]),
                          nullptr);
                EXPECT_NE(
                    std::dynamic_pointer_cast<
                        Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
                        providers[1]),
                    nullptr);
            }

            setenv("AWS_EC2_METADATA_ENABLE", "TrUe", 1);
            milvus_storage::AwsCredentialsProviderChain enabled_chain;
            const auto node_credentials = enabled_chain.GetAWSCredentials();
            EXPECT_EQ(node_credentials.GetAWSAccessKeyId(), "NODE_AK");
            EXPECT_EQ(node_credentials.GetSessionToken(), "NODE_TOKEN");
            EXPECT_GT(http.metadata_requests, 0);

            const auto metadata_requests = http.metadata_requests;
            setenv("AWS_ACCESS_KEY_ID", "ENV_AK", 1);
            setenv("AWS_SECRET_ACCESS_KEY", "ENV_SK", 1);
            milvus_storage::AwsCredentialsProviderChain environment_chain;
            const auto credentials = environment_chain.GetAWSCredentials();
            EXPECT_EQ(credentials.GetAWSAccessKeyId(), "ENV_AK");
            EXPECT_TRUE(credentials.GetSessionToken().empty());
            EXPECT_EQ(http.metadata_requests, metadata_requests);
        }),
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(AwsCredentialsDeathTest,
       AwsCredentialsDoNotFallBackToNodeRoleOnWebIdentityFailure) {
    ASSERT_EXIT(RunWithSdk([](RecordingHttpClient& http) {
                    unsetenv("AWS_ACCESS_KEY_ID");
                    unsetenv("AWS_SECRET_ACCESS_KEY");
                    unsetenv("AWS_SESSION_TOKEN");
                    unsetenv("AWS_EC2_METADATA_ENABLE");
                    setenv("AWS_EC2_METADATA_DISABLED", "false", 1);
                    setenv("AWS_DEFAULT_REGION", "us-east-1", 1);
                    setenv("AWS_ROLE_ARN",
                           "arn:aws:iam::123456789012:role/pod-role",
                           1);
                    // Reading this token fails before any STS request can be sent.
                    setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/dev/null/token", 1);

                    milvus_storage::AwsCredentialsProviderChain chain;
                    EXPECT_TRUE(chain.GetAWSCredentials().IsEmpty());
                    EXPECT_TRUE(chain.GetAWSCredentials().IsEmpty());
                    EXPECT_EQ(http.metadata_requests, 0);
                }),
                ::testing::ExitedWithCode(0),
                "");
}

}  // namespace
}  // namespace milvus::storage
