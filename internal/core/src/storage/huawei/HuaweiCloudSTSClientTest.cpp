// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

#include "aws/core/Aws.h"
#include "aws/core/http/HttpTypes.h"
#include "storage/huawei/HuaweiCloudSTSClient.h"

namespace Aws {
namespace Internal {

class HuaweiCloudSTSClientTestHelper {
 public:
    using Client = HuaweiCloudSTSCredentialsClient;

    struct CallResultView {
        bool success;
        Aws::Auth::AWSCredentials credentials;
        Aws::String errorMessage;
    };

    static CallResultView
    parseSTSResponse(Aws::Http::HttpResponseCode httpResponseCode,
                     const Aws::String& responseBody) {
        auto result = Client::parseSTSResponse(httpResponseCode, responseBody);
        return {result.success, result.credentials, result.errorMessage};
    }
};

}  // namespace Internal
}  // namespace Aws

namespace {
class HuaweiCloudSTSClientTest : public ::testing::Test {
 protected:
    static Aws::SDKOptions sdkOptions_;

    static void
    SetUpTestSuite() {
        sdkOptions_.loggingOptions.logLevel =
            Aws::Utils::Logging::LogLevel::Off;
        Aws::InitAPI(sdkOptions_);
    }

    static void
    TearDownTestSuite() {
        Aws::ShutdownAPI(sdkOptions_);
    }

    using Helper = Aws::Internal::HuaweiCloudSTSClientTestHelper;
};

Aws::SDKOptions HuaweiCloudSTSClientTest::sdkOptions_;

TEST_F(HuaweiCloudSTSClientTest,
       CallHuaweiCloudSTSReturnsErrorWithHttpBodyOnNon200) {
    const std::string body = R"({"error":"access denied"})";
    auto result =
        Helper::parseSTSResponse(Aws::Http::HttpResponseCode::FORBIDDEN, body);

    EXPECT_FALSE(result.success);
    EXPECT_THAT(result.errorMessage, testing::HasSubstr("HTTP code: 403"));
    EXPECT_THAT(result.errorMessage, testing::HasSubstr("access denied"));
}

TEST_F(HuaweiCloudSTSClientTest, CallHuaweiCloudSTSRejectsInvalidJson) {
    auto result = Helper::parseSTSResponse(Aws::Http::HttpResponseCode::OK,
                                           "{invalid-json");

    EXPECT_FALSE(result.success);
    EXPECT_THAT(result.errorMessage,
                testing::HasSubstr("Failed to parse STS response as JSON"));
}

TEST_F(HuaweiCloudSTSClientTest, CallHuaweiCloudSTSRejectsMissingExpiresAt) {
    const std::string body = R"({
        "credential": {
            "access": "AKID_TEST",
            "secret": "SECRET_TEST",
            "securitytoken": "SESSION_TOKEN"
        }
    })";
    auto result =
        Helper::parseSTSResponse(Aws::Http::HttpResponseCode::OK, body);

    EXPECT_FALSE(result.success);
    EXPECT_THAT(result.errorMessage,
                testing::HasSubstr("missing 'expires_at' field"));
}

TEST_F(HuaweiCloudSTSClientTest,
       CallHuaweiCloudSTSRejectsInvalidExpiresAtFormat) {
    const std::string body = R"({
        "credential": {
            "access": "AKID_TEST",
            "secret": "SECRET_TEST",
            "securitytoken": "SESSION_TOKEN",
            "expires_at": "not-a-timestamp"
        }
    })";
    auto result =
        Helper::parseSTSResponse(Aws::Http::HttpResponseCode::OK, body);

    EXPECT_FALSE(result.success);
    EXPECT_THAT(result.errorMessage,
                testing::HasSubstr("invalid format: not-a-timestamp"));
}

TEST_F(HuaweiCloudSTSClientTest,
       CallHuaweiCloudSTSAcceptsValidExpiresAtAndReturnsCredentials) {
    const std::string body = R"({
        "credential": {
            "access": "AKID_TEST",
            "secret": "SECRET_TEST",
            "securitytoken": "SESSION_TOKEN",
            "expires_at": "2026-03-15T12:34:56Z"
        }
    })";
    auto result =
        Helper::parseSTSResponse(Aws::Http::HttpResponseCode::OK, body);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.credentials.GetAWSAccessKeyId(), "AKID_TEST");
    EXPECT_EQ(result.credentials.GetAWSSecretKey(), "SECRET_TEST");
    EXPECT_EQ(result.credentials.GetSessionToken(), "SESSION_TOKEN");
    EXPECT_FALSE(result.credentials.GetExpiration()
                     .ToGmtString(Aws::Utils::DateFormat::ISO_8601)
                     .empty());
}

}  // namespace
