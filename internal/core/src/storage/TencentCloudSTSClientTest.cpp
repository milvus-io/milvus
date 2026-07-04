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
#include "storage/TencentCloudSTSClient.h"

namespace Aws {
namespace Internal {

class TencentCloudSTSClientTestHelper {
 public:
    using Client = TencentCloudSTSCredentialsClient;

    struct ParseResultView {
        bool success;
        Aws::Auth::AWSCredentials credentials;
    };

    static ParseResultView
    parseSTSResponse(const Aws::String& responseBody) {
        auto result = Client::parseSTSResponse(responseBody);
        return {result.success, result.credentials};
    }
};

}  // namespace Internal
}  // namespace Aws

namespace {

class TencentCloudSTSClientTest : public ::testing::Test {
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

    using Helper = Aws::Internal::TencentCloudSTSClientTestHelper;
};

Aws::SDKOptions TencentCloudSTSClientTest::sdkOptions_;

TEST_F(TencentCloudSTSClientTest, RejectsEmptyResponseBody) {
    auto result = Helper::parseSTSResponse("");

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsInvalidJson) {
    auto result = Helper::parseSTSResponse("{invalid-json");

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsMissingResponseObject) {
    const std::string body = R"({
        "Credentials": {
            "TmpSecretId": "AKID_TEST",
            "TmpSecretKey": "SECRET_TEST",
            "Token": "SESSION_TOKEN"
        },
        "Expiration": "2026-07-03T12:34:56Z"
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsMissingCredentialsObject) {
    const std::string body = R"({
        "Response": {
            "Expiration": "2026-07-03T12:34:56Z"
        }
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsMissingExpiration) {
    const std::string body = R"({
        "Response": {
            "Credentials": {
                "TmpSecretId": "AKID_TEST",
                "TmpSecretKey": "SECRET_TEST",
                "Token": "SESSION_TOKEN"
            }
        }
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsInvalidExpirationFormat) {
    const std::string body = R"({
        "Response": {
            "Credentials": {
                "TmpSecretId": "AKID_TEST",
                "TmpSecretKey": "SECRET_TEST",
                "Token": "SESSION_TOKEN"
            },
            "Expiration": "not-a-timestamp"
        }
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsMissingAccessKeyId) {
    const std::string body = R"({
        "Response": {
            "Credentials": {
                "TmpSecretKey": "SECRET_TEST",
                "Token": "SESSION_TOKEN"
            },
            "Expiration": "2026-07-03T12:34:56Z"
        }
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsMissingSecretKey) {
    const std::string body = R"({
        "Response": {
            "Credentials": {
                "TmpSecretId": "AKID_TEST",
                "Token": "SESSION_TOKEN"
            },
            "Expiration": "2026-07-03T12:34:56Z"
        }
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest, RejectsMissingSessionToken) {
    const std::string body = R"({
        "Response": {
            "Credentials": {
                "TmpSecretId": "AKID_TEST",
                "TmpSecretKey": "SECRET_TEST"
            },
            "Expiration": "2026-07-03T12:34:56Z"
        }
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.credentials.IsEmpty());
}

TEST_F(TencentCloudSTSClientTest,
       AcceptsValidResponseAndReturnsNonEmptyCredentials) {
    const std::string body = R"({
        "Response": {
            "Credentials": {
                "TmpSecretId": "AKID_TEST",
                "TmpSecretKey": "SECRET_TEST",
                "Token": "SESSION_TOKEN"
            },
            "Expiration": "2026-07-03T12:34:56Z",
            "RequestId": "req-123"
        }
    })";
    auto result = Helper::parseSTSResponse(body);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.credentials.GetAWSAccessKeyId(), "AKID_TEST");
    EXPECT_EQ(result.credentials.GetAWSSecretKey(), "SECRET_TEST");
    EXPECT_EQ(result.credentials.GetSessionToken(), "SESSION_TOKEN");
    EXPECT_FALSE(result.credentials.GetExpiration()
                     .ToGmtString(Aws::Utils::DateFormat::ISO_8601)
                     .empty());
}

}  // namespace
