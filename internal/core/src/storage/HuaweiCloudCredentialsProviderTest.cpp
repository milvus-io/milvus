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
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>

#include "aws/core/Aws.h"
#include "aws/core/auth/AWSCredentials.h"
#include "aws/core/client/ClientConfiguration.h"
#include "aws/core/utils/DateTime.h"
#include "aws/core/utils/memory/AWSMemory.h"
#include "HuaweiCloudCredentialsProvider.h"
#include "HuaweiCloudSTSClient.h"

using STSRequest = Aws::Internal::HuaweiCloudSTSCredentialsClient::
    STSAssumeRoleWithWebIdentityRequest;
using STSResult = Aws::Internal::HuaweiCloudSTSCredentialsClient::
    STSAssumeRoleWithWebIdentityResult;

// Mock STS client for testing without real HTTP calls
class MockSTSClient : public Aws::Internal::HuaweiCloudSTSCredentialsClient {
 public:
    explicit MockSTSClient(const Aws::Client::ClientConfiguration& config)
        : HuaweiCloudSTSCredentialsClient(config) {
    }

    MOCK_METHOD(STSResult,
                GetAssumeRoleWithWebIdentityCredentials,
                (const STSRequest&),
                (override));
};

// Friend class to access private members of the provider
class HuaweiCloudCredentialsProviderTestHelper {
 public:
    using Provider =
        Aws::Auth::HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider;

    static bool
    isInCooldown(const Provider& p) {
        return p.IsInCooldown();
    }

    static bool
    expiresSoon(const Provider& p) {
        return p.ExpiresSoon();
    }

    static void
    setCredentials(Provider& p, const Aws::Auth::AWSCredentials& creds) {
        p.m_credentials = creds;
    }

    static Aws::Auth::AWSCredentials
    getCredentials(const Provider& p) {
        return p.m_credentials;
    }

    static void
    setLastReloadFailed(Provider& p,
                        bool failed,
                        std::chrono::steady_clock::time_point time =
                            std::chrono::steady_clock::now()) {
        p.m_lastReloadFailed = failed;
        p.m_lastFailedReloadTime = time;
    }

    static void
    setClient(
        Provider& p,
        Aws::UniquePtr<Aws::Internal::HuaweiCloudSTSCredentialsClient> client) {
        p.m_client = std::move(client);
    }

    static void
    setTokenFile(Provider& p, const Aws::String& path) {
        p.m_tokenFile = path;
    }

    static bool
    isInitialized(const Provider& p) {
        return p.m_initialized;
    }

    static void
    setInitialized(Provider& p, bool val) {
        p.m_initialized = val;
    }

    static void
    callReload(Provider& p) {
        p.Reload();
    }

    static void
    callRefreshIfExpired(Provider& p) {
        p.RefreshIfExpired();
    }

    static bool
    lastReloadFailed(const Provider& p) {
        return p.m_lastReloadFailed;
    }
};

using Helper = HuaweiCloudCredentialsProviderTestHelper;

class HuaweiCloudCredentialsProviderTest : public ::testing::Test {
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

    void
    SetUp() override {
        // Set env vars so the provider constructor initializes successfully
        setenv("HUAWEICLOUD_SDK_REGION", "cn-north-4", 1);
        setenv("HUAWEICLOUD_SDK_PROJECT_ID", "test-project-id", 1);
        setenv("HUAWEICLOUD_SDK_IDP_ID", "test-idp-id", 1);

        // Create a temporary token file
        tokenFilePath_ =
            "/tmp/huaweicloud_test_token_" + std::to_string(getpid()) + ".txt";
        std::ofstream ofs(tokenFilePath_);
        ofs << "test-oidc-token-content";
        ofs.close();
        setenv("HUAWEICLOUD_SDK_ID_TOKEN_FILE", tokenFilePath_.c_str(), 1);
    }

    void
    TearDown() override {
        unsetenv("HUAWEICLOUD_SDK_REGION");
        unsetenv("HUAWEICLOUD_SDK_PROJECT_ID");
        unsetenv("HUAWEICLOUD_SDK_IDP_ID");
        unsetenv("HUAWEICLOUD_SDK_ID_TOKEN_FILE");
        std::remove(tokenFilePath_.c_str());
    }

    using Provider =
        Aws::Auth::HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider;

    // Create a provider and inject a mock STS client.
    // Returns raw pointers; provider_ and mock_ are owned by the fixture.
    void
    CreateProviderWithMock() {
        provider_ = std::make_unique<Provider>();
        Aws::Client::ClientConfiguration config;
        auto mockClient =
            Aws::MakeUnique<MockSTSClient>("TestMockSTSClient", config);
        mock_ = mockClient.get();
        Helper::setClient(*provider_, std::move(mockClient));
    }

    std::unique_ptr<Provider> provider_;
    MockSTSClient* mock_ = nullptr;

    // Create valid credentials with a future expiration
    Aws::Auth::AWSCredentials
    MakeFreshCredentials(int expiresInSeconds = 7200) {
        Aws::Auth::AWSCredentials creds(
            "AKID_TEST", "SECRET_TEST", "SESSION_TOKEN");
        creds.SetExpiration(Aws::Utils::DateTime::Now() +
                            std::chrono::milliseconds(expiresInSeconds * 1000));
        return creds;
    }

    // Create credentials that expire soon
    Aws::Auth::AWSCredentials
    MakeExpiringSoonCredentials(int expiresInSeconds = 60) {
        return MakeFreshCredentials(expiresInSeconds);
    }

    // Create credentials already expired
    Aws::Auth::AWSCredentials
    MakeExpiredCredentials() {
        Aws::Auth::AWSCredentials creds(
            "AKID_TEST", "SECRET_TEST", "SESSION_TOKEN");
        creds.SetExpiration(Aws::Utils::DateTime::Now() -
                            std::chrono::milliseconds(60 * 1000));
        return creds;
    }

    // Create a successful STS result
    STSResult
    MakeSuccessfulSTSResult(const std::string& akid = "NEW_AKID",
                            const std::string& secret = "NEW_SECRET",
                            int expiresInSeconds = 7200) {
        STSResult result;
        result.success = true;
        result.creds.SetAWSAccessKeyId(akid.c_str());
        result.creds.SetAWSSecretKey(secret.c_str());
        result.creds.SetSessionToken("NEW_SESSION_TOKEN");
        result.creds.SetExpiration(
            Aws::Utils::DateTime::Now() +
            std::chrono::milliseconds(expiresInSeconds * 1000));
        return result;
    }

    std::string tokenFilePath_;
};

Aws::SDKOptions HuaweiCloudCredentialsProviderTest::sdkOptions_;

// ============================================================================
// Group 1: IsInCooldown tests
// ============================================================================

TEST_F(HuaweiCloudCredentialsProviderTest,
       IsInCooldownReturnsFalseWhenNotFailed) {
    CreateProviderWithMock();
    Helper::setLastReloadFailed(*provider_, false);
    EXPECT_FALSE(Helper::isInCooldown(*provider_));
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       IsInCooldownUrgentWithEmptyCredentials) {
    // Empty credentials + just failed → urgent cooldown (5s), should be true
    CreateProviderWithMock();
    // Credentials are empty by default
    Helper::setLastReloadFailed(
        *provider_, true, std::chrono::steady_clock::now());
    EXPECT_TRUE(Helper::isInCooldown(*provider_));
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       IsInCooldownUrgentExpiresAfterTimeout) {
    // Empty credentials + failed 6s ago → urgent cooldown (5s) expired
    CreateProviderWithMock();
    Helper::setLastReloadFailed(
        *provider_,
        true,
        std::chrono::steady_clock::now() - std::chrono::seconds(6));
    EXPECT_FALSE(Helper::isInCooldown(*provider_));
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       IsInCooldownNormalWithValidCredentials) {
    // Valid (non-expired) credentials + just failed → normal cooldown (30s)
    CreateProviderWithMock();
    Helper::setCredentials(*provider_, MakeFreshCredentials());
    Helper::setLastReloadFailed(
        *provider_, true, std::chrono::steady_clock::now());
    EXPECT_TRUE(Helper::isInCooldown(*provider_));
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       IsInCooldownNormalExpiresAfterTimeout) {
    // Valid credentials + failed 31s ago → normal cooldown (30s) expired
    CreateProviderWithMock();
    Helper::setCredentials(*provider_, MakeFreshCredentials());
    Helper::setLastReloadFailed(
        *provider_,
        true,
        std::chrono::steady_clock::now() - std::chrono::seconds(31));
    EXPECT_FALSE(Helper::isInCooldown(*provider_));
}

// ============================================================================
// Group 2: ExpiresSoon tests
// ============================================================================

TEST_F(HuaweiCloudCredentialsProviderTest,
       ExpiresSoonReturnsFalseForFreshCredentials) {
    CreateProviderWithMock();
    Helper::setCredentials(*provider_, MakeFreshCredentials(7200));
    EXPECT_FALSE(Helper::expiresSoon(*provider_));
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       ExpiresSoonReturnsTrueForExpiringCredentials) {
    // Grace period is 180s (180000ms), credentials expiring in 60s → true
    CreateProviderWithMock();
    Helper::setCredentials(*provider_, MakeExpiringSoonCredentials(60));
    EXPECT_TRUE(Helper::expiresSoon(*provider_));
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       ExpiresSoonReturnsTrueForExpiredCredentials) {
    CreateProviderWithMock();
    Helper::setCredentials(*provider_, MakeExpiredCredentials());
    EXPECT_TRUE(Helper::expiresSoon(*provider_));
}

// ============================================================================
// Group 3: Reload error handling tests
// ============================================================================

TEST_F(HuaweiCloudCredentialsProviderTest,
       ReloadSetsFailedOnTokenFileNotFound) {
    CreateProviderWithMock();
    auto oldCreds = MakeFreshCredentials();
    Helper::setCredentials(*provider_, oldCreds);
    Helper::setTokenFile(*provider_, "/tmp/nonexistent_token_file_12345.txt");

    Helper::callReload(*provider_);

    EXPECT_TRUE(Helper::lastReloadFailed(*provider_));
    // Old credentials should be retained
    EXPECT_EQ(Helper::getCredentials(*provider_).GetAWSAccessKeyId(),
              oldCreds.GetAWSAccessKeyId());
}

TEST_F(HuaweiCloudCredentialsProviderTest, ReloadSuccessUpdatesCreds) {
    CreateProviderWithMock();
    Helper::setCredentials(*provider_, MakeFreshCredentials());
    Helper::setTokenFile(*provider_, tokenFilePath_);

    auto stsResult = MakeSuccessfulSTSResult("FRESH_AKID", "FRESH_SECRET");
    EXPECT_CALL(*mock_, GetAssumeRoleWithWebIdentityCredentials(testing::_))
        .WillOnce(testing::Return(stsResult));

    Helper::callReload(*provider_);

    EXPECT_FALSE(Helper::lastReloadFailed(*provider_));
    EXPECT_EQ(Helper::getCredentials(*provider_).GetAWSAccessKeyId(),
              "FRESH_AKID");
    EXPECT_EQ(Helper::getCredentials(*provider_).GetAWSSecretKey(),
              "FRESH_SECRET");
}

TEST_F(HuaweiCloudCredentialsProviderTest, ReloadRetainsCredsOnStsFailure) {
    CreateProviderWithMock();
    auto oldCreds = MakeFreshCredentials();
    Helper::setCredentials(*provider_, oldCreds);
    Helper::setTokenFile(*provider_, tokenFilePath_);

    STSResult failResult;
    failResult.success = false;
    EXPECT_CALL(*mock_, GetAssumeRoleWithWebIdentityCredentials(testing::_))
        .WillOnce(testing::Return(failResult));

    Helper::callReload(*provider_);

    EXPECT_TRUE(Helper::lastReloadFailed(*provider_));
    EXPECT_EQ(Helper::getCredentials(*provider_).GetAWSAccessKeyId(),
              oldCreds.GetAWSAccessKeyId());
}

TEST_F(HuaweiCloudCredentialsProviderTest, ReloadRetainsCredsOnEmptyKeys) {
    CreateProviderWithMock();
    auto oldCreds = MakeFreshCredentials();
    Helper::setCredentials(*provider_, oldCreds);
    Helper::setTokenFile(*provider_, tokenFilePath_);

    // STS returns success=true but with empty keys
    STSResult emptyResult;
    emptyResult.success = true;
    emptyResult.creds.SetAWSAccessKeyId("");
    emptyResult.creds.SetAWSSecretKey("");
    EXPECT_CALL(*mock_, GetAssumeRoleWithWebIdentityCredentials(testing::_))
        .WillOnce(testing::Return(emptyResult));

    Helper::callReload(*provider_);

    EXPECT_TRUE(Helper::lastReloadFailed(*provider_));
    EXPECT_EQ(Helper::getCredentials(*provider_).GetAWSAccessKeyId(),
              oldCreds.GetAWSAccessKeyId());
}

// ============================================================================
// Group 4: RefreshIfExpired + cooldown integration tests
// ============================================================================

TEST_F(HuaweiCloudCredentialsProviderTest,
       RefreshIfExpiredSkipsWhenCredsValid) {
    CreateProviderWithMock();
    Helper::setCredentials(*provider_, MakeFreshCredentials(7200));
    Helper::setTokenFile(*provider_, tokenFilePath_);

    // Reload should NOT be called because credentials are still valid
    EXPECT_CALL(*mock_, GetAssumeRoleWithWebIdentityCredentials(testing::_))
        .Times(0);

    Helper::callRefreshIfExpired(*provider_);
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       RefreshIfExpiredCallsReloadWhenCredsEmpty) {
    CreateProviderWithMock();
    // Credentials are empty by default, no cooldown
    Helper::setLastReloadFailed(*provider_, false);
    Helper::setTokenFile(*provider_, tokenFilePath_);

    auto stsResult = MakeSuccessfulSTSResult();
    EXPECT_CALL(*mock_, GetAssumeRoleWithWebIdentityCredentials(testing::_))
        .WillOnce(testing::Return(stsResult));

    Helper::callRefreshIfExpired(*provider_);

    EXPECT_EQ(Helper::getCredentials(*provider_).GetAWSAccessKeyId(),
              "NEW_AKID");
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       RefreshIfExpiredSkipsDuringCooldownWithValidCreds) {
    CreateProviderWithMock();
    // Credentials expiring soon but in normal cooldown (30s)
    Helper::setCredentials(*provider_, MakeExpiringSoonCredentials(60));
    Helper::setLastReloadFailed(
        *provider_, true, std::chrono::steady_clock::now());
    Helper::setTokenFile(*provider_, tokenFilePath_);

    // Should NOT call Reload because we're in 30s cooldown
    EXPECT_CALL(*mock_, GetAssumeRoleWithWebIdentityCredentials(testing::_))
        .Times(0);

    Helper::callRefreshIfExpired(*provider_);
}

TEST_F(HuaweiCloudCredentialsProviderTest,
       RefreshIfExpiredRetriesSoonerWhenCredsEmpty) {
    CreateProviderWithMock();
    // Empty credentials + failed 6s ago → urgent cooldown (5s) has expired
    Helper::setLastReloadFailed(
        *provider_,
        true,
        std::chrono::steady_clock::now() - std::chrono::seconds(6));
    Helper::setTokenFile(*provider_, tokenFilePath_);

    auto stsResult = MakeSuccessfulSTSResult();
    EXPECT_CALL(*mock_, GetAssumeRoleWithWebIdentityCredentials(testing::_))
        .WillOnce(testing::Return(stsResult));

    Helper::callRefreshIfExpired(*provider_);

    EXPECT_FALSE(Helper::lastReloadFailed(*provider_));
    EXPECT_EQ(Helper::getCredentials(*provider_).GetAWSAccessKeyId(),
              "NEW_AKID");
}
