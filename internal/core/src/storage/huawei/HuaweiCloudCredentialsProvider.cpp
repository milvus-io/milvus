// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "HuaweiCloudCredentialsProvider.h"
#include <fstream>
#include "HuaweiCloudSTSClient.h"
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/utils/UUID.h>
#include "log/Log.h"

static const char STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG[] =
    "HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider";
static const int STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD =
    7200;  // huawei cloud support 7200s.
namespace Aws {
namespace Auth {

HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider::
    HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider()
    : m_initialized(false) {
    m_region = Aws::Environment::GetEnv("HUAWEICLOUD_SDK_REGION");
    m_roleArn = Aws::Environment::GetEnv("HUAWEICLOUD_SDK_PROJECT_ID");
    m_tokenFile = Aws::Environment::GetEnv("HUAWEICLOUD_SDK_ID_TOKEN_FILE");
    m_providerId = Aws::Environment::GetEnv("HUAWEICLOUD_SDK_IDP_ID");
    LOG_INFO(
        "HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider: region={} "
        "roleArn={} tokenFile={} providerId={}",
        m_region,
        m_roleArn,
        m_tokenFile,
        m_providerId);

    auto currentTimePoint = std::chrono::high_resolution_clock::now();
    auto nanoseconds = std::chrono::time_point_cast<std::chrono::nanoseconds>(
        currentTimePoint);
    auto timestamp = nanoseconds.time_since_epoch().count();
    m_sessionName = "huaweicloud-cpp-sdk-" + std::to_string(timestamp / 1000);

    if (m_roleArn.empty() || m_tokenFile.empty() || m_region.empty()) {
        auto profile = Aws::Config::GetCachedConfigProfile(
            Aws::Auth::GetConfigProfileName());
        m_roleArn = profile.GetRoleArn();
        m_tokenFile = profile.GetValue("web_identity_token_file");
        m_sessionName = profile.GetValue("role_session_name");
    }

    if (m_tokenFile.empty()) {
        AWS_LOGSTREAM_WARN(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                           "Token file must be specified to use STS AssumeRole "
                           "web identity creds provider.");
        return;  // No need to do further constructing
    } else {
        AWS_LOGSTREAM_DEBUG(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                            "Resolved token_file from profile_config or "
                            "environment variable to be "
                                << m_tokenFile);
    }

    if (m_roleArn.empty()) {
        AWS_LOGSTREAM_WARN(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                           "RoleArn must be specified to use STS AssumeRole "
                           "web identity creds provider.");
        return;  // No need to do further constructing
    } else {
        AWS_LOGSTREAM_DEBUG(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                            "Resolved role_arn from profile_config or "
                            "environment variable to be "
                                << m_roleArn);
    }

    if (m_region.empty()) {
        AWS_LOGSTREAM_WARN(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                           "Region must be specified to use STS AssumeRole "
                           "web identity creds provider.");
        return;  // No need to do further constructing
    } else {
        AWS_LOGSTREAM_DEBUG(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                            "Resolved region from profile_config or "
                            "environment variable to be "
                                << m_region);
    }

    if (m_sessionName.empty()) {
        m_sessionName = Aws::Utils::UUID::RandomUUID();
    } else {
        AWS_LOGSTREAM_DEBUG(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                            "Resolved session_name from profile_config or "
                            "environment variable to be "
                                << m_sessionName);
    }

    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTPS;
    config.region = m_region;

    Aws::Vector<Aws::String> retryableErrors;
    retryableErrors.push_back("IDPCommunicationError");
    retryableErrors.push_back("InvalidIdentityToken");

    config.retryStrategy =
        Aws::MakeShared<Aws::Client::SpecifiedRetryableErrorsRetryStrategy>(
            STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
            retryableErrors,
            3 /*maxRetries*/);

    m_client = Aws::MakeUnique<Aws::Internal::HuaweiCloudSTSCredentialsClient>(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG, config);
    m_initialized = true;
    AWS_LOGSTREAM_INFO(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Creating STS AssumeRole with web identity creds provider.");
}

Aws::Auth::AWSCredentials
HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider::GetAWSCredentials() {
    if (!m_initialized) {
        return Aws::Auth::AWSCredentials();
    }
    RefreshIfExpired();
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    return m_credentials;
}

void
HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider::Reload() {
    AWS_LOGSTREAM_INFO(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Credentials have expired, attempting to renew from STS.");

    Aws::IFStream tokenFile(m_tokenFile.c_str());
    if (tokenFile) {
        Aws::String token((std::istreambuf_iterator<char>(tokenFile)),
                          std::istreambuf_iterator<char>());
        if (!token.empty() && token.back() == '\n') {
            token.pop_back();
        }
        m_token = token;
    } else {
        AWS_LOGSTREAM_ERROR(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
                            "Can't open token file: " << m_tokenFile);
        return;
    }
    Aws::Internal::HuaweiCloudSTSCredentialsClient::
        STSAssumeRoleWithWebIdentityRequest request{
            m_region, m_providerId, m_token, m_roleArn, m_sessionName};

    auto result = m_client->GetAssumeRoleWithWebIdentityCredentials(request);
    AWS_LOGSTREAM_TRACE(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Successfully retrieved credentials with AWS_ACCESS_KEY: "
            << result.creds.GetAWSAccessKeyId());
    m_credentials = result.creds;
}

bool
HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider::ExpiresSoon() const {
    return (
        (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count() <
        STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
}

void
HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider::RefreshIfExpired() {
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    if (!m_credentials.IsEmpty() && !ExpiresSoon()) {
        return;
    }

    guard.UpgradeToWriterLock();
    if (!m_credentials.IsExpiredOrEmpty() && !ExpiresSoon()) {
        return;
    }

    Reload();
}

}  // namespace Auth
}  // namespace Aws