/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/internal/AWSHttpResourceClient.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <memory>
#include "AliyunSTSClient.h"

namespace Aws {
namespace Auth {
/**
 * To support retrieving credentials of STS AssumeRole with web identity.
 * Note that STS accepts request with protocol of queryxml. Calling GetAWSCredentials() will trigger (if expired)
 * a query request using AWSHttpResourceClient under the hood.
 */
class AWS_CORE_API AliyunSTSAssumeRoleWebIdentityCredentialsProvider
    : public AWSCredentialsProvider {
 public:
    AliyunSTSAssumeRoleWebIdentityCredentialsProvider();

    /**
     * Retrieves the credentials if found, otherwise returns empty credential set.
     */
    AWSCredentials
    GetAWSCredentials() override;

 protected:
    void
    Reload() override;

 private:
    void
    RefreshIfExpired();
    Aws::String
    CalculateQueryString() const;

    Aws::UniquePtr<Aws::Internal::AliyunSTSCredentialsClient> m_client;
    Aws::Auth::AWSCredentials m_credentials;
    Aws::String m_roleArn;
    Aws::String m_tokenFile;
    Aws::String m_sessionName;
    Aws::String m_token;
    bool m_initialized;
    bool
    ExpiresSoon() const;
};
}  // namespace Auth
}  // namespace Aws
