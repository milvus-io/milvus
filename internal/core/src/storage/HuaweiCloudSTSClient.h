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

#pragma once

#include <aws/core/internal/AWSHttpResourceClient.h>

namespace Aws {
namespace Http {
class HttpClient;
class HttpRequest;
enum class HttpResponseCode;
}  // namespace Http

namespace Internal {
class AWS_CORE_API HuaweiCloudSTSCredentialsClient
    : public AWSHttpResourceClient {
 public:
    explicit HuaweiCloudSTSCredentialsClient(
        const Aws::Client::ClientConfiguration& clientConfiguration);

    HuaweiCloudSTSCredentialsClient&
    operator=(HuaweiCloudSTSCredentialsClient& rhs) = delete;
    HuaweiCloudSTSCredentialsClient(
        const HuaweiCloudSTSCredentialsClient& rhs) = delete;
    HuaweiCloudSTSCredentialsClient&
    operator=(HuaweiCloudSTSCredentialsClient&& rhs) = delete;
    HuaweiCloudSTSCredentialsClient(
        const HuaweiCloudSTSCredentialsClient&& rhs) = delete;

    struct STSAssumeRoleWithWebIdentityRequest {
        Aws::String region;
        Aws::String providerId;
        Aws::String webIdentityToken;
        Aws::String roleArn;
        Aws::String roleSessionName;
    };

    struct STSAssumeRoleWithWebIdentityResult {
        Aws::Auth::AWSCredentials creds;
    };

    STSAssumeRoleWithWebIdentityResult
    GetAssumeRoleWithWebIdentityCredentials(
        const STSAssumeRoleWithWebIdentityRequest& request);

 private:
    Aws::String m_token_endpoint;
    std::shared_ptr<Aws::Http::HttpClient> m_httpClient;

    struct STSCallResult {
        bool success;
        Aws::Auth::AWSCredentials credentials;
        Aws::String errorMessage;
    };

    STSCallResult
    callHuaweiCloudSTS(const Aws::String& userToken,
                       const STSAssumeRoleWithWebIdentityRequest& request);
};
}  // namespace Internal
}  // namespace Aws