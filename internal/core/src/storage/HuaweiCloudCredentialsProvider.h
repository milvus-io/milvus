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

#include <aws/core/auth/AWSCredentialsProvider.h>
#include "HuaweiCloudSTSClient.h"

namespace Aws {
namespace Auth {
class HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider
    : public AWSCredentialsProvider {
 public:
    HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider();
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

    Aws::UniquePtr<Aws::Internal::HuaweiCloudSTSCredentialsClient> m_client;
    Aws::Auth::AWSCredentials m_credentials;
    Aws::String m_region;
    Aws::String m_providerId;
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