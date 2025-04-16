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

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <mutex>
#include <sstream>
#include <aws/core/internal/AWSHttpResourceClient.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/client/AWSError.h>
#include "TencentCloudSTSClient.h"
#include <aws/core/client/ClientConfiguration.h>

namespace Aws {
namespace Http {
class HttpClient;
class HttpRequest;
enum class HttpResponseCode;
}  // namespace Http

namespace Internal {

static const char STS_RESOURCE_CLIENT_LOG_TAG[] =
    "TencentCloudSTSResourceClient";  // [tencent cloud]

TencentCloudSTSCredentialsClient::TencentCloudSTSCredentialsClient(
    const Aws::Client::ClientConfiguration& clientConfiguration)
    : AWSHttpResourceClient(clientConfiguration, STS_RESOURCE_CLIENT_LOG_TAG) {
    SetErrorMarshaller(Aws::MakeUnique<Aws::Client::XmlErrorMarshaller>(
        STS_RESOURCE_CLIENT_LOG_TAG));

    // [tencent cloud]
    m_endpoint = "https://sts.tencentcloudapi.com";

    AWS_LOGSTREAM_INFO(
        STS_RESOURCE_CLIENT_LOG_TAG,
        "Creating STS ResourceClient with endpoint: " << m_endpoint);
}

TencentCloudSTSCredentialsClient::STSAssumeRoleWithWebIdentityResult
TencentCloudSTSCredentialsClient::GetAssumeRoleWithWebIdentityCredentials(
    const STSAssumeRoleWithWebIdentityRequest& request) {
    // Calculate query string
    Aws::StringStream ss;
    // curl -X POST "https://sts.tencentcloudapi.com"
    // -d "{\"ProviderId\": $ProviderId, \"WebIdentityToken\": $WebIdentityToken,\"RoleArn\":$RoleArn,\"RoleSessionName\":$RoleSessionName,\"DurationSeconds\":7200}"
    // -H "Authorization: SKIP"
    // -H "Content-Type: application/json; charset=utf-8"
    // -H "Host: sts.tencentcloudapi.com"
    // -H "X-TC-Action: AssumeRoleWithWebIdentity"
    // -H "X-TC-Timestamp: $timestamp"
    // -H "X-TC-Version: 2018-08-13"
    // -H "X-TC-Region: $region"
    // -H "X-TC-Token: $token"

    ss << R"({"ProviderId": ")" << request.providerId
       << R"(", "WebIdentityToken": ")" << request.webIdentityToken
       << R"(", "RoleArn": ")" << request.roleArn
       << R"(", "RoleSessionName": ")" << request.roleSessionName << R"("})";

    std::shared_ptr<Aws::Http::HttpRequest> httpRequest(
        Aws::Http::CreateHttpRequest(
            m_endpoint,
            Aws::Http::HttpMethod::HTTP_POST,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));

    httpRequest->SetUserAgent(Aws::Client::ComputeUserAgentString());
    httpRequest->SetHeaderValue("Authorization", "SKIP");
    httpRequest->SetHeaderValue("Host", "sts.tencentcloudapi.com");
    httpRequest->SetHeaderValue("X-TC-Action", "AssumeRoleWithWebIdentity");
    httpRequest->SetHeaderValue(
        "X-TC-Timestamp",
        std::to_string(Aws::Utils::DateTime::Now().Seconds()));
    httpRequest->SetHeaderValue("X-TC-Version", "2018-08-13");
    httpRequest->SetHeaderValue("X-TC-Region", request.region);
    httpRequest->SetHeaderValue("X-TC-Token", "");

    std::shared_ptr<Aws::IOStream> body =
        Aws::MakeShared<Aws::StringStream>("STS_RESOURCE_CLIENT_LOG_TAG");
    *body << ss.str();

    httpRequest->AddContentBody(body);
    body->seekg(0, body->end);
    auto streamSize = body->tellg();
    body->seekg(0, body->beg);
    Aws::StringStream contentLength;
    contentLength << streamSize;
    httpRequest->SetContentLength(contentLength.str());
    //    httpRequest->SetContentType("application/x-www-form-urlencoded");
    httpRequest->SetContentType("application/json; charset=utf-8");

    auto headers = httpRequest->GetHeaders();
    Aws::String credentialsStr =
        GetResourceWithAWSWebServiceResult(httpRequest).GetPayload();

    // Parse credentials
    STSAssumeRoleWithWebIdentityResult result;
    if (credentialsStr.empty()) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Get an empty credential from sts");
        return result;
    }

    auto json = Utils::Json::JsonView(credentialsStr);
    auto rootNode = json.GetObject("Response");
    if (rootNode.IsNull()) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Get Response from credential result failed");
        return result;
    }

    auto credentialsNode = rootNode.GetObject("Credentials");
    if (credentialsNode.IsNull()) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Get Credentials from Response failed");
        return result;
    }
    result.creds.SetAWSAccessKeyId(credentialsNode.GetString("TmpSecretId"));
    result.creds.SetAWSSecretKey(credentialsNode.GetString("TmpSecretKey"));
    result.creds.SetSessionToken(credentialsNode.GetString("Token"));
    result.creds.SetExpiration(Aws::Utils::DateTime(
        Aws::Utils::StringUtils::Trim(rootNode.GetString("Expiration").c_str())
            .c_str(),
        Aws::Utils::DateFormat::ISO_8601));

    return result;
}
}  // namespace Internal
}  // namespace Aws
