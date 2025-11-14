// Copyright C 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <aws/core/internal/AWSHttpResourceClient.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <limits.h>
#include <mutex>
#include <sstream>
#include <random>
#include <iostream>
#include "VolcengineSTSClient.h"
#include <fstream>
#include <nlohmann/json.hpp>

namespace Aws {
namespace Http {
class HttpClient;
class HttpRequest;
enum class HttpResponseCode;
}  // namespace Http

namespace Client {
Aws::String
ComputeUserAgentString();
}

namespace Internal {

static const char STS_RESOURCE_CLIENT_LOG_TAG[] = "VolcengineSTSResourceClient";
static const int DefaultDurationSeconds = 8 * 60 * 60;  // 8h

VolcengineSTSCredentialsClient::VolcengineSTSCredentialsClient(
    const Aws::Client::ClientConfiguration& clientConfiguration)
    : AWSHttpResourceClient(clientConfiguration, STS_RESOURCE_CLIENT_LOG_TAG) {
    SetErrorMarshaller(Aws::MakeUnique<Aws::Client::XmlErrorMarshaller>(
        STS_RESOURCE_CLIENT_LOG_TAG));

    m_endpoint = Aws::Environment::GetEnv("VOLCENGINE_OIDC_STS_ENDPOINT");
    if (m_endpoint.empty()) {
        m_endpoint = "sts.volcengineapi.com";
    }

    AWS_LOGSTREAM_INFO(
        STS_RESOURCE_CLIENT_LOG_TAG,
        "Creating STS ResourceClient with endpoint: " << m_endpoint);
}

VolcengineSTSCredentialsClient::STSAssumeRoleWithWebIdentityResult
VolcengineSTSCredentialsClient::GetAssumeRoleWithWebIdentityCredentials(
    const STSAssumeRoleWithWebIdentityRequest& request) {
    Aws::StringStream ss;
    ss << "OIDCToken="
       << Aws::Utils::StringUtils::URLEncode(request.webIdentityToken.c_str())
       << "&DurationSeconds="
       << Aws::Utils::StringUtils::URLEncode(DefaultDurationSeconds)
       << "&RoleSessionName="
       << Aws::Utils::StringUtils::URLEncode(request.roleSessionName.c_str());

    Aws::StringStream urlStream;
    urlStream << "https://" << m_endpoint << "/?Action=AssumeRoleWithOIDC"
              << "&Version=2018-01-01"
              << "&RoleTrn="
              << Aws::Utils::StringUtils::URLEncode(request.roleArn.c_str());

    std::shared_ptr<Aws::Http::HttpRequest> httpRequest(
        Aws::Http::CreateHttpRequest(
            urlStream.str(),
            Aws::Http::HttpMethod::HTTP_POST,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));

    httpRequest->SetHeaderValue("Host", m_endpoint);
    httpRequest->SetHeaderValue("Content-Type",
                                "application/x-www-form-urlencoded");

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

    Aws::String credentialsStr =
        GetResourceWithAWSWebServiceResult(httpRequest).GetPayload();

    // Parse credentials
    STSAssumeRoleWithWebIdentityResult result;
    if (credentialsStr.empty()) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Get an empty credential from sts");
        return result;
    }

    nlohmann::json jsonDoc = nlohmann::json::parse(credentialsStr);
    if (!jsonDoc.contains("Result")) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Get 'Result' node from response failed");
        return result;
    }
    auto& resultNode = jsonDoc["Result"];

    if (!resultNode.contains("Credentials")) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Get 'Credentials' node from Result failed");
        return result;
    }
    auto& credentialsNode = resultNode["Credentials"];

    result.creds.SetAWSAccessKeyId(
        credentialsNode["AccessKeyId"].get<std::string>());
    result.creds.SetAWSSecretKey(
        credentialsNode["SecretAccessKey"].get<std::string>());
    result.creds.SetSessionToken(
        credentialsNode["SessionToken"].get<std::string>());
    std::string expiredTimeStr =
        credentialsNode["Expiration"].get<std::string>();
    result.creds.SetExpiration(Aws::Utils::DateTime(
        Aws::Utils::StringUtils::Trim(expiredTimeStr.c_str()).c_str(),
        Aws::Utils::DateFormat::ISO_8601));

    return result;
}
}  // namespace Internal
}  // namespace Aws