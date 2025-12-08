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

#include "HuaweiCloudSTSClient.h"
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/utils/DateTime.h>

namespace Aws {
namespace Http {
class HttpClient;
class HttpRequest;
enum class HttpResponseCode;
}  // namespace Http

namespace Internal {

static const char STS_RESOURCE_CLIENT_LOG_TAG[] =
    "HuaweiCloudSTSResourceClient";

HuaweiCloudSTSCredentialsClient::HuaweiCloudSTSCredentialsClient(
    const Aws::Client::ClientConfiguration& clientConfiguration)
    : AWSHttpResourceClient(clientConfiguration, STS_RESOURCE_CLIENT_LOG_TAG) {
    SetErrorMarshaller(Aws::MakeUnique<Aws::Client::XmlErrorMarshaller>(
        STS_RESOURCE_CLIENT_LOG_TAG));
    m_token_endpoint =
        "https://iam.{region}.myhuaweicloud.com/v3.0/OS-AUTH/id-token/tokens";
    m_httpClient = Aws::Http::CreateHttpClient(clientConfiguration);
    AWS_LOGSTREAM_INFO(
        STS_RESOURCE_CLIENT_LOG_TAG,
        "Creating STS ResourceClient with endpoint: " << m_token_endpoint);
}

HuaweiCloudSTSCredentialsClient::STSAssumeRoleWithWebIdentityResult
HuaweiCloudSTSCredentialsClient::GetAssumeRoleWithWebIdentityCredentials(
    const STSAssumeRoleWithWebIdentityRequest& request) {
    Aws::StringStream ss;
    ss << R"({
        "auth": {
          "id_token": {
            "id": ")"
       << request.webIdentityToken << R"("
          },
          "scope": {
            "project": {
              "id": ")"
       << request.roleArn << R"("
            }
          }
        }
      })";

    Aws::String endpoint = m_token_endpoint;
    size_t pos = endpoint.find("{region}");
    if (pos != Aws::String::npos) {
        endpoint.replace(pos, 8, request.region);
    }
    std::shared_ptr<Aws::Http::HttpRequest> httpRequest(
        Aws::Http::CreateHttpRequest(
            endpoint,
            Aws::Http::HttpMethod::HTTP_POST,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
    httpRequest->SetUserAgent(Aws::Client::ComputeUserAgentString());
    httpRequest->SetHeaderValue("X-Idp-Id", request.providerId);
    httpRequest->SetHeaderValue("Content-Type", "application/json");

    std::shared_ptr<Aws::IOStream> body =
        Aws::MakeShared<Aws::StringStream>("STS_RESOURCE_CLIENT_LOG_TAG");
    *body << ss.str();
    body->seekg(0, body->end);
    auto streamSize = body->tellg();
    body->seekg(0, body->beg);
    httpRequest->SetContentLength(std::to_string(streamSize));
    httpRequest->AddContentBody(body);
    httpRequest->SetContentType("application/json; charset=utf-8");

    STSAssumeRoleWithWebIdentityResult result;

    auto awsResult = GetResourceWithAWSWebServiceResult(httpRequest);
    auto responseCode = awsResult.GetResponseCode();
    if (responseCode != Aws::Http::HttpResponseCode::OK &&
        responseCode != Aws::Http::HttpResponseCode::CREATED) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Failed to get credentials token from Huawei Cloud "
                           "STS, response code: "
                               << static_cast<int>(responseCode));
        return result;
    }

    auto responseHeaders = awsResult.GetHeaderValueCollection();
    auto subjectTokenIter = responseHeaders.find("x-subject-token");
    if (subjectTokenIter == responseHeaders.end()) {
        AWS_LOGSTREAM_WARN(
            STS_RESOURCE_CLIENT_LOG_TAG,
            "No x-subject-token in huawei cloud sts response headers");
        return result;
    }

    const Aws::String subjectToken = subjectTokenIter->second;
    auto stsResult = callHuaweiCloudSTS(subjectToken, request);
    if (!stsResult.success) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Failed to get credentials from Huawei Cloud STS: "
                               << stsResult.errorMessage);
        return result;
    }

    result.creds = stsResult.credentials;
    return result;
}

HuaweiCloudSTSCredentialsClient::STSCallResult
HuaweiCloudSTSCredentialsClient::callHuaweiCloudSTS(
    const Aws::String& userToken,
    const STSAssumeRoleWithWebIdentityRequest& request) {
    auto respFactory = []() -> IOStream* {
        return Aws::New<StringStream>("STS_RESPONSE");
    };

    Aws::String stsEndpoint =
        "https://iam." + request.region +
        ".myhuaweicloud.com/v3.0/OS-CREDENTIAL/securitytokens";
    auto req = Aws::Http::CreateHttpRequest(
        stsEndpoint, Http::HttpMethod::HTTP_POST, respFactory);
    req->SetHeaderValue("Content-Type", "application/json;charset=utf8");
    req->SetHeaderValue("Accept", "application/json");
    req->SetHeaderValue("X-Auth-Token", userToken);

    auto body = Aws::MakeShared<StringStream>("STS_REQUEST");
    *body << R"({
            "auth": {
            "identity": {
                "methods": ["token"]
            }
            }
        })";
    body->seekg(0, body->end);
    auto streamSize = body->tellg();
    body->seekg(0, body->beg);

    req->SetContentLength(std::to_string(streamSize));
    req->AddContentBody(body);

    auto resp = m_httpClient->MakeRequest(req);
    std::ostringstream oss;
    oss << resp->GetResponseBody().rdbuf();
    Aws::String credentialsStr = oss.str();
    STSCallResult result;
    if (credentialsStr.empty()) {
        result.errorMessage = "Get an empty credential from Huawei Cloud STS";
        return result;
    }
    auto json = Utils::Json::JsonView(credentialsStr);
    auto rootNode = json.GetObject("credential");
    if (rootNode.IsNull()) {
        result.errorMessage = "Get credential from STS result failed";
        return result;
    }
    result.credentials.SetAWSAccessKeyId(rootNode.GetString("access"));
    result.credentials.SetAWSSecretKey(rootNode.GetString("secret"));
    result.credentials.SetSessionToken(rootNode.GetString("securitytoken"));

    auto expiresAt = rootNode.GetString("expires_at");
    if (!expiresAt.empty()) {
        result.credentials.SetExpiration(Aws::Utils::DateTime(
            Aws::Utils::StringUtils::Trim(expiresAt.c_str()).c_str(),
            Aws::Utils::DateFormat::ISO_8601));
    }
    result.success = true;
    return result;
}

}  // namespace Internal
}  // namespace Aws