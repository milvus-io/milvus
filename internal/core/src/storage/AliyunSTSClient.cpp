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
#include "AliyunSTSClient.h"

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

static const char STS_RESOURCE_CLIENT_LOG_TAG[] =
    "AliyunSTSResourceClient";  // [aliyun]

int
IntRand(const int& min, const int& max) {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(min, max);
    return distribution(generator);
}

AliyunSTSCredentialsClient::AliyunSTSCredentialsClient(
    const Aws::Client::ClientConfiguration& clientConfiguration)
    : AWSHttpResourceClient(clientConfiguration, STS_RESOURCE_CLIENT_LOG_TAG) {
    m_aliyunOidcProviderArn =
        Aws::Environment::GetEnv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN");
    if (m_aliyunOidcProviderArn.empty()) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "oidc role arn must be specified to use STS "
                           "AssumeRole web identity creds provider.");
        return;
    }
    SetErrorMarshaller(Aws::MakeUnique<Aws::Client::XmlErrorMarshaller>(
        STS_RESOURCE_CLIENT_LOG_TAG));

    // [aliyun]
    m_endpoint = "https://sts.aliyuncs.com";

    AWS_LOGSTREAM_INFO(
        STS_RESOURCE_CLIENT_LOG_TAG,
        "Creating STS ResourceClient with endpoint: " << m_endpoint);
}

AliyunSTSCredentialsClient::STSAssumeRoleWithWebIdentityResult
AliyunSTSCredentialsClient::GetAssumeRoleWithWebIdentityCredentials(
    const STSAssumeRoleWithWebIdentityRequest& request) {
    // Calculate query string
    Aws::StringStream ss;
    // [aliyun]
    // linux curl example:
    // Action=AssumeRoleWithOIDC
    // Timestamp=`date -Iseconds`
    // Version="2015-04-01"
    // SignatureNonce=`$RANDOM`
    // RoleSessionName="default_session"
    // RoleArn=$ALIBABA_CLOUD_ROLE_ARN
    // OIDCProviderArn=$ALIBABA_CLOUD_OIDC_PROVIDER_ARN
    // OIDCToken=`cat $ALIBABA_CLOUD_OIDC_TOKEN_FILE`
    // curl "https://sts.aliyuncs.com?Action=$Action&Timestamp=$time" \
            //     -H "Host: sts.aliyuncs.com" \
            //     -H "Accept-Encoding: identity" \
            //     -H "SignatureNonce: $SignatureNonce" \
            //     -d
    //     "RoleArn=$RoleArn&OIDCProviderArn=$OIDCProviderArn&OIDCToken=$OIDCToken&RoleSessionName=$RoleSessionName&Version=$Version"
    ss << "Action=AssumeRoleWithOIDC"
       << "&Timestamp=" /*iso8601*/
       << Aws::Utils::StringUtils::URLEncode(
              Aws::Utils::DateTime::Now()
                  .ToGmtString(Aws::Utils::DateFormat::ISO_8601)
                  .c_str())
       << "&Version=2015-04-01"
       << "&SignatureNonce="
       << Aws::Utils::HashingUtils::HashString(
              Aws::Utils::StringUtils::to_string(IntRand(0, INT_MAX)).c_str())
       << "&RoleSessionName="
       << Aws::Utils::StringUtils::URLEncode(request.roleSessionName.c_str())
       << "&RoleArn="
       << Aws::Utils::StringUtils::URLEncode(request.roleArn.c_str())
       << "&OIDCProviderArn="
       << Aws::Utils::StringUtils::URLEncode(m_aliyunOidcProviderArn.c_str())
       << "&OIDCToken="
       << Aws::Utils::StringUtils::URLEncode(request.webIdentityToken.c_str());

    std::shared_ptr<Aws::Http::HttpRequest> httpRequest(
        Aws::Http::CreateHttpRequest(
            m_endpoint,
            Aws::Http::HttpMethod::HTTP_POST,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));

    httpRequest->SetUserAgent(Aws::Client::ComputeUserAgentString());

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
    httpRequest->SetContentType("application/x-www-form-urlencoded");

    Aws::String credentialsStr =
        GetResourceWithAWSWebServiceResult(httpRequest).GetPayload();

    // Parse credentials
    STSAssumeRoleWithWebIdentityResult result;
    if (credentialsStr.empty()) {
        AWS_LOGSTREAM_WARN(STS_RESOURCE_CLIENT_LOG_TAG,
                           "Get an empty credential from sts");
        return result;
    }

    // [aliyun] output example
    // <?xml version='1.0' encoding='UTF-8'?>
    // <AssumeRoleWithOIDCResponse>
    //     <RequestId>D94AFCC3-54CA-508C-B6AF-21481E761BDB</RequestId>
    //     <OIDCTokenInfo>
    //         <Issuer>https://oidc-ack-cn-shanghai.oss-cn-shanghai.aliyuncs.com/c532c4ce5e84048a1972535df283f737d</Issuer>
    //         <Subject>system:serviceaccount:default:my-release</Subject>
    //         <ClientIds>sts.aliyuncs.com</ClientIds>
    //     </OIDCTokenInfo>
    //     <AssumedRoleUser>
    //         <Arn>acs:ram::1413891078881348:role/vdc-poc-milvus/default</Arn>
    //         <AssumedRoleId>383373758575348335:default</AssumedRoleId>
    //     </AssumedRoleUser>
    //     <Credentials>
    //         <SecurityToken>xxx</SecurityToken>
    //         <AccessKeyId>xxx</AccessKeyId>
    //         <AccessKeySecret>xxx</AccessKeySecret>
    //         <Expiration>2023-03-02T07:39:09Z</Expiration>
    //     </Credentials>
    // </AssumeRoleWithOIDCResponse>
    const Utils::Xml::XmlDocument xmlDocument =
        Utils::Xml::XmlDocument::CreateFromXmlString(credentialsStr);
    Utils::Xml::XmlNode rootNode = xmlDocument.GetRootElement();
    Utils::Xml::XmlNode resultNode = rootNode;
    if (!rootNode.IsNull() &&
        (rootNode.GetName() != "AssumeRoleWithOIDCResponse")) {
        resultNode =
            rootNode.FirstChild("AssumeRoleWithOIDCResponse");  // [aliyun]
    }

    if (!resultNode.IsNull()) {
        Utils::Xml::XmlNode credentialsNode =
            resultNode.FirstChild("Credentials");
        if (!credentialsNode.IsNull()) {
            Utils::Xml::XmlNode accessKeyIdNode =
                credentialsNode.FirstChild("AccessKeyId");
            if (!accessKeyIdNode.IsNull()) {
                result.creds.SetAWSAccessKeyId(accessKeyIdNode.GetText());
            }

            Utils::Xml::XmlNode secretAccessKeyNode =
                credentialsNode.FirstChild("AccessKeySecret");  // [aliyun]
            if (!secretAccessKeyNode.IsNull()) {
                result.creds.SetAWSSecretKey(secretAccessKeyNode.GetText());
            }

            Utils::Xml::XmlNode sessionTokenNode =
                credentialsNode.FirstChild("SecurityToken");  // [aliyun]
            if (!sessionTokenNode.IsNull()) {
                result.creds.SetSessionToken(sessionTokenNode.GetText());
            }

            Utils::Xml::XmlNode expirationNode =
                credentialsNode.FirstChild("Expiration");
            if (!expirationNode.IsNull()) {
                result.creds.SetExpiration(
                    Aws::Utils::DateTime(Aws::Utils::StringUtils::Trim(
                                             expirationNode.GetText().c_str())
                                             .c_str(),
                                         Aws::Utils::DateFormat::ISO_8601));
            }
        }
    }
    return result;
}
}  // namespace Internal
}  // namespace Aws
