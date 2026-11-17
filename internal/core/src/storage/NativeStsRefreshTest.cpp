// Copyright 2026 Zilliz
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <aws/core/Aws.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include "storage/aliyun/AliyunCredentialsProvider.h"
#include "storage/tencent/TencentCloudCredentialsProvider.h"
#include "storage/huawei/HuaweiCloudCredentialsProvider.h"
namespace {
using Aws::Auth::AliyunSTSAssumeRoleWebIdentityCredentialsProvider;
using Aws::Auth::HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider;
using Aws::Auth::TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider;

// ============================================================================
// RAII environment variable helper
// ============================================================================

class ScopedEnvVar {
 public:
    ScopedEnvVar(const std::string& name, const std::string& value)
        : name_(name) {
        const char* old = std::getenv(name.c_str());
        if (old) {
            had_value_ = true;
            old_value_ = old;
        }
        setenv(name.c_str(), value.c_str(), 1);
    }

    ~ScopedEnvVar() {
        if (had_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar&
    operator=(const ScopedEnvVar&) = delete;

 private:
    std::string name_;
    std::string old_value_;
    bool had_value_ = false;
};

// Unsets an env var for the scope lifetime, restoring it on destruction.
class ScopedEnvUnset {
 public:
    explicit ScopedEnvUnset(const std::string& name) : name_(name) {
        const char* old = std::getenv(name.c_str());
        if (old) {
            had_value_ = true;
            old_value_ = old;
        }
        unsetenv(name.c_str());
    }

    ~ScopedEnvUnset() {
        if (had_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ScopedEnvUnset(const ScopedEnvUnset&) = delete;
    ScopedEnvUnset&
    operator=(const ScopedEnvUnset&) = delete;

 private:
    std::string name_;
    std::string old_value_;
    bool had_value_ = false;
};

// RAII helper for temporary files
class TempFile {
 public:
    explicit TempFile(const std::string& content) {
        path_ = "/tmp/test_oidc_token_" +
                std::to_string(reinterpret_cast<uintptr_t>(this));
        std::ofstream ofs(path_);
        ofs << content;
        ofs.close();
    }

    ~TempFile() {
        std::remove(path_.c_str());
    }

    [[nodiscard]] const std::string&
    path() const {
        return path_;
    }

    TempFile(const TempFile&) = delete;
    TempFile&
    operator=(const TempFile&) = delete;

 private:
    std::string path_;
};

// ============================================================================
// Mock HTTP infrastructure
// ============================================================================

struct MockResponseSpec {
    Aws::Http::HttpResponseCode code;
    std::string body;
    Aws::Http::HeaderValueCollection headers;
};

class MockHttpClient : public Aws::Http::HttpClient {
 public:
    explicit MockHttpClient(bool consume_request_bodies = false)
        : consume_request_bodies_(consume_request_bodies) {
    }

    std::shared_ptr<Aws::Http::HttpResponse>
    MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request,
        Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
        Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter =
            nullptr) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        recorded_requests_.push_back(request);

        auto uri = request->GetURIString();

        // Find a matching response queue by URL substring
        for (auto& [url_key, q] : response_map_) {
            if (!q.empty() && uri.find(url_key) != Aws::String::npos) {
                auto spec = q.front();
                q.pop();
                return BuildResponse(request, spec);
            }
        }

        // Default: return 404 for unmatched requests (e.g., background SDK requests)
        return BuildResponse(request,
                             {Aws::Http::HttpResponseCode::NOT_FOUND, "", {}});
    }

    void
    EnqueueResponse(const std::string& url_match,
                    Aws::Http::HttpResponseCode code,
                    const std::string& body,
                    const Aws::Http::HeaderValueCollection& headers = {}) {
        response_map_[url_match].push({code, body, headers});
    }

    std::vector<std::shared_ptr<Aws::Http::HttpRequest>>
    GetRecordedRequests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return recorded_requests_;
    }

    std::vector<std::string>
    GetRecordedRequestBodies() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return recorded_request_bodies_;
    }

 private:
    std::shared_ptr<Aws::Http::HttpResponse>
    BuildResponse(const std::shared_ptr<Aws::Http::HttpRequest>& request,
                  const MockResponseSpec& spec) const {
        auto resp = Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>(
            "MockHttpClient", request);
        // Curl and CRT create the response stream before consuming the POST body.
        if (consume_request_bodies_) {
            const auto& body = request->GetContentBody();
            std::string sent_body;
            if (body) {
                sent_body.assign(std::istreambuf_iterator<char>(*body),
                                 std::istreambuf_iterator<char>());
                body->setstate(std::ios::eofbit | std::ios::failbit);
            }
            recorded_request_bodies_.push_back(std::move(sent_body));
        }
        resp->SetResponseCode(spec.code);
        for (const auto& h : spec.headers) {
            resp->AddHeader(h.first, h.second);
        }
        if (!spec.body.empty()) {
            resp->GetResponseBody() << spec.body;
        }
        return resp;
    }

    const bool consume_request_bodies_;
    mutable std::mutex mutex_;
    mutable std::map<std::string, std::queue<MockResponseSpec>> response_map_;
    mutable std::vector<std::shared_ptr<Aws::Http::HttpRequest>>
        recorded_requests_;
    mutable std::vector<std::string> recorded_request_bodies_;
};

class MockHttpClientFactory : public Aws::Http::HttpClientFactory {
 public:
    explicit MockHttpClientFactory(std::shared_ptr<MockHttpClient> client)
        : mock_client_(std::move(client)) {
    }

    [[nodiscard]] std::shared_ptr<Aws::Http::HttpClient>
    CreateHttpClient(const Aws::Client::ClientConfiguration&
                         clientConfiguration) const override {
        return mock_client_;
    }

    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(
        const Aws::String& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override {
        auto request =
            Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
                "MockHttpClientFactory", uri, method);
        request->SetResponseStreamFactory(streamFactory);
        return request;
    }

    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(
        const Aws::Http::URI& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override {
        auto request =
            Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
                "MockHttpClientFactory", uri, method);
        request->SetResponseStreamFactory(streamFactory);
        return request;
    }

 private:
    std::shared_ptr<MockHttpClient> mock_client_;
};

class NativeStsRefreshTest : public ::testing::Test {
 protected:
    inline static Aws::SDKOptions sdk_options_;
    static void
    SetUpTestSuite() {
        Aws::InitAPI(sdk_options_);
    }
    static void
    TearDownTestSuite() {
        Aws::ShutdownAPI(sdk_options_);
    }
    void
    SetUp() override {
        mock_client_ = std::make_shared<MockHttpClient>();
        auto factory = std::make_shared<MockHttpClientFactory>(mock_client_);
        Aws::Http::SetHttpClientFactory(factory);
    }
    void
    TearDown() override {
        Aws::Http::CleanupHttp();
        Aws::Http::InitHttp();
    }
    std::shared_ptr<MockHttpClient> mock_client_;
};
std::string
Expiry(int seconds) {
    return (Aws::Utils::DateTime::Now() + std::chrono::seconds(seconds))
        .ToGmtString(Aws::Utils::DateFormat::ISO_8601)
        .c_str();
}
std::string
TencentJson(const std::string& prefix,
            int seconds = 120,
            const std::string& token = "TOKEN") {
    return "{\"Response\":{\"Credentials\":{\"TmpSecretId\":\"" + prefix +
           "_AK\",\"TmpSecretKey\":\"SK\",\"Token\":\"" + token +
           "\"},\"Expiration\":\"" + Expiry(seconds) + "\"}}";
}
std::string
AliXml(const std::string& root,
       const std::string& prefix,
       int seconds = 120,
       const std::string& token = "TOKEN",
       bool expiry = true) {
    return "<" + root + "><Credentials><AccessKeyId>" + prefix +
           "_AK</AccessKeyId>"
           "<AccessKeySecret>SK</AccessKeySecret><SecurityToken>" +
           token + "</SecurityToken>" +
           (expiry ? "<Expiration>" + Expiry(seconds) + "</Expiration>" : "") +
           "</Credentials></" + root + ">";
}
std::string
HuaweiJson(const std::string& prefix,
           int seconds = 120,
           const std::string& token = "TOKEN") {
    return "{\"credential\":{\"access\":\"" + prefix +
           "_AK\",\"secret\":\"SK\",\"securitytoken\":\"" + token +
           "\",\"expires_at\":\"" + Expiry(seconds) + "\"}}";
}
struct TencentEnv {
    TempFile file{"synthetic_token"};
    ScopedEnvVar region{"TKE_REGION", "ap-guangzhou"};
    ScopedEnvVar role{"TKE_ROLE_ARN", "qcs::cam::uin/123:roleName/machine"};
    ScopedEnvVar path{"TKE_WEB_IDENTITY_TOKEN_FILE", file.path()};
    ScopedEnvVar id{"TKE_PROVIDER_ID", "synthetic_provider"};
};
struct AliEnv {
    TempFile file{"synthetic_token"};
    ScopedEnvVar role{"ALIBABA_CLOUD_ROLE_ARN", "acs:ram::1111:role/machine"};
    ScopedEnvVar path{"ALIBABA_CLOUD_OIDC_TOKEN_FILE", file.path()};
    ScopedEnvVar id{"ALIBABA_CLOUD_OIDC_PROVIDER_ARN",
                    "acs:ram::1111:oidc-provider/synthetic"};
};
struct HuaweiEnv {
    TempFile file{"synthetic_token"};
    ScopedEnvVar region{"HUAWEICLOUD_SDK_REGION", "cn-north-4"};
    ScopedEnvVar role{"HUAWEICLOUD_SDK_PROJECT_ID", "synthetic_project"};
    ScopedEnvVar path{"HUAWEICLOUD_SDK_ID_TOKEN_FILE", file.path()};
    ScopedEnvVar id{"HUAWEICLOUD_SDK_IDP_ID", "synthetic_idp"};
};
void
HuaweiReply(MockHttpClient& mock, const std::string& body) {
    mock.EnqueueResponse("id-token/tokens",
                         Aws::Http::HttpResponseCode::OK,
                         "{}",
                         {{"x-subject-token", "SYNTHETIC"}});
    mock.EnqueueResponse(
        "securitytokens", Aws::Http::HttpResponseCode::OK, body);
}
template <class Provider>
class ReloadableProvider : public Provider {
 public:
    using Provider::Provider;
    void
    RetryNow() {
        Aws::Utils::Threading::WriterLockGuard guard(this->m_reloadLock);
        this->Reload();
    }
};

std::vector<std::string>
InvalidTencentResponses() {
    return {
        "",
        "{broken",
        "[]",
        R"({"Response":[]})",
        TencentJson("BAD", 120, ""),
        TencentJson("EXPIRED", -60),
        R"({"Response":{"Credentials":{"TmpSecretId":42,"TmpSecretKey":"SK","Token":"TOKEN"},"Expiration":"2099-01-01T00:00:00Z"}})"};
}

TEST_F(NativeStsRefreshTest, TencentRejectsFailedCandidatesAndRecovers) {
    TencentEnv env;
    for (const auto& body : InvalidTencentResponses()) {
        SCOPED_TRACE(body);
        mock_client_->EnqueueResponse("sts.tencentcloudapi.com",
                                      Aws::Http::HttpResponseCode::OK,
                                      TencentJson("OLD"));
        mock_client_->EnqueueResponse(
            "sts.tencentcloudapi.com", Aws::Http::HttpResponseCode::OK, body);
        mock_client_->EnqueueResponse("sts.tencentcloudapi.com",
                                      Aws::Http::HttpResponseCode::OK,
                                      TencentJson("NEW", 3600));
        TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider provider;
        const auto old = provider.GetAWSCredentials();
        ASSERT_EQ(old.GetAWSAccessKeyId(), "OLD_AK");
        EXPECT_EQ(provider.GetAWSCredentials(), old);
        EXPECT_EQ(provider.GetAWSCredentials().GetAWSAccessKeyId(), "NEW_AK");
    }
}
TEST_F(NativeStsRefreshTest, TencentHttpFailureRetainsCache) {
    TencentEnv env;
    mock_client_->EnqueueResponse("sts.tencentcloudapi.com",
                                  Aws::Http::HttpResponseCode::OK,
                                  TencentJson("OLD"));
    mock_client_->EnqueueResponse(
        "sts.tencentcloudapi.com",
        Aws::Http::HttpResponseCode::FORBIDDEN,
        "{\"Response\":{\"Error\":{\"Code\":\"AuthFailure\"}}}");
    TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider provider;
    const auto old = provider.GetAWSCredentials();
    ASSERT_EQ(old.GetAWSAccessKeyId(), "OLD_AK");
    EXPECT_EQ(provider.GetAWSCredentials(), old);
}
TEST_F(NativeStsRefreshTest, InitialInvalidCandidatesReturnEmptyAndCanRecover) {
    TencentEnv env;
    for (const auto& body : InvalidTencentResponses()) {
        mock_client_->EnqueueResponse(
            "sts.tencentcloudapi.com", Aws::Http::HttpResponseCode::OK, body);
        mock_client_->EnqueueResponse("sts.tencentcloudapi.com",
                                      Aws::Http::HttpResponseCode::OK,
                                      TencentJson("NEW", 3600));
        TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider provider;
        EXPECT_TRUE(provider.GetAWSCredentials().IsEmpty());
        EXPECT_EQ(provider.GetAWSCredentials().GetAWSAccessKeyId(), "NEW_AK");
    }
}
TEST_F(NativeStsRefreshTest, HuaweiRejectsFailedCandidatesAndRecovers) {
    HuaweiEnv env;
    for (const auto& body : {std::string(""),
                             std::string("{broken"),
                             std::string("[]"),
                             std::string(R"({"credential":42})"),
                             HuaweiJson("BAD", 120, ""),
                             HuaweiJson("EXPIRED", -60)}) {
        HuaweiReply(*mock_client_, HuaweiJson("OLD"));
        HuaweiReply(*mock_client_, body);
        ReloadableProvider<
            HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider>
            provider;
        const auto old = provider.GetAWSCredentials();
        ASSERT_EQ(old.GetAWSAccessKeyId(), "OLD_AK");
        EXPECT_EQ(provider.GetAWSCredentials(), old);
        HuaweiReply(*mock_client_, HuaweiJson("NEW", 3600));
        provider
            .RetryNow();  // Exercise recovery without waiting for the existing 30-second cooldown.
        EXPECT_EQ(provider.GetAWSCredentials().GetAWSAccessKeyId(), "NEW_AK");
    }
}
TEST_F(NativeStsRefreshTest,
       ConcurrentReadersSeeOnlyCompleteOldOrNewCredentials) {
    TencentEnv env;
    mock_client_->EnqueueResponse("sts.tencentcloudapi.com",
                                  Aws::Http::HttpResponseCode::OK,
                                  TencentJson("OLD"));
    TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider provider;
    const auto old = provider.GetAWSCredentials();
    ASSERT_EQ(old.GetAWSAccessKeyId(), "OLD_AK");
    mock_client_->EnqueueResponse(
        "sts.tencentcloudapi.com", Aws::Http::HttpResponseCode::OK, "");
    mock_client_->EnqueueResponse("sts.tencentcloudapi.com",
                                  Aws::Http::HttpResponseCode::OK,
                                  TencentJson("NEW", 3600));
    std::promise<void> start;
    auto ready = start.get_future().share();
    std::vector<std::future<Aws::Auth::AWSCredentials>> tasks;
    for (int i = 0; i < 16; ++i)
        tasks.push_back(std::async(std::launch::async, [&provider, ready] {
            ready.wait();
            return provider.GetAWSCredentials();
        }));
    start.set_value();
    std::vector<Aws::Auth::AWSCredentials> observed;
    for (auto& task : tasks) observed.push_back(task.get());
    const auto next = provider.GetAWSCredentials();
    ASSERT_EQ(next.GetAWSAccessKeyId(), "NEW_AK");
    for (const auto& creds : observed)
        EXPECT_TRUE(creds == old || creds == next);
}
TEST_F(NativeStsRefreshTest, ExpiredCacheIsNotReturnedAfterTokenFileFailure) {
    TencentEnv tc;
    HuaweiEnv hw;
    AliEnv ali;
    mock_client_->EnqueueResponse("sts.tencentcloudapi.com",
                                  Aws::Http::HttpResponseCode::OK,
                                  TencentJson("OLD", 2));
    HuaweiReply(*mock_client_, HuaweiJson("OLD", 2));
    mock_client_->EnqueueResponse(
        "sts.aliyuncs.com",
        Aws::Http::HttpResponseCode::OK,
        AliXml("AssumeRoleWithOIDCResponse", "OLD", 2));
    TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider tp;
    HuaweiCloudSTSAssumeRoleWebIdentityCredentialsProvider hp;
    AliyunSTSAssumeRoleWebIdentityCredentialsProvider ap;
    const auto t = tp.GetAWSCredentials();
    const auto h = hp.GetAWSCredentials();
    const auto a = ap.GetAWSCredentials();
    ASSERT_EQ(t.GetAWSAccessKeyId(), "OLD_AK");
    ASSERT_EQ(h.GetAWSAccessKeyId(), "OLD_AK");
    ASSERT_EQ(a.GetAWSAccessKeyId(), "OLD_AK");
    const auto expiry = std::max({t.GetExpiration().Millis(),
                                  h.GetExpiration().Millis(),
                                  a.GetExpiration().Millis()});
    std::this_thread::sleep_until(std::chrono::system_clock::time_point(
        std::chrono::milliseconds(expiry + 100)));
    ASSERT_EQ(std::remove(tc.file.path().c_str()), 0);
    ASSERT_EQ(std::remove(hw.file.path().c_str()), 0);
    ASSERT_EQ(std::remove(ali.file.path().c_str()), 0);
    EXPECT_TRUE(tp.GetAWSCredentials().IsEmpty());
    EXPECT_TRUE(hp.GetAWSCredentials().IsEmpty());
    EXPECT_TRUE(ap.GetAWSCredentials().IsEmpty());
}
TEST_F(NativeStsRefreshTest, AliyunRejectsEmptyPartialAndMalformedResponses) {
    AliEnv env;
    for (const auto& body :
         {std::string(""),
          std::string("<broken"),
          AliXml("AssumeRoleWithOIDCResponse", "PARTIAL", 120, ""),
          AliXml("AssumeRoleWithOIDCResponse", "NOEXPIRY", 120, "TOKEN", false),
          AliXml("AssumeRoleWithOIDCResponse", "EXPIRED", -60)}) {
        mock_client_->EnqueueResponse(
            "sts.aliyuncs.com",
            Aws::Http::HttpResponseCode::OK,
            AliXml("AssumeRoleWithOIDCResponse", "OLD"));
        mock_client_->EnqueueResponse(
            "sts.aliyuncs.com", Aws::Http::HttpResponseCode::OK, body);
        mock_client_->EnqueueResponse(
            "sts.aliyuncs.com",
            Aws::Http::HttpResponseCode::OK,
            AliXml("AssumeRoleWithOIDCResponse", "NEW", 3600));
        AliyunSTSAssumeRoleWebIdentityCredentialsProvider provider;
        const auto old = provider.GetAWSCredentials();
        ASSERT_EQ(old.GetAWSAccessKeyId(), "OLD_AK");
        EXPECT_EQ(provider.GetAWSCredentials(), old);
        EXPECT_EQ(provider.GetAWSCredentials().GetAWSAccessKeyId(), "NEW_AK");
    }
}

}  // namespace
