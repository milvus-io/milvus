// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3EndpointProvider.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "common/CGoCatch.h"
#include "storage/minio/MinioChunkManager.h"

namespace milvus::storage {
namespace {

class EmptyAuthEndpointProvider : public Aws::S3::Endpoint::S3EndpointProvider {
 public:
    Aws::Endpoint::ResolveEndpointOutcome
    ResolveEndpoint(const Aws::Endpoint::EndpointParameters&) const override {
        Aws::Endpoint::AWSEndpoint endpoint;
        endpoint.SetURL("http://127.0.0.1:1/test-bucket");
        endpoint.SetAttributes(
            Aws::Endpoint::AWSEndpoint::EndpointAttributes{});
        return endpoint;
    }
};

class PrecheckAuthChunkManager : public MinioChunkManager {
 public:
    PrecheckAuthChunkManager() {
        Aws::Client::ClientConfiguration config;
        config.region = "us-east-1";
        config.endpointOverride = "http://127.0.0.1:1";
        config.retryStrategy =
            std::make_shared<Aws::Client::DefaultRetryStrategy>(0);
        client_ = std::make_shared<Aws::S3::S3Client>(
            Aws::Auth::AWSCredentials("test-key", "test-secret"),
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);
        client_->accessEndpointProvider() =
            std::make_shared<EmptyAuthEndpointProvider>();
        SetBucketName("test-bucket");
        // Deliberately do NOT install the guard in this test setup. PreCheck
        // itself must install it, otherwise this test aborts inside the SDK.
    }
};

CStatus
PrecheckStatus(PrecheckAuthChunkManager& manager) {
    try {
        manager.PreCheck(StorageConfig{});
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

class MinioEndpointAuthGuardDeathTest : public ::testing::Test {
    void
    SetUp() override {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
};

// Native integration coverage: PreCheck -> ListObjects -> typed SegcoreError
// -> the same CStatus catch tail used by InitRemoteChunkManagerSingleton.
TEST_F(MinioEndpointAuthGuardDeathTest, PrecheckReturnsCStatusNotSignal) {
    ASSERT_EXIT(
        {
            setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
            setenv("AWS_CONFIG_FILE", "/dev/null", 1);
            setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null", 1);
            Aws::SDKOptions options;
            Aws::InitAPI(options);
            {
                PrecheckAuthChunkManager manager;
                auto status = PrecheckStatus(manager);
                EXPECT_EQ(status.error_code,
                          static_cast<int>(ErrorCode::UnexpectedError));
                EXPECT_NE(std::string(status.error_msg ? status.error_msg : "")
                              .find("empty signer"),
                          std::string::npos);
                std::free(status.error_msg);
            }
            Aws::ShutdownAPI(options);
            const auto* result = ::testing::UnitTest::GetInstance()
                                     ->current_test_info()
                                     ->result();
            for (int i = 0; i < result->total_part_count(); ++i) {
                const auto& part = result->GetTestPartResult(i);
                if (part.failed()) {
                    std::fprintf(stderr,
                                 "%s:%d: %s\n",
                                 part.file_name(),
                                 part.line_number(),
                                 part.message());
                }
            }
            std::_Exit(result->Failed() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

}  // namespace
}  // namespace milvus::storage
