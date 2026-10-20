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
#include <aws/core/endpoint/AWSPartitions.h>
#include <aws/crt/endpoints/RuleEngine.h>
#include <aws/s3/S3EndpointRules.h>

#include <cstdio>
#include <cstdlib>

namespace {

class AwsRuntimeDeathTest : public ::testing::Test {
    void
    SetUp() override {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
};

TEST_F(AwsRuntimeDeathTest, CrtRuleEngineUsesSdkInitialization) {
    ASSERT_EXIT(
        {
            setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
            setenv("AWS_CONFIG_FILE", "/dev/null", 1);
            setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null", 1);
            Aws::SDKOptions options;
            Aws::InitAPI(options);
            {
                // SDK initialization must reach the CRT state used here. A
                // private static CRT copy has an uninitialized JSON allocator.
                Aws::Crt::Endpoints::RuleEngine engine(
                    Aws::Crt::ByteCursorFromArray(
                        reinterpret_cast<const uint8_t*>(
                            Aws::S3::S3EndpointRules::GetRulesBlob()),
                        Aws::S3::S3EndpointRules::RulesBlobSize),
                    Aws::Crt::ByteCursorFromArray(
                        reinterpret_cast<const uint8_t*>(
                            Aws::Endpoint::AWSPartitions::GetPartitionsBlob()),
                        Aws::Endpoint::AWSPartitions::PartitionsBlobSize));
                EXPECT_TRUE(engine);
            }
            // Destroy the engine before shutting down SDK-owned CRT resources.
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
