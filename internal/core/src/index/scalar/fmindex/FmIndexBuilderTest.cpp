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

#include <array>
#include <string>
#include <string_view>
#include <utility>

#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

using StringBuilder = BuilderRegistry<ScalarBuildInput<std::string_view>>;

TEST(FmIndexBuilderTest, RejectsInvalidNumericParametersWithInputCode) {
    const std::array<Config, 7> bad_values = {Config("abc"),
                                              Config("3"),
                                              Config("257"),
                                              Config("8junk"),
                                              Config(-1),
                                              Config(8.5),
                                              Config(true)};
    for (const auto& value : bad_values) {
        SCOPED_TRACE(value.dump());
        const Config params = {{FM_SA_SAMPLE_RATE, value}};
        ExpectSegcoreError(ErrorCode::InvalidParameter, [&] {
            static_cast<void>(
                StringBuilder::Instance().Create(families::kFmIndex, params));
        });
    }
    for (const auto& value : {Config("abc"),
                              Config("4"),
                              Config("24"),
                              Config("256"),
                              Config("64junk"),
                              Config(-1),
                              Config(64.5),
                              Config(false)}) {
        SCOPED_TRACE(value.dump());
        const Config params = {{FM_BLOCK_BYTES, value}};
        ExpectSegcoreError(ErrorCode::InvalidParameter, [&] {
            static_cast<void>(
                StringBuilder::Instance().Create(families::kFmIndex, params));
        });
    }
}

TEST(FmIndexBuilderTest, AcceptsBoundariesAndLeadingPlusThenBuilds) {
    for (const auto& sample : {"4", "+8", "256"}) {
        for (const auto& block : {"8", "+64", "128"}) {
            SCOPED_TRACE(std::string(sample) + "/" + block);
            const Config params = {{FM_SA_SAMPLE_RATE, sample},
                                   {FM_BLOCK_BYTES, block}};
            auto builder =
                StringBuilder::Instance().Create(families::kFmIndex, params);
            ASSERT_NE(builder, nullptr);
            ScalarTestData<std::string_view> data({"alpha", "beta"});
            data.validity_present = false;
            const ScalarTestInput<std::string_view> input(data);
            auto artifact = std::move(*builder).Build(input.View());
            ASSERT_NE(artifact, nullptr);
            const auto& backend = ScalarReaderBackends().Get<std::string_view>(
                "FmIndexVarcharNonNull");
            auto reader = backend.Open(std::move(artifact), {.row_count = 2});
            ASSERT_NE(reader, nullptr);
            const auto* pattern =
                dynamic_cast<const IPatternMatchReader*>(reader.get());
            ASSERT_NE(pattern, nullptr);
            const auto hits =
                pattern->PatternMatch("alp", PatternOp::PrefixMatch);
            ASSERT_EQ(hits.size(), 2);
            EXPECT_TRUE(hits[0]);
            EXPECT_FALSE(hits[1]);
        }
    }
}

}  // namespace
}  // namespace milvus::index::test
