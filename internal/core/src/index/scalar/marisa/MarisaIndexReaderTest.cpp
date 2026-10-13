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

#include <string_view>

#include "index/contracts/query/IPatternMatchReader.h"
#include "index/test_utils/ScalarReaderFactory.h"

namespace milvus::index::test {
namespace {

TEST(MarisaIndexReaderTest, AdvertisesVarcharQueries) {
    for (const auto name : {"MarisaVarchar",
                            "MarisaVarcharMmap",
                            "MarisaVarcharNonNull",
                            "MarisaVarcharNonNullMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        const auto caps = backend.DeriveCaps();
        EXPECT_TRUE(caps.predicate) << name;
        EXPECT_TRUE(caps.pattern_match) << name;
        EXPECT_TRUE(caps.value_lookup) << name;
        EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PrefixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PostfixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::InnerMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::RegexMatch),
                  PatternQueryPolicy::UseAndRun);
    }
}

}  // namespace
}  // namespace milvus::index::test
