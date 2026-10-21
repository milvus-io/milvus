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

#include <cstdint>
#include <string>
#include <string_view>

#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/test_utils/ScalarReaderFactory.h"

namespace milvus::index::test {
namespace {

template <typename T>
void
ExpectPredicateProfiles(std::string_view type_name) {
    for (const auto suffix : {std::string_view{},
                              std::string_view{"Mmap"},
                              std::string_view{"NonNull"},
                              std::string_view{"NonNullMmap"}}) {
        const auto name =
            "Inverted" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.DeriveCaps().predicate) << name;
    }
}

TEST(InvertedIndexReaderTest, AdvertisesPrimitivePredicateMatrix) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

TEST(InvertedIndexReaderTest, AdvertisesVarcharPatternPolicy) {
    for (const auto name : {"InvertedVarchar",
                            "InvertedVarcharMmap",
                            "InvertedVarcharNonNull",
                            "InvertedVarcharNonNullMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        EXPECT_TRUE(backend.DeriveCaps().pattern_match) << name;
        EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PrefixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PostfixMatch),
                  PatternQueryPolicy::DeclineButRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::InnerMatch),
                  PatternQueryPolicy::DeclineButRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::RegexMatch),
                  PatternQueryPolicy::DeclineButRun);
    }
}

TEST(InvertedIndexReaderTest, BorrowedValidityWhileReaderIsPinned) {
    ScalarTestData<int64_t> data(std::vector<int64_t>(130, 7));
    data.validity.reset(1);
    data.validity.reset(65);
    data.validity.reset(129);
    ScalarTestInput<int64_t> input(data);
    const auto& backend = ScalarReaderBackends().Get<int64_t>("InvertedInt64");
    auto reader = backend.Create(input.View());
    // Model the expression's retained index pin. Views do not retain this owner.
    std::shared_ptr<IIndexReaderBase> pin(std::move(reader));
    const auto* nulls = dynamic_cast<const INullReader*>(pin.get());
    ASSERT_NE(nulls, nullptr);
    std::vector<BitsetView> leaves;
    for (int i = 0; i < 32; ++i) {
        auto validity = nulls->GetValidityView();
        ASSERT_TRUE(validity.has_value());
        ASSERT_EQ(validity->size(), 130);
        leaves.emplace_back(*validity);
        EXPECT_EQ(leaves.front().data(), leaves.back().data());
    }
    auto mutable_full = nulls->IsNotNull();
    mutable_full.flip();
    EXPECT_FALSE(leaves[0].test(1));
    EXPECT_TRUE(leaves[0].test(2));
    EXPECT_FALSE(leaves[2].test(129));
    EXPECT_TRUE(leaves[2].test(128));
    leaves.clear();
    pin.reset();  // No borrowed view is read after the reader is released.
}

TEST(InvertedIndexReaderTest, AllValidViewHasNoFullBitmap) {
    ScalarTestData<int64_t> data(std::vector<int64_t>(130, 7));
    data.validity_present = false;
    ScalarTestInput<int64_t> input(data);
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("InvertedInt64NonNull");
    auto reader = backend.Create(input.View());
    auto view = dynamic_cast<const INullReader&>(*reader).GetValidityView();
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->size(), 0);
    EXPECT_EQ(view->data(), nullptr);
    EXPECT_EQ(reader->Count(), 130);
}

}  // namespace
}  // namespace milvus::index::test
