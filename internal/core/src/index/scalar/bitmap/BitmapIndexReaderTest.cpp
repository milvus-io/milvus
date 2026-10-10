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
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

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
            "Bitmap" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.DeriveCaps().predicate) << name;
    }
}

TEST(BitmapIndexReaderTest, AdvertisesPrimitivePredicateMatrix) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

TEST(BitmapIndexReaderTest, ProvidesInt64Queries) {
    // This promise is checked without capability filtering, so a regression
    // cannot silently remove this backend from the shared query suites.
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.value_lookup);

    const std::array<int64_t, 1> values{42};
    const std::array<ScalarBuildBatch<int64_t>, 1> batches{
        ScalarBuildBatch<int64_t>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<int64_t>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().value_lookup);
    EXPECT_NE(dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get()),
              nullptr);
    EXPECT_NE(dynamic_cast<const IScalarValueReader<int64_t>*>(reader.get()),
              nullptr);
}

TEST(BitmapIndexReaderTest, ProvidesVarcharQueries) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.pattern_match);
    EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
              PatternQueryPolicy::UseAndRun);

    const std::array<std::string_view, 1> values{"value"};
    const std::array<ScalarBuildBatch<std::string_view>, 1> batches{
        ScalarBuildBatch<std::string_view>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<std::string_view>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().pattern_match);
    EXPECT_NE(dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                  reader.get()),
              nullptr);
    EXPECT_NE(dynamic_cast<const IPatternMatchReader*>(reader.get()), nullptr);
}

TEST(BitmapIndexReaderTest, NullablePackedWordBoundaryKeepsFinalAllocation) {
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        const auto& backend = ScalarReaderBackends().Get<int32_t>(
            mmap ? "BitmapInt32Mmap" : "BitmapInt32");
        std::array<int64_t, 2> resident{};
        for (const size_t rows : {size_t{64}, size_t{65}}) {
            ScalarTestData<int32_t> data(
                std::vector<int32_t>(rows, int32_t{7}));
            data.validity.reset(0);
            const ScalarTestInput<int32_t> input(data);
            auto artifact = backend.Build(input.View(), {.row_count = rows});
            auto reader =
                OpenV3(backend, SerializeV3(*artifact), {.row_count = rows});
            ASSERT_NE(reader, nullptr);
            const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<int32_t>*>(
                    reader.get());
            ASSERT_NE(nulls, nullptr);
            ASSERT_NE(predicate, nullptr);
            ExpectHits(nulls->IsNull(), rows, {0});
            const int32_t key = 7;
            EXPECT_EQ(predicate->In(1, &key).count(), rows - 1);
            resident[rows == 64 ? 0 : 1] = reader->CellByteSize().memory_bytes;
        }
        // The 65th row expands one validity word and one dense posting word.
        // Retaining a second validity staging buffer would add another word.
        EXPECT_EQ(resident[1] - resident[0], 2 * sizeof(uint64_t));
    }
}

}  // namespace
}  // namespace milvus::index::test
