// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the
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

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "index/contracts/query/INullReader.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

template <typename T>
void
ExpectNullMasks(const ScalarTestData<T>& data, const INullReader& reader) {
    auto nulls = reader.IsNull();
    const auto not_nulls = reader.IsNotNull();
    ASSERT_EQ(nulls.size(), data.values.size());
    ASSERT_EQ(not_nulls.size(), data.values.size());

    const auto expected_nulls = ExpectedNulls(data);
    for (size_t i = 0; i < data.values.size(); ++i) {
        EXPECT_EQ(nulls[i], !data.validity[i]) << "offset=" << i;
        EXPECT_EQ(not_nulls[i], data.validity[i]) << "offset=" << i;
        EXPECT_NE(nulls[i], not_nulls[i]) << "offset=" << i;
    }
    ExpectBitmap(nulls, expected_nulls);

    // Returned bitmaps own their state. Mutating one result cannot alter the
    // reader or a later result.
    if (nulls.size() != 0) {
        nulls.flip();
    }
    auto repeated_nulls = reader.IsNull();
    ASSERT_EQ(repeated_nulls.size(), nulls.size());
    ExpectBitmap(repeated_nulls, expected_nulls);
}

template <typename T>
void
AddNullCase(IndexTestCases& cases,
            std::string name,
            std::string dataset,
            BackendInputShape input_shape = BackendInputShape::Scalar,
            Domain domain = Domain::Row) {
    cases.Add(IndexTestCase<T>{
        .name = std::move(name),
        .dataset = std::move(dataset),
        .input_shape = input_shape,
        .domain = domain,
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .body =
            Observe<T>{
                .run =
                    [](const auto&, const auto& data, const auto& reader) {
                        const auto* nulls =
                            dynamic_cast<const INullReader*>(reader.get());
                        ASSERT_NE(nulls, nullptr);
                        ExpectNullMasks(data, *nulls);
                    },
            },
    });
}

template <typename T>
void
AddCoreNullCases(IndexTestCases& cases) {
    AddNullCase<T>(cases, "MixedValidity", "PredicateEdges");
    AddNullCase<T>(cases, "AbsentValidityIsAllValid", "PredicateAllValid");
    AddNullCase<T>(cases, "AllNull", "PredicateAllNull");
    AddNullCase<T>(cases, "PresentAllValidSingleRow", "PredicateSingleRow");
    AddNullCase<T>(cases,
                   "NestedElementsAreAllValid",
                   "NestedElements",
                   BackendInputShape::NestedElements,
                   Domain::Element);
}

const IndexTestCases&
NullCases() {
    static const auto cases = [] {
        IndexTestCases result;
        AddCoreNullCases<bool>(result);
        AddCoreNullCases<int8_t>(result);
        AddCoreNullCases<int16_t>(result);
        AddCoreNullCases<int32_t>(result);
        AddCoreNullCases<int64_t>(result);
        AddCoreNullCases<float>(result);
        AddCoreNullCases<double>(result);
        AddCoreNullCases<std::string_view>(result);

        AddNullCase<std::string_view>(
            result, "AcrossBatches", "PredicateEdgesMultiBatch");
        AddNullCase<int64_t>(
            result, "AcrossEmptyBatches", "PredicateEdgesWithEmptyBatches");
        result.Add(IndexTestCase<int64_t>{
            .name = "AcrossPackedBitBoundary",
            .dataset = "BitBoundaryNullable",
            .input_lifetime = InputLifetime::ReleaseBeforeBody,
            .body =
                Observe<int64_t>{
                    .run =
                        [](const auto&, const auto& data, const auto& reader) {
                            const auto* nulls =
                                dynamic_cast<const INullReader*>(reader.get());
                            ASSERT_NE(nulls, nullptr);
                            ExpectNullMasks(data, *nulls);
                        },
                },
        });
        AddNullCase<std::string_view>(
            result, "NullIsDistinctFromEmptyString", "NullVsEmptyString");
        return result;
    }();
    return cases;
}

class NullReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(NullReaderTest, ReturnsExpectedComplementaryMasks) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarReaders,
                         NullReaderTest,
                         ::testing::ValuesIn(NullCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
