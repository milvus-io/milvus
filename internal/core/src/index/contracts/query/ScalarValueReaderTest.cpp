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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/contracts/query/IScalarValueReader.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

template <typename T, typename Actual>
void
ExpectScalarValue(const ScalarTestValue<T>& expected,
                  const Actual& actual,
                  size_t position) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        EXPECT_EQ(std::string_view(actual), std::string_view(expected))
            << "position=" << position;
    } else {
        EXPECT_EQ(actual, expected) << "position=" << position;
    }
}

template <typename T>
const IScalarValueReader<T>*
ValueReader(const IIndexReaderBasePtr& reader) {
    return dynamic_cast<const IScalarValueReader<T>*>(reader.get());
}

template <typename T>
void
ExpectLookupOffsets(const ScalarTestData<T>& data,
                    const IScalarValueReader<T>& reader,
                    const std::vector<size_t>& offsets) {
    for (const auto offset : offsets) {
        ASSERT_LT(offset, data.values.size());
        const auto actual = reader.Lookup(static_cast<int64_t>(offset));
        if (!data.validity[offset]) {
            EXPECT_FALSE(actual.has_value()) << "offset=" << offset;
            continue;
        }
        ASSERT_TRUE(actual.has_value()) << "offset=" << offset;
        ExpectScalarValue<T>(data.values[offset], *actual, offset);
    }
}

template <typename T>
void
ExpectLookupAll(const ScalarTestData<T>& data,
                const IScalarValueReader<T>& reader) {
    std::vector<size_t> offsets(data.values.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        offsets[i] = i;
    }
    ExpectLookupOffsets(data, reader, offsets);
}

template <typename T>
void
ExpectGather(const ScalarTestData<T>& data,
             const IScalarValueReader<T>& reader,
             const std::vector<int64_t>& offsets) {
    std::vector<int> visits(offsets.size(), 0);
    std::vector<std::optional<ScalarTestValue<T>>> values(offsets.size());
    const auto callback = [&](int64_t i, const T* value, bool valid) {
        ASSERT_GE(i, 0);
        ASSERT_LT(static_cast<size_t>(i), offsets.size());
        const auto position = static_cast<size_t>(i);
        ++visits[position];
        EXPECT_EQ(visits[position], 1) << "position=" << position;

        const auto offset = offsets[position];
        ASSERT_GE(offset, 0);
        ASSERT_LT(static_cast<size_t>(offset), data.values.size());
        const auto expected_valid = data.validity[static_cast<size_t>(offset)];
        EXPECT_EQ(valid, expected_valid) << "position=" << position;
        if (!valid) {
            // The pointer has no contract when valid is false.
            return;
        }
        ASSERT_NE(value, nullptr) << "position=" << position;
        if constexpr (std::is_same_v<T, std::string_view>) {
            // Gather lends this view only for the callback duration.
            values[position] = std::string(*value);
        } else {
            values[position] = *value;
        }
    };

    reader.Gather(offsets.empty() ? nullptr : offsets.data(),
                  static_cast<int64_t>(offsets.size()),
                  callback);

    for (size_t position = 0; position < offsets.size(); ++position) {
        EXPECT_EQ(visits[position], 1) << "position=" << position;
        const auto offset = static_cast<size_t>(offsets[position]);
        if (!data.validity[offset]) {
            EXPECT_FALSE(values[position].has_value())
                << "position=" << position;
            continue;
        }
        ASSERT_TRUE(values[position].has_value()) << "position=" << position;
        ExpectScalarValue<T>(data.values[offset], *values[position], position);
    }
}

template <typename T>
void
AddLookupAllCase(IndexTestCases& cases,
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
                .capability = &ReaderCaps::value_lookup,
                .run =
                    [](const auto&, const auto& data, auto& reader) {
                        const auto* values = ValueReader<T>(reader);
                        ASSERT_NE(values, nullptr);
                        ExpectLookupAll(data, *values);
                    },
            },
    });
}

template <typename T>
void
AddGatherCase(IndexTestCases& cases,
              std::string name,
              std::string dataset,
              std::vector<int64_t> offsets,
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
                .capability = &ReaderCaps::value_lookup,
                .run =
                    [offsets = std::move(offsets)](
                        const auto&, const auto& data, auto& reader) {
                        const auto* values = ValueReader<T>(reader);
                        ASSERT_NE(values, nullptr);
                        ExpectGather(data, *values, offsets);
                    },
            },
    });
}

template <typename T>
std::vector<int64_t>
MixedGatherOffsets(const ScalarTestData<T>& data) {
    std::vector<int64_t> result;
    if (data.values.size() < 2) {
        ADD_FAILURE() << "mixed Gather dataset must have at least two rows";
        return result;
    }
    size_t null_offset = data.values.size();
    for (size_t i = 0; i < data.values.size(); ++i) {
        if (!data.validity[i]) {
            null_offset = i;
            break;
        }
    }
    if (null_offset == data.values.size()) {
        ADD_FAILURE() << "mixed Gather dataset must contain a null row";
        return result;
    }
    return {static_cast<int64_t>(data.values.size() - 1),
            0,
            static_cast<int64_t>(null_offset),
            1,
            0};
}

template <typename T>
void
AddCoreValueCases(IndexTestCases& cases) {
    AddLookupAllCase<T>(cases, "LookupMixedValidity", "PredicateEdges");
    AddLookupAllCase<T>(cases, "LookupAbsentValidity", "PredicateAllValid");
    AddLookupAllCase<T>(cases, "LookupAllNull", "PredicateAllNull");
    AddLookupAllCase<T>(cases, "LookupRepeatedValues", "PredicateAllEqual");

    AddGatherCase<T>(cases, "GatherEmptyRequest", "PredicateSingleRow", {});
    AddGatherCase<T>(cases, "GatherSingleOffset", "PredicateEdges", {0});
    cases.Add(IndexTestCase<T>{
        .name = "GatherPermutedDuplicateAndNullOffsets",
        .dataset = "PredicateEdges",
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .body =
            Observe<T>{
                .capability = &ReaderCaps::value_lookup,
                .run =
                    [](const auto&, const auto& data, auto& reader) {
                        const auto* values = ValueReader<T>(reader);
                        ASSERT_NE(values, nullptr);
                        ExpectGather(data, *values, MixedGatherOffsets(data));
                    },
            },
    });
    AddGatherCase<T>(
        cases, "GatherAbsentValidity", "PredicateAllValid", {1, 0, 1});
    AddGatherCase<T>(cases, "GatherAllNull", "PredicateAllNull", {1, 0, 1});

    AddLookupAllCase<T>(cases,
                        "LookupNestedElements",
                        "NestedElements",
                        BackendInputShape::NestedElements,
                        Domain::Element);
    AddGatherCase<T>(cases,
                     "GatherNestedElements",
                     "NestedElements",
                     {3, 0, 1, 0},
                     BackendInputShape::NestedElements,
                     Domain::Element);
}

template <typename T>
void
AddHighCardinalityLookup(IndexTestCases& cases) {
    cases.Add(IndexTestCase<T>{
        .name = "LookupHighCardinalityBoundaries",
        .dataset = "TenThousandHighCardinality",
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .body =
            Observe<T>{
                .capability = &ReaderCaps::value_lookup,
                .run =
                    [](const auto&, const auto& data, auto& reader) {
                        const auto* values = ValueReader<T>(reader);
                        ASSERT_NE(values, nullptr);
                        ExpectLookupOffsets(
                            data, *values, {0, 1999, 2000, 9999});
                    },
            },
    });
}

const IndexTestCases&
ValueCases() {
    static const auto cases = [] {
        IndexTestCases result;
        AddCoreValueCases<bool>(result);
        AddCoreValueCases<int8_t>(result);
        AddCoreValueCases<int16_t>(result);
        AddCoreValueCases<int32_t>(result);
        AddCoreValueCases<int64_t>(result);
        AddCoreValueCases<float>(result);
        AddCoreValueCases<double>(result);
        AddCoreValueCases<std::string_view>(result);

        AddHighCardinalityLookup<int8_t>(result);
        AddHighCardinalityLookup<int16_t>(result);
        AddHighCardinalityLookup<int32_t>(result);
        AddHighCardinalityLookup<int64_t>(result);
        AddHighCardinalityLookup<float>(result);
        AddHighCardinalityLookup<double>(result);
        AddHighCardinalityLookup<std::string_view>(result);

        AddLookupAllCase<float>(
            result, "LookupInfinities", "PredicateFloatInfinities");
        AddLookupAllCase<double>(
            result, "LookupInfinities", "PredicateFloatInfinities");

        AddGatherCase<int64_t>(result,
                               "GatherAcrossBatches",
                               "PredicateEdgesMultiBatch",
                               {5, 0, 2, 1, 0});
        AddGatherCase<std::string_view>(result,
                                        "GatherAcrossBatches",
                                        "PredicateEdgesMultiBatch",
                                        {6, 0, 3, 1, 0});
        AddGatherCase<int64_t>(result,
                               "GatherAcrossEmptyBatches",
                               "PredicateEdgesWithEmptyBatches",
                               {5, 0, 2, 1, 0});

        result.Add(IndexTestCase<std::string_view>{
            .name = "LookupOwnsStringAfterLaterQueryAndReaderDestruction",
            .dataset = "PredicateAllValid",
            .input_lifetime = InputLifetime::ReleaseBeforeBody,
            .body =
                Observe<std::string_view>{
                    .capability = &ReaderCaps::value_lookup,
                    .run =
                        [](const auto&, const auto& data, auto& reader) {
                            const auto* values =
                                ValueReader<std::string_view>(reader);
                            ASSERT_NE(values, nullptr);
                            ASSERT_GT(data.values.size(), 6);
                            auto retained = values->Lookup(6);
                            ASSERT_TRUE(retained.has_value());
                            ASSERT_GT(retained->size(), 64);
                            static_cast<void>(values->Lookup(0));
                            reader.reset();
                            EXPECT_EQ(*retained, data.values[6]);
                        },
                },
        });
        return result;
    }();
    return cases;
}

class ScalarValueReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(ScalarValueReaderTest, ObservesExpectedValues) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarReaders,
                         ScalarValueReaderTest,
                         ::testing::ValuesIn(ValueCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
