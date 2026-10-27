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
#include <array>
#include <cstddef>
#include <cstdint>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/contracts/query/IScalarValueReader.h"
#include "index/test_utils/CaseTestDriver.h"
#include "test_utils/OriginalCohereFixture.h"
#include "test_utils/OriginalNqTextFixture.h"

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
    int64_t next_lane = 0;
    const auto callback = [&](int64_t i, const T* value, bool valid) {
        EXPECT_EQ(i, next_lane++);
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
        AddLookupAllCase<int64_t>(
            result, "LookupAcrossPackedBitBoundary", "BitBoundaryNullable");
        AddGatherCase<int64_t>(result,
                               "GatherAcrossPackedBitBoundary",
                               "BitBoundaryNullable",
                               {65, 64, 63, 62, 61, 66, 0, 69, 62});
        AddGatherCase<std::string_view>(result,
                                        "GatherBinaryUnicodeAndNull",
                                        "PredicateEdges",
                                        {3, 4, 5, 3, 0, 6});

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

// These regressions use unchanged source records, not the generated datasets
// used by the older broad contract suite above. Offset schedules are protocol
// inputs; repeating an offset does not create or duplicate a dataset record.
class OriginalScalarValueReaderTest
    : public ::testing::TestWithParam<const char*> {
 protected:
    void
    SetUp() override {
        ids_ = LoadOriginalCohereIds();
        ASSERT_EQ(ids_.size(), 1024);
        const auto& backend = ScalarReaderBackends().Get<int64_t>(GetParam());
        {
            // The expectation has a different owner from the builder input.
            // Destroy input storage before any lookup, as in a loaded reader.
            const auto source = ids_;
            const std::array<ScalarBuildBatch<int64_t>, 1> batches{
                ScalarBuildBatch<int64_t>{source, {}}};
            reader_ = backend.Create(ScalarBuildInput<int64_t>{batches});
        }
        ASSERT_NE(reader_, nullptr);
        ASSERT_TRUE(reader_->Caps().value_lookup);
        values_ =
            dynamic_cast<const IScalarValueReader<int64_t>*>(reader_.get());
        ASSERT_NE(values_, nullptr);
    }

    std::vector<int64_t>
    Offsets(size_t count) const {
        std::vector<int64_t> offsets;
        offsets.reserve(count);
        for (size_t lane = 0; lane < count; ++lane) {
            offsets.push_back(
                static_cast<int64_t>((ids_.size() - 1 - lane % ids_.size())));
        }
        if (offsets.size() > 1) {
            offsets.back() = offsets.front();
        }
        return offsets;
    }

    std::vector<int64_t> ids_;
    IIndexReaderBasePtr reader_;
    const IScalarValueReader<int64_t>* values_{nullptr};
};

TEST_P(OriginalScalarValueReaderTest, GatherKeepsRequestLanesBeyondMaskWidth) {
    for (const size_t count : {0, 1, 31, 32, 33, 63, 64, 65, 257, 1025, 2053}) {
        SCOPED_TRACE(count);
        auto offsets = Offsets(count);
        const auto original = offsets;
        std::vector<size_t> visits(count, 0);
        int64_t next_lane = 0;
        values_->Gather(
            offsets.empty() ? nullptr : offsets.data(),
            static_cast<int64_t>(count),
            [&](int64_t lane, const int64_t* value, bool valid) {
                EXPECT_EQ(lane, next_lane++);
                ASSERT_GE(lane, 0);
                ASSERT_LT(static_cast<size_t>(lane), count);
                const auto output = static_cast<size_t>(lane);
                EXPECT_EQ(++visits[output], 1);
                ASSERT_TRUE(valid);
                ASSERT_NE(value, nullptr);
                const auto offset = static_cast<size_t>(offsets[output]);
                EXPECT_EQ(*value, ids_[offset]);
                const auto scalar = values_->Lookup(offsets[output]);
                ASSERT_TRUE(scalar.has_value());
                EXPECT_EQ(*value, *scalar);
            });
        EXPECT_EQ(offsets, original);
        EXPECT_TRUE(std::all_of(
            visits.begin(), visits.end(), [](size_t n) { return n == 1; }));
    }
}

TEST_P(OriginalScalarValueReaderTest, EmptyInputAndFailuresRemainReusable) {
    values_->Gather(nullptr, 0, {});
    const auto callback = [](int64_t, const int64_t*, bool) {};
    EXPECT_ANY_THROW(values_->Gather(nullptr, 1, callback));
    EXPECT_ANY_THROW(values_->Gather(nullptr, -1, callback));
    const int64_t negative = -1;
    const int64_t beyond = static_cast<int64_t>(ids_.size());
    EXPECT_ANY_THROW(values_->Gather(&negative, 1, callback));
    EXPECT_ANY_THROW(values_->Gather(&beyond, 1, callback));

    const auto offsets = Offsets(65);
    EXPECT_THROW(
        values_->Gather(offsets.data(),
                        offsets.size(),
                        [](int64_t, const int64_t*, bool) {
                            throw std::runtime_error("consumer stopped");
                        }),
        std::runtime_error);
    size_t visits = 0;
    values_->Gather(offsets.data(),
                    offsets.size(),
                    [&](int64_t lane, const int64_t* value, bool valid) {
                        ASSERT_TRUE(valid);
                        ASSERT_NE(value, nullptr);
                        EXPECT_EQ(*value, ids_[offsets[lane]]);
                        ++visits;
                    });
    EXPECT_EQ(visits, offsets.size());
}

TEST_P(OriginalScalarValueReaderTest,
       IndependentGatherCallsShareReadOnlySource) {
    const auto offsets = Offsets(257);
    for (size_t concurrency : {1, 4, 8}) {
        SCOPED_TRACE(concurrency);
        std::vector<std::future<void>> tasks;
        for (size_t task = 0; task < concurrency; ++task) {
            tasks.push_back(std::async(std::launch::async, [&] {
                for (size_t batch = 0; batch < 4; ++batch) {
                    size_t visits = 0;
                    values_->Gather(
                        offsets.data(),
                        offsets.size(),
                        [&](int64_t lane, const int64_t* value, bool valid) {
                            ASSERT_GE(lane, 0);
                            ASSERT_LT(static_cast<size_t>(lane),
                                      offsets.size());
                            ASSERT_TRUE(valid);
                            ASSERT_NE(value, nullptr);
                            EXPECT_EQ(*value, ids_[offsets[lane]]);
                            ++visits;
                        });
                    EXPECT_EQ(visits, offsets.size());
                }
            }));
        }
        for (auto& task : tasks) {
            task.get();
        }
    }
}

INSTANTIATE_TEST_SUITE_P(OriginalCohere,
                         OriginalScalarValueReaderTest,
                         ::testing::Values("SortedInt64",
                                           "BitmapInt64",
                                           "HybridInt64"),
                         [](const ::testing::TestParamInfo<const char*>& info) {
                             return std::string(info.param);
                         });

struct OriginalStringReaderProfile {
    const char* backend;
    const char* field;
    size_t rows;
};

class OriginalStringValueReaderTest
    : public ::testing::TestWithParam<OriginalStringReaderProfile> {
 protected:
    void
    SetUp() override {
        const auto rows = LoadOriginalNqTextRows();
        ASSERT_EQ(rows.size(), 129);
        const auto& profile = GetParam();
        ASSERT_LE(profile.rows, rows.size());
        expected_.reserve(profile.rows);
        for (size_t row = 0; row < profile.rows; ++row) {
            ASSERT_GE(rows[row].snapshot_offset, 0);
            if (row != 0) {
                ASSERT_GT(rows[row].snapshot_offset,
                          rows[row - 1].snapshot_offset);
            }
            expected_.push_back(std::string_view(profile.field) == "Title"
                                    ? rows[row].title
                                    : rows[row].text);
        }
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(profile.backend);
        {
            // A separate, temporary builder input must not own reader values.
            const auto source = expected_;
            std::vector<std::string_view> views;
            views.reserve(source.size());
            for (const auto& value : source) {
                views.emplace_back(value);
            }
            const std::array<ScalarBuildBatch<std::string_view>, 1> batches{
                ScalarBuildBatch<std::string_view>{views, {}}};
            reader_ =
                backend.Create(ScalarBuildInput<std::string_view>{batches});
        }
        ASSERT_NE(reader_, nullptr);
        ASSERT_TRUE(reader_->Caps().value_lookup);
        values_ = dynamic_cast<const IScalarValueReader<std::string_view>*>(
            reader_.get());
        ASSERT_NE(values_, nullptr);
    }

    std::vector<int64_t>
    Offsets(size_t count) const {
        std::vector<int64_t> result;
        result.reserve(count);
        for (size_t lane = 0; lane < count; ++lane) {
            result.push_back(static_cast<int64_t>(expected_.size() - 1 -
                                                  lane % expected_.size()));
        }
        if (result.size() > 1) {
            result.back() = result.front();
        }
        return result;
    }

    std::vector<std::string> expected_;
    IIndexReaderBasePtr reader_;
    const IScalarValueReader<std::string_view>* values_{nullptr};
};

TEST_P(OriginalStringValueReaderTest, ScopedGatherPreservesOrderAndCopies) {
    for (const size_t count : {0, 1, 31, 32, 33, 63, 64, 65, 257, 1025}) {
        SCOPED_TRACE(count);
        const auto offsets = Offsets(count);
        std::vector<std::optional<std::string>> retained(count);
        int64_t next_lane = 0;
        values_->Gather(
            offsets.empty() ? nullptr : offsets.data(),
            count,
            [&](int64_t lane, const std::string_view* value, bool valid) {
                ASSERT_EQ(lane, next_lane++);
                ASSERT_GE(lane, 0);
                ASSERT_LT(static_cast<size_t>(lane), offsets.size());
                ASSERT_TRUE(valid);
                ASSERT_NE(value, nullptr);
                EXPECT_EQ(*value, expected_[offsets[lane]]);
                // A trie may overwrite borrowed characters on its next lane.
                // Only this explicit owning copy may survive the callback.
                retained[lane] = std::string(*value);
            });
        EXPECT_EQ(next_lane, count);
        auto moved = std::move(retained);
        std::vector<std::optional<std::string>> assigned;
        assigned = std::move(moved);
        for (size_t lane = 0; lane < count; ++lane) {
            ASSERT_TRUE(assigned[lane].has_value());
            EXPECT_EQ(*assigned[lane], expected_[offsets[lane]]);
            EXPECT_EQ(values_->Lookup(offsets[lane]), assigned[lane]);
        }
    }
}

TEST_P(OriginalStringValueReaderTest, LookupOwnsBytesAfterReaderDestruction) {
    const auto shortest = std::min_element(
        expected_.begin(), expected_.end(), [](const auto& a, const auto& b) {
            return a.size() < b.size();
        });
    const auto longest = std::max_element(
        expected_.begin(), expected_.end(), [](const auto& a, const auto& b) {
            return a.size() < b.size();
        });
    const auto short_offset = std::distance(expected_.begin(), shortest);
    const auto long_offset = std::distance(expected_.begin(), longest);
    auto short_value = values_->Lookup(short_offset);
    auto long_value = values_->Lookup(long_offset);
    ASSERT_TRUE(short_value.has_value());
    ASSERT_TRUE(long_value.has_value());
    // Later query/agent work may not invalidate Lookup's owning values.
    const auto offsets = Offsets(257);
    values_->Gather(offsets.data(),
                    offsets.size(),
                    [](int64_t, const std::string_view*, bool) {});
    auto moved_short = std::move(short_value);
    auto moved_long = std::move(long_value);
    reader_.reset();
    values_ = nullptr;
    EXPECT_EQ(*moved_short, *shortest);
    EXPECT_EQ(*moved_long, *longest);
}

TEST_P(OriginalStringValueReaderTest, CallbackFailureAndConcurrentCalls) {
    const auto offsets = Offsets(65);
    EXPECT_THROW(
        values_->Gather(offsets.data(),
                        offsets.size(),
                        [](int64_t, const std::string_view*, bool) {
                            throw std::runtime_error("string consumer stopped");
                        }),
        std::runtime_error);
    for (size_t concurrency : {1, 4, 8}) {
        std::vector<std::future<void>> tasks;
        for (size_t task = 0; task < concurrency; ++task) {
            tasks.push_back(std::async(std::launch::async, [&] {
                int64_t next_lane = 0;
                values_->Gather(offsets.data(),
                                offsets.size(),
                                [&](int64_t lane,
                                    const std::string_view* value,
                                    bool valid) {
                                    ASSERT_EQ(lane, next_lane++);
                                    ASSERT_TRUE(valid);
                                    ASSERT_NE(value, nullptr);
                                    EXPECT_EQ(*value, expected_[offsets[lane]]);
                                });
                EXPECT_EQ(next_lane, offsets.size());
            }));
        }
        for (auto& task : tasks) {
            task.get();
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    OriginalNq,
    OriginalStringValueReaderTest,
    ::testing::Values(
        OriginalStringReaderProfile{"SortedVarchar", "Text", 129},
        OriginalStringReaderProfile{"SortedVarchar", "Title", 129},
        OriginalStringReaderProfile{"MarisaVarchar", "Text", 129},
        OriginalStringReaderProfile{"MarisaVarchar", "Title", 129},
        OriginalStringReaderProfile{"BitmapVarchar", "Text", 129},
        OriginalStringReaderProfile{"BitmapVarchar", "Title", 129},
        // Hybrid's existing high-cardinality VARCHAR choice is an inverted
        // index without value lookup. This original 15-row subset stays BELOW
        // the threshold 16 (equality selects the high-cardinality family) and
        // exercises the supported Bitmap delegate, not fabricated rows.
        OriginalStringReaderProfile{"HybridVarchar", "Text", 15},
        OriginalStringReaderProfile{"HybridVarchar", "Title", 15}),
    [](const ::testing::TestParamInfo<OriginalStringReaderProfile>& info) {
        return std::string(info.param.backend) + info.param.field;
    });

}  // namespace
}  // namespace milvus::index::test
