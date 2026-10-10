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
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"

namespace milvus::indexbuilder::test {
namespace {

template <typename T>
T
PrefixValue(int value) {
    if constexpr (std::is_same_v<T, std::string>) {
        return std::to_string(value);
    } else if constexpr (std::is_same_v<T, bool>) {
        return value % 2 == 0;
    } else {
        return static_cast<T>(value);
    }
}

template <typename T>
class InvertedMissingBinlogPrefixTest : public SourceBuildTest {
 protected:
    using QueryT =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;
    static constexpr int64_t kMissingRows = 100;
    static constexpr size_t kSourceRows = 9;

    PreparedBuild
    PreparePrefix(bool with_default) const {
        proto::indexcgo::BuildIndexInfo info;
        info.set_collectionid(1);
        info.set_partitionid(2);
        info.set_segmentid(3);
        info.set_buildid(1000);
        info.set_index_version(1);
        info.set_num_rows(kMissingRows + kSourceRows);
        info.set_lack_binlog_rows(kMissingRows);
        info.set_current_scalar_index_version(3);
        auto* field = info.mutable_field_schema();
        field->set_fieldid(100);
        field->set_name("values");
        field->set_data_type(
            static_cast<proto::schema::DataType>(index::CppDataType<T>()));
        field->set_nullable(true);
        if (with_default) {
            auto* value = field->mutable_default_value();
            if constexpr (std::is_same_v<T, std::string>) {
                value->set_string_data("20");
            } else if constexpr (std::is_same_v<T, bool>) {
                value->set_bool_data(true);
            } else if constexpr (std::is_same_v<T, int64_t>) {
                value->set_long_data(20);
            } else if constexpr (std::is_same_v<T, float>) {
                value->set_float_data(20);
            } else if constexpr (std::is_same_v<T, double>) {
                value->set_double_data(20);
            } else {
                value->set_int_data(20);
            }
        }
        auto* family = info.add_index_params();
        family->set_key(index::INDEX_TYPE);
        family->set_value(index::INVERTED_INDEX_TYPE);
        info.add_insert_files(directory_->Path() + "/insert/1");
        info.mutable_storage_config()->set_storage_type("local");
        info.mutable_storage_config()->set_root_path(directory_->Path());
        auto prepared = AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
        prepared.request.staging_parent = directory_->Path();
        prepared.request.params["nullable"] = true;
        prepared.request.params["num_rows"] = info.num_rows();
        return prepared;
    }

    void
    CheckPrefix(bool with_default) {
        const std::array<T, kSourceRows> values{PrefixValue<T>(1),
                                                PrefixValue<T>(20),
                                                PrefixValue<T>(10),
                                                PrefixValue<T>(20),
                                                PrefixValue<T>(30),
                                                PrefixValue<T>(2),
                                                PrefixValue<T>(20),
                                                PrefixValue<T>(10),
                                                PrefixValue<T>(20)};
        const std::array<uint8_t, 2> validity{0b10110101, 0b00000001};
        const auto prepared = PreparePrefix(with_default);
        // The session derives the missing prefix from the decoded source rows.
        ASSERT_EQ(prepared.request.expected_rows -
                      static_cast<int64_t>(values.size()),
                  kMissingRows);
        ASSERT_EQ(prepared.request.family, index::families::kInverted);
        {
            auto data = storage::CreateFieldData(
                index::CppDataType<T>(), DataType::NONE, true);
            data->FillFieldData(
                values.data(), validity.data(), values.size(), 0);
            WriteInsert(prepared, data);
        }
        const auto stats = Publish(prepared);
        ASSERT_EQ(stats.Files().size(), 1);
        EXPECT_GT(stats.Files().front().file_size, 0);
        // Publish destroys the session and materialized input before opening.
        const auto reader = Open(prepared, stats);
        ASSERT_NE(reader, nullptr);
        ASSERT_EQ(reader->Count(), kMissingRows + kSourceRows);
        EXPECT_EQ(reader->CoordDomain(), index::Domain::Row);
        const auto* predicate =
            dynamic_cast<const index::IScalarPredicateReader<QueryT>*>(
                reader.get());
        const auto* nulls =
            dynamic_cast<const index::INullReader*>(reader.get());
        ASSERT_NE(predicate, nullptr);
        ASSERT_NE(nulls, nullptr);
        const T default_value = PrefixValue<T>(20);
        const auto is_valid = [&](size_t row) {
            if (row < kMissingRows) {
                return with_default;
            }
            const auto source_row = row - kMissingRows;
            return (validity[source_row / 8] &
                    (uint8_t{1} << (source_row % 8))) != 0;
        };
        const auto value_at = [&](size_t row) -> const T& {
            return row < kMissingRows ? default_value
                                      : values[row - kMissingRows];
        };
        const auto expect_hits = [&](const TargetBitmap& actual,
                                     const auto& matches) {
            ASSERT_EQ(actual.size(), reader->Count());
            for (size_t row = 0; row < actual.size(); ++row) {
                EXPECT_EQ(actual[row], is_valid(row) && matches(value_at(row)))
                    << row;
            }
        };
        const auto is_null = nulls->IsNull();
        const auto is_not_null = nulls->IsNotNull();
        ASSERT_EQ(is_null.size(), reader->Count());
        ASSERT_EQ(is_not_null.size(), reader->Count());
        for (size_t row = 0; row < is_null.size(); ++row) {
            EXPECT_EQ(is_null[row], !is_valid(row)) << row;
            EXPECT_EQ(is_not_null[row], is_valid(row)) << row;
        }
        for (const T& key : {default_value, PrefixValue<T>(1)}) {
            const QueryT query = key;
            expect_hits(predicate->In(1, &query),
                        [&](const T& value) { return value == key; });
            expect_hits(predicate->NotIn(1, &query),
                        [&](const T& value) { return value != key; });
        }
        if constexpr (!std::is_same_v<T, bool>) {
            for (const auto op : {index::CompareOp::LessThan,
                                  index::CompareOp::LessEqual,
                                  index::CompareOp::GreaterThan,
                                  index::CompareOp::GreaterEqual}) {
                expect_hits(predicate->Range(QueryT(default_value), op),
                            [&](const T& value) {
                                switch (op) {
                                    case index::CompareOp::LessThan:
                                        return value < default_value;
                                    case index::CompareOp::LessEqual:
                                        return value <= default_value;
                                    case index::CompareOp::GreaterThan:
                                        return value > default_value;
                                    default:
                                        return value >= default_value;
                                }
                            });
            }
            const T lower = PrefixValue<T>(1);
            for (const bool lower_inclusive : {false, true}) {
                for (const bool upper_inclusive : {false, true}) {
                    expect_hits(predicate->Range(QueryT(lower),
                                                 lower_inclusive,
                                                 QueryT(default_value),
                                                 upper_inclusive),
                                [&](const T& value) {
                                    return (lower_inclusive ? value >= lower
                                                            : value > lower) &&
                                           (upper_inclusive
                                                ? value <= default_value
                                                : value < default_value);
                                });
                }
            }
        }
        if constexpr (std::is_same_v<T, std::string>) {
            const auto* pattern =
                dynamic_cast<const index::IPatternMatchReader*>(reader.get());
            ASSERT_NE(pattern, nullptr);
            const auto starts_with_two = [](const T& value) {
                return value.starts_with("2");
            };
            expect_hits(
                pattern->PatternMatch("2", index::PatternOp::PrefixMatch),
                starts_with_two);
            expect_hits(pattern->PatternMatch("2%", index::PatternOp::Match),
                        starts_with_two);
        }
    }
};

using PrefixTypes = ::testing::
    Types<bool, int8_t, int16_t, int32_t, int64_t, float, double, std::string>;
TYPED_TEST_SUITE(InvertedMissingBinlogPrefixTest, PrefixTypes);

TYPED_TEST(InvertedMissingBinlogPrefixTest,
           MissingPrefixIsNullBeforeSourceRows) {
    this->CheckPrefix(false);
}

TYPED_TEST(InvertedMissingBinlogPrefixTest, MissingPrefixUsesSchemaDefault) {
    this->CheckPrefix(true);
}

}  // namespace
}  // namespace milvus::indexbuilder::test
