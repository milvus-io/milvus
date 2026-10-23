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
#include <bit>
#include <cmath>
#include <filesystem>
#include <limits>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

// An independent SQL oracle: NaNs form one equivalence class above +infinity.
template <typename T>
int
SqlCompare(T left, T right) {
    if (std::isnan(left)) {
        return std::isnan(right) ? 0 : 1;
    }
    if (std::isnan(right)) {
        return -1;
    }
    return left < right ? -1 : left > right ? 1 : 0;
}

template <typename T>
std::array<T, 2>
NaNPayloads() {
    if constexpr (std::is_same_v<T, float>) {
        return {std::bit_cast<float>(uint32_t{0x7fc00001}),
                std::bit_cast<float>(uint32_t{0xffc00042})};
    } else {
        return {std::bit_cast<double>(uint64_t{0x7ff8000000000001}),
                std::bit_cast<double>(uint64_t{0xfff8000000000042})};
    }
}

BackendSpec
NaNSpec(const char* family,
        DataType type,
        bool array,
        bool nested,
        bool mmap,
        bool high_cardinality) {
    BackendSpec spec;
    spec.name = "NaN";
    spec.family = family;
    spec.nullable = true;
    spec.field_type = array || nested ? DataType::ARRAY : type;
    spec.value_type = type;
    spec.array_element_type = array || nested ? type : DataType::NONE;
    spec.input_shape = array    ? BackendInputShape::ArrayRows
                       : nested ? BackendInputShape::NestedElements
                                : BackendInputShape::Scalar;
    spec.expected_domain = nested ? Domain::Element : Domain::Row;
    spec.enable_mmap = mmap;
    spec.build_params = {
        {FIELD_ID, 101},
        {SCALAR_INDEX_ENGINE_VERSION, 6},
        {ENABLE_ARRAY_SORT_INDEX, true},
        {BITMAP_INDEX_CARDINALITY_LIMIT, high_cardinality ? 4 : 1024},
        {HYBRID_LOW_CARDINALITY_INDEX_TYPE, BITMAP_INDEX_TYPE},
        {HYBRID_HIGH_CARDINALITY_INDEX_TYPE, ASCENDING_SORT},
        {"local_dir", std::filesystem::temp_directory_path().string()},
    };
    spec.load_params = {{SCALAR_INDEX_ENGINE_VERSION, 6}};
    spec.load_families = {
        families::kBitmap, families::kSort, families::kInverted};
    return spec;
}

template <typename T>
void
CheckPredicates(const IIndexReaderBase& reader,
                const std::vector<std::vector<T>>& rows,
                const std::vector<bool>& validity,
                bool array) {
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<T>*>(&reader);
    ASSERT_NE(predicate, nullptr);
    const auto* nulls = dynamic_cast<const INullReader*>(&reader);
    ASSERT_NE(nulls, nullptr);
    const auto nans = NaNPayloads<T>();
    const T inf = std::numeric_limits<T>::infinity();
    const std::vector<T> bounds{
        -inf, -T(3), -T(0), T(0), T(1), T(3), inf, nans[0], nans[1]};
    const auto valid = nulls->IsNotNull();
    ASSERT_EQ(valid.size(), rows.size());
    for (size_t row = 0; row < rows.size(); ++row) {
        EXPECT_EQ(valid[row], validity[row]) << row;
    }
    for (const auto& queries : std::vector<std::vector<T>>{
             {},
             {T(0)},
             {nans[0]},
             {nans[1], T(1), nans[0], T(0), inf, T(3), T(9), -inf, T(0)}}) {
        auto in = predicate->In(queries.size(), queries.data());
        auto not_in = predicate->NotIn(queries.size(), queries.data());
        ASSERT_EQ(in.size(), rows.size());
        ASSERT_EQ(not_in.size(), rows.size());
        for (size_t row = 0; row < rows.size(); ++row) {
            bool hit = false;
            for (const auto value : rows[row]) {
                for (const auto query : queries) {
                    hit |= SqlCompare(value, query) == 0;
                }
            }
            EXPECT_EQ(in[row], validity[row] && hit) << row;
            EXPECT_EQ(not_in[row], validity[row] && !hit) << row;
        }
    }
    for (const auto key : bounds) {
        for (const auto op : {CompareOp::Equal,
                              CompareOp::NotEqual,
                              CompareOp::LessThan,
                              CompareOp::LessEqual,
                              CompareOp::GreaterThan,
                              CompareOp::GreaterEqual}) {
            const auto actual = predicate->Range(key, op);
            for (size_t row = 0; row < rows.size(); ++row) {
                bool hit = false;
                for (const auto value : rows[row]) {
                    const auto cmp = SqlCompare(value, key);
                    switch (op) {
                        case CompareOp::Equal:
                            hit |= cmp == 0;
                            break;
                        case CompareOp::NotEqual:
                            hit |= cmp == 0;
                            break;
                        case CompareOp::LessThan:
                            hit |= cmp < 0;
                            break;
                        case CompareOp::LessEqual:
                            hit |= cmp <= 0;
                            break;
                        case CompareOp::GreaterThan:
                            hit |= cmp > 0;
                            break;
                        case CompareOp::GreaterEqual:
                            hit |= cmp >= 0;
                            break;
                    }
                }
                if (op == CompareOp::NotEqual) {
                    hit = !hit;
                }
                EXPECT_EQ(actual[row], validity[row] && hit)
                    << "row=" << row << " op=" << static_cast<int>(op)
                    << " key=" << key << " array=" << array;
            }
        }
        for (const auto upper : bounds) {
            for (const bool lower_inclusive : {false, true}) {
                for (const bool upper_inclusive : {false, true}) {
                    const auto actual = predicate->Range(
                        key, lower_inclusive, upper, upper_inclusive);
                    for (size_t row = 0; row < rows.size(); ++row) {
                        bool hit = false;
                        for (const auto value : rows[row]) {
                            const auto lo = SqlCompare(value, key);
                            const auto hi = SqlCompare(value, upper);
                            hit |= (lo > 0 || (lo == 0 && lower_inclusive)) &&
                                   (hi < 0 || (hi == 0 && upper_inclusive));
                        }
                        EXPECT_EQ(actual[row], validity[row] && hit)
                            << "row=" << row << " lower=" << key
                            << " upper=" << upper;
                    }
                }
            }
        }
    }
}

template <typename T>
void
CheckScalarAndNested() {
    const auto nans = NaNPayloads<T>();
    const T inf = std::numeric_limits<T>::infinity();
    const std::vector<T> values{
        nans[0], -inf, -T(3), -T(0), T(0), T(3), inf, nans[1], nans[0]};
    const std::vector<bool> validity{
        true, true, true, true, true, true, true, true, false};
    std::vector<std::vector<T>> rows;
    for (const auto value : values) rows.push_back({value});
    for (const auto family : {families::kSort,
                              families::kBitmap,
                              families::kInverted,
                              families::kHybrid}) {
        for (const bool nested : {false, true}) {
            for (const bool mmap : {false, true}) {
                for (const bool high : {false, true}) {
                    if (family != std::string_view(families::kHybrid) && high)
                        continue;
                    SCOPED_TRACE(std::string(family) +
                                 "/nested=" + std::to_string(nested) +
                                 "/mmap=" + std::to_string(mmap) +
                                 "/high=" + std::to_string(high));
                    BackendCatalog catalog;
                    catalog.Add<T>(NaNSpec(
                        family, CppDataType<T>(), false, nested, mmap, high));
                    const auto& backend = catalog.Get<T>("NaN");
                    ScalarTestData<T> data(values);
                    if (!nested) {
                        data.validity.reset(values.size() - 1);
                    }
                    data.batch_sizes = {3, 0, values.size() - 3};
                    const ScalarTestInput<T> input(data);
                    auto artifact = backend.Build(input.View());
                    const auto persisted = SerializeV3(*artifact);
                    if (family == std::string_view(families::kHybrid)) {
                        EXPECT_EQ(persisted.metadata.at(INDEX_TYPE)
                                      .template get<uint8_t>(),
                                  static_cast<uint8_t>(
                                      high ? ScalarIndexType::STLSORT
                                           : ScalarIndexType::BITMAP));
                    }
                    for (const bool async : {false, true}) {
                        auto reader = OpenV3(backend,
                                             persisted,
                                             {.row_count = values.size()},
                                             async);
                        ASSERT_NE(reader, nullptr);
                        EXPECT_EQ(reader->CoordDomain(),
                                  nested ? Domain::Element : Domain::Row);
                        auto expected_validity = validity;
                        if (nested)
                            expected_validity.back() = true;
                        CheckPredicates(
                            *reader, rows, expected_validity, false);
                    }
                }
            }
        }
    }
}

template <typename T>
void
CheckArrayRows() {
    const auto nans = NaNPayloads<T>();
    const T inf = std::numeric_limits<T>::infinity();
    const std::vector<std::vector<T>> rows{{T(1), nans[0], nans[1], T(1)},
                                           {T(3)},
                                           {},
                                           {nans[1]},
                                           {nans[0]},
                                           {T(0), -T(0), inf, -inf}};
    const std::vector<bool> validity{true, true, true, true, false, true};
    for (const auto family : {families::kSort,
                              families::kBitmap,
                              families::kInverted,
                              families::kHybrid}) {
        for (const bool mmap : {false, true}) {
            for (const bool high : {false, true}) {
                if (family != std::string_view(families::kHybrid) && high)
                    continue;
                SCOPED_TRACE(std::string(family) +
                             "/array/mmap=" + std::to_string(mmap) +
                             "/high=" + std::to_string(high));
                BackendCatalog catalog;
                catalog.Add<ArrayView>(
                    NaNSpec(family, CppDataType<T>(), true, false, mmap, high));
                const auto& backend = catalog.Get<ArrayView>("NaN");
                std::vector<OwnedArrayValue> values;
                for (const auto& row : rows) {
                    values.push_back(OwnedArrayValue::FromFixed<T>(
                        CppDataType<T>(), std::span<const T>(row)));
                }
                ScalarTestData<ArrayView> data(std::move(values));
                data.validity.reset(4);
                data.batch_sizes = {2, 0, rows.size() - 2};
                const ScalarTestInput<ArrayView> input(data);
                auto artifact = backend.Build(input.View());
                const auto persisted = SerializeV3(*artifact);
                if (family == std::string_view(families::kHybrid)) {
                    EXPECT_EQ(
                        persisted.metadata.at(INDEX_TYPE).get<uint8_t>(),
                        static_cast<uint8_t>(high ? ScalarIndexType::STLSORT
                                                  : ScalarIndexType::BITMAP));
                }
                for (const bool async : {false, true}) {
                    auto reader = OpenV3(
                        backend, persisted, {.row_count = rows.size()}, async);
                    ASSERT_NE(reader, nullptr);
                    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
                    EXPECT_FALSE(reader->Caps().value_lookup);
                    CheckPredicates(*reader, rows, validity, true);
                }
            }
        }
    }
}

TEST(NaNIndexTest, ScalarAndNestedFloatTotalOrderAfterReload) {
    CheckScalarAndNested<float>();
}
TEST(NaNIndexTest, ScalarAndNestedDoubleTotalOrderAfterReload) {
    CheckScalarAndNested<double>();
}
TEST(NaNIndexTest, ArrayFloatTotalOrderAfterReload) {
    CheckArrayRows<float>();
}
TEST(NaNIndexTest, ArrayDoubleTotalOrderAfterReload) {
    CheckArrayRows<double>();
}

}  // namespace
}  // namespace milvus::index::test
