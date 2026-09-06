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

#include <initializer_list>
#include <memory>
#include <vector>

#include "common/Types.h"
#include "common/Vector.h"
#include "expr/ITypeExpr.h"
#include "exec/operator/FilterBitsNode.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {
namespace {

TargetBitmap
MakeBitmap(std::initializer_list<bool> bits) {
    TargetBitmap bitmap(bits.size(), false);
    size_t i = 0;
    for (const auto bit : bits) {
        bitmap.set(i++, bit);
    }
    return bitmap;
}

std::vector<bool>
ToVector(const TargetBitmap& bitmap) {
    std::vector<bool> bits;
    bits.reserve(bitmap.size());
    for (size_t i = 0; i < bitmap.size(); ++i) {
        bits.push_back(bitmap[i]);
    }
    return bits;
}

std::vector<bool>
ToVector(TargetBitmapView bitmap) {
    std::vector<bool> bits;
    bits.reserve(bitmap.size());
    for (size_t i = 0; i < bitmap.size(); ++i) {
        bits.push_back(bitmap[i]);
    }
    return bits;
}

TEST(FilterBitsNodeTest, PredicateConversionUsesFastPathForAllValidResults) {
    auto data = MakeBitmap({true, false, true, false});
    TargetBitmap valid(data.size(), true);

    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        TargetBitmapView(data), TargetBitmapView(valid), data.size());

    EXPECT_TRUE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data), (std::vector<bool>{false, true, false, true}));
    EXPECT_TRUE(valid.all());
}

TEST(FilterBitsNodeTest, PredicateConversionUsesLiveAllValidBitmap) {
    auto data = MakeBitmap({true, false, true, false});
    TargetBitmap valid(data.size(), true);
    auto col_vec =
        std::make_shared<ColumnVector>(std::move(data), std::move(valid));

    TargetBitmapView data_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        data_view, valid_view, col_vec->size());

    EXPECT_TRUE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data_view),
              (std::vector<bool>{false, true, false, true}));
    EXPECT_TRUE(valid_view.all());
}

TEST(FilterBitsNodeTest, PredicateConversionUsesLiveInvalidBitmap) {
    auto data = MakeBitmap({true, false, true, false});
    auto valid = MakeBitmap({true, true, false, false});
    auto col_vec =
        std::make_shared<ColumnVector>(std::move(data), std::move(valid));

    TargetBitmapView data_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        data_view, valid_view, col_vec->size());

    EXPECT_FALSE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data_view),
              (std::vector<bool>{false, true, true, true}));
    EXPECT_TRUE(valid_view.all());
}

TEST(FilterBitsNodeTest, PredicateConversionFiltersOutInvalidResults) {
    auto data = MakeBitmap({true, false, true, false});
    auto valid = MakeBitmap({true, true, false, false});

    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        TargetBitmapView(data), TargetBitmapView(valid), data.size());

    EXPECT_FALSE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data), (std::vector<bool>{false, true, true, true}));
    EXPECT_TRUE(valid.all());
}

TEST(FilterBitsNodeTest, ExprCacheKeyIncludesRuntimeTTLState) {
    auto filter = plan::FilterBitsNode(
        DEFAULT_PLANNODE_ID, std::make_shared<expr::AlwaysTrueExpr>());
    auto make_key = [&](std::optional<FieldId> ttl_field_id,
                        int64_t physical_time_us) {
        query::PlanOptions options;
        options.entity_ttl_field_id = ttl_field_id;
        QueryContext context(
            "test",
            nullptr,
            0,
            1,
            0,
            0,
            options,
            std::make_shared<QueryConfig>(),
            nullptr,
            std::unordered_map<std::string, std::shared_ptr<BaseConfig>>(),
            physical_time_us);
        return BuildExprCacheKey(filter, &context);
    };

    const auto field_a_at_t1 = make_key(FieldId(100), 1000);
    const auto field_b_at_t1 = make_key(FieldId(101), 1000);
    const auto field_a_at_t2 = make_key(FieldId(100), 2000);
    const auto no_ttl = make_key(std::nullopt, 1000);

    EXPECT_NE(field_a_at_t1, field_b_at_t1);
    EXPECT_NE(field_a_at_t1, field_a_at_t2);
    EXPECT_NE(field_a_at_t1, no_ttl);
    EXPECT_NE(field_a_at_t1.find("entity_ttl_field_id:100"), std::string::npos);
    EXPECT_NE(field_a_at_t1.find("entity_ttl_physical_time_us:1000"),
              std::string::npos);
}

}  // namespace
}  // namespace exec
}  // namespace milvus
