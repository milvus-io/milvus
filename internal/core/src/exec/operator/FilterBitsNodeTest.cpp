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
#include <optional>
#include <vector>

#include "common/Types.h"
#include "common/Vector.h"
#include "exec/operator/FilterBitsNode.h"

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

TEST(FilterBitsNodeTest, PredicateConversionUsesKnownAllValidMetadata) {
    auto data = MakeBitmap({true, false, true, false});
    TargetBitmap valid(data.size(), true);
    auto col_vec = std::make_shared<ColumnVector>(
        std::move(data), std::move(valid), std::optional<size_t>{0});

    ASSERT_TRUE(col_vec->AllValidKnown());

    TargetBitmapView data_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        data_view, valid_view, col_vec->size(), col_vec->AllValidKnown());
    col_vec->MarkAllValid();

    EXPECT_TRUE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data_view),
              (std::vector<bool>{false, true, false, true}));
    EXPECT_TRUE(col_vec->AllValidKnown());
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

}  // namespace
}  // namespace exec
}  // namespace milvus
