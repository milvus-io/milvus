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

TEST(FilterBitsNodeTest, PredicateConversionPreservesUnalignedViewBoundaries) {
    for (const size_t size : {0, 1, 7, 8, 63, 64, 65, 127, 128, 129, 4097}) {
        for (const size_t offset : {0, 1, 7, 63}) {
            TargetBitmap data(size + offset + 70, true);
            TargetBitmap valid(size + offset + 70, false);
            TargetBitmapView data_view(data.data(), offset, size);
            TargetBitmapView valid_view(valid.data(), offset + 3, size);
            for (size_t i = 0; i < size; ++i) {
                data_view[i] = i % 2 == 0;
                valid_view[i] = i % 3 != 0;
            }
            EXPECT_EQ(
                ConvertPredicateToFilteredBitset(data_view, valid_view, size),
                size == 0);
            for (size_t i = 0; i < size; ++i) {
                EXPECT_EQ(data_view[i], !(i % 2 == 0 && i % 3 != 0));
                EXPECT_TRUE(valid_view[i]);
            }
            for (size_t i = 0; i < data.size(); ++i) {
                if (i < offset || i >= offset + size) {
                    EXPECT_TRUE(data[i]);
                }
                if (i < offset + 3 || i >= offset + 3 + size) {
                    EXPECT_FALSE(valid[i]);
                }
            }
        }
    }
}

}  // namespace
}  // namespace exec
}  // namespace milvus
