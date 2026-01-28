// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include "common/Types.h"
#include "common/Vector.h"
namespace milvus {
namespace common {

class ThreeValuedLogicOp {
 public:
    // apply not operation to the left column vector
    // and store the result in the left column vector
    static void
    Not(const ColumnVectorPtr& left) {
        TargetBitmapView data(left->GetRawData(), left->size());
        TargetBitmapView valid_data(left->GetValidRawData(), left->size());

        data.flip();
        data.inplace_and(valid_data, left->size());
    }

    // apply and operation to the left and right column vectors
    // and store the result in the left column vector (right is not modified)
    //
    // Three-valued logic AND truth table:
    // | A     | B     | A AND B |
    // |-------|-------|---------|
    // | T     | T     | T       |
    // | T     | F     | F       |
    // | T     | NULL  | NULL    |
    // | F     | T     | F       |
    // | F     | F     | F       |
    // | F     | NULL  | F       |  <- F AND NULL = F (determined)
    // | NULL  | T     | NULL    |
    // | NULL  | F     | F       |  <- NULL AND F = F (determined)
    // | NULL  | NULL  | NULL    |
    //
    // result_valid = (left_valid & ~left_data) |    // left is definitely false
    //                (right_valid & ~right_data) |  // right is definitely false
    //                (left_valid & right_valid)     // both are valid
    // =>
    // result_valid = left_valid & (right_valid | ~left_data) |
    //                right_valid & ~right_data
    static void
    And(const ColumnVectorPtr& left, const ColumnVectorPtr& right) {
        const size_t size = left->size();
        TargetBitmapView left_data(left->GetRawData(), size);
        TargetBitmapView left_valid(left->GetValidRawData(), size);
        TargetBitmapView right_data(right->GetRawData(), size);
        TargetBitmapView right_valid(right->GetValidRawData(), size);

        // tmp = ~left_data | right_valid
        TargetBitmap tmp(size, true);
        tmp.inplace_xor(left_data, size);
        tmp.inplace_or(right_valid, size);

        // left_valid = left_valid & (right_valid | ~left_data)
        left_valid.inplace_and(tmp, size);

        // tmp = right_valid & ~right_data (reuse tmp)
        tmp.set();
        tmp.inplace_xor(right_data, size);   // tmp = ~right_data
        tmp.inplace_and(right_valid, size);  // tmp = right_valid & ~right_data

        // left_valid |= (right_valid & ~right_data)
        left_valid.inplace_or(tmp, size);

        // left_data = left_data & right_data
        left_data.inplace_and(right_data, size);
    }

    // apply or operation to the left and right column vectors
    // and store the result in the left column vector (right is not modified)
    //
    // Three-valued logic OR truth table:
    // | A     | B     | A OR B |
    // |-------|-------|--------|
    // | T     | T     | T      |
    // | T     | F     | T      |
    // | T     | NULL  | T      |  <- T OR NULL = T (determined)
    // | F     | T     | T      |
    // | F     | F     | F      |
    // | F     | NULL  | NULL   |
    // | NULL  | T     | T      |  <- NULL OR T = T (determined)
    // | NULL  | F     | NULL   |
    // | NULL  | NULL  | NULL   |
    //
    // result_valid = (left_valid & left_data) |     // left is definitely true
    //                (right_valid & right_data) |   // right is definitely true
    //                (left_valid & right_valid)     // both are valid
    // =>
    // result_valid = left_valid & (right_valid | left_data) |
    //                right_valid & right_data
    static void
    Or(const ColumnVectorPtr& left, const ColumnVectorPtr& right) {
        const size_t size = left->size();
        TargetBitmapView left_data(left->GetRawData(), size);
        TargetBitmapView left_valid(left->GetValidRawData(), size);
        TargetBitmapView right_data(right->GetRawData(), size);
        TargetBitmapView right_valid(right->GetValidRawData(), size);

        // tmp = left_data | right_valid
        TargetBitmap tmp(size);
        tmp.inplace_or(left_data, size);
        tmp.inplace_or(right_valid, size);

        // left_valid = left_valid & (right_valid | left_data)
        left_valid.inplace_and(tmp, size);

        // tmp = right_valid & right_data (reuse tmp)
        tmp.reset();
        tmp.inplace_or(right_data, size);
        tmp.inplace_and(right_valid, size);

        // left_valid |= (right_valid & right_data)
        left_valid.inplace_or(tmp, size);

        // left_data = left_data | right_data
        left_data.inplace_or(right_data, size);
    }

    // Count the number of rows that are definitely TRUE (valid=1, data=1)
    // Uses zero-copy SIMD popcount for efficiency
    static size_t
    TrueCount(const ColumnVectorPtr& res) {
        const size_t size = res->size();
        if (size == 0) {
            return 0;
        }

        const uint64_t* data =
            reinterpret_cast<const uint64_t*>(res->GetRawData());
        const uint64_t* valid =
            reinterpret_cast<const uint64_t*>(res->GetValidRawData());

        const size_t num_full_words = size / 64;
        const size_t tail_bits = size % 64;

        size_t count = 0;
        // Process full 64-bit words
        for (size_t i = 0; i < num_full_words; ++i) {
            count += __builtin_popcountll(data[i] & valid[i]);
        }

        // Process remaining bits (if any)
        if (tail_bits > 0) {
            const uint64_t mask = (1ULL << tail_bits) - 1;
            count += __builtin_popcountll(data[num_full_words] &
                                          valid[num_full_words] & mask);
        }

        return count;
    }

    // Count the number of rows that are definitely FALSE (valid=1, data=0)
    // Uses zero-copy SIMD popcount for efficiency
    static size_t
    FalseCount(const ColumnVectorPtr& res) {
        const size_t size = res->size();
        if (size == 0) {
            return 0;
        }

        const uint64_t* data =
            reinterpret_cast<const uint64_t*>(res->GetRawData());
        const uint64_t* valid =
            reinterpret_cast<const uint64_t*>(res->GetValidRawData());

        const size_t num_full_words = size / 64;
        const size_t tail_bits = size % 64;

        size_t count = 0;
        // Process full 64-bit words
        // FalseCount = popcount(valid & ~data)
        for (size_t i = 0; i < num_full_words; ++i) {
            count += __builtin_popcountll(valid[i] & ~data[i]);
        }

        // Process remaining bits (if any)
        if (tail_bits > 0) {
            const uint64_t mask = (1ULL << tail_bits) - 1;
            count += __builtin_popcountll(valid[num_full_words] &
                                          ~data[num_full_words] & mask);
        }

        return count;
    }
};
}  // namespace common
}  // namespace milvus