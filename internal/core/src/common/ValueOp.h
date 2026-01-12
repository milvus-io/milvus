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
    Not(ColumnVectorPtr left) {
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
    And(ColumnVectorPtr left, ColumnVectorPtr right) {
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
    Or(ColumnVectorPtr left, ColumnVectorPtr right) {
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

    static size_t
    TrueCount(ColumnVectorPtr res) {
        TargetBitmapView data(res->GetRawData(), res->size());
        TargetBitmapView valid_data(res->GetValidRawData(), res->size());
        TargetBitmap tmp(data);
        tmp.inplace_and(valid_data, res->size());
        return tmp.count();
    }
};
}  // namespace common
}  // namespace milvus