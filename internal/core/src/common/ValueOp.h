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
    // and return an independent, read-only result.
    static ColumnVectorPtr
    Not(const ColumnVectorPtr& input) {
        TargetBitmap data(input->GetBitmap());
        TargetBitmap valid(input->GetValidBitmap());
        data.flip();
        data.inplace_and(valid, input->size());
        return std::make_shared<ColumnVector>(std::move(data),
                                              std::move(valid));
    }

    // apply and operation to the left and right column vectors
    // and return an independent, read-only result.
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
    static ColumnVectorPtr
    And(const ColumnVectorPtr& left, const ColumnVectorPtr& right) {
        TargetBitmap data(left->GetBitmap());
        TargetBitmap valid(left->GetValidBitmap());
        And(data.write_view(),
            valid.write_view(),
            right->GetBitmap(),
            right->GetValidBitmap(),
            left->size());
        return std::make_shared<ColumnVector>(std::move(data),
                                              std::move(valid));
    }

    static void
    And(TargetBitmapWriteView left_data,
        TargetBitmapWriteView left_valid,
        TargetBitmapView right_data,
        TargetBitmapView right_valid,
        size_t size) {
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
    // and return an independent, read-only result.
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
    static ColumnVectorPtr
    Or(const ColumnVectorPtr& left, const ColumnVectorPtr& right) {
        TargetBitmap data(left->GetBitmap());
        TargetBitmap valid(left->GetValidBitmap());
        Or(data.write_view(),
           valid.write_view(),
           right->GetBitmap(),
           right->GetValidBitmap(),
           left->size());
        return std::make_shared<ColumnVector>(std::move(data),
                                              std::move(valid));
    }

    static void
    Or(TargetBitmapWriteView left_data,
       TargetBitmapWriteView left_valid,
       TargetBitmapView right_data,
       TargetBitmapView right_valid,
       size_t size) {
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
};
}  // namespace common
}  // namespace milvus