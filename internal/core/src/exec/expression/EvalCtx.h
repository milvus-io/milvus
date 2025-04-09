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

#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "common/Vector.h"
#include "exec/QueryContext.h"

namespace milvus {
namespace exec {

class ExprSet;

using OffsetVector = FixedVector<int32_t>;
class EvalCtx {
 public:
    EvalCtx(ExecContext* exec_ctx,
            ExprSet* expr_set,
            OffsetVector* offset_input)
        : exec_ctx_(exec_ctx),
          expr_set_(expr_set),
          offset_input_(offset_input) {
        assert(exec_ctx_ != nullptr);
        assert(expr_set_ != nullptr);
    }

    explicit EvalCtx(ExecContext* exec_ctx, ExprSet* expr_set)
        : exec_ctx_(exec_ctx), expr_set_(expr_set) {
    }

    explicit EvalCtx(ExecContext* exec_ctx) : exec_ctx_(exec_ctx) {
    }

    ExecContext*
    get_exec_context() {
        return exec_ctx_;
    }

    std::shared_ptr<QueryConfig>
    get_query_config() {
        return exec_ctx_->get_query_config();
    }

    inline OffsetVector*
    get_offset_input() {
        return offset_input_;
    }

    inline void
    set_offset_input(OffsetVector* offset_input) {
        offset_input_ = offset_input;
    }

    inline void
    set_bitmap_input(TargetBitmap&& bitmap_input) {
        bitmap_input_ = std::move(bitmap_input);
    }

    inline const TargetBitmap&
    get_bitmap_input() const {
        return bitmap_input_;
    }

    void
    clear_bitmap_input() {
        bitmap_input_.clear();
    }

    void
    set_apply_valid_data_after_flip(bool apply_valid_data_after_flip) {
        apply_valid_data_after_flip_ = apply_valid_data_after_flip;
    }

    bool
    get_apply_valid_data_after_flip() const {
        return apply_valid_data_after_flip_;
    }

 private:
    ExecContext* exec_ctx_ = nullptr;
    ExprSet* expr_set_ = nullptr;
    // we may accept offsets array as input and do expr filtering on these data
    OffsetVector* offset_input_ = nullptr;
    bool input_no_nulls_ = false;

    // used for expr pre filter, that avoid unnecessary execution on filtered data
    TargetBitmap bitmap_input_;

    // for some expr(eg. exists), we do not need to apply valid data after flip
    bool apply_valid_data_after_flip_ = true;
};

}  // namespace exec
}  // namespace milvus