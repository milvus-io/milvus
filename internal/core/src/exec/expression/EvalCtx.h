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
#include <utility>
#include <variant>
#include <vector>

#include "common/Vector.h"
#include "exec/QueryContext.h"

namespace milvus {
namespace exec {

using OffsetVector = FixedVector<int32_t>;
class EvalCtx {
 public:
    EvalCtx(ExecContext* exec_ctx, OffsetVector* offset_input)
        : exec_ctx_(exec_ctx), offset_input_(offset_input) {
        assert(exec_ctx_ != nullptr);
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

    // Candidate masks are read-only. A borrowed view must outlive Eval and
    // must not be resized or mutated while it is installed in the context.
    // Keep ownership with saved masks when AND/OR temporarily replaces them.
    class BitmapInput {
     public:
        BitmapInput() = default;
        explicit BitmapInput(TargetBitmap&& bitmap)
            : storage_(std::move(bitmap)) {
        }
        explicit BitmapInput(TargetBitmapView view) : storage_(view) {
        }
        BitmapInput(BitmapInput&&) = default;
        BitmapInput&
        operator=(BitmapInput&&) = default;

        const TargetBitmapView
        view() const {
            return std::visit([](const auto& bitmap) { return bitmap.view(); },
                              storage_);
        }

     private:
        std::variant<TargetBitmap, TargetBitmapView> storage_;
    };

    void
    set_bitmap_input(TargetBitmap&& bitmap) {
        bitmap_input_ = BitmapInput(std::move(bitmap));
    }

    void
    set_bitmap_input(TargetBitmapView view) {
        bitmap_input_ = BitmapInput(view);
    }

    void
    set_bitmap_input(BitmapInput&& bitmap) {
        bitmap_input_ = std::move(bitmap);
    }

    BitmapInput
    take_bitmap_input() {
        return std::exchange(bitmap_input_, BitmapInput{});
    }

    const TargetBitmapView
    get_bitmap_input() const {
        return bitmap_input_.view();
    }

    void
    clear_bitmap_input() {
        bitmap_input_ = BitmapInput{};
    }

 private:
    ExecContext* exec_ctx_ = nullptr;
    // we may accept offsets array as input and do expr filtering on these data
    OffsetVector* offset_input_ = nullptr;

    // used for expr pre filter, that avoid unnecessary execution on filtered data
    BitmapInput bitmap_input_;
};

}  // namespace exec
}  // namespace milvus