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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/Vector.h"
#include "common/RegexIndexCostModel.h"
#include "exec/QueryContext.h"

namespace milvus {
namespace exec {

using OffsetVector = FixedVector<int32_t>;
class EvalCtx {
 public:
    using RegexWorkProvider = std::function<std::optional<RegexIndexWorkStats>(
        size_t, size_t, size_t)>;

    // Borrow only for one successor Eval. Replacing the mask or offset input
    // invalidates the association; destruction must not resurrect stale stats.
    class ScopedRegexWorkProvider {
     public:
        ScopedRegexWorkProvider(EvalCtx& context,
                                const RegexWorkProvider* provider)
            : context_(context),
              saved_(context.regex_work_provider_),
              generation_(context.bitmap_input_generation_) {
            context_.regex_work_provider_ = provider;
        }
        ~ScopedRegexWorkProvider() {
            context_.regex_work_provider_ =
                generation_ == context_.bitmap_input_generation_ ? saved_
                                                                 : nullptr;
        }
        ScopedRegexWorkProvider(const ScopedRegexWorkProvider&) = delete;
        ScopedRegexWorkProvider&
        operator=(const ScopedRegexWorkProvider&) = delete;

     private:
        EvalCtx& context_;
        const RegexWorkProvider* saved_;
        size_t generation_;
    };

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
        InvalidateRegexWorkProvider();
        offset_input_ = offset_input;
    }

    inline void
    set_bitmap_input(TargetBitmap&& bitmap_input) {
        InvalidateRegexWorkProvider();
        bitmap_input_ = std::move(bitmap_input);
    }

    inline const TargetBitmap&
    get_bitmap_input() const {
        return bitmap_input_;
    }

    void
    clear_bitmap_input() {
        InvalidateRegexWorkProvider();
        bitmap_input_.clear();
    }

    std::optional<RegexIndexWorkStats>
    get_regex_work_stats(size_t begin,
                         size_t remaining_rows,
                         size_t batch_rows) {
        if (offset_input_ != nullptr || regex_work_provider_ == nullptr) {
            return std::nullopt;
        }
        return (*regex_work_provider_)(begin, remaining_rows, batch_rows);
    }

    bool
    has_regex_work_provider() const {
        return regex_work_provider_ != nullptr;
    }

 private:
    void
    InvalidateRegexWorkProvider() {
        regex_work_provider_ = nullptr;
        ++bitmap_input_generation_;
    }
    ExecContext* exec_ctx_ = nullptr;
    // we may accept offsets array as input and do expr filtering on these data
    OffsetVector* offset_input_ = nullptr;

    // used for expr pre filter, that avoid unnecessary execution on filtered data
    TargetBitmap bitmap_input_;
    const RegexWorkProvider* regex_work_provider_ = nullptr;
    size_t bitmap_input_generation_ = 0;
};

}  // namespace exec
}  // namespace milvus
