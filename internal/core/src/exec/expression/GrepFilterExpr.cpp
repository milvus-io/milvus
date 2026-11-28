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

#include "exec/expression/GrepFilterExpr.h"

#include <algorithm>
#include <string_view>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Vector.h"
#include "grep/CompiledPlan.h"
#include "segcore/SegcoreConfig.h"

namespace milvus {
namespace exec {

namespace {
// The predicate keeps no matching details, but the matcher API still returns
// the first matching line of each selected value. Do not let that transient
// copy fail a request on a result budget meant for returned details.
constexpr uint64_t kMatcherResultBytes = uint64_t{1} << 40;
}  // namespace

PhyGrepFilterExpr::PhyGrepFilterExpr(
    std::vector<ExprPtr>&& input,
    const std::shared_ptr<const milvus::expr::GrepExpr>& expr,
    const std::string& name,
    milvus::OpContext* op_ctx,
    const segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    int64_t batch_size,
    std::shared_ptr<const segcore::SegmentReadSnapshot> snapshot)
    : Expr(DataType::BOOL, std::move(input), name, op_ctx),
      expr_(expr),
      segment_(segment),
      active_count_(active_count),
      batch_size_(batch_size),
      snapshot_(std::move(snapshot)) {
    AssertInfo(segment_ != nullptr, "grep filter requires a segment");
    AssertInfo(batch_size_ > 0,
               "expr batch size should greater than zero, but now: {}",
               batch_size_);
}

void
PhyGrepFilterExpr::Prepare() {
    if (prepared_) {
        return;
    }
    const auto field = expr_->column_.field_id_;
    const auto schema = segment_->get_schema_snapshot();
    meta_.emplace((*schema)[field]);
    const auto data_type = meta_->get_data_type();
    AssertInfo(data_type == DataType::VARCHAR || data_type == DataType::TEXT,
               "grep requires a text field");

    const auto& config = segcore::SegcoreConfig::default_config();
    read_batch_size_ = std::max<int64_t>(1, config.get_grep_batch_size());

    AssertInfo(expr_->plan_ != nullptr, "grep patterns are not compiled");
    session_.emplace(expr_->plan_->CreateSession(grep::PredicateSelection()));
    if (op_ctx_ != nullptr) {
        cancel_callback_ = std::make_unique<folly::CancellationCallback>(
            op_ctx_->cancellation_token,
            [token = session_->GetCancellationToken()] { token.Cancel(); });
    }

    if (snapshot_ == nullptr) {
        snapshot_ = segment_->CaptureReadSnapshot();
    }
    rows_ = segment_->get_row_count();
    const bool borrow_inline_text = snapshot_ && data_type == DataType::TEXT &&
                                    !meta_->is_external_field() &&
                                    segment_->HasFieldData(field);
    const auto* view_column =
        snapshot_ && (data_type == DataType::VARCHAR || borrow_inline_text)
            ? snapshot_->GetColumn(field)
            : nullptr;
    use_views_ = data_type == DataType::VARCHAR &&
                 segment_->HasFieldData(field) &&
                 (!snapshot_ || view_column != nullptr);
    read_column_ = (use_views_ || borrow_inline_text) ? view_column : nullptr;

    // Rows the NGRAM bitmap excludes are reported FALSE without reading them.
    // That loses the NULL/FALSE distinction, which only a null-rejecting
    // consumer may ignore; under NOT a nullable field is always read.
    if (null_rejecting_ || !meta_->is_nullable()) {
        index_ = grep::PrepareGrepIndex(
            segment_, op_ctx_, *meta_, *expr_->plan_, /*size=*/1, rows_);
    }
    lazy_index_ =
        index_.has_value() && data_type == DataType::VARCHAR && use_views_;
    prepared_ = true;
}

void
PhyGrepFilterExpr::MaterializeCandidates() {
    TargetBitmap candidates(static_cast<size_t>(rows_), true);
    index_->reader->ConditionTreeCandidates(index_->condition, candidates);
    candidates_.emplace(std::move(candidates));
    index_.reset();
    ++index_queries_;
}

void
PhyGrepFilterExpr::EvalRows(std::vector<int64_t>& offsets,
                            std::vector<int64_t>& positions,
                            TargetBitmap& res,
                            TargetBitmap& valid) {
    if (offsets.empty()) {
        return;
    }
    Prepare();

    // Query the index up front when refining these rows without it would
    // exceed the row threshold, or when lengths cannot be checked first.
    if (index_.has_value() &&
        (!lazy_index_ ||
         raw_rows_ + static_cast<int64_t>(offsets.size()) > kLazyIndexRows)) {
        MaterializeCandidates();
    }
    if (candidates_.has_value()) {
        size_t kept = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            AssertInfo(offsets[i] >= 0 && offsets[i] < rows_,
                       "grep offset out of range");
            if ((*candidates_)[offsets[i]]) {
                offsets[kept] = offsets[i];
                positions[kept] = positions[i];
                ++kept;
            }
        }
        offsets.resize(kept);
        positions.resize(kept);
    }

    std::vector<std::string_view> texts;
    std::vector<int64_t> text_positions;
    const auto total = static_cast<int64_t>(offsets.size());
    int64_t begin = 0;
    while (begin < total) {
        if (op_ctx_ != nullptr &&
            op_ctx_->cancellation_token.isCancellationRequested()) {
            ThrowInfo(FollyCancel, "grep cancelled");
        }
        const auto count =
            grep::GrepReadCount(segment_,
                                *meta_,
                                offsets.data() + begin,
                                std::min(read_batch_size_, total - begin),
                                rows_,
                                use_views_,
                                read_column_);
        auto batch = grep::ReadGrepBatch(segment_,
                                         snapshot_,
                                         op_ctx_,
                                         *meta_,
                                         offsets.data() + begin,
                                         count,
                                         use_views_,
                                         read_column_);
        if (index_.has_value()) {
            // Still refining without the bitmap: charge this window, and build
            // the bitmap before matching if it crosses the byte threshold.
            size_t bytes = 0;
            for (int64_t i = 0; i < count; ++i) {
                if (auto text = batch.Text(i)) {
                    bytes += text->size();
                }
            }
            if (bytes >
                kLazyIndexBytes - std::min(raw_bytes_, kLazyIndexBytes)) {
                MaterializeCandidates();
            } else {
                raw_rows_ += count;
                raw_bytes_ += bytes;
            }
        }

        texts.clear();
        text_positions.clear();
        for (int64_t i = 0; i < count; ++i) {
            auto text = batch.Text(i);
            if (!text) {
                valid.reset(positions[begin + i]);
                continue;
            }
            if (candidates_.has_value() &&
                !(*candidates_)[offsets[begin + i]]) {
                continue;
            }
            texts.push_back(*text);
            text_positions.push_back(positions[begin + i]);
        }
        if (!texts.empty()) {
            auto matched = session_->ExecuteBatch(texts, kMatcherResultBytes);
            for (size_t i = 0; i < texts.size(); ++i) {
                if (matched.Selected(i)) {
                    res.set(text_positions[i]);
                }
            }
            refined_rows_ += static_cast<int64_t>(texts.size());
        }
        begin += count;
    }
}

void
PhyGrepFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto* offset_input = context.get_offset_input();
    int64_t size = 0;
    if (offset_input != nullptr) {
        size = static_cast<int64_t>(offset_input->size());
    } else {
        size = NextBatchSize();
        if (size == 0) {
            result = nullptr;
            return;
        }
    }

    TargetBitmap res(size, false);
    TargetBitmap valid(size, true);
    if (!expr_->select_none_) {
        // Rows outside bitmap_input were already decided by earlier items of
        // the enclosing conjunction; their local result is unspecified.
        const auto& pre = context.get_bitmap_input();
        AssertInfo(pre.empty() || static_cast<int64_t>(pre.size()) == size,
                   "bitmap_input size {} != grep batch size {}",
                   pre.size(),
                   size);
        offsets_.clear();
        positions_.clear();
        for (int64_t i = 0; i < size; ++i) {
            if (!pre.empty() && !pre[i]) {
                continue;
            }
            offsets_.push_back(offset_input != nullptr ? (*offset_input)[i]
                                                       : current_pos_ + i);
            positions_.push_back(i);
        }
        EvalRows(offsets_, positions_, res, valid);
    }

    result = std::make_shared<ColumnVector>(std::move(res), std::move(valid));
    if (offset_input == nullptr) {
        current_pos_ += size;
    }
}

}  // namespace exec
}  // namespace milvus
