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

#include "exec/expression/RoaringFilterExpr.h"

#include "common/RoaringMembership.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"

namespace milvus {

namespace expr {

// Out-of-line because ITypeExpr.h only forward-declares RoaringMembership.
std::string
RoaringFilterExpr::ToString() const {
    // Never render the bitmap itself: it is user data and can be tens of
    // megabytes. The body size and cardinality are enough to identify the
    // filter in a log line.
    return fmt::format(
        "RoaringFilterExpr:[Column: {}, PortableBodyBytes: {}, "
        "Cardinality: {}]",
        column_.ToString(),
        membership_->serialized_size(),
        membership_->cardinality());
}

}  // namespace expr

namespace exec {

PhyRoaringFilterExpr::PhyRoaringFilterExpr(
    const std::vector<std::shared_ptr<Expr>>& input,
    const std::shared_ptr<const milvus::expr::RoaringFilterExpr>& expr,
    const std::string& name,
    milvus::OpContext* op_ctx,
    const segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    int64_t batch_size,
    int32_t consistency_level)
    : SegmentExpr(std::move(input),
                  name,
                  op_ctx,
                  segment,
                  expr->column_.field_id_,
                  expr->column_.nested_path_,
                  DataType::NONE,
                  active_count,
                  batch_size,
                  consistency_level),
      expr_(expr) {
    // The bitmap was decoded once by ProtoParser and is shared by every
    // per-segment physical expression of this query; a null one would mean the
    // logical node was built without going through that path.
    AssertInfo(expr_->membership_ != nullptr,
               "roaring_match physical expression requires membership");
    switch (expr_->column_.data_type_) {
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
            break;
        default:
            ThrowInfo(ExprInvalid,
                      "roaring_match does not support field data type: {}",
                      expr_->column_.data_type_);
    }
}

void
PhyRoaringFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    // Honor iterative-filter candidate offsets: with an offset input,
    // ExecVisitorImpl must evaluate those candidate rows via
    // ProcessDataByOffsets rather than the first N rows sequentially.
    // Mirrors TermExpr and PhyBloomFilterExpr.
    auto* input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);

    switch (expr_->column_.data_type_) {
        case DataType::INT8:
            result = ExecVisitorImpl<int8_t>(context);
            break;
        case DataType::INT16:
            result = ExecVisitorImpl<int16_t>(context);
            break;
        case DataType::INT32:
            result = ExecVisitorImpl<int32_t>(context);
            break;
        case DataType::INT64:
            result = ExecVisitorImpl<int64_t>(context);
            break;
        default:
            ThrowInfo(ExprInvalid,
                      "roaring_match does not support field data type: {}",
                      expr_->column_.data_type_);
    }
}

std::string
PhyRoaringFilterExpr::ToString() const {
    return expr_->ToString();
}

std::optional<milvus::expr::ColumnInfo>
PhyRoaringFilterExpr::GetColumnInfo() const {
    return expr_->column_;
}

template <typename T>
VectorPtr
PhyRoaringFilterExpr::ExecVisitorImpl(EvalCtx& context) {
    auto* input = context.get_offset_input();

    // Index-only sealed field: no raw data to scan. DetermineExecPath()
    // committed to ScalarIndex iff a reverse-lookup-capable index was pinned;
    // route the probe through Reverse_Lookup. If it did NOT, fail with a clear
    // SegcoreError rather than reading zero rows and tripping the batch-size
    // assertion. Field data being absent is a load/state condition, not the
    // request's fault, so this is a System error and stays retriable.
    if (UseIndexCursor()) {
        return ExecVisitorImplForIndex<T>(context);
    }
    if (segment_->type() == SegmentType::Sealed && !has_field_data_at_init_) {
        ThrowInfo(
            FieldNotLoaded,
            "roaring_match cannot evaluate field {}: raw field data is not "
            "loaded and no scalar index with a cheap per-row reverse lookup "
            "is available (a BITMAP index without its offset cache is "
            "excluded because it reverse-looks-up in O(cardinality) per row; "
            "load the raw field data or set "
            "queryNode.indexOffsetCacheEnabled=true)",
            field_id_.get());
    }

    const auto& bitmap_input = context.get_bitmap_input();

    auto real_batch_size_opt = GetNextRealBatchSize(input, false);
    if (!real_batch_size_opt.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *real_batch_size_opt;
    // bitmap_input is indexed by batch-local position below, so a size that
    // disagrees with the batch would silently read past its end.
    AssertInfo(bitmap_input.empty() ||
                   bitmap_input.size() == static_cast<size_t>(real_batch_size),
               "roaring_match bitmap input size {} does not match batch size "
               "{}",
               bitmap_input.size(),
               real_batch_size);

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    const auto& membership = *expr_->membership_;
    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &bitmap_input, &
         membership ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            ValidityView valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res) {
        // data == nullptr means the chunk was skipped upstream; only the
        // bitmap_input cursor needs to advance.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        bool has_bitmap_input = !bitmap_input.empty();
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            if (valid_data && !valid_data[offset]) {
                // NULL never matches, under either roaring_match or
                // not roaring_match — same three-valued behavior as TermExpr.
                // An upstream-excluded candidate is checked first and stays at
                // its initial (false, valid), even when its field value is
                // NULL.
                res[i] = valid_res[i] = false;
                continue;
            }
            // Widen through int64_t so a narrow signed value keeps its
            // two's-complement key: INT8(-1) must probe 0xffffffffffffffff,
            // which is where the client's builder put it.
            res[i] = membership.Contains(static_cast<int64_t>(data[offset]));
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<T>(
            execute_sub_batch, std::nullptr_t{}, input, res, valid_res);
    } else {
        processed_size = ProcessDataChunks<T>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: roaring_match processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyRoaringFilterExpr::ExecVisitorImplForIndex(EvalCtx& context) {
    // Index-only path: recover each value from the scalar index via
    // Reverse_Lookup and probe the bitmap, reusing ProcessIndexLookupByOffsets
    // (the framework's reverse-lookup helper). It applies FilterType::random
    // per row, so the same execute_sub_batch shape as the raw-data path works.
    auto* input = context.get_offset_input();

    auto real_batch_size_opt = GetNextRealBatchSize(input, false);
    if (!real_batch_size_opt.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *real_batch_size_opt;

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    const auto& bitmap_input = context.get_bitmap_input();
    AssertInfo(bitmap_input.empty() ||
                   bitmap_input.size() == static_cast<size_t>(real_batch_size),
               "roaring_match index path bitmap input size {} does not match "
               "batch size {}",
               bitmap_input.size(),
               real_batch_size);

    const auto& membership = *expr_->membership_;
    auto execute_sub_batch =
        [&membership]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            ValidityView valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res) {
        // data == nullptr means the helper either pruned an inactive candidate
        // before reverse lookup (leaving false/valid untouched), or found an
        // active NULL row (after writing false/invalid). In both cases the
        // reader already owns the result.
        if (data == nullptr) {
            return;
        }
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = membership.Contains(static_cast<int64_t>(data[offset]));
        }
    };

    int64_t processed_size;
    if (has_offset_input_) {
        // ProcessDataByOffsets routes to ProcessIndexLookupByOffsets because
        // UseIndexCursor() && num_data_chunk_ == 0, which recovers each value
        // via Reverse_Lookup and drives execute_sub_batch per row.
        processed_size = ProcessDataByOffsetsWithMask<T>(execute_sub_batch,
                                                         std::nullptr_t{},
                                                         input,
                                                         res,
                                                         valid_res,
                                                         bitmap_input);
    } else {
        // No offset input: reverse-look-up the contiguous global row range
        // [current_index_chunk_pos_, +real_batch_size) for this batch.
        OffsetVector batch_offsets(real_batch_size);
        auto start = current_index_chunk_pos_;
        for (int64_t i = 0; i < real_batch_size; ++i) {
            batch_offsets[i] = static_cast<int32_t>(start + i);
        }
        processed_size = ProcessIndexLookupByOffsetsWithMask<T>(
            execute_sub_batch, &batch_offsets, res, valid_res, bitmap_input);
        // ProcessIndexLookupByOffsets is stateless; advance the index cursor
        // for the next batch.
        MoveCursor();
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: roaring_match index path processed rows {} not "
               "equal expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

}  // namespace exec
}  // namespace milvus
