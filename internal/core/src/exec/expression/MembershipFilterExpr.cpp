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

#include "exec/expression/MembershipFilterExpr.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/Json.h"
#include "storage/MmapManager.h"

namespace milvus {
namespace exec {

template <typename LogicalExpr, typename ProbePolicy>
void
PhyMembershipFilterExpr<LogicalExpr, ProbePolicy>::Eval(EvalCtx& context,
                                                        VectorPtr& result) {
    WaitPrefetch();
    // Honor iterative-filter candidate offsets: when the upstream passes an
    // offset input, ExecVisitorImpl must evaluate those candidate rows via
    // ProcessDataByOffsets rather than the first N rows sequentially.
    // Mirrors TermExpr.
    auto* input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);
    auto data_type = expr_->column_.data_type_;
    switch (data_type) {
        case DataType::INT8: {
            result = ExecVisitorImpl<int8_t>(context);
            break;
        }
        case DataType::INT16: {
            result = ExecVisitorImpl<int16_t>(context);
            break;
        }
        case DataType::INT32: {
            result = ExecVisitorImpl<int32_t>(context);
            break;
        }
        case DataType::INT64: {
            result = ExecVisitorImpl<int64_t>(context);
            break;
        }
        case DataType::VARCHAR: {
            if constexpr (ProbePolicy::kSupportsVarChar) {
                if (segment_->type() == SegmentType::Growing &&
                    !storage::MmapManager::GetInstance()
                         .GetMmapConfig()
                         .growing_enable_mmap) {
                    result = ExecVisitorImpl<std::string>(context);
                } else {
                    result = ExecVisitorImpl<std::string_view>(context);
                }
            } else {
                ThrowInfo(ExprInvalid,
                          "{} does not support field data type: {}",
                          ProbePolicy::kKindName,
                          data_type);
            }
            break;
        }
        case DataType::JSON: {
            if constexpr (ProbePolicy::kSupportsJson) {
                result = ExecVisitorImplJson(context);
            } else {
                ThrowInfo(ExprInvalid,
                          "{} does not support field data type: {}",
                          ProbePolicy::kKindName,
                          data_type);
            }
            break;
        }
        default:
            ThrowInfo(ExprInvalid,
                      "{} does not support field data type: {}",
                      ProbePolicy::kKindName,
                      data_type);
    }
}

template <typename LogicalExpr, typename ProbePolicy>
template <typename T>
VectorPtr
PhyMembershipFilterExpr<LogicalExpr, ProbePolicy>::ExecVisitorImpl(
    EvalCtx& context) {
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
            "{} cannot evaluate field {}: raw field data is not loaded and "
            "no scalar index with a cheap per-row reverse lookup is available "
            "(a BITMAP index without its offset cache is excluded because it "
            "reverse-looks-up in O(cardinality) per row; load the raw field "
            "data or set queryNode.indexOffsetCacheEnabled=true)",
            ProbePolicy::kKindName,
            field_id_.get());
    }

    const auto& bitmap_input = context.get_bitmap_input();

    auto next_batch_size = GetNextRealBatchSize(input, false);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *next_batch_size;
    // bitmap_input is indexed by batch-local position below, so a size that
    // disagrees with the batch would silently read past its end.
    AssertInfo(bitmap_input.empty() ||
                   bitmap_input.size() == static_cast<size_t>(real_batch_size),
               "{} bitmap input size {} does not match batch size {}",
               ProbePolicy::kKindName,
               bitmap_input.size(),
               real_batch_size);

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    int processed_cursor = 0;
    auto execute_sub_batch =
        [ this, &processed_cursor, &
          bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            ValidityView valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res) {
        // A null data pointer means evaluation was suppressed because the
        // payload was skipped, the candidate was inactive, or an index
        // reverse-lookup miss already applied its invalid result. Ordinary
        // nullable Scan/Take rows carry a placeholder plus real validity.
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
            // Upstream-excluded candidates are checked FIRST and stay at
            // their initial (false, valid) even when the field value is
            // NULL — the contract pinned across all three membership test
            // suites (BitmapInputPrunesByCandidatePosition,
            // ScalarBitmapInputLeavesExcludedNullCandidatesUntouched) and
            // mirrored by the framework's index-path helper, which leaves
            // excluded slots untouched so raw and index-only load states
            // return bit-identical columns. A probed NULL row never
            // matches, under either polarity: res = valid = false.
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            if (valid_data && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = probe_(data[offset]);
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsetsWithMask<T>(execute_sub_batch,
                                                         std::nullptr_t{},
                                                         input,
                                                         res,
                                                         valid_res,
                                                         bitmap_input);
    } else {
        processed_size = ProcessDataChunks<T>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: {} processed rows {} not equal expect batch "
               "size {}",
               ProbePolicy::kKindName,
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename LogicalExpr, typename ProbePolicy>
template <typename T>
VectorPtr
PhyMembershipFilterExpr<LogicalExpr, ProbePolicy>::ExecVisitorImplForIndex(
    EvalCtx& context) {
    // Index-only path: recover each value from the scalar index via
    // Reverse_Lookup and probe it exactly as the raw-data path would,
    // reusing the framework's mask-aware reverse-lookup helper. An empty
    // candidate mask degenerates to the unmasked behavior, so one code path
    // serves both.
    auto* input = context.get_offset_input();

    auto next_batch_size = GetNextRealBatchSize(input, false);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *next_batch_size;

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    const auto& bitmap_input = context.get_bitmap_input();
    AssertInfo(bitmap_input.empty() ||
                   bitmap_input.size() == static_cast<size_t>(real_batch_size),
               "{} index path bitmap input size {} does not match batch "
               "size {}",
               ProbePolicy::kKindName,
               bitmap_input.size(),
               real_batch_size);

    auto execute_sub_batch = [this]<FilterType filter_type =
                                        FilterType::sequential>(
        const T* data,
        ValidityView valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        // data == nullptr means the helper either pruned an inactive candidate
        // before reverse lookup (leaving false/valid untouched), or found an
        // active missing value (after writing false/invalid). In both cases
        // the reader already owns the result.
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
            res[i] = probe_(data[offset]);
        }
    };

    int64_t processed_size;
    if (has_offset_input_) {
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
        // for the next batch. MoveCursor() honors the has_offset_input_ guard
        // and, on the ScalarIndex path with no raw data, advances only the
        // index cursor.
        MoveCursor();
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: {} index path processed rows {} not equal "
               "expect batch size {}",
               ProbePolicy::kKindName,
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename LogicalExpr, typename ProbePolicy>
template <typename Dummy>
VectorPtr
PhyMembershipFilterExpr<LogicalExpr, ProbePolicy>::ExecVisitorImplJson(
    EvalCtx& context) {
    static_assert(ProbePolicy::kSupportsJson,
                  "JSON probing requires a policy that supports it");
    auto* input = context.get_offset_input();

    // JSON paths are data-path only: DetermineExecPath() never commits to
    // ScalarIndex for JSON (no per-row reverse lookup exists for a JSON path),
    // so an index-only sealed JSON field cannot be probed at all.
    if (segment_->type() == SegmentType::Sealed && !has_field_data_at_init_) {
        ThrowInfo(FieldNotLoaded,
                  "{} cannot evaluate JSON field {}: raw field data is not "
                  "loaded, and a JSON path has no scalar index with a per-row "
                  "reverse lookup; load the raw JSON field data",
                  ProbePolicy::kKindName,
                  field_id_.get());
    }

    const auto& bitmap_input = context.get_bitmap_input();

    auto next_batch_size = GetNextRealBatchSize(input, false);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *next_batch_size;

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    const auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    int processed_cursor = 0;
    auto execute_sub_batch =
        [ this, &processed_cursor, &
          bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            ValidityView valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer) {
        // A null data pointer means evaluation was suppressed because the
        // payload was skipped or the candidate was inactive. Ordinary
        // nullable Scan/Take rows carry a placeholder plus real validity.
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
            // Excluded candidates first (untouched semantics, matching the
            // scalar paths and the JsonBitmapInput test pin), then the
            // whole-row NULL rule: NULL never matches, either polarity.
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            if (valid_data && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            // STRICTLY TYPED probe: the hash domain has exactly two kinds,
            // int64 (8-byte LE) and raw UTF-8 bytes, and a JSON value is
            // hashed only when it is stored AS that type:
            //   - string -> raw UTF-8 bytes (same as a VARCHAR probe)
            //   - int64  -> 8-byte-LE int64 hash
            //   - double / uint64-beyond-int64 -> NEVER a member (res=false,
            //     valid=true, so `not ...` returns the row). Deliberate
            //     divergence from exact `in`, whose JSON semantics unify
            //     5.0 == 5: no numeric canonicalization rule to keep
            //     bit-identical across every prober/SDK forever.
            //   - missing key / JSON null / bool / object / array -> the
            //     probe has no value: res=false AND valid=false (three-valued).
            // The blob's declared value domains gate the probes themselves,
            // so a JSON string cannot alias an int64-only filter.
            const auto& json = data[offset];
            auto str = json.template at<std::string_view>(pointer);
            if (!str.error()) {
                res[i] = probe_.TestBytesValue(str.value().data(),
                                               str.value().size());
                continue;
            }
            auto num = json.at_numeric(pointer);
            if (num.error()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (auto n = num.value(); n.is_int64()) {
                res[i] = probe_.TestInt64Value(n.get_int64());
            }
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size =
            ProcessDataByOffsetsWithMask<milvus::Json>(execute_sub_batch,
                                                       std::nullptr_t{},
                                                       input,
                                                       res,
                                                       valid_res,
                                                       bitmap_input,
                                                       pointer);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, pointer);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: {} json path processed rows {} not equal "
               "expect batch size {}",
               ProbePolicy::kKindName,
               processed_size,
               real_batch_size);
    return res_vec;
}

// The two kinds of the membership family. The aliases keep the historical
// class names so factory construction strings and logs stay stable.
template class PhyMembershipFilterExpr<milvus::expr::BloomFilterExpr,
                                       BloomMembershipProbe>;
template class PhyMembershipFilterExpr<milvus::expr::RoaringFilterExpr,
                                       RoaringMembershipProbe>;

}  // namespace exec
}  // namespace milvus
