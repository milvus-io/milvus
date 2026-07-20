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

#include "ExistsExpr.h"

#include <algorithm>
#include <set>

#include "bitset/bitset.h"
#include "exec/expression/ExprCacheHelper.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Tracer.h"
#include "common/ScopedTimer.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/bson_view.h"
#include "common/type_c.h"
#include "exec/expression/EvalCtx.h"
#include "folly/FBVector.h"
#include "monitor/Monitor.h"
#include "index/Index.h"
#include "index/JsonFlatIndex.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/utils.h"
#include "opentelemetry/trace/span.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"

namespace milvus {
namespace exec {

namespace {

TargetBitmap
GetJsonFieldValidity(const segcore::SegmentInternalInterface* segment,
                     index::IndexBase* index,
                     milvus::OpContext* op_ctx,
                     FieldId field_id,
                     int64_t active_count) {
    if (auto index_valid = index->FieldIsNotNull(op_ctx);
        index_valid.has_value()) {
        AssertInfo(index_valid->size() == static_cast<size_t>(active_count),
                   "JSON field validity size {} not equal to row count {}",
                   index_valid->size(),
                   active_count);
        return std::move(*index_valid);
    }

    TargetBitmap valid(active_count, true);
    if (!segment->is_nullable(field_id)) {
        return valid;
    }

    // Backward compatibility for JSON indexes built before field-level
    // validity was persisted.  New indexes are self-contained; old indexes
    // can still execute while the raw column is loaded.
    AssertInfo(segment->HasFieldData(field_id),
               "nullable JSON index does not contain field validity and raw "
               "field data is unavailable");
    TargetBitmapView valid_view(valid);
    int64_t processed = 0;
    for (int64_t chunk_id = 0; processed < active_count; ++chunk_id) {
        auto chunk_size = std::min(segment->chunk_size(field_id, chunk_id),
                                   active_count - processed);
        segment->ApplyFieldValidData(
            op_ctx, field_id, chunk_id, 0, chunk_size, valid_view + processed);
        processed += chunk_size;
    }
    return valid;
}

VectorPtr
GatherJsonExistsByOffsets(const TargetBitmap& cached_res,
                          const TargetBitmap& cached_valid_res,
                          const OffsetVector& offsets,
                          const TargetBitmap& bitmap_input) {
    AssertInfo(bitmap_input.empty() || bitmap_input.size() == offsets.size(),
               "bitmap input size {} not equal to offset count {}",
               bitmap_input.size(),
               offsets.size());

    auto result =
        std::make_shared<ColumnVector>(TargetBitmap(offsets.size(), false),
                                       TargetBitmap(offsets.size(), true));
    TargetBitmapView result_view(result->GetRawData(), offsets.size());
    TargetBitmapView valid_view(result->GetValidRawData(), offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        auto offset = offsets[i];
        AssertInfo(
            offset >= 0 && static_cast<size_t>(offset) < cached_res.size(),
            "JSON exists offset {} out of range [0, {})",
            offset,
            cached_res.size());
        valid_view[i] = cached_valid_res[offset];
        if (!valid_view[i]) {
            result_view[i] = false;
            continue;
        }
        if (bitmap_input.empty() || bitmap_input[i]) {
            result_view[i] = cached_res[offset];
        }
    }
    return result;
}

}  // namespace

void
PhyExistsFilterExpr::DetermineExecPath() {
    if (CanUseJsonStatsAtInit()) {
        exec_path_ = ExprExecPath::JsonStats;
        return;
    }
    SegmentExpr::DetermineExecPath();
}

void
PhyExistsFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    tracer::AutoSpan span(
        "PhyExistsFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("data_type",
                                 static_cast<int>(expr_->column_.data_type_));

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    auto data_type = expr_->column_.data_type_;
    if (expr_->column_.element_level_) {
        data_type = expr_->column_.element_type_;
    }
    switch (data_type) {
        case DataType::JSON: {
            span.GetSpan()->SetAttribute("json_filter_expr_type", "exists");
            if (exec_path_ == ExprExecPath::ScalarIndex) {
                result = EvalJsonExistsForIndex(context);
            } else {
                result = EvalJsonExistsForDataSegment(context);
            }
            break;
        }
        default:
            ThrowInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

VectorPtr
PhyExistsFilterExpr::EvalJsonExistsForIndex(EvalCtx& context) {
    auto* input = context.get_offset_input();
    auto real_batch_size =
        input != nullptr ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (cached_index_chunk_id_ != 0) {
        cached_index_chunk_id_ = 0;

        // Use ExprResCache for the full-segment bitset.
        auto cached = ExprCacheHelper::GetOrCompute(
            segment_,
            this->ToString(),
            active_count_,
            [&]() -> ExprCacheHelper::ComputeResult {
                auto pointer =
                    milvus::Json::pointer(expr_->column_.nested_path_);
                auto* index = pinned_index_[cached_index_chunk_id_].get();
                AssertInfo(index != nullptr,
                           "Cannot find json index with path: " + pointer);
                auto* mutable_index = const_cast<index::IndexBase*>(index);
                TargetBitmap res;
                if (index->GetCastType().data_type() ==
                    JsonCastType::DataType::JSON) {
                    // JsonFlatIndex needs special handling via executor.
                    auto* json_flat_index = const_cast<index::JsonFlatIndex*>(
                        dynamic_cast<const index::JsonFlatIndex*>(index));
                    auto executor =
                        json_flat_index->create_executor<double>(pointer);
                    res = executor->Exists();
                } else {
                    // All other JSON path indexes (Inverted, Sort, Bitmap,
                    // Hybrid) return a fresh clone from Exists().
                    res = mutable_index->Exists();
                }
                // Column-level validity (three-valued logic): a NULL-JSON row
                // is UNKNOWN for exists, so its valid bit must be false — else
                // a wrapping NOT exists() wrongly matches it. The index only
                // reports whether the PATH exists, not whether the COLUMN is
                // null, so apply the field's null bitmap over the whole
                // segment (the raw-data path does the equivalent per row).
                auto valid = GetJsonFieldValidity(segment_,
                                                  mutable_index,
                                                  op_ctx_,
                                                  expr_->column_.field_id_,
                                                  active_count_);
                return {std::move(res), std::move(valid)};
            });
        cached_index_chunk_res_ = cached.result;
        cached_index_chunk_valid_res_ = cached.valid;
    }
    if (input != nullptr) {
        return GatherJsonExistsByOffsets(*cached_index_chunk_res_,
                                         *cached_index_chunk_valid_res_,
                                         *input,
                                         context.get_bitmap_input());
    }
    // These bitmaps may be shared with ExprResCacheManager, so the all-at-once
    // path must clone instead of moving from them (which would corrupt the
    // cached entry for the next query).
    VectorPtr res;
    if (execute_all_at_once_) {
        res = std::make_shared<ColumnVector>(
            cached_index_chunk_res_->clone(),
            cached_index_chunk_valid_res_->clone());
    } else {
        res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                *cached_index_chunk_valid_res_,
                                current_index_chunk_pos_,
                                real_batch_size);
    }
    current_index_chunk_pos_ += real_batch_size;
    return res;
}

VectorPtr
PhyExistsFilterExpr::EvalJsonExistsForDataSegment(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    if (exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer("exists_json_by_stats", [this](double us) {
            json_filter_stats_latency_us_ += us;
        });
        return EvalJsonExistsForDataSegmentByStats(context);
    }

    milvus::ScopedTimer timer("exists_json_bruteforce", [this](double us) {
        json_filter_bruteforce_latency_us_ += us;
    });

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    int processed_cursor = 0;
    auto execute_sub_batch =
        [&bitmap_input,
         &processed_cursor]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer) {
            // If data is nullptr, this chunk was skipped by SkipIndex.
            // We only need to update processed_cursor for bitmap_input indexing.
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
                if (valid_data != nullptr && !valid_data[offset]) {
                    // Column-null row: three-valued logic requires marking it
                    // invalid too, else a wrapping NOT flips res=false to true and
                    // wrongly matches the null row.
                    res[i] = valid_res[i] = false;
                    continue;
                }
                if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                    continue;
                }
                res[i] = data[offset].exist(pointer);
            }
            processed_cursor += size;
        };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<Json>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    pointer);
    } else {
        processed_size = ProcessDataChunks<Json>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, pointer);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

VectorPtr
PhyExistsFilterExpr::EvalJsonExistsForDataSegmentByStats(EvalCtx& context) {
    auto* input = context.get_offset_input();
    auto real_batch_size =
        input != nullptr ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto pointer = milvus::index::JsonPointer(expr_->column_.nested_path_);
    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        cached_index_chunk_id_ = 0;

        auto cached = ExprCacheHelper::GetOrCompute(
            segment_,
            this->ToString(),
            active_count_,
            [&]() -> ExprCacheHelper::ComputeResult {
                auto segment =
                    static_cast<const segcore::SegmentSealed*>(segment_);
                auto field_id = expr_->column_.field_id_;
                auto index = segment->GetJsonStats(op_ctx_, field_id);
                Assert(index.get() != nullptr);

                TargetBitmap res(active_count_);
                TargetBitmapView res_view(res);

                // process shredding data
                {
                    milvus::ScopedTimer timer(
                        "exists_json_stats_shredding_data", [this](double us) {
                            json_stats_shredding_latency_us_ += us;
                        });
                    auto shredding_fields =
                        index->GetShreddingFieldsWithPrefix(pointer);
                    for (const auto& field : shredding_fields) {
                        TargetBitmap temp_valid(active_count_, true);
                        TargetBitmapView temp_valid_view(temp_valid);
                        index->ExecutorForGettingValid(
                            op_ctx_, field, temp_valid_view);
                        res_view |= temp_valid_view;
                    }
                }

                // process shared data
                {
                    milvus::ScopedTimer timer(
                        "exists_json_stats_shared_data", [this](double us) {
                            json_stats_shared_latency_us_ += us;
                        });
                    index->ExecuteForSharedData(
                        op_ctx_,
                        bson_index_,
                        pointer,
                        [&](BsonView bson, uint32_t row_id, uint32_t offset) {
                            res_view[row_id] = !bson.IsBsonValueEmpty(offset);
                        });
                }

                // Column-level validity (three-valued logic): a NULL-JSON row
                // is UNKNOWN for exists, so its valid bit must be false — else
                // a wrapping NOT exists() wrongly matches it. JSON stats report
                // path existence, not column null, so apply the field's null
                // bitmap over the whole segment.
                auto valid = index->FieldIsNotNull(op_ctx_);
                AssertInfo(valid.has_value(),
                           "JSON stats must contain field-level validity");
                AssertInfo(valid->size() == static_cast<size_t>(active_count_),
                           "JSON stats validity size {} not equal to row count "
                           "{}",
                           valid->size(),
                           active_count_);
                return {std::move(res), std::move(*valid)};
            });
        cached_index_chunk_res_ = cached.result;
        cached_index_chunk_valid_res_ = cached.valid;
    }

    if (input != nullptr) {
        return GatherJsonExistsByOffsets(*cached_index_chunk_res_,
                                         *cached_index_chunk_valid_res_,
                                         *input,
                                         context.get_bitmap_input());
    }

    // These bitmaps may be shared with ExprResCacheManager, so the all-at-once
    // path must clone instead of moving from them.
    VectorPtr res;
    if (execute_all_at_once_) {
        res = std::make_shared<ColumnVector>(
            cached_index_chunk_res_->clone(),
            cached_index_chunk_valid_res_->clone());
    } else {
        res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                *cached_index_chunk_valid_res_,
                                current_data_global_pos_,
                                real_batch_size);
    }
    MoveCursor();
    return res;
}

}  //namespace exec
}  // namespace milvus
