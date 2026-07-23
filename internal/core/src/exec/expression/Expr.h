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

#include <algorithm>
#include <bit>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>

#include "common/Array.h"
#include "common/ArrayOffsets.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/ExprCacheHelper.h"
#include "exec/expression/Utils.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "index/Index.h"
#include "index/JsonFlatIndex.h"
#include "log/Log.h"
#include "query/PlanProto.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentGrowingImpl.h"
namespace milvus {
namespace exec {

enum class FilterType { sequential = 0, random = 1 };

// Execution path for expression evaluation.
// Determines how the expression result bitmap is produced.
enum class ExprExecPath {
    RawData,      // brute-force scan raw data
    ScalarIndex,  // pinned_index_ scalar index
    PkIndex,      // segment_->pk_range / search_ids
    TextIndex,    // segment_->GetTextIndex
    JsonStats,    // segment_->GetJsonStats
};

inline std::vector<PinWrapper<const index::IndexBase*>>
PinIndex(milvus::OpContext* op_ctx,
         const segcore::SegmentInternalInterface* segment,
         const FieldMeta& field_meta,
         const std::vector<std::string>& path = {},
         DataType data_type = DataType::NONE,
         bool any_type = false,
         bool is_array = false) {
    if (field_meta.get_data_type() == DataType::JSON) {
        auto pointer = milvus::Json::pointer(path);
        return segment->PinJsonIndex(op_ctx,
                                     field_meta.get_id(),
                                     pointer,
                                     data_type,
                                     any_type,
                                     is_array);
    } else {
        return segment->PinIndex(op_ctx, field_meta.get_id());
    }
}

// Mask null rows out of a filter result: wherever valid_data marks a row null
// (false), clear both the result bit and the validity bit. No-op when
// valid_data is null (a non-nullable column carries no validity array). This
// is the shared validity-masking primitive used by SegmentExpr::ApplyValidData
// and by the per-kernel sequential masking sites.
//
// Packs 64 rows into a word (the fixed-trip inner loop vectorizes) and then
// walks only the null bits via std::countr_zero. An all-valid block has no null
// bits set, so the common no-null case costs nothing. Bit-identical to the
// straightforward `if (!valid_data[i]) res[i] = valid_res[i] = false;` loop.
//
// SEQUENTIAL only: row i maps to position i. The scattered / by-offsets case
// (valid_data[offsets[i]]) is a gather and must keep its own per-row loop.
inline void
ApplyValidMask(const bool* valid_data,
               TargetBitmapView res,
               TargetBitmapView valid_res,
               const int size) {
    if (valid_data == nullptr) {
        return;
    }
    int i = 0;
    for (; i + 64 <= size; i += 64) {
        uint64_t m = 0;
        for (int k = 0; k < 64; ++k) {
            m |= uint64_t(valid_data[i + k] != 0) << k;
        }
        for (uint64_t nulls = ~m; nulls != 0; nulls &= nulls - 1) {
            const int k = std::countr_zero(nulls);
            res[i + k] = false;
            valid_res[i + k] = false;
        }
    }
    for (; i < size; i++) {
        if (!valid_data[i]) {
            res[i] = valid_res[i] = false;
        }
    }
}

class Expr : public std::enable_shared_from_this<Expr> {
 public:
    Expr(DataType type,
         const std::vector<std::shared_ptr<Expr>>&& inputs,
         const std::string& name,
         milvus::OpContext* op_ctx)
        : type_(type),
          inputs_(std::move(inputs)),
          name_(name),
          op_ctx_(op_ctx) {
    }

    virtual ~Expr() = default;

    const DataType&
    type() const {
        return type_;
    }

    std::string
    name() {
        return name_;
    }

    virtual void
    Eval(EvalCtx& context, VectorPtr& result) {
        ThrowInfo(ErrorCode::NotImplemented, "not implemented");
    }

    // Only move cursor to next batch
    // but not do real eval for optimization
    virtual void
    MoveCursor() {
        ThrowInfo(ErrorCode::NotImplemented, "not implemented");
    }

    void
    SetHasOffsetInput(bool has_offset_input) {
        has_offset_input_ = has_offset_input;
    }

    virtual bool
    SupportOffsetInput() {
        return true;
    }

    virtual std::string
    ToString() const {
        ThrowInfo(ErrorCode::NotImplemented, "not implemented");
    }

    // check if this expression can be executed all at once without batch iteration.
    virtual bool
    CanExecuteAllAtOnce() const {
        return false;
    }

    // set batch size to active count to execute all at once.
    // should only be called when CanExecuteAllAtOnce() returns true.
    virtual void
    SetExecuteAllAtOnce() {
        for (auto& input : inputs_) {
            input->SetExecuteAllAtOnce();
        }
    }

    virtual bool
    IsSource() const {
        return false;
    }

    // Called when this expression's output feeds a null-rejecting consumer:
    // one that treats an UNKNOWN (NULL) row exactly like FALSE, such as the
    // top-level filter, which folds UNKNOWN into the excluded set. Nodes
    // whose operands keep that property (AND/OR) override this to accept the
    // mark and propagate it to their inputs; everywhere else (in particular
    // NOT, where FALSE and UNKNOWN produce different results) the default
    // no-op stops the propagation.
    virtual void
    MarkNullRejecting() {
    }

    virtual bool
    CanUseNestedIndex() const {
        return false;
    }

    virtual std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const {
        ThrowInfo(ErrorCode::NotImplemented, "not implemented");
    }

    std::vector<std::shared_ptr<Expr>>&
    GetInputsRef() {
        return inputs_;
    }

    virtual void
    PrefetchAsync(
        const std::shared_ptr<folly::CPUThreadPoolExecutor> prefetch_pool) {
        for (const auto& input : inputs_) {
            input->PrefetchAsync(prefetch_pool);
        }
    }

    virtual void
    WaitPrefetch() {
        for (const auto& input : inputs_) {
            input->WaitPrefetch();
        }
    }

 protected:
    DataType type_;
    std::vector<std::shared_ptr<Expr>> inputs_;
    std::string name_;
    milvus::OpContext* op_ctx_;

    // whether we have offset input and do expr filtering on these data
    // default is false which means we will do expr filtering on the total segment data
    bool has_offset_input_ = false;
};

using ExprPtr = std::shared_ptr<milvus::exec::Expr>;

/*
 * The expr has only one column.
 */
class SegmentExpr : public Expr {
 public:
    SegmentExpr(const std::vector<ExprPtr>&& input,
                const std::string& name,
                milvus::OpContext* op_ctx,
                const segcore::SegmentInternalInterface* segment,
                const FieldId field_id,
                const std::vector<std::string> nested_path,
                const DataType value_type,
                int64_t active_count,
                int64_t batch_size,
                int32_t consistency_level,
                bool allow_any_json_cast_type = false,
                bool is_json_contains = false,
                const query::PlanOptions& plan_options = {})
        : Expr(DataType::BOOL, std::move(input), name, op_ctx),
          segment_(const_cast<segcore::SegmentInternalInterface*>(segment)),
          field_id_(field_id),
          nested_path_(nested_path),
          value_type_(value_type),
          allow_any_json_cast_type_(allow_any_json_cast_type),
          active_count_(active_count),
          batch_size_(batch_size),
          consistency_level_(consistency_level),
          is_json_contains_(is_json_contains),
          plan_options_(plan_options) {
        size_per_chunk_ = segment_->size_per_chunk();
        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
        InitSegmentExpr();
    }

    virtual ~SegmentExpr();

    void
    InitSegmentExpr() {
        auto& schema = segment_->get_schema();
        auto& field_meta = schema[field_id_];
        field_type_ = field_meta.get_data_type();

        if (schema.get_primary_field_id().has_value() &&
            schema.get_primary_field_id().value() == field_id_ &&
            IsPrimaryKeyDataType(field_meta.get_data_type())) {
            is_pk_field_ = true;
            pk_type_ = field_meta.get_data_type();
        }

        // Scalar index is pinned lazily by EnsurePinnedIndex() when (and only
        // when) DetermineExecPath() commits to ExprExecPath::ScalarIndex.
        // num_index_chunk_ stays 0 here and is set to pinned_index_.size()
        // inside EnsurePinnedIndex(), so the invariant
        //   num_index_chunk_ == pinned_index_.size()
        // always holds. Pre-pin existence checks go through
        // HasCompatibleScalarIndex(), which asks the segment directly and
        // does not require a pin -- so short-circuit exec paths
        // (TextIndex/PkIndex/JsonStats) and the RawData path never pay for a
        // PinCells() cold fetch under tiered storage.

        // if index not include raw data, also need load data
        if (segment_->HasFieldData(field_id_)) {
            if (segment_->is_chunked()) {
                num_data_chunk_ = segment_->num_chunk_data(field_id_);
            } else {
                num_data_chunk_ = upper_div(active_count_, size_per_chunk_);
            }
        }
    }

    // Pin the scalar index cell. Called by DetermineExecPath() only after the
    // expression has committed to ExprExecPath::ScalarIndex, so the pin's
    // lifetime matches real usage: short-circuit paths
    // (TextIndex/PkIndex/JsonStats) and the RawData path never call it and
    // the scalar index cell stays cold in tiered storage. Idempotent.
    void
    EnsurePinnedIndex() {
        if (pinned_index_initialized_) {
            return;
        }
        pinned_index_initialized_ = true;
        auto& schema = segment_->get_schema();
        auto& field_meta = schema[field_id_];
        pinned_index_ = PinIndex(op_ctx_,
                                 segment_,
                                 field_meta,
                                 nested_path_,
                                 value_type_,
                                 allow_any_json_cast_type_,
                                 is_json_contains_);
        num_index_chunk_ = pinned_index_.size();
    }

    virtual bool
    IsSource() const override {
        return true;
    }

    void
    MoveCursorForDataMultipleChunk() {
        int64_t processed_size = 0;
        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
            // if segment is chunked, type won't be growing
            int64_t size = segment_->chunk_size(field_id_, i) - data_pos;

            size = std::min(size, batch_size_ - processed_size);

            processed_size += size;
            if (processed_size >= batch_size_) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                current_data_global_pos_ =
                    current_data_global_pos_ + processed_size;
                break;
            }
            // }
        }
    }
    // Non-chunked segments are always Growing (Sealed is always chunked).
    void
    MoveCursorForDataSingleChunk() {
        int64_t processed_size = 0;
        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
            auto size = (i == (num_data_chunk_ - 1) &&
                         active_count_ % size_per_chunk_ != 0)
                            ? active_count_ % size_per_chunk_ - data_pos
                            : size_per_chunk_ - data_pos;

            size = std::min(size, batch_size_ - processed_size);

            processed_size += size;
            if (processed_size >= batch_size_) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                current_data_global_pos_ =
                    current_data_global_pos_ + processed_size;
                break;
            }
        }
    }

    void
    MoveCursorForData() {
        if (segment_->is_chunked()) {
            MoveCursorForDataMultipleChunk();
        } else {
            MoveCursorForDataSingleChunk();
        }
    }

    void
    MoveCursorForIndex() {
        AssertInfo(segment_->type() == SegmentType::Sealed,
                   "index mode only for sealed segment");
        auto size =
            std::min(active_count_ - current_index_chunk_pos_, batch_size_);

        current_index_chunk_pos_ += size;
    }

    void
    MoveCursor() override {
        if (!has_offset_input_) {
            if (execute_all_at_once_) {
                // One-shot execution, no cursor movement needed.
                return;
            }
            if (UseIndexCursor()) {
                MoveCursorForIndex();
                if (segment_->HasFieldData(field_id_)) {
                    MoveCursorForData();
                }
            } else {
                // RawData, PkIndex, TextIndex, JsonStats all use data cursor.
                MoveCursorForData();
            }
        }
    }

    void
    ApplyValidData(const bool* valid_data,
                   TargetBitmapView res,
                   TargetBitmapView valid_res,
                   const int size) {
        ApplyValidMask(valid_data, res, valid_res, size);
    }

    // Try to load the full bitset from ExprResCache.
    // Returns true if cache hit (cached_index_chunk_res_ populated).
    // Call at the top of ByStats / ByIndex methods to skip computation.
    bool
    TryCacheGet() {
        if (!ExprResCacheManager::IsEnabled() || segment_ == nullptr) {
            return false;
        }
        if (ExprResCacheManager::Instance().GetMode() == CacheMode::Disk &&
            segment_->type() != SegmentType::Sealed) {
            return false;
        }
        ExprResCacheManager::Key key{segment_->get_segment_id(),
                                     this->ToString()};
        ExprResCacheManager::Value got;
        got.active_count = active_count_;
        if (ExprResCacheManager::Instance().Get(key, got)) {
            cached_index_chunk_res_ = got.result;
            cached_index_chunk_valid_res_ = got.valid_result;
            cached_index_chunk_id_ = 0;
            return true;
        }
        return false;
    }

    // Put the current cached_index_chunk_res_ into ExprResCache.
    // Call after full bitset computation completes.
    void
    CachePut(int64_t eval_duration_us) {
        if (!ExprResCacheManager::IsEnabled() || segment_ == nullptr) {
            return;
        }
        if (ExprResCacheManager::Instance().GetMode() == CacheMode::Disk &&
            segment_->type() != SegmentType::Sealed) {
            return;
        }
        if (!cached_index_chunk_res_ || !cached_index_chunk_valid_res_) {
            return;
        }
        ExprResCacheManager::Key key{segment_->get_segment_id(),
                                     this->ToString()};
        ExprResCacheManager::Value v;
        v.result = cached_index_chunk_res_;
        v.valid_result = cached_index_chunk_valid_res_;
        v.active_count = active_count_;
        v.eval_duration_us = eval_duration_us;
        ExprResCacheManager::Instance().Put(key, v);
    }

    using CacheClock = std::chrono::steady_clock;

    static int64_t
    CacheElapsedUs(CacheClock::time_point start) {
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              CacheClock::now() - start)
                              .count();
        return std::max<int64_t>(elapsed_us, 1);
    }

    // The IsNotNull() virtual rebuilds a segment-sized bitmap on every
    // call (allocation + fill + AND); per-batch callers must reuse one
    // copy. The all-valid flag short-circuits per-row bitmap reads.
    template <typename Index>
    const TargetBitmap&
    GetCachedIndexValidBitmap(Index* index_ptr) {
        if (!cached_index_valid_res_) {
            cached_index_valid_res_ =
                std::make_shared<TargetBitmap>(index_ptr->IsNotNull());
            cached_index_all_valid_ = cached_index_valid_res_->all();
        }
        return *cached_index_valid_res_;
    }

    int64_t
    GetNextBatchSize() {
        auto current_chunk =
            UseIndexCursor() ? current_index_chunk_ : current_data_chunk_;
        auto current_chunk_pos = UseIndexCursor() ? current_index_chunk_pos_
                                                  : current_data_chunk_pos_;
        auto current_rows = 0;
        if (segment_->is_chunked()) {
            current_rows =
                UseIndexCursor() && segment_->type() == SegmentType::Sealed
                    ? current_chunk_pos
                    : segment_->num_rows_until_chunk(field_id_, current_chunk) +
                          current_chunk_pos;
        } else {
            current_rows = current_chunk * size_per_chunk_ + current_chunk_pos;
        }
        return current_rows + batch_size_ >= active_count_
                   ? active_count_ - current_rows
                   : batch_size_;
    }

    int64_t
    GetNextRealBatchSize(const OffsetVector* input, bool element_level) {
        if (input != nullptr) {
            return input->size();
        } else if (element_level) {
            auto [_, elem_count] = GetNextBatchSizeForElementLevel();
            return elem_count;
        }
        return GetNextBatchSize();
    }

    // Get the next batch size for element-level processing
    // Returns: (batch_rows, elem_count) where batch_rows is number of rows to process
    // and elem_count is the total number of elements in those rows
    std::pair<int64_t, int64_t>
    GetNextBatchSizeForElementLevel() {
        auto array_offsets = segment_->GetArrayOffsets(field_id_);
        AssertInfo(array_offsets != nullptr,
                   "ArrayOffsets not found for field {}",
                   field_id_.get());

        // Use index cursor or data cursor based on execution path
        auto current_chunk =
            UseIndexCursor() ? current_index_chunk_ : current_data_chunk_;
        auto current_chunk_pos = UseIndexCursor() ? current_index_chunk_pos_
                                                  : current_data_chunk_pos_;

        int64_t current_rows = 0;
        if (UseIndexCursor() && segment_->type() == SegmentType::Sealed) {
            // For sealed segment with index, position is already global
            current_rows = current_chunk_pos;
        } else if (segment_->is_chunked()) {
            current_rows =
                segment_->num_rows_until_chunk(field_id_, current_chunk) +
                current_chunk_pos;
        } else {
            current_rows = current_chunk * size_per_chunk_ + current_chunk_pos;
        }

        auto batch_rows = std::min(batch_size_, active_count_ - current_rows);

        if (batch_rows == 0) {
            return {0, 0};
        }

        // Calculate elem_count based on global row positions
        auto [elem_start, _] = array_offsets->ElementIDRangeOfRow(current_rows);
        auto [elem_end, __] =
            array_offsets->ElementIDRangeOfRow(current_rows + batch_rows);
        auto elem_count = elem_end - elem_start;

        return {batch_rows, elem_count};
    }

    // used for processing raw data expr for sealed segments.
    // now only used for std::string_view && json
    // TODO: support more types
    template <typename T,
              bool NeedSegmentOffsets = false,
              typename FUNC,
              typename... ValTypes>
    int64_t
    ProcessChunkForSealedSeg(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        // For sealed segment, only single chunk
        Assert(num_data_chunk_ == 1);
        auto need_size =
            std::min(active_count_ - current_data_chunk_pos_, batch_size_);
        if (need_size == 0)
            return 0;  //do not go empty-loop at the bound of the chunk

        auto skip_index = segment_->GetSkipIndex();
        auto pw = segment_->get_batch_views<T>(
            op_ctx_, field_id_, 0, current_data_chunk_pos_, need_size);
        auto views_info = pw.get();
        if (!skip_func || !skip_func(*skip_index, field_id_, 0)) {
            // first is the raw data, second is valid_data
            // use valid_data to see if raw data is null
            if constexpr (NeedSegmentOffsets) {
                // For GIS functions: construct segment offsets array
                std::vector<int32_t> segment_offsets_array(need_size);
                for (int64_t j = 0; j < need_size; ++j) {
                    segment_offsets_array[j] =
                        static_cast<int32_t>(current_data_chunk_pos_ + j);
                }
                func(views_info.first.data(),
                     views_info.second.data(),
                     nullptr,
                     segment_offsets_array.data(),
                     need_size,
                     res,
                     valid_res,
                     values...);
            } else {
                func(views_info.first.data(),
                     views_info.second.data(),
                     nullptr,
                     need_size,
                     res,
                     valid_res,
                     values...);
            }
        } else {
            ApplyValidData(views_info.second.data(), res, valid_res, need_size);
        }
        current_data_chunk_pos_ += need_size;
        return need_size;
    }

    // accept offsets array and process on the scalar data by offsets
    // stateless! Just check and set bitset as result, does not need to move cursor
    // used for processing raw data expr for sealed segments.
    // now only used for std::string_view && json
    // TODO: support more types
    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessDataByOffsetsForSealedSeg(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        OffsetVector* input,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        // For non_chunked sealed segment, only single chunk
        Assert(num_data_chunk_ == 1);

        auto skip_index = segment_->GetSkipIndex();
        auto pw =
            segment_->get_views_by_offsets<T>(op_ctx_, field_id_, 0, *input);
        auto [data_vec, valid_data] = pw.get();
        if (!skip_func || !skip_func(*skip_index, field_id_, 0)) {
            func(data_vec.data(),
                 valid_data.data(),
                 nullptr,
                 input->size(),
                 res,
                 valid_res,
                 values...);
        } else {
            ApplyValidData(valid_data.data(), res, valid_res, input->size());
        }
        return input->size();
    }

    template <typename T, typename FUNC, typename... ValTypes>
    VectorPtr
    ProcessIndexChunksByOffsets(FUNC func,
                                OffsetVector* input,
                                const ValTypes&... values) {
        AssertInfo(num_index_chunk_ == 1, "scalar index chunk num must be 1");
        using IndexInnerType = std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
        using Index = index::ScalarIndex<IndexInnerType>;
        TargetBitmap valid_res(input->size());

        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        auto* index_ptr = const_cast<Index*>(scalar_index);

        const auto& valid_result = GetCachedIndexValidBitmap(index_ptr);
        if (cached_index_all_valid_) {
            valid_res.set();
        } else {
            for (auto i = 0; i < input->size(); ++i) {
                valid_res[i] = valid_result[(*input)[i]];
            }
        }
        auto result = std::move(func.template operator()<FilterType::random>(
            index_ptr, values..., input->data()));
        return std::make_shared<ColumnVector>(std::move(result),
                                              std::move(valid_res));
    }

    // when we have scalar index and index contains raw data, could go with index chunk by offsets
    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessIndexLookupByOffsets(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        OffsetVector* input,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        AssertInfo(num_index_chunk_ == 1, "scalar index chunk num must be 1");
        auto skip_index = segment_->GetSkipIndex();

        using IndexInnerType = std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
        using Index = index::ScalarIndex<IndexInnerType>;
        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        auto* index_ptr = const_cast<Index*>(scalar_index);
        const auto& valid_result = GetCachedIndexValidBitmap(index_ptr);
        const bool all_valid = cached_index_all_valid_;
        auto batch_size = input->size();

        if (!skip_func || !skip_func(*skip_index, field_id_, 0)) {
            for (auto i = 0; i < batch_size; ++i) {
                auto offset = (*input)[i];
                auto raw = index_ptr->Reverse_Lookup(offset);
                if (!raw.has_value()) {
                    res[i] = false;
                    continue;
                }
                T raw_data = raw.value();
                bool valid_data = all_valid || valid_result[offset];
                func.template operator()<FilterType::random>(&raw_data,
                                                             &valid_data,
                                                             nullptr,
                                                             1,
                                                             res + i,
                                                             valid_res + i,
                                                             values...);
            }
        } else if (all_valid) {
            res.set(0, batch_size);
            valid_res.set(0, batch_size);
        } else {
            for (auto i = 0; i < batch_size; ++i) {
                auto offset = (*input)[i];
                // materialize the bool once: chaining proxies would read
                // back the word just stored into valid_res
                const bool valid = valid_result[offset];
                valid_res[i] = valid;
                res[i] = valid;
            }
        }

        return batch_size;
    }

    // accept offsets array and process on the scalar data by offsets
    // stateless! Just check and set bitset as result, does not need to move cursor
    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessDataByOffsets(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        OffsetVector* input,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        int64_t processed_size = 0;

        // index reverse lookup (only for ScalarIndex path)
        if constexpr (!std::is_same_v<T, VectorArrayView>) {
            if (UseIndexCursor() && num_data_chunk_ == 0) {
                return ProcessIndexLookupByOffsets<T>(
                    func, skip_func, input, res, valid_res, values...);
            }
        }

        auto skip_index = segment_->GetSkipIndex();

        if constexpr (std::is_same_v<T, VectorArrayView>) {
            for (size_t i = 0; i < input->size(); ++i) {
                int64_t offset = (*input)[i];
                auto [chunk_id, chunk_offset] =
                    segment_->get_chunk_by_offset(field_id_, offset);
                // chunk_data<VectorArrayView> would read the wrong layout:
                // storage holds VectorArray, and nullable rows may be compacted.
                // Use chunk_view to build logical VectorArrayView rows.
                auto pw = segment_->chunk_view<VectorArrayView>(
                    op_ctx_,
                    field_id_,
                    chunk_id,
                    std::make_pair(chunk_offset, int64_t{1}));
                const auto& [data_vec, valid_data] = pw.get();
                if (!skip_func ||
                    !skip_func(*skip_index, field_id_, chunk_id)) {
                    func.template operator()<FilterType::random>(
                        data_vec.data(),
                        valid_data.data(),
                        nullptr,
                        1,
                        res + processed_size,
                        valid_res + processed_size,
                        values...);
                } else {
                    ApplyValidData(valid_data.data(),
                                   res + processed_size,
                                   valid_res + processed_size,
                                   1);
                }
                processed_size++;
            }
            return input->size();
        } else if (segment_->type() == SegmentType::Sealed) {
            // raw data scan
            // sealed segment
            if (segment_->is_chunked()) {
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json> ||
                              std::is_same_v<T, ArrayView>) {
                    for (size_t i = 0; i < input->size(); ++i) {
                        int64_t offset = (*input)[i];
                        auto [chunk_id, chunk_offset] =
                            segment_->get_chunk_by_offset(field_id_, offset);
                        auto pw = segment_->get_views_by_offsets<T>(
                            op_ctx_,
                            field_id_,
                            chunk_id,
                            {int32_t(chunk_offset)});
                        auto [data_vec, valid_data] = pw.get();
                        if (!skip_func ||
                            !skip_func(*skip_index, field_id_, chunk_id)) {
                            func.template operator()<FilterType::random>(
                                data_vec.data(),
                                valid_data.data(),
                                nullptr,
                                1,
                                res + processed_size,
                                valid_res + processed_size,
                                values...);
                        } else {
                            if (!valid_data.empty() && !valid_data[0]) {
                                res[processed_size] =
                                    valid_res[processed_size] = false;
                            }
                        }
                        processed_size++;
                    }
                    return input->size();
                }
                for (size_t i = 0; i < input->size(); ++i) {
                    int64_t offset = (*input)[i];
                    auto [chunk_id, chunk_offset] =
                        segment_->get_chunk_by_offset(field_id_, offset);
                    auto pw =
                        segment_->chunk_data<T>(op_ctx_, field_id_, chunk_id);
                    auto chunk = pw.get();
                    const T* data = chunk.data() + chunk_offset;
                    const bool* valid_data = chunk.valid_data();
                    if (valid_data != nullptr) {
                        valid_data += chunk_offset;
                    }
                    if (!skip_func ||
                        !skip_func(*skip_index, field_id_, chunk_id)) {
                        func.template operator()<FilterType::random>(
                            data,
                            valid_data,
                            nullptr,
                            1,
                            res + processed_size,
                            valid_res + processed_size,
                            values...);
                    } else {
                        ApplyValidData(valid_data,
                                       res + processed_size,
                                       valid_res + processed_size,
                                       1);
                    }
                    processed_size++;
                }
                return input->size();
            } else {
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json> ||
                              std::is_same_v<T, ArrayView>) {
                    return ProcessDataByOffsetsForSealedSeg<T>(
                        func, skip_func, input, res, valid_res, values...);
                }
                auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, 0);
                auto chunk = pw.get();
                const T* data = chunk.data();
                const bool* valid_data = chunk.valid_data();
                if (!skip_func || !skip_func(*skip_index, field_id_, 0)) {
                    func.template operator()<FilterType::random>(data,
                                                                 valid_data,
                                                                 input->data(),
                                                                 input->size(),
                                                                 res,
                                                                 valid_res,
                                                                 values...);
                } else {
                    ApplyValidData(valid_data, res, valid_res, input->size());
                }
                return input->size();
            }
        } else {
            // growing segment
            for (size_t i = 0; i < input->size(); ++i) {
                int64_t offset = (*input)[i];
                auto chunk_id = offset / size_per_chunk_;
                auto chunk_offset = offset % size_per_chunk_;
                auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, chunk_id);
                auto chunk = pw.get();
                const T* data = chunk.data() + chunk_offset;
                const bool* valid_data = chunk.valid_data();
                if (valid_data != nullptr) {
                    valid_data += chunk_offset;
                }
                if (!skip_func ||
                    !skip_func(*skip_index, field_id_, chunk_id)) {
                    func.template operator()<FilterType::random>(
                        data,
                        valid_data,
                        nullptr,
                        1,
                        res + processed_size,
                        valid_res + processed_size,
                        values...);
                } else {
                    ApplyValidData(valid_data,
                                   res + processed_size,
                                   valid_res + processed_size,
                                   1);
                }
                processed_size++;
            }
        }
        return input->size();
    }

    // Process element-level data by element IDs
    // Handles the type mismatch between storage (ArrayView) and element type
    // Currently only implemented for sealed chunked segments
    template <typename ElementType, typename FUNC, typename... ValTypes>
    int64_t
    ProcessElementLevelByOffsets(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        OffsetVector* element_ids,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        auto skip_index = segment_->GetSkipIndex();
        if (segment_->type() == SegmentType::Sealed) {
            AssertInfo(
                segment_->is_chunked(),
                "Element-level filtering requires chunked segment for sealed");

            auto array_offsets = segment_->GetArrayOffsets(field_id_);
            if (!array_offsets) {
                ThrowInfo(ErrorCode::UnexpectedError,
                          "IArrayOffsets not found for field {}",
                          field_id_.get());
            }

            // Batch process consecutive elements belonging to the same chunk
            size_t processed_size = 0;
            size_t i = 0;

            // Reuse these vectors to avoid repeated heap allocations
            FixedVector<int32_t> offsets;
            FixedVector<int32_t> elem_indices;

            while (i < element_ids->size()) {
                // Start of a new chunk batch
                int64_t element_id = (*element_ids)[i];
                auto [doc_id, elem_idx] =
                    array_offsets->ElementIDToRowID(element_id);
                auto [chunk_id, chunk_offset] =
                    segment_->get_chunk_by_offset(field_id_, doc_id);

                // Collect consecutive elements belonging to the same chunk
                offsets.clear();
                elem_indices.clear();
                offsets.push_back(chunk_offset);
                elem_indices.push_back(elem_idx);

                size_t batch_start = i;
                i++;

                // Look ahead for more elements in the same chunk
                while (i < element_ids->size()) {
                    int64_t next_element_id = (*element_ids)[i];
                    auto [next_doc_id, next_elem_idx] =
                        array_offsets->ElementIDToRowID(next_element_id);
                    auto [next_chunk_id, next_chunk_offset] =
                        segment_->get_chunk_by_offset(field_id_, next_doc_id);

                    if (next_chunk_id != chunk_id) {
                        break;  // Different chunk, process current batch
                    }

                    offsets.push_back(next_chunk_offset);
                    elem_indices.push_back(next_elem_idx);
                    i++;
                }

                // Batch fetch all ArrayViews for this chunk
                auto pw = segment_->get_views_by_offsets<ArrayView>(
                    op_ctx_, field_id_, chunk_id, offsets);

                auto [array_vec, valid_data] = pw.get();

                // Process each element in this batch
                for (size_t j = 0; j < offsets.size(); j++) {
                    size_t result_idx = batch_start + j;

                    if (!skip_func ||
                        !skip_func(*skip_index, field_id_, chunk_id)) {
                        // Extract element from ArrayView
                        auto value =
                            array_vec[j].template get_data<ElementType>(
                                elem_indices[j]);
                        bool is_valid = !valid_data.data() || valid_data[j];

                        func.template operator()<FilterType::random>(
                            &value,
                            &is_valid,
                            nullptr,
                            1,
                            res + result_idx,
                            valid_res + result_idx,
                            values...);
                    } else {
                        // Chunk is skipped - handle exactly like ProcessDataByOffsets
                        if (valid_data.size() > j && !valid_data[j]) {
                            res[result_idx] = valid_res[result_idx] = false;
                        }
                    }

                    processed_size++;
                }
            }
            return processed_size;
        } else {
            auto array_offsets = segment_->GetArrayOffsets(field_id_);
            if (!array_offsets) {
                ThrowInfo(ErrorCode::UnexpectedError,
                          "IArrayOffsets not found for field {}",
                          field_id_.get());
            }

            auto skip_index = segment_->GetSkipIndex();
            size_t processed_size = 0;

            for (size_t i = 0; i < element_ids->size(); i++) {
                int64_t element_id = (*element_ids)[i];

                auto [doc_id, elem_idx] =
                    array_offsets->ElementIDToRowID(element_id);

                // Calculate chunk_id and chunk_offset for this doc
                auto chunk_id = doc_id / size_per_chunk_;
                auto chunk_offset = doc_id % size_per_chunk_;

                // Get the Array chunk (Growing segment stores Array, not ArrayView)
                auto pw =
                    segment_->chunk_data<Array>(op_ctx_, field_id_, chunk_id);
                auto chunk = pw.get();
                const Array* array_ptr = chunk.data() + chunk_offset;
                const bool* valid_data = chunk.valid_data();
                if (valid_data != nullptr) {
                    valid_data += chunk_offset;
                }

                if (!skip_func ||
                    !skip_func(*skip_index, field_id_, chunk_id)) {
                    // Extract element from Array
                    auto value = array_ptr->get_data<ElementType>(elem_idx);
                    bool is_valid = !valid_data || valid_data[0];

                    func.template operator()<FilterType::random>(
                        &value,
                        &is_valid,
                        nullptr,
                        1,
                        res + processed_size,
                        valid_res + processed_size,
                        values...);
                } else {
                    // Chunk is skipped
                    if (valid_data && !valid_data[0]) {
                        res[processed_size] = valid_res[processed_size] = false;
                    }
                }

                processed_size++;
            }

            return processed_size;
        }
    }

    // Process element-level data without offset input
    // This is the counterpart of ProcessDataChunks for element-level expressions
    // Iterates over rows in batch, but returns element-level results
    // The caller must pre-allocate res/valid_res with elem_count size (from GetNextBatchSizeForElementLevel)
    template <typename ElementType, typename FUNC, typename... ValTypes>
    int64_t
    ProcessDataChunksForElementLevel(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        static_assert(!std::is_same_v<ElementType, Json>,
                      "Json element type is not supported for "
                      "element-level filtering");

        int64_t processed_rows = 0;
        int64_t processed_elems = 0;

        // Prefetch chunks to reduce cache miss latency
        if (!prefetched_) {
            std::vector<int64_t> pf_chunk_ids;
            pf_chunk_ids.reserve(num_data_chunk_ - current_data_chunk_);
            for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
                pf_chunk_ids.push_back(i);
            }
            segment_->prefetch_chunks(op_ctx_, field_id_, pf_chunk_ids);
            prefetched_ = true;
        }

        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                i == current_data_chunk_ ? current_data_chunk_pos_ : 0;
            int64_t size;
            if (segment_->is_chunked()) {
                size = segment_->chunk_size(field_id_, i) - data_pos;
            } else {
                size = (i == num_data_chunk_ - 1)
                           ? (active_count_ % size_per_chunk_ == 0
                                  ? size_per_chunk_ - data_pos
                                  : active_count_ % size_per_chunk_ - data_pos)
                           : size_per_chunk_ - data_pos;
            }
            size = std::min(size, batch_size_ - processed_rows);
            if (size <= 0) {
                continue;
            }

            auto skip_index = segment_->GetSkipIndex();
            if ((!skip_func || !skip_func(*skip_index, field_id_, i))) {
                if (segment_->type() == SegmentType::Sealed) {
                    auto pw = segment_->get_batch_views<ArrayView>(
                        op_ctx_, field_id_, i, data_pos, size);
                    auto [data_vec, valid_data] = pw.get();

                    for (size_t j = 0; j < static_cast<size_t>(size); j++) {
                        auto elem_count = data_vec[j].length();
                        bool is_row_valid = !valid_data.data() || valid_data[j];

                        if (!is_row_valid) {
                            // Row is invalid, mark all elements as false
                            for (size_t k = 0; k < elem_count; k++) {
                                res[processed_elems + k] =
                                    valid_res[processed_elems + k] = false;
                            }
                        } else {
                            // Row is valid, process array elements
                            if constexpr (std::is_same_v<ElementType,
                                                         std::string_view> ||
                                          std::is_same_v<ElementType,
                                                         std::string>) {
                                // String type: extract one by one
                                for (size_t k = 0; k < elem_count; k++) {
                                    auto str_view =
                                        data_vec[j]
                                            .template get_data<
                                                std::string_view>(k);
                                    ElementType str_val(str_view);
                                    func(&str_val,
                                         nullptr,
                                         nullptr,
                                         1,
                                         res + processed_elems + k,
                                         valid_res + processed_elems + k,
                                         values...);
                                }
                            } else {
                                // Fixed-length numeric types
                                // Note: int8_t/int16_t are stored as int32_t in Array
                                using StorageType = std::conditional_t<
                                    std::is_same_v<ElementType, int8_t> ||
                                        std::is_same_v<ElementType, int16_t>,
                                    int32_t,
                                    ElementType>;

                                auto* raw_data =
                                    reinterpret_cast<const StorageType*>(
                                        data_vec[j].data());

                                if constexpr (std::is_same_v<StorageType,
                                                             ElementType>) {
                                    // Types match, batch process
                                    func(raw_data,
                                         nullptr,
                                         nullptr,
                                         elem_count,
                                         res + processed_elems,
                                         valid_res + processed_elems,
                                         values...);
                                } else {
                                    // int8_t/int16_t: need conversion
                                    for (size_t k = 0; k < elem_count; k++) {
                                        ElementType val =
                                            static_cast<ElementType>(
                                                raw_data[k]);
                                        func(&val,
                                             nullptr,
                                             nullptr,
                                             1,
                                             res + processed_elems + k,
                                             valid_res + processed_elems + k,
                                             values...);
                                    }
                                }
                            }
                        }
                        processed_elems += elem_count;
                    }
                } else {
                    // Growing segment: use Array
                    auto pw =
                        segment_->chunk_data<Array>(op_ctx_, field_id_, i);
                    auto chunk = pw.get();
                    const Array* data = chunk.data() + data_pos;
                    const bool* valid_data = chunk.valid_data();
                    if (valid_data != nullptr) {
                        valid_data += data_pos;
                    }

                    for (size_t j = 0; j < static_cast<size_t>(size); j++) {
                        auto elem_count = data[j].length();
                        bool is_row_valid = !valid_data || valid_data[j];

                        if (!is_row_valid) {
                            // Row is invalid, mark all elements as false
                            for (size_t k = 0; k < elem_count; k++) {
                                res[processed_elems + k] =
                                    valid_res[processed_elems + k] = false;
                            }
                        } else {
                            // Row is valid, process array elements
                            if constexpr (std::is_same_v<ElementType,
                                                         std::string_view> ||
                                          std::is_same_v<ElementType,
                                                         std::string>) {
                                // String type: extract one by one
                                for (size_t k = 0; k < elem_count; k++) {
                                    auto str_view =
                                        data[j]
                                            .template get_data<
                                                std::string_view>(k);
                                    ElementType str_val(str_view);
                                    func(&str_val,
                                         nullptr,
                                         nullptr,
                                         1,
                                         res + processed_elems + k,
                                         valid_res + processed_elems + k,
                                         values...);
                                }
                            } else {
                                // Fixed-length numeric types
                                // Note: int8_t/int16_t are stored as int32_t in Array
                                using StorageType = std::conditional_t<
                                    std::is_same_v<ElementType, int8_t> ||
                                        std::is_same_v<ElementType, int16_t>,
                                    int32_t,
                                    ElementType>;

                                auto* raw_data =
                                    reinterpret_cast<const StorageType*>(
                                        data[j].data());

                                if constexpr (std::is_same_v<StorageType,
                                                             ElementType>) {
                                    // Types match, batch process
                                    func(raw_data,
                                         nullptr,
                                         nullptr,
                                         elem_count,
                                         res + processed_elems,
                                         valid_res + processed_elems,
                                         values...);
                                } else {
                                    // int8_t/int16_t: need conversion
                                    for (size_t k = 0; k < elem_count; k++) {
                                        ElementType val =
                                            static_cast<ElementType>(
                                                raw_data[k]);
                                        func(&val,
                                             nullptr,
                                             nullptr,
                                             1,
                                             res + processed_elems + k,
                                             valid_res + processed_elems + k,
                                             values...);
                                    }
                                }
                            }
                        }
                        processed_elems += elem_count;
                    }
                }
            } else {
                // Chunk is skipped, mark all elements as false
                if (segment_->type() == SegmentType::Sealed) {
                    auto pw = segment_->get_batch_views<ArrayView>(
                        op_ctx_, field_id_, i, data_pos, size);
                    auto [data_vec, valid_data] = pw.get();

                    for (size_t j = 0; j < static_cast<size_t>(size); j++) {
                        auto elem_count = data_vec[j].length();
                        for (size_t k = 0; k < elem_count; k++) {
                            res[processed_elems + k] =
                                valid_res[processed_elems + k] = false;
                        }
                        processed_elems += elem_count;
                    }
                } else {
                    auto pw =
                        segment_->chunk_data<Array>(op_ctx_, field_id_, i);
                    auto chunk = pw.get();
                    const Array* data = chunk.data() + data_pos;

                    for (size_t j = 0; j < static_cast<size_t>(size); j++) {
                        auto elem_count = data[j].length();
                        for (size_t k = 0; k < elem_count; k++) {
                            res[processed_elems + k] =
                                valid_res[processed_elems + k] = false;
                        }
                        processed_elems += elem_count;
                    }
                }
            }

            processed_rows += size;
            if (processed_rows >= batch_size_) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                break;
            }
        }

        return processed_elems;
    }

    // Template parameter to control whether segment offsets are needed (for GIS functions)
    template <typename T,
              bool NeedSegmentOffsets = false,
              typename FUNC,
              typename... ValTypes>
    int64_t
    ProcessDataChunksForSingleChunk(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        int64_t processed_size = 0;
        if constexpr (std::is_same_v<T, std::string_view> ||
                      std::is_same_v<T, Json>) {
            if (segment_->type() == SegmentType::Sealed) {
                return ProcessChunkForSealedSeg<T, NeedSegmentOffsets>(
                    func, skip_func, res, valid_res, values...);
            }
        }

        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
            auto size =
                (i == (num_data_chunk_ - 1))
                    ? (segment_->type() == SegmentType::Growing
                           ? (active_count_ % size_per_chunk_ == 0
                                  ? size_per_chunk_ - data_pos
                                  : active_count_ % size_per_chunk_ - data_pos)
                           : active_count_ - data_pos)
                    : size_per_chunk_ - data_pos;

            size = std::min(size, batch_size_ - processed_size);
            if (size == 0)
                continue;  //do not go empty-loop at the bound of the chunk

            auto skip_index = segment_->GetSkipIndex();
            auto process_chunk = [&](const T* data, const bool* valid_data) {
                auto skipped =
                    skip_func && skip_func(*skip_index, field_id_, i);
                if (!skipped) {
                    if constexpr (NeedSegmentOffsets) {
                        // For GIS functions: construct segment offsets array
                        std::vector<int32_t> segment_offsets_array(size);
                        for (int64_t j = 0; j < size; ++j) {
                            segment_offsets_array[j] = static_cast<int32_t>(
                                size_per_chunk_ * i + data_pos + j);
                        }
                        func(data,
                             valid_data,
                             nullptr,
                             segment_offsets_array.data(),
                             size,
                             res + processed_size,
                             valid_res + processed_size,
                             values...);
                    } else {
                        func(data,
                             valid_data,
                             nullptr,
                             size,
                             res + processed_size,
                             valid_res + processed_size,
                             values...);
                    }
                    return;
                }

                // Chunk is skipped by SkipIndex.
                // We still need to:
                // 1. Apply valid_data to handle nullable fields
                // 2. Call func with nullptr to update internal cursors
                //    (e.g., processed_cursor for bitmap_input indexing)
                ApplyValidData(valid_data,
                               res + processed_size,
                               valid_res + processed_size,
                               size);
                if constexpr (NeedSegmentOffsets) {
                    std::vector<int32_t> segment_offsets_array(size);
                    for (int64_t j = 0; j < size; ++j) {
                        segment_offsets_array[j] = static_cast<int32_t>(
                            size_per_chunk_ * i + data_pos + j);
                    }
                    func(nullptr,
                         nullptr,
                         nullptr,
                         segment_offsets_array.data(),
                         size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                } else {
                    func(nullptr,
                         nullptr,
                         nullptr,
                         size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                }
            };

            if constexpr (std::is_same_v<T, VectorArrayView>) {
                // chunk_data<VectorArrayView> would read the wrong layout:
                // storage holds VectorArray, and nullable rows may be compacted.
                // Use chunk_view to build logical VectorArrayView rows.
                auto pw = segment_->chunk_view<VectorArrayView>(
                    op_ctx_, field_id_, i, std::make_pair(data_pos, size));
                const auto& [data_vec, valid_data] = pw.get();
                process_chunk(data_vec.data(), valid_data.data());
            } else {
                auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, i);
                auto chunk = pw.get();
                const bool* valid_data = chunk.valid_data();
                if (valid_data != nullptr) {
                    valid_data += data_pos;
                }
                process_chunk(chunk.data() + data_pos, valid_data);
            }

            processed_size += size;
            if (processed_size >= batch_size_) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                break;
            }
        }

        return processed_size;
    }

    template <typename T,
              bool NeedSegmentOffsets = false,
              typename FUNC,
              typename... ValTypes>
    int64_t
    ProcessDataChunksForMultipleChunk(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        int64_t processed_size = 0;

        // prefetch chunks to reduce cache miss latency
        if (!prefetched_) {
            std::vector<int64_t> pf_chunk_ids;
            pf_chunk_ids.reserve(num_data_chunk_ - current_data_chunk_);
            for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
                pf_chunk_ids.push_back(i);
            }
            segment_->prefetch_chunks(op_ctx_, field_id_, pf_chunk_ids);
            prefetched_ = true;
        }

        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                i == current_data_chunk_ ? current_data_chunk_pos_ : 0;

            // if segment is chunked, type won't be growing
            int64_t size = segment_->chunk_size(field_id_, i) - data_pos;
            size = std::min(size, batch_size_ - processed_size);

            if (size == 0)
                continue;  //do not go empty-loop at the bound of the chunk
            std::vector<int32_t> segment_offsets_array(size);
            auto start_offset =
                segment_->num_rows_until_chunk(field_id_, i) + data_pos;
            for (int64_t j = 0; j < size; ++j) {
                int64_t offset = start_offset + j;
                segment_offsets_array[j] = static_cast<int32_t>(offset);
            }
            auto skip_index = segment_->GetSkipIndex();
            if (!skip_func || !skip_func(*skip_index, field_id_, i)) {
                bool is_seal = false;
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json> ||
                              std::is_same_v<T, ArrayView> ||
                              std::is_same_v<T, VectorArrayView>) {
                    if (segment_->type() == SegmentType::Sealed) {
                        // first is the raw data, second is valid_data
                        // use valid_data to see if raw data is null
                        auto pw = segment_->get_batch_views<T>(
                            op_ctx_, field_id_, i, data_pos, size);
                        const auto& [data_vec, valid_data] = pw.get();

                        if constexpr (NeedSegmentOffsets) {
                            func(data_vec.data(),
                                 valid_data.data(),
                                 nullptr,
                                 segment_offsets_array.data(),
                                 size,
                                 res + processed_size,
                                 valid_res + processed_size,
                                 values...);
                        } else {
                            func(data_vec.data(),
                                 valid_data.data(),
                                 nullptr,
                                 size,
                                 res + processed_size,
                                 valid_res + processed_size,
                                 values...);
                        }

                        is_seal = true;
                    }
                }
                if constexpr (std::is_same_v<T, VectorArrayView>) {
                    AssertInfo(is_seal,
                               "VectorArrayView must be read through chunk "
                               "views");
                } else {
                    if (!is_seal) {
                        auto pw =
                            segment_->chunk_data<T>(op_ctx_, field_id_, i);
                        auto chunk = pw.get();
                        const T* data = chunk.data() + data_pos;
                        const bool* valid_data = chunk.valid_data();
                        if (valid_data != nullptr) {
                            valid_data += data_pos;
                        }

                        if constexpr (NeedSegmentOffsets) {
                            // For GIS functions: construct segment offsets array
                            func(data,
                                 valid_data,
                                 nullptr,
                                 segment_offsets_array.data(),
                                 size,
                                 res + processed_size,
                                 valid_res + processed_size,
                                 values...);
                        } else {
                            func(data,
                                 valid_data,
                                 nullptr,
                                 size,
                                 res + processed_size,
                                 valid_res + processed_size,
                                 values...);
                        }
                    }
                }
            } else {
                // Chunk is skipped by SkipIndex.
                // We still need to:
                // 1. Apply valid_data to handle nullable fields
                // 2. Call func with nullptr to update internal cursors
                //    (e.g., processed_cursor for bitmap_input indexing)
                const bool* valid_data;
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json> ||
                              std::is_same_v<T, ArrayView> ||
                              std::is_same_v<T, VectorArrayView>) {
                    auto pw = segment_->get_batch_views<T>(
                        op_ctx_, field_id_, i, data_pos, size);
                    valid_data = pw.get().second.data();
                    ApplyValidData(valid_data,
                                   res + processed_size,
                                   valid_res + processed_size,
                                   size);
                } else {
                    auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, i);
                    auto chunk = pw.get();
                    valid_data = chunk.valid_data();
                    if (valid_data != nullptr) {
                        valid_data += data_pos;
                    }
                    ApplyValidData(valid_data,
                                   res + processed_size,
                                   valid_res + processed_size,
                                   size);
                }
                // Call func with nullptr to update internal cursors
                if constexpr (NeedSegmentOffsets) {
                    func(nullptr,
                         nullptr,
                         nullptr,
                         segment_offsets_array.data(),
                         size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                } else {
                    func(nullptr,
                         nullptr,
                         nullptr,
                         size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                }
            }

            processed_size += size;

            if (processed_size >= batch_size_) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                break;
            }
        }

        return processed_size;
    }

    template <typename T,
              bool NeedSegmentOffsets = false,
              typename FUNC,
              typename... ValTypes>
    int64_t
    ProcessDataChunks(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        const ValTypes&... values) {
        if (segment_->is_chunked()) {
            return ProcessDataChunksForMultipleChunk<T, NeedSegmentOffsets>(
                func, skip_func, res, valid_res, values...);
        } else {
            return ProcessDataChunksForSingleChunk<T, NeedSegmentOffsets>(
                func, skip_func, res, valid_res, values...);
        }
    }

    // Specialized method for ngram post-filter: processes data in a specific range
    // - Starts from segment_offset (global offset across all chunks)
    // - Processes exactly 'size' rows
    // - Does NOT modify segment state variables (current_data_chunk_, etc.)
    template <typename T, typename FUNC>
    int64_t
    ProcessDataChunkForRange(FUNC func,
                             TargetBitmapView res,
                             int64_t segment_offset,
                             int64_t size) {
        static_assert(std::is_same_v<T, std::string_view> ||
                          std::is_same_v<T, Json> ||
                          std::is_same_v<T, ArrayView>,
                      "ProcessDataChunkForRange only supports string_view, "
                      "Json, and ArrayView types");

        AssertInfo(segment_->is_chunked(),
                   "ProcessDataChunkForRange requires chunked segment");
        AssertInfo(segment_->type() == SegmentType::Sealed,
                   "ProcessDataChunkForRange requires sealed segment");

        int64_t processed_size = 0;
        int64_t remaining = size;

        // Find starting chunk and offset
        auto [start_chunk_id, start_chunk_offset] =
            segment_->get_chunk_by_offset(field_id_, segment_offset);

        for (size_t chunk_id = start_chunk_id;
             chunk_id < num_data_chunk_ && remaining > 0;
             chunk_id++) {
            int64_t chunk_size = segment_->chunk_size(field_id_, chunk_id);
            int64_t chunk_offset =
                (chunk_id == start_chunk_id) ? start_chunk_offset : 0;

            while (chunk_offset < chunk_size && remaining > 0) {
                int64_t batch_size = std::min(
                    {batch_size_, chunk_size - chunk_offset, remaining});

                auto pw = segment_->get_batch_views<T>(
                    op_ctx_, field_id_, chunk_id, chunk_offset, batch_size);
                auto data_vec = std::move(pw.get().first);

                func(data_vec.data(), batch_size, res + processed_size);

                chunk_offset += batch_size;
                processed_size += batch_size;
                remaining -= batch_size;
            }
        }

        return processed_size;
    }

    enum class IndexValidityMode {
        Default,
        JsonExactPath,
    };

    // ProcessIndexChunks: execute index query and batch results
    template <typename T, typename FUNC, typename... ValTypes>
    VectorPtr
    ProcessIndexChunks(FUNC func, const ValTypes&... values) {
        return ProcessIndexChunksImpl<T>(
            func, false, IndexValidityMode::Default, values...);
    }

    // ProcessIndexChunks with func_returns_row_level flag
    // func_returns_row_level: if true, func returns row-level bitset even for nested index
    //   (used when func already handles element-to-row conversion internally)
    template <typename T, typename FUNC, typename... ValTypes>
    VectorPtr
    ProcessIndexChunksWithRowLevel(FUNC func,
                                   IndexValidityMode validity_mode,
                                   const ValTypes&... values) {
        return ProcessIndexChunksImpl<T>(func, true, validity_mode, values...);
    }

    TargetBitmap
    GetFieldRowValidity(int64_t row_count) const {
        TargetBitmap valid_result(row_count, true);
        if (row_count == 0) {
            return valid_result;
        }

        int64_t processed_size = 0;
        for (int64_t chunk_id = 0;
             chunk_id < num_data_chunk_ && processed_size < row_count;
             ++chunk_id) {
            auto chunk_size = segment_->is_chunked()
                                  ? segment_->chunk_size(field_id_, chunk_id)
                                  : size_per_chunk_;
            auto size = std::min(chunk_size, row_count - processed_size);
            if (size == 0) {
                continue;
            }
            segment_->ApplyFieldValidData(op_ctx_,
                                          field_id_,
                                          chunk_id,
                                          0,
                                          size,
                                          valid_result.view() + processed_size);
            processed_size += size;
        }
        AssertInfo(processed_size == row_count,
                   "field validity row count mismatch: expected {}, got {}",
                   row_count,
                   processed_size);
        return valid_result;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    VectorPtr
    ProcessIndexChunksImpl(FUNC func,
                           bool func_returns_row_level,
                           IndexValidityMode validity_mode,
                           const ValTypes&... values) {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;

        AssertInfo(num_index_chunk_ == 1,
                   "scalar index should have exactly 1 chunk, got {}",
                   num_index_chunk_);

        // Cache index result (execute only once)
        if (cached_index_chunk_id_ != 0) {
            Index* index_ptr = nullptr;
            PinWrapper<const index::IndexBase*> json_pw;
            std::shared_ptr<index::JsonFlatIndexQueryExecutor<IndexInnerType>>
                executor;
            const auto json_pointer = field_type_ == DataType::JSON
                                          ? milvus::Json::pointer(nested_path_)
                                          : std::string();
            auto prepare_index = [&]() {
                if (index_ptr != nullptr) {
                    return;
                }
                if (field_type_ == DataType::JSON) {
                    json_pw = pinned_index_[0];
                    auto json_flat_index =
                        dynamic_cast<const index::JsonFlatIndex*>(
                            json_pw.get());

                    if (json_flat_index) {
                        auto index_path = json_flat_index->GetNestedPath();
                        executor =
                            json_flat_index
                                ->template create_executor<IndexInnerType>(
                                    json_pointer.substr(index_path.size()));
                        index_ptr = executor.get();
                    } else {
                        auto json_index =
                            const_cast<index::IndexBase*>(json_pw.get());
                        index_ptr = dynamic_cast<Index*>(json_index);
                    }
                } else {
                    auto scalar_index =
                        dynamic_cast<const Index*>(pinned_index_[0].get());
                    index_ptr = const_cast<Index*>(scalar_index);
                }
            };
            prepare_index();
            cached_is_nested_index_ = index_ptr->IsNestedIndex();

            auto cached = ExprCacheHelper::GetOrCompute(
                segment_,
                this->ToString(),
                active_count_,
                [&]() -> ExprCacheHelper::ComputeResult {
                    prepare_index();
                    TargetBitmap res = func(index_ptr, values...);

                    TargetBitmap valid_res;
                    std::optional<index::JsonValueType> json_value_type;
                    if (executor != nullptr) {
                        if (validity_mode == IndexValidityMode::JsonExactPath) {
                            json_value_type = index::JsonValueType::Any;
                        } else if constexpr (std::is_same_v<IndexInnerType,
                                                            bool>) {
                            json_value_type = index::JsonValueType::Bool;
                        } else if constexpr (std::is_integral_v<
                                                 IndexInnerType> ||
                                             std::is_floating_point_v<
                                                 IndexInnerType>) {
                            json_value_type = index::JsonValueType::Numeric;
                        } else if constexpr (std::is_same_v<IndexInnerType,
                                                            std::string>) {
                            json_value_type = index::JsonValueType::String;
                        }
                    }

                    if (json_value_type.has_value()) {
                        const auto family =
                            static_cast<unsigned int>(json_value_type.value());
                        const auto signature = fmt::format(
                            "json-flat-validity:v1:field={}:path-length={}:"
                            "path={}:family={}",
                            field_id_.get(),
                            json_pointer.size(),
                            json_pointer,
                            family);
                        if (ExprResCacheManager::IsEnabled()) {
                            auto validity = ExprCacheHelper::GetOrComputeBitmap(
                                segment_, signature, active_count_, [&]() {
                                    return executor->ExactPathExists(
                                        json_value_type.value());
                                });
                            valid_res = std::move(*validity);
                        } else {
                            valid_res = executor->ExactPathExists(
                                json_value_type.value());
                        }
                    } else if (cached_is_nested_index_ &&
                               func_returns_row_level) {
                        valid_res = GetFieldRowValidity(active_count_);
                    } else {
                        valid_res = index_ptr->IsNotNull();
                    }
                    return {std::move(res), std::move(valid_res)};
                });
            cached_index_chunk_res_ = cached.result;
            cached_index_chunk_valid_res_ = cached.valid;
            cached_index_chunk_id_ = 0;
        }

        TargetBitmap result;
        TargetBitmap valid_result;

        // If func already returns row-level bitset, skip element-to-row conversion
        bool need_element_slicing =
            cached_is_nested_index_ && !func_returns_row_level;

        if (need_element_slicing) {
            // Nested index with element-level result: batch by rows, slice elements
            auto array_offsets = segment_->GetArrayOffsets(field_id_);

            auto data_pos = current_index_chunk_pos_;
            auto batch_rows = std::min(batch_size_, active_count_ - data_pos);

            // Calculate corresponding element range
            auto [elem_start, _] = array_offsets->ElementIDRangeOfRow(data_pos);
            auto [elem_end, __] =
                array_offsets->ElementIDRangeOfRow(data_pos + batch_rows);
            auto elem_count = elem_end - elem_start;

            result.append(*cached_index_chunk_res_, elem_start, elem_count);
            valid_result.append(
                *cached_index_chunk_valid_res_, elem_start, elem_count);

            current_index_chunk_pos_ = data_pos + batch_rows;
        } else if (execute_all_at_once_) {
            // Fast path: move cached bitmap directly, no copy
            current_index_chunk_pos_ += cached_index_chunk_res_->size();
            return std::make_shared<ColumnVector>(
                std::move(*cached_index_chunk_res_),
                std::move(*cached_index_chunk_valid_res_));
        } else {
            // Normal index or row-level result: batch by rows directly
            auto data_pos = current_index_chunk_pos_;
            auto size =
                std::min(std::min(size_per_chunk_ - data_pos, batch_size_),
                         int64_t(cached_index_chunk_res_->size()));

            result.append(*cached_index_chunk_res_, data_pos, size);
            valid_result.append(*cached_index_chunk_valid_res_, data_pos, size);

            current_index_chunk_pos_ = data_pos + size;
        }

        return std::make_shared<ColumnVector>(std::move(result),
                                              std::move(valid_result));
    }

    template <typename T>
    TargetBitmap
    ProcessChunksForValid(bool use_index) {
        if constexpr (std::is_same_v<T, VectorArray>) {
            return ProcessDataChunksForValid<T>();
        } else {
            if (use_index) {
                // when T is ArrayView, the ScalarIndex<T> shall be ScalarIndex<ElementType>
                // NOT ScalarIndex<ArrayView>
                if (std::is_same_v<T, ArrayView>) {
                    auto element_type =
                        segment_->get_schema()[field_id_].get_element_type();
                    switch (element_type) {
                        case DataType::BOOL: {
                            return ProcessIndexChunksForValid<bool>();
                        }
                        case DataType::INT8: {
                            return ProcessIndexChunksForValid<int8_t>();
                        }
                        case DataType::INT16: {
                            return ProcessIndexChunksForValid<int16_t>();
                        }
                        case DataType::INT32: {
                            return ProcessIndexChunksForValid<int32_t>();
                        }
                        case DataType::INT64: {
                            return ProcessIndexChunksForValid<int64_t>();
                        }
                        case DataType::FLOAT: {
                            return ProcessIndexChunksForValid<float>();
                        }
                        case DataType::DOUBLE: {
                            return ProcessIndexChunksForValid<double>();
                        }
                        case DataType::STRING:
                        case DataType::VARCHAR: {
                            return ProcessIndexChunksForValid<std::string>();
                        }
                        case DataType::GEOMETRY: {
                            return ProcessIndexChunksForValid<std::string>();
                        }
                        default:
                            ThrowInfo(UnexpectedError,
                                      "unsupported element type: {}",
                                      element_type);
                    }
                }
                return ProcessIndexChunksForValid<T>();
            } else {
                return ProcessDataChunksForValid<T>();
            }
        }
    }

    template <typename T>
    TargetBitmap
    ProcessChunksForValidByOffsets(bool use_index, const OffsetVector& input) {
        auto batch_size = input.size();
        TargetBitmap valid_result(batch_size);
        valid_result.set();

        auto apply_field_valid_data = [&]() {
            std::vector<int64_t> offsets(input.begin(), input.end());
            segment_->ApplyFieldValidDataByOffsets(
                op_ctx_,
                field_id_,
                offsets.data(),
                batch_size,
                TargetBitmapView(valid_result));
        };

        if constexpr (std::is_same_v<T, VectorArray>) {
            apply_field_valid_data();
        } else {
            typedef std::conditional_t<std::is_same_v<T, std::string_view>,
                                       std::string,
                                       T>
                IndexInnerType;
            using Index = index::ScalarIndex<IndexInnerType>;

            if (use_index) {
                // when T is ArrayView, the ScalarIndex<T> shall be ScalarIndex<ElementType>
                // NOT ScalarIndex<ArrayView>
                if (std::is_same_v<T, ArrayView>) {
                    auto element_type =
                        segment_->get_schema()[field_id_].get_element_type();
                    switch (element_type) {
                        case DataType::BOOL: {
                            return ProcessChunksForValidByOffsets<bool>(
                                use_index, input);
                        }
                        case DataType::INT8: {
                            return ProcessChunksForValidByOffsets<int8_t>(
                                use_index, input);
                        }
                        case DataType::INT16: {
                            return ProcessChunksForValidByOffsets<int16_t>(
                                use_index, input);
                        }
                        case DataType::INT32: {
                            return ProcessChunksForValidByOffsets<int32_t>(
                                use_index, input);
                        }
                        case DataType::INT64: {
                            return ProcessChunksForValidByOffsets<int64_t>(
                                use_index, input);
                        }
                        case DataType::FLOAT: {
                            return ProcessChunksForValidByOffsets<float>(
                                use_index, input);
                        }
                        case DataType::DOUBLE: {
                            return ProcessChunksForValidByOffsets<double>(
                                use_index, input);
                        }
                        case DataType::STRING:
                        case DataType::VARCHAR: {
                            return ProcessChunksForValidByOffsets<std::string>(
                                use_index, input);
                        }
                        default:
                            ThrowInfo(UnexpectedError,
                                      "unsupported element type: {}",
                                      element_type);
                    }
                }
                auto scalar_index =
                    dynamic_cast<const Index*>(pinned_index_[0].get());
                auto* index_ptr = const_cast<Index*>(scalar_index);
                const auto& res = GetCachedIndexValidBitmap(index_ptr);
                if (!cached_index_all_valid_) {
                    for (auto i = 0; i < batch_size; ++i) {
                        valid_result[i] = res[input[i]];
                    }
                }  // else: valid_result is already all-set
            } else {
                apply_field_valid_data();
            }
        }
        return valid_result;
    }

    template <typename T>
    TargetBitmap
    ProcessDataChunksForValid() {
        TargetBitmap valid_result(GetNextBatchSize());
        valid_result.set();
        int64_t processed_size = 0;
        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
            int64_t size = 0;
            if (segment_->is_chunked()) {
                size = segment_->chunk_size(field_id_, i) - data_pos;
            } else {
                size = (i == (num_data_chunk_ - 1))
                           ? (segment_->type() == SegmentType::Growing
                                  ? (active_count_ % size_per_chunk_ == 0
                                         ? size_per_chunk_ - data_pos
                                         : active_count_ % size_per_chunk_ -
                                               data_pos)
                                  : active_count_ - data_pos)
                           : size_per_chunk_ - data_pos;
            }

            size = std::min(size, batch_size_ - processed_size);
            if (size == 0)
                continue;  //do not go empty-loop at the bound of the chunk
            segment_->ApplyFieldValidData(op_ctx_,
                                          field_id_,
                                          i,
                                          data_pos,
                                          size,
                                          valid_result + processed_size);

            processed_size += size;
            if (processed_size >= batch_size_) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                break;
            }
        }
        return valid_result;
    }

    template <typename T>
    TargetBitmap
    ProcessIndexChunksForValid() {
        using IndexInnerType =
            std::conditional_t<std::is_same_v<T, std::string_view> ||
                                   std::is_same_v<T, milvus::Json>,
                               std::string,
                               T>;
        using Index = index::ScalarIndex<IndexInnerType>;

        AssertInfo(num_index_chunk_ == 1,
                   "scalar index should have exactly 1 chunk, got {}",
                   num_index_chunk_);

        // Cache valid result (execute only once)
        if (cached_index_chunk_id_ != 0) {
            Index* index_ptr = nullptr;
            PinWrapper<const index::IndexBase*> json_pw;
            // Executor for JsonFlatIndex. Must outlive index_ptr. Only used for JSON type.
            std::shared_ptr<index::JsonFlatIndexQueryExecutor<IndexInnerType>>
                executor;

            if (field_type_ == DataType::JSON) {
                auto pointer = milvus::Json::pointer(nested_path_);
                json_pw = pinned_index_[0];
                auto json_flat_index =
                    dynamic_cast<const index::JsonFlatIndex*>(json_pw.get());

                if (json_flat_index) {
                    auto index_path = json_flat_index->GetNestedPath();
                    executor =
                        json_flat_index
                            ->template create_executor<IndexInnerType>(
                                pointer.substr(index_path.size()), false);
                    index_ptr = executor.get();
                } else {
                    auto json_index =
                        const_cast<index::IndexBase*>(json_pw.get());
                    index_ptr = dynamic_cast<Index*>(json_index);
                }
            } else {
                auto scalar_index =
                    dynamic_cast<const Index*>(pinned_index_[0].get());
                index_ptr = const_cast<Index*>(scalar_index);
            }

            cached_index_chunk_valid_res_ =
                std::make_shared<TargetBitmap>(index_ptr->IsNotNull());
            cached_index_chunk_id_ = 0;
        }

        // Process current batch
        TargetBitmap valid_result;
        valid_result.set();

        auto data_pos = current_index_chunk_pos_;
        auto size = std::min(std::min(size_per_chunk_ - data_pos, batch_size_),
                             int64_t(cached_index_chunk_valid_res_->size()));

        valid_result.append(*cached_index_chunk_valid_res_, data_pos, size);

        current_index_chunk_pos_ = data_pos + size;

        return valid_result;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    void
    ProcessIndexChunksV2(FUNC func, const ValTypes&... values) {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;

        // For scalar index, num_index_chunk_ can only be 1
        AssertInfo(num_index_chunk_ == 1,
                   "scalar index should have exactly 1 chunk, got {}",
                   num_index_chunk_);

        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        auto* index_ptr = const_cast<Index*>(scalar_index);
        func(index_ptr, values...);
    }

 protected:
    // Check if a compatible scalar index exists for this expression.
    // Only called internally by DetermineExecPath().
    bool
    HasCompatibleScalarIndex() const {
        // Queries segment metadata directly -- no pin required, so short-
        // circuit exec paths and the RawData fallback skip the cold fetch.
        // JSON-specific path compatibility is handled by the separate
        // IsJsonPathCompatible() helper, which also avoids pinning.
        // Ngram index should be used in specific execution path (CanUseNgramIndex -> ExecNgramMatch).
        //
        // JSON indexes are tracked separately from scalar/vector/binlog
        // indexes (see SegmentInterface::HasJsonIndex), so dispatch by
        // field type to avoid widening HasIndex() semantics -- which
        // ReorderConjunctExpr and other callers rely on remaining narrow.
        bool has = (field_type_ == DataType::JSON)
                       ? segment_->HasJsonIndex(field_id_)
                       : segment_->HasIndex(field_id_);
        return has && !CanUseNgramIndex();
    }

    // JSON fields only: verify that a JsonFlatIndex exists for the expr's
    // nested path and that prefix matching is valid. Reads segment-level
    // JSON index metadata via GetJsonFlatIndexNestedPath() -- does NOT pin
    // the index cell, so callers can use this to skip ScalarIndex
    // commitment (and its associated cold fetch) before paying for
    // EnsurePinnedIndex(). Returns true for non-JSON fields and for JSON
    // fields without a JsonFlatIndex covering the query path.
    bool
    IsJsonPathCompatible() const {
        // For JSON fields with JsonFlatIndex, check if prefix matching is valid.
        // Tantivy JSON index can handle nested object paths (e.g., "a.b") but NOT
        // numeric array indices (e.g., "a.0"). Per RFC 6901, JSON Pointer doesn't
        // distinguish between array indices and object keys syntactically. Since
        // Tantivy doesn't store array index information, we must fall back to
        // brute-force search when the relative path contains numeric segments.
        if (field_type_ != DataType::JSON) {
            return true;
        }

        auto query_path = milvus::Json::pointer(nested_path_);
        auto index_path =
            segment_->GetJsonFlatIndexNestedPath(field_id_, query_path);
        if (index_path.empty()) {
            // No JsonFlatIndex covers this path; nothing JSON-specific to
            // reject here. The caller will decide between ScalarIndex and
            // RawData based on HasCompatibleScalarIndex() alone.
            return true;
        }

        // Exact match - safe to use index
        if (index_path == query_path) {
            return true;
        }

        // GetJsonFlatIndexNestedPath guarantees index_path is a prefix of
        // query_path.

        // Get relative path (e.g., if index_path="/a" and query_path="/a/0/b",
        // relative_path="/0/b")
        auto relative_path = query_path.substr(index_path.length());

        // Check if any path segment is numeric (potential array index)
        size_t pos = 0;
        while (pos < relative_path.length()) {
            if (relative_path[pos] == '/') {
                pos++;
                continue;
            }
            size_t end = relative_path.find('/', pos);
            if (end == std::string::npos) {
                end = relative_path.length();
            }
            auto segment = relative_path.substr(pos, end - pos);
            if (!segment.empty() && milvus::IsInteger(segment)) {
                return false;
            }
            pos = end;
        }

        return true;
    }

    bool
    PinnedJsonIndexIsFlat() const {
        return field_type_ == DataType::JSON && !pinned_index_.empty() &&
               dynamic_cast<const index::JsonFlatIndex*>(
                   pinned_index_[0].get()) != nullptr;
    }

    static bool
    IsInt64SafeForJsonDoubleIndex(int64_t value) {
        constexpr int64_t kFirstNonInjectiveInteger = int64_t{1} << 53;
        return value > -kFirstNonInjectiveInteger &&
               value < kFirstNonInjectiveInteger;
    }

 public:
    bool
    CanUseNestedIndex() const override {
        EnsureExecPathDetermined();
        return PinnedIndexIsNested();
    }

    template <typename T>
    bool
    CanUseIndexForOp(OpType op) const {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        if constexpr (!std::is_same_v<IndexInnerType, std::string>) {
            return true;
        }

        using Index = index::ScalarIndex<IndexInnerType>;
        AssertInfo(num_index_chunk_ == 1,
                   "scalar index should have exactly 1 chunk, got {}",
                   num_index_chunk_);
        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        AssertInfo(scalar_index != nullptr, "invalid scalar index type");
        return scalar_index->ShouldUseOp(op);
    }

    template <typename T>
    bool
    IndexHasRawData() const {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;

        using Index = index::ScalarIndex<IndexInnerType>;

        AssertInfo(num_index_chunk_ == 1,
                   "scalar index should have exactly 1 chunk, got {}",
                   num_index_chunk_);
        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        auto* index_ptr = const_cast<Index*>(scalar_index);
        return index_ptr->HasRawData();
    }

    bool
    HasJsonStats(FieldId field_id) const {
        return segment_->type() == SegmentType::Sealed &&
               static_cast<const segcore::SegmentSealed*>(segment_)
                       ->GetJsonStats(op_ctx_, field_id)
                       .get() != nullptr;
    }

    static bool
    PathContainsInteger(const std::vector<std::string>& path) {
        for (const auto& p : path) {
            if (milvus::IsInteger(p)) {
                return true;
            }
        }
        return false;
    }

    // Check whether this expression can use JsonStats without pinning.
    // All conditions are available before execution path determination.
    bool
    CanUseJsonStatsAtInit() const {
        return plan_options_.expr_use_json_stats && HasJsonStats(field_id_) &&
               !nested_path_.empty() && !PathContainsInteger(nested_path_);
    }

    virtual bool
    CanUseNgramIndex() const {
        return false;
    };

    // check if this expression can be executed all at once without batch iteration.
    bool
    CanExecuteAllAtOnce() const override {
        EnsureExecPathDetermined();
        return exec_path_ != ExprExecPath::RawData;
    }

    void
    SetExecuteAllAtOnce() override {
        batch_size_ = active_count_;
        execute_all_at_once_ = true;
    }

    // Returns true if the expression uses the ScalarIndex cursor.
    // Only ScalarIndex maintains a separate index cursor; PkIndex, TextIndex,
    // and JsonStats cache full results and slice via data cursor.
    bool
    UseIndexCursor() const {
        EnsureExecPathDetermined();
        return exec_path_ == ExprExecPath::ScalarIndex;
    }

    void
    EnsureExecPathDetermined() const {
        std::call_once(determine_exec_path_once_, [this]() {
            const_cast<SegmentExpr*>(this)->DetermineExecPath();
        });
    }

    // Determine the execution path for this expression.
    // Called from PrefetchAsync() on the prefetch pool, or lazily by direct
    // call paths that do not prefetch before evaluating expressions.
    // Subclasses should override to implement operator-specific logic.
    // The scalar index is pinned only when we commit to the ScalarIndex
    // path. JSON path compatibility is checked via segment-level metadata
    // (GetJsonFlatIndexNestedPath) before any pin, so an incompatible
    // nested path falls back to RawData without ever touching the cache
    // slot. Short-circuit subclass paths (TextIndex/PkIndex/JsonStats)
    // bypass this method entirely and never pin either.
    virtual void
    DetermineExecPath() {
        if (!HasCompatibleScalarIndex() || !IsJsonPathCompatible()) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
        EnsurePinnedIndex();
        // HasCompatibleScalarIndex() queries HasIndex(), which can return
        // true for a vector/binlog-index-only field or a mid-load state
        // where PinIndex() still yields nothing. Fall back to RawData when
        // the pin actually came up empty so pinned_index_ is non-empty iff
        // exec_path_ == ScalarIndex holds.
        if (pinned_index_.empty()) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
        exec_path_ = ExprExecPath::ScalarIndex;
    }

    // Slice cached result bitmap for the current batch.
    // Used by all index paths (ScalarIndex, PkIndex, TextIndex, JsonStats).
    // Prerequisites: cached_result_ and cached_valid_result_ must be populated.
    VectorPtr
    SliceCachedResult() {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }
        if (execute_all_at_once_) {
            MoveCursor();
            return std::make_shared<ColumnVector>(
                std::move(*cached_result_), std::move(*cached_valid_result_));
        }
        TargetBitmap result;
        TargetBitmap valid_result;
        result.append(
            *cached_result_, current_data_global_pos_, real_batch_size);
        valid_result.append(
            *cached_valid_result_, current_data_global_pos_, real_batch_size);
        MoveCursor();
        return std::make_shared<ColumnVector>(std::move(result),
                                              std::move(valid_result));
    }

    // Move or slice a locally-owned cached bitmap.
    // When execute_all_at_once_, moves the bitmap content to avoid a copy.
    // WARNING: Only use on non-shared bitmaps (not from ExprResCacheManager).
    VectorPtr
    MoveOrSliceBitmap(TargetBitmap& cached_res,
                      TargetBitmap& cached_valid_res,
                      int64_t pos,
                      int64_t size) {
        if (execute_all_at_once_) {
            return std::make_shared<ColumnVector>(std::move(cached_res),
                                                  std::move(cached_valid_res));
        }
        TargetBitmap result;
        TargetBitmap valid_result;
        result.append(cached_res, pos, size);
        valid_result.append(cached_valid_res, pos, size);
        return std::make_shared<ColumnVector>(std::move(result),
                                              std::move(valid_result));
    }

    // Overload for paths where valid bitmap is always all-true.
    VectorPtr
    MoveOrSliceBitmap(TargetBitmap& cached_res, int64_t pos, int64_t size) {
        if (execute_all_at_once_) {
            auto valid = TargetBitmap(cached_res.size(), true);
            return std::make_shared<ColumnVector>(std::move(cached_res),
                                                  std::move(valid));
        }
        TargetBitmap result;
        result.append(cached_res, pos, size);
        return std::make_shared<ColumnVector>(std::move(result),
                                              TargetBitmap(size, true));
    }

    void
    PrefetchAsync(const std::shared_ptr<folly::CPUThreadPoolExecutor>
                      prefetch_pool) override {
        auto self = std::static_pointer_cast<SegmentExpr>(shared_from_this());
        prefetch_future_.emplace(folly::via(prefetch_pool.get(), [self]() {
            if (self->op_ctx_ != nullptr &&
                self->op_ctx_->cancellation_token.isCancellationRequested()) {
                return;
            }
            self->EnsureExecPathDetermined();
            if (self->exec_path_ == ExprExecPath::RawData) {
                self->PrefetchRawData();
                self->prefetched_ = true;
            }
        }));
    }

    virtual void
    PrefetchRawData() {
        PrefetchRawData(field_id_);
    }

    void
    PrefetchRawData(FieldId field_id) {
        segment_->prefetch_chunks(op_ctx_, field_id);
    }

    void
    WaitPrefetch() override {
        if (prefetch_future_.has_value()) {
            auto future = std::move(*prefetch_future_);
            prefetch_future_.reset();
            std::move(future).get();
            return;
        }
        EnsureExecPathDetermined();
    }

 protected:
    // Non-reentrant nested-index check for callers already inside
    // DetermineExecPath(). Do not call CanUseNestedIndex() from there because
    // it re-enters EnsureExecPathDetermined()'s std::call_once.
    bool
    PinnedIndexIsNested() const {
        if (exec_path_ != ExprExecPath::ScalarIndex || pinned_index_.empty()) {
            return false;
        }
        auto* index_ptr = pinned_index_[0].get();
        return index_ptr != nullptr && index_ptr->IsNestedIndex();
    }

    const segcore::SegmentInternalInterface* segment_;
    const FieldId field_id_;
    bool is_pk_field_{false};
    DataType pk_type_;
    int64_t batch_size_;

    std::vector<std::string> nested_path_;
    DataType field_type_;
    DataType value_type_;
    bool allow_any_json_cast_type_{false};
    bool is_json_contains_{false};
    bool is_data_mode_{false};
    query::PlanOptions plan_options_;
    // Execution path determined once, preferably by PrefetchAsync() on the
    // prefetch pool. Direct callers that do not prefetch determine it lazily.
    ExprExecPath exec_path_{ExprExecPath::RawData};
    mutable std::once_flag determine_exec_path_once_;
    // Flag set by SetExecuteAllAtOnce() to enable move-based fast paths,
    // avoiding bitmap copies in ProcessIndexChunks/SliceCachedResult.
    bool execute_all_at_once_{false};
    // used for reducing cache miss latency in tiered storage
    bool prefetched_{false};
    // Scalar index is pinned lazily by EnsurePinnedIndex(). Pre-pin
    // existence checks (HasCompatibleScalarIndex) query segment metadata
    // directly, so expressions on short-circuit paths (TextIndex, PkIndex,
    // JsonStats) and RawData never force a PinCells() cold fetch. After
    // the pin, num_index_chunk_ == pinned_index_.size() always.
    std::vector<PinWrapper<const index::IndexBase*>> pinned_index_{};
    bool pinned_index_initialized_{false};

    int64_t active_count_{0};
    int64_t num_data_chunk_{0};
    int64_t num_index_chunk_{0};
    // State indicate position that expr computing at
    // because expr maybe called for every batch.
    int64_t current_data_chunk_{0};
    int64_t current_data_chunk_pos_{0};
    int64_t current_data_global_pos_{0};
    int64_t current_index_chunk_{0};
    int64_t current_index_chunk_pos_{0};
    int64_t size_per_chunk_{0};

    // Unified cache for all index paths (ScalarIndex, PkIndex, TextIndex, JsonStats).
    // Populated once per segment, then sliced per batch via SliceCachedResult().
    std::shared_ptr<TargetBitmap> cached_result_{nullptr};
    std::shared_ptr<TargetBitmap> cached_valid_result_{nullptr};

    // Cached scalar-index IsNotNull() bitmap for the ByOffsets paths
    // (single-index-chunk only); see GetCachedIndexValidBitmap().
    std::shared_ptr<TargetBitmap> cached_index_valid_res_{nullptr};
    bool cached_index_all_valid_{false};

    // Legacy cache fields — TODO: remove after all subclasses migrated to cached_result_.
    int64_t cached_index_chunk_id_{-1};
    std::shared_ptr<TargetBitmap> cached_index_chunk_res_{nullptr};
    std::shared_ptr<TargetBitmap> cached_index_chunk_valid_res_{nullptr};
    bool cached_is_nested_index_{false};
    std::shared_ptr<TargetBitmap> cached_match_res_{nullptr};

    int32_t consistency_level_{0};

    // Cache for ngram Phase1 result (stays independent, not part of unified path).
    std::shared_ptr<TargetBitmap> cached_phase1_res_{nullptr};

    // Accumulated latency for JSON filter metrics (in microseconds).
    // These are recorded in destructor (converted to ms) to avoid per-batch metric overhead.
    double json_filter_bruteforce_latency_us_{0.0};
    double json_filter_stats_latency_us_{0.0};
    double json_stats_shredding_latency_us_{0.0};
    double json_stats_shared_latency_us_{0.0};
    std::optional<folly::Future<folly::Unit>> prefetch_future_;
};

bool
IsLikeExpr(std::shared_ptr<Expr> expr);

void
OptimizeCompiledExprs(ExecContext* context, const std::vector<ExprPtr>& exprs);

std::vector<ExprPtr>
CompileExpressions(const std::vector<expr::TypedExprPtr>& logical_exprs,
                   ExecContext* context,
                   const std::unordered_set<std::string>& flatten_cadidates =
                       std::unordered_set<std::string>(),
                   bool enable_constant_folding = false);

std::vector<ExprPtr>
CompileInputs(const expr::TypedExprPtr& expr,
              QueryContext* config,
              const std::unordered_set<std::string>& flatten_cadidates);

ExprPtr
CompileExpression(const expr::TypedExprPtr& expr,
                  QueryContext* context,
                  const std::unordered_set<std::string>& flatten_cadidates,
                  bool enable_constant_folding);

class ExprSet {
 public:
    // null_rejecting: the consumer of these expressions' output treats
    // UNKNOWN rows exactly like FALSE (e.g. a filter that folds UNKNOWN into
    // the excluded set); lets conjunctions drop UNKNOWN rows from their
    // active sets early. See Expr::MarkNullRejecting.
    explicit ExprSet(const std::vector<expr::TypedExprPtr>& logical_exprs,
                     ExecContext* exec_ctx,
                     bool null_rejecting = false)
        : exec_ctx_(exec_ctx) {
        exprs_ = CompileExpressions(logical_exprs, exec_ctx);
        if (null_rejecting) {
            for (auto& compiled_expr : exprs_) {
                compiled_expr->MarkNullRejecting();
            }
        }
    }

    virtual ~ExprSet() = default;

    void
    Eval(EvalCtx& ctx, std::vector<VectorPtr>& results) {
        Eval(0, exprs_.size(), true, ctx, results);
    }

    virtual void
    Eval(int32_t begin,
         int32_t end,
         bool initialize,
         EvalCtx& ctx,
         std::vector<VectorPtr>& result);

    void
    Clear() {
        exprs_.clear();
    }

    ExecContext*
    get_exec_context() const {
        return exec_ctx_;
    }

    size_t
    size() const {
        return exprs_.size();
    }

    const std::vector<std::shared_ptr<Expr>>&
    exprs() const {
        return exprs_;
    }

    const std::shared_ptr<Expr>&
    expr(int32_t index) const {
        return exprs_[index];
    }

    // check if all expressions can be executed all at once without batch iteration.
    // only if all expressions support this optimization.
    bool
    CanExecuteAllAtOnce() const {
        for (const auto& expr : exprs_) {
            if (!expr->CanExecuteAllAtOnce()) {
                return false;
            }
        }
        return !exprs_.empty();
    }

    // set batch size to active count to execute all at once.
    // propagates to all expressions in the set.
    void
    SetExecuteAllAtOnce() {
        for (auto& expr : exprs_) {
            expr->SetExecuteAllAtOnce();
        }
    }

    void
    PrefetchAsync(
        const std::shared_ptr<folly::CPUThreadPoolExecutor> prefetch_pool) {
        for (const auto& expr : exprs_) {
            expr->PrefetchAsync(prefetch_pool);
        }
    }

    void
    WaitPrefetch() {
        for (const auto& expr : exprs_) {
            expr->WaitPrefetch();
        }
    }

 private:
    std::vector<std::shared_ptr<Expr>> exprs_;
    ExecContext* exec_ctx_;
};

// Forward declaration for CreateTTLFieldFilterExpression
expr::TypedExprPtr
CreateTTLFieldFilterExpression(QueryContext* query_context);

}  //namespace exec
}  // namespace milvus
