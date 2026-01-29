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
#include <memory>
#include <string>
#include <type_traits>

#include "common/Array.h"
#include "common/ArrayOffsets.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
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

class Expr {
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
    }

    // Only move cursor to next batch
    // but not do real eval for optimization
    virtual void
    MoveCursor() {
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

    virtual bool
    IsSource() const {
        return false;
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

    using SkipNamespaceFunc = std::function<bool(int64_t chunk_id)>;
    virtual void
    SetNamespaceSkipFunc(SkipNamespaceFunc skip_namespace_func) {
        namespace_skip_func_ = std::move(skip_namespace_func);
    }

 protected:
    DataType type_;
    std::vector<std::shared_ptr<Expr>> inputs_;
    std::string name_;
    milvus::OpContext* op_ctx_;

    // whether we have offset input and do expr filtering on these data
    // default is false which means we will do expr filtering on the total segment data
    bool has_offset_input_ = false;
    // check if we can skip a chunk for namespace field.
    // if there's no namespace field, this is std::nullopt.
    // TODO: for expression like f1 > 1 and f2 > 2, we can use skip function of f1 when evaluating f2.
    std::optional<SkipNamespaceFunc> namespace_skip_func_;
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
                bool is_json_contains = false)
        : Expr(DataType::BOOL, std::move(input), name, op_ctx),
          segment_(const_cast<segcore::SegmentInternalInterface*>(segment)),
          field_id_(field_id),
          nested_path_(nested_path),
          value_type_(value_type),
          allow_any_json_cast_type_(allow_any_json_cast_type),
          active_count_(active_count),
          batch_size_(batch_size),
          consistency_level_(consistency_level),
          is_json_contains_(is_json_contains) {
        size_per_chunk_ = segment_->size_per_chunk();
        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
        InitSegmentExpr();
    }

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

        pinned_index_ = PinIndex(op_ctx_,
                                 segment_,
                                 field_meta,
                                 nested_path_,
                                 value_type_,
                                 allow_any_json_cast_type_,
                                 is_json_contains_);
        if (pinned_index_.size() > 0) {
            num_index_chunk_ = pinned_index_.size();
        }
        // if index not include raw data, also need load data
        if (segment_->HasFieldData(field_id_)) {
            if (segment_->is_chunked()) {
                num_data_chunk_ = segment_->num_chunk_data(field_id_);
            } else {
                num_data_chunk_ = upper_div(active_count_, size_per_chunk_);
            }
        }
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
    void
    MoveCursorForDataSingleChunk() {
        if (segment_->type() == SegmentType::Sealed) {
            auto size =
                std::min(active_count_ - current_data_chunk_pos_, batch_size_);
            current_data_chunk_pos_ += size;
            current_data_global_pos_ += size;
        } else {
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
        // when we specify input, do not maintain states
        if (!has_offset_input_) {
            // CanUseIndex excludes ngram index and this is true even ngram index is used as ExecNgramMatch
            // uses data cursor.
            if (SegmentExpr::CanUseIndex()) {
                MoveCursorForIndex();
                if (segment_->HasFieldData(field_id_)) {
                    MoveCursorForData();
                }
            } else {
                MoveCursorForData();
            }
        }
    }

    void
    ApplyValidData(const bool* valid_data,
                   TargetBitmapView res,
                   TargetBitmapView valid_res,
                   const int size) {
        if (valid_data != nullptr) {
            for (int i = 0; i < size; i++) {
                if (!valid_data[i]) {
                    res[i] = valid_res[i] = false;
                }
            }
        }
    }

    int64_t
    GetNextBatchSize() {
        auto current_chunk = SegmentExpr::CanUseIndex() && use_index_
                                 ? current_index_chunk_
                                 : current_data_chunk_;
        auto current_chunk_pos = SegmentExpr::CanUseIndex() && use_index_
                                     ? current_index_chunk_pos_
                                     : current_data_chunk_pos_;
        auto current_rows = 0;
        if (segment_->is_chunked()) {
            current_rows =
                SegmentExpr::CanUseIndex() && use_index_ &&
                        segment_->type() == SegmentType::Sealed
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

        // Use index path or data path based on whether index is being used
        auto current_chunk = SegmentExpr::CanUseIndex() && use_index_
                                 ? current_index_chunk_
                                 : current_data_chunk_;
        auto current_chunk_pos = SegmentExpr::CanUseIndex() && use_index_
                                     ? current_index_chunk_pos_
                                     : current_data_chunk_pos_;

        int64_t current_rows = 0;
        if (SegmentExpr::CanUseIndex() && use_index_ &&
            segment_->type() == SegmentType::Sealed) {
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

        auto& skip_index = segment_->GetSkipIndex();
        auto pw = segment_->get_batch_views<T>(
            op_ctx_, field_id_, 0, current_data_chunk_pos_, need_size);
        auto views_info = pw.get();
        if ((!skip_func || !skip_func(skip_index, field_id_, 0)) &&
            (!namespace_skip_func_.has_value() ||
             !namespace_skip_func_.value()(0))) {
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

        auto& skip_index = segment_->GetSkipIndex();
        auto pw =
            segment_->get_views_by_offsets<T>(op_ctx_, field_id_, 0, *input);
        auto [data_vec, valid_data] = pw.get();
        if ((!skip_func || !skip_func(skip_index, field_id_, 0)) &&
            (!namespace_skip_func_.has_value() ||
             !namespace_skip_func_.value()(0))) {
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

        auto valid_result = index_ptr->IsNotNull();
        for (auto i = 0; i < input->size(); ++i) {
            valid_res[i] = valid_result[(*input)[i]];
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
        auto& skip_index = segment_->GetSkipIndex();

        using IndexInnerType = std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
        using Index = index::ScalarIndex<IndexInnerType>;
        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        auto* index_ptr = const_cast<Index*>(scalar_index);
        auto valid_result = index_ptr->IsNotNull();
        auto batch_size = input->size();

        if ((!skip_func || !skip_func(skip_index, field_id_, 0)) &&
            (!namespace_skip_func_.has_value() ||
             !namespace_skip_func_.value()(0))) {
            for (auto i = 0; i < batch_size; ++i) {
                auto offset = (*input)[i];
                auto raw = index_ptr->Reverse_Lookup(offset);
                if (!raw.has_value()) {
                    res[i] = false;
                    continue;
                }
                T raw_data = raw.value();
                bool valid_data = valid_result[offset];
                func.template operator()<FilterType::random>(&raw_data,
                                                             &valid_data,
                                                             nullptr,
                                                             1,
                                                             res + i,
                                                             valid_res + i,
                                                             values...);
            }
        } else {
            for (auto i = 0; i < batch_size; ++i) {
                auto offset = (*input)[i];
                res[i] = valid_res[i] = valid_result[offset];
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

        // index reverse lookup
        if (SegmentExpr::CanUseIndex() && num_data_chunk_ == 0) {
            return ProcessIndexLookupByOffsets<T>(
                func, skip_func, input, res, valid_res, values...);
        }

        auto& skip_index = segment_->GetSkipIndex();

        // raw data scan
        // sealed segment
        if (segment_->type() == SegmentType::Sealed) {
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
                        if ((!skip_func ||
                             !skip_func(skip_index, field_id_, chunk_id)) &&
                            (!namespace_skip_func_.has_value() ||
                             !namespace_skip_func_.value()(chunk_id))) {
                            func.template operator()<FilterType::random>(
                                data_vec.data(),
                                valid_data.data(),
                                nullptr,
                                1,
                                res + processed_size,
                                valid_res + processed_size,
                                values...);
                        } else {
                            if (valid_data.size() > processed_size &&
                                !valid_data[processed_size]) {
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
                    if ((!skip_func ||
                         !skip_func(skip_index, field_id_, chunk_id)) &&
                        (!namespace_skip_func_.has_value() ||
                         !namespace_skip_func_.value()(chunk_id))) {
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
                if ((!skip_func || !skip_func(skip_index, field_id_, 0)) &&
                    (!namespace_skip_func_.has_value() ||
                     !namespace_skip_func_.value()(0))) {
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
                if ((!skip_func ||
                     !skip_func(skip_index, field_id_, chunk_id)) &&
                    (!namespace_skip_func_.has_value() ||
                     !namespace_skip_func_.value()(chunk_id))) {
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
        auto& skip_index = segment_->GetSkipIndex();
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

                    if ((!skip_func ||
                         !skip_func(skip_index, field_id_, chunk_id)) &&
                        (!namespace_skip_func_.has_value() ||
                         !namespace_skip_func_.value()(chunk_id))) {
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

            auto& skip_index = segment_->GetSkipIndex();
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

                if ((!skip_func ||
                     !skip_func(skip_index, field_id_, chunk_id)) &&
                    (!namespace_skip_func_.has_value() ||
                     !namespace_skip_func_.value()(chunk_id))) {
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
            int64_t size = segment_->chunk_size(field_id_, i) - data_pos;
            size = std::min(size, batch_size_ - processed_rows);
            if (size == 0)
                continue;

            auto& skip_index = segment_->GetSkipIndex();
            if ((!skip_func || !skip_func(skip_index, field_id_, i)) &&
                (!namespace_skip_func_.has_value() ||
                 !namespace_skip_func_.value()(i))) {
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

            auto& skip_index = segment_->GetSkipIndex();
            auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, i);
            auto chunk = pw.get();
            const bool* valid_data = chunk.valid_data();
            if (valid_data != nullptr) {
                valid_data += data_pos;
            }
            if ((!skip_func || !skip_func(skip_index, field_id_, i)) &&
                (!namespace_skip_func_.has_value() ||
                 !namespace_skip_func_.value()(i))) {
                const T* data = chunk.data() + data_pos;

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
            } else {
                // Chunk is skipped by SkipIndex.
                // We still need to:
                // 1. Apply valid_data to handle nullable fields
                // 2. Call func with nullptr to update internal cursors
                //    (e.g., processed_cursor for bitmap_input indexing)
                ApplyValidData(valid_data,
                               res + processed_size,
                               valid_res + processed_size,
                               size);
                // Call func with nullptr to update internal cursors
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
            auto& skip_index = segment_->GetSkipIndex();
            if ((!skip_func || !skip_func(skip_index, field_id_, i)) &&
                (!namespace_skip_func_.has_value() ||
                 !namespace_skip_func_.value()(i))) {
                bool is_seal = false;
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json> ||
                              std::is_same_v<T, ArrayView>) {
                    if (segment_->type() == SegmentType::Sealed) {
                        // first is the raw data, second is valid_data
                        // use valid_data to see if raw data is null
                        auto pw = segment_->get_batch_views<T>(
                            op_ctx_, field_id_, i, data_pos, size);
                        auto [data_vec, valid_data] = pw.get();

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
                if (!is_seal) {
                    auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, i);
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
            } else {
                // Chunk is skipped by SkipIndex.
                // We still need to:
                // 1. Apply valid_data to handle nullable fields
                // 2. Call func with nullptr to update internal cursors
                //    (e.g., processed_cursor for bitmap_input indexing)
                const bool* valid_data;
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json> ||
                              std::is_same_v<T, ArrayView>) {
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

    template <typename T, typename FUNC, typename... ValTypes>
    VectorPtr
    ProcessIndexChunks(FUNC func, const ValTypes&... values) {
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
                    executor = json_flat_index
                                   ->template create_executor<IndexInnerType>(
                                       pointer.substr(index_path.size()));
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

            cached_index_chunk_res_ = std::make_shared<TargetBitmap>(
                std::move(func(index_ptr, values...)));
            cached_index_chunk_valid_res_ =
                std::make_shared<TargetBitmap>(index_ptr->IsNotNull());
            cached_index_chunk_id_ = 0;
            cached_is_nested_index_ = index_ptr->IsNestedIndex();
        }

        TargetBitmap result;
        TargetBitmap valid_result;

        if (cached_is_nested_index_) {
            // Nested index: batch by rows, return corresponding elements
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
        } else {
            // Normal index: batch by rows
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
                        ThrowInfo(DataTypeInvalid,
                                  "unsupported element type: {}",
                                  element_type);
                }
            }
            return ProcessIndexChunksForValid<T>();
        } else {
            return ProcessDataChunksForValid<T>();
        }
    }

    template <typename T>
    TargetBitmap
    ProcessChunksForValidByOffsets(bool use_index, const OffsetVector& input) {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;
        auto batch_size = input.size();
        TargetBitmap valid_result(batch_size);
        valid_result.set();

        if (use_index) {
            // when T is ArrayView, the ScalarIndex<T> shall be ScalarIndex<ElementType>
            // NOT ScalarIndex<ArrayView>
            if (std::is_same_v<T, ArrayView>) {
                auto element_type =
                    segment_->get_schema()[field_id_].get_element_type();
                switch (element_type) {
                    case DataType::BOOL: {
                        return ProcessChunksForValidByOffsets<bool>(use_index,
                                                                    input);
                    }
                    case DataType::INT8: {
                        return ProcessChunksForValidByOffsets<int8_t>(use_index,
                                                                      input);
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
                        return ProcessChunksForValidByOffsets<float>(use_index,
                                                                     input);
                    }
                    case DataType::DOUBLE: {
                        return ProcessChunksForValidByOffsets<double>(use_index,
                                                                      input);
                    }
                    case DataType::STRING:
                    case DataType::VARCHAR: {
                        return ProcessChunksForValidByOffsets<std::string>(
                            use_index, input);
                    }
                    default:
                        ThrowInfo(DataTypeInvalid,
                                  "unsupported element type: {}",
                                  element_type);
                }
            }
            auto scalar_index =
                dynamic_cast<const Index*>(pinned_index_[0].get());
            auto* index_ptr = const_cast<Index*>(scalar_index);
            const auto& res = index_ptr->IsNotNull();
            for (auto i = 0; i < batch_size; ++i) {
                valid_result[i] = res[input[i]];
            }
        } else {
            for (auto i = 0; i < batch_size; ++i) {
                auto offset = input[i];
                auto [chunk_id,
                      chunk_offset] = [&]() -> std::pair<int64_t, int64_t> {
                    if (segment_->type() == SegmentType::Growing) {
                        return {offset / size_per_chunk_,
                                offset % size_per_chunk_};
                    } else if (segment_->is_chunked()) {
                        return segment_->get_chunk_by_offset(field_id_, offset);
                    } else {
                        return {0, offset};
                    }
                }();
                auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, chunk_id);
                auto chunk = pw.get();
                const bool* valid_data = chunk.valid_data();
                if (valid_data != nullptr) {
                    valid_result[i] = valid_data[chunk_offset];
                } else {
                    break;
                }
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
            bool access_sealed_variable_column = false;
            if constexpr (std::is_same_v<T, std::string_view> ||
                          std::is_same_v<T, Json> ||
                          std::is_same_v<T, ArrayView>) {
                if (segment_->type() == SegmentType::Sealed) {
                    auto pw = segment_->get_batch_views<T>(
                        op_ctx_, field_id_, i, data_pos, size);
                    auto [data_vec, valid_data] = pw.get();
                    ApplyValidData(valid_data.data(),
                                   valid_result + processed_size,
                                   valid_result + processed_size,
                                   size);
                    access_sealed_variable_column = true;
                }
            }

            if (!access_sealed_variable_column) {
                auto pw = segment_->chunk_data<T>(op_ctx_, field_id_, i);
                auto chunk = pw.get();
                const bool* valid_data = chunk.valid_data();
                if (valid_data == nullptr) {
                    return valid_result;
                }
                valid_data += data_pos;
                ApplyValidData(valid_data,
                               valid_result + processed_size,
                               valid_result + processed_size,
                               size);
            }

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
                    executor = json_flat_index
                                   ->template create_executor<IndexInnerType>(
                                       pointer.substr(index_path.size()));
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

    bool
    CanUseIndex() const {
        // Ngram index should be used in specific execution path (CanUseNgramIndex -> ExecNgramMatch).
        // TODO: if multiple indexes are supported, this logic should be changed
        if (num_index_chunk_ == 0 || CanUseNgramIndex()) {
            return false;
        }

        // For JSON fields with JsonFlatIndex, check if prefix matching is valid.
        // Tantivy JSON index can handle nested object paths (e.g., "a.b") but NOT
        // numeric array indices (e.g., "a.0"). Per RFC 6901, JSON Pointer doesn't
        // distinguish between array indices and object keys syntactically. Since
        // Tantivy doesn't store array index information, we must fall back to
        // brute-force search when the relative path contains numeric segments.
        if (field_type_ != DataType::JSON || pinned_index_.empty()) {
            return true;
        }

        auto json_flat_index =
            dynamic_cast<const index::JsonFlatIndex*>(pinned_index_[0].get());
        if (json_flat_index == nullptr) {
            return true;
        }

        auto index_path = json_flat_index->GetNestedPath();
        auto query_path = milvus::Json::pointer(nested_path_);

        // Exact match - safe to use index
        if (index_path == query_path) {
            return true;
        }

        // PinJsonIndex guarantees index_path is a prefix of query_path

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
    CanUseNestedIndex() const override {
        if (!CanUseIndex() || pinned_index_.empty()) {
            return false;
        }
        auto* index_ptr = pinned_index_[0].get();
        return index_ptr != nullptr && index_ptr->IsNestedIndex();
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
        if (op == OpType::Match || op == OpType::InnerMatch ||
            op == OpType::PostfixMatch) {
            AssertInfo(num_index_chunk_ == 1,
                       "scalar index should have exactly 1 chunk, got {}",
                       num_index_chunk_);
            auto scalar_index =
                dynamic_cast<const Index*>(pinned_index_[0].get());
            auto* index_ptr = const_cast<Index*>(scalar_index);
            // 1, index support pattern query and try use it, then index handles the query;
            // 2, index has raw data, then call index.Reverse_Lookup to handle the query;
            return (index_ptr->TryUsePatternQuery() &&
                    index_ptr->SupportPatternQuery()) ||
                   index_ptr->HasRawData();
        }
        return true;
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

    void
    SetNotUseIndex() {
        use_index_ = false;
    }

    bool
    PlanUseJsonStats(EvalCtx& context) const {
        return context.get_exec_context()
            ->get_query_context()
            ->get_plan_options()
            .expr_use_json_stats;
    }

    bool
    HasJsonStats(FieldId field_id) const {
        return segment_->type() == SegmentType::Sealed &&
               static_cast<const segcore::SegmentSealed*>(segment_)
                       ->GetJsonStats(op_ctx_, field_id)
                       .get() != nullptr;
    }

    bool
    CanUseJsonStats(EvalCtx& context,
                    FieldId field_id,
                    const std::vector<std::string>& nested_path) const {
        // if path contains integer, we can't use json stats such as "a.1.b", "a.1",
        // because we can't know the integer is a key or a array indice
        auto path_contains_integer = [](const std::vector<std::string>& path) {
            for (auto i = 0; i < path.size(); i++) {
                if (milvus::IsInteger(path[i])) {
                    return true;
                }
            }
            return false;
        };

        // if path is empty, json stats can not know key name,
        // so we can't use json shredding data
        return PlanUseJsonStats(context) && HasJsonStats(field_id) &&
               !nested_path.empty() && !path_contains_integer(nested_path);
    }

    virtual bool
    CanUseNgramIndex() const {
        return false;
    };

 protected:
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
    // sometimes need to skip index and using raw data
    // default true means use index as much as possible
    bool use_index_{true};
    // used for reducing cache miss latency in tiered storage
    bool prefetched_{false};
    std::vector<PinWrapper<const index::IndexBase*>> pinned_index_{};

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

    // Cache for index scan to avoid search index every batch
    int64_t cached_index_chunk_id_{-1};
    std::shared_ptr<TargetBitmap> cached_index_chunk_res_{nullptr};
    // Cache for chunk valid res.
    std::shared_ptr<TargetBitmap> cached_index_chunk_valid_res_{nullptr};
    // Cache whether index is nested index
    bool cached_is_nested_index_{false};

    // Cache for text match.
    std::shared_ptr<TargetBitmap> cached_match_res_{nullptr};
    int32_t consistency_level_{0};

    // Cache for ngram Phase1 result (index query, before post-filter).
    std::shared_ptr<TargetBitmap> cached_phase1_res_{nullptr};
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
    explicit ExprSet(const std::vector<expr::TypedExprPtr>& logical_exprs,
                     ExecContext* exec_ctx)
        : exec_ctx_(exec_ctx) {
        exprs_ = CompileExpressions(logical_exprs, exec_ctx);
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

 private:
    std::vector<std::shared_ptr<Expr>> exprs_;
    ExecContext* exec_ctx_;
};

}  //namespace exec
}  // namespace milvus
