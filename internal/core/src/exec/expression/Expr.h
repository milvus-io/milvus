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

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/VectorFunction.h"
#include "exec/expression/Utils.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "log/Log.h"
#include "query/PlanProto.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentGrowingImpl.h"
namespace milvus {
namespace exec {

enum class FilterType { sequential = 0, random = 1 };

class Expr {
 public:
    Expr(DataType type,
         const std::vector<std::shared_ptr<Expr>>&& inputs,
         const std::string& name)
        : type_(type),
          inputs_(std::move(inputs)),
          name_(name),
          vector_func_(nullptr) {
    }

    Expr(DataType type,
         const std::vector<std::shared_ptr<Expr>>&& inputs,
         std::shared_ptr<VectorFunction> vec_func,
         const std::string& name)
        : type_(type),
          inputs_(std::move(inputs)),
          name_(name),
          vector_func_(vec_func) {
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
        PanicInfo(ErrorCode::NotImplemented, "not implemented");
    }

    virtual bool
    IsSource() const {
        return false;
    }

    virtual std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const {
        PanicInfo(ErrorCode::NotImplemented, "not implemented");
    }

    std::vector<std::shared_ptr<Expr>>&
    GetInputsRef() {
        return inputs_;
    }

 protected:
    DataType type_;
    std::vector<std::shared_ptr<Expr>> inputs_;
    std::string name_;
    // NOTE: unused
    std::shared_ptr<VectorFunction> vector_func_;

    // whether we have offset input and do expr filtering on these data
    // default is false which means we will do expr filtering on the total segment data
    bool has_offset_input_ = false;
};

using ExprPtr = std::shared_ptr<milvus::exec::Expr>;

using SkipFunc = bool (*)(const milvus::SkipIndex&, FieldId, int);

/*
 * The expr has only one column.
 */
class SegmentExpr : public Expr {
 public:
    SegmentExpr(const std::vector<ExprPtr>&& input,
                const std::string& name,
                const segcore::SegmentInternalInterface* segment,
                const FieldId field_id,
                const std::vector<std::string> nested_path,
                const DataType value_type,
                int64_t active_count,
                int64_t batch_size,
                int32_t consistency_level,
                bool allow_any_json_cast_type = false)

        : Expr(DataType::BOOL, std::move(input), name),
          segment_(segment),
          field_id_(field_id),
          nested_path_(nested_path),
          value_type_(value_type),
          allow_any_json_cast_type_(allow_any_json_cast_type),
          active_count_(active_count),
          batch_size_(batch_size),
          consistency_level_(consistency_level) {
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

        if (field_meta.get_data_type() == DataType::JSON) {
            auto pointer = milvus::Json::pointer(nested_path_);
            if (is_index_mode_ =
                    segment_->HasIndex(field_id_,
                                       pointer,
                                       value_type_,
                                       allow_any_json_cast_type_)) {
                num_index_chunk_ = 1;
            }
        } else {
            is_index_mode_ = segment_->HasIndex(field_id_);
            if (is_index_mode_) {
                num_index_chunk_ = segment_->num_chunk_index(field_id_);
            }
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

    int64_t
     GetProcessedRows() {
        int64_t total_pos = 0;

        for (size_t i = 0; i < current_data_chunk_; i++) {
            total_pos += segment_->chunk_size(field_id_, i);
        }

        total_pos += current_data_chunk_pos_;

        LOG_DEBUG(
            " GetProcessedRows - total_pos: {}, current_chunk: {}, "
            "current_pos: {}",
            total_pos,
            current_data_chunk_,
            current_data_chunk_pos_);

        return total_pos;
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
            if (is_index_mode_) {
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
        auto current_chunk = is_index_mode_ && use_index_ ? current_index_chunk_
                                                          : current_data_chunk_;
        auto current_chunk_pos = is_index_mode_ && use_index_
                                     ? current_index_chunk_pos_
                                     : current_data_chunk_pos_;
        auto current_rows = 0;
        if (segment_->is_chunked()) {
            current_rows =
                is_index_mode_ && use_index_ &&
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

    // used for processing raw data expr for sealed segments.
    // now only used for std::string_view && json
    // TODO: support more types
    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessChunkForSealedSeg(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        ValTypes... values) {
        // For sealed segment, only single chunk
        Assert(num_data_chunk_ == 1);
        auto need_size =
            std::min(active_count_ - current_data_chunk_pos_, batch_size_);

        auto& skip_index = segment_->GetSkipIndex();
        auto views_info = segment_->get_batch_views<T>(
            field_id_, 0, current_data_chunk_pos_, need_size);
        if (!skip_func || !skip_func(skip_index, field_id_, 0)) {
            // first is the raw data, second is valid_data
            // use valid_data to see if raw data is null
            func(views_info.first.data(),
                 views_info.second.data(),
                 nullptr,
                 need_size,
                 res,
                 valid_res,
                 values...);
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
        ValTypes... values) {
        // For non_chunked sealed segment, only single chunk
        Assert(num_data_chunk_ == 1);

        auto& skip_index = segment_->GetSkipIndex();
        auto [data_vec, valid_data] =
            segment_->get_views_by_offsets<T>(field_id_, 0, *input);
        if (!skip_func || !skip_func(skip_index, field_id_, 0)) {
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
                                ValTypes... values) {
        AssertInfo(num_index_chunk_ == 1, "scalar index chunk num must be 1");
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;
        TargetBitmap valid_res(input->size());

        const Index& index =
            segment_->chunk_scalar_index<IndexInnerType>(field_id_, 0);
        auto* index_ptr = const_cast<Index*>(&index);
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
        ValTypes... values) {
        AssertInfo(num_index_chunk_ == 1, "scalar index chunk num must be 1");
        auto& skip_index = segment_->GetSkipIndex();

        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;
        const Index& index =
            segment_->chunk_scalar_index<IndexInnerType>(field_id_, 0);
        auto* index_ptr = const_cast<Index*>(&index);
        auto valid_result = index_ptr->IsNotNull();
        auto batch_size = input->size();

        if (!skip_func || !skip_func(skip_index, field_id_, 0)) {
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
        ValTypes... values) {
        int64_t processed_size = 0;

        // index reverse lookup
        if (is_index_mode_ && num_data_chunk_ == 0) {
            return ProcessIndexLookupByOffsets<T>(
                func, skip_func, input, res, valid_res, values...);
        }

        auto& skip_index = segment_->GetSkipIndex();

        // raw data scan
        // sealed segment
        if (segment_->type() == SegmentType::Sealed) {
            if (segment_->is_chunked()) {
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json>) {
                    for (size_t i = 0; i < input->size(); ++i) {
                        int64_t offset = (*input)[i];
                        auto [chunk_id, chunk_offset] =
                            segment_->get_chunk_by_offset(field_id_, offset);
                        auto [data_vec, valid_data] =
                            segment_->get_views_by_offsets<T>(
                                field_id_, chunk_id, {int32_t(chunk_offset)});
                        if (!skip_func ||
                            !skip_func(skip_index, field_id_, chunk_id)) {
                            func.template operator()<FilterType::random>(
                                data_vec.data(),
                                valid_data.data(),
                                nullptr,
                                1,
                                res + processed_size,
                                valid_res + processed_size,
                                values...);
                        } else {
                            res[processed_size] = valid_res[processed_size] =
                                (valid_data[0]);
                        }
                        processed_size++;
                    }
                    return input->size();
                }
                for (size_t i = 0; i < input->size(); ++i) {
                    int64_t offset = (*input)[i];
                    auto [chunk_id, chunk_offset] =
                        segment_->get_chunk_by_offset(field_id_, offset);
                    auto chunk = segment_->chunk_data<T>(field_id_, chunk_id);
                    const T* data = chunk.data() + chunk_offset;
                    const bool* valid_data = chunk.valid_data();
                    if (valid_data != nullptr) {
                        valid_data += chunk_offset;
                    }
                    if (!skip_func ||
                        !skip_func(skip_index, field_id_, chunk_id)) {
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
                              std::is_same_v<T, Json>) {
                    return ProcessDataByOffsetsForSealedSeg<T>(
                        func, skip_func, input, res, valid_res, values...);
                }
                auto chunk = segment_->chunk_data<T>(field_id_, 0);
                const T* data = chunk.data();
                const bool* valid_data = chunk.valid_data();
                if (!skip_func || !skip_func(skip_index, field_id_, 0)) {
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
                auto chunk = segment_->chunk_data<T>(field_id_, chunk_id);
                const T* data = chunk.data() + chunk_offset;
                const bool* valid_data = chunk.valid_data();
                if (valid_data != nullptr) {
                    valid_data += chunk_offset;
                }
                if (!skip_func || !skip_func(skip_index, field_id_, chunk_id)) {
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

    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessDataChunksForSingleChunk(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        ValTypes... values) {
        int64_t processed_size = 0;

        if constexpr (std::is_same_v<T, std::string_view> ||
                      std::is_same_v<T, Json>) {
            if (segment_->type() == SegmentType::Sealed) {
                return ProcessChunkForSealedSeg<T>(
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

            auto& skip_index = segment_->GetSkipIndex();
            auto chunk = segment_->chunk_data<T>(field_id_, i);
            const bool* valid_data = chunk.valid_data();
            if (valid_data != nullptr) {
                valid_data += data_pos;
            }
            if (!skip_func || !skip_func(skip_index, field_id_, i)) {
                const T* data = chunk.data() + data_pos;
                func(data,
                     valid_data,
                     nullptr,
                     size,
                     res + processed_size,
                     valid_res + processed_size,
                     values...);
            } else {
                ApplyValidData(valid_data,
                               res + processed_size,
                               valid_res + processed_size,
                               size);
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
    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessDataChunksForMultipleChunk(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        ValTypes... values) {
        int64_t processed_size = 0;

        // if constexpr (std::is_same_v<T, std::string_view> ||
        //               std::is_same_v<T, Json>) {
        //     if (segment_->type() == SegmentType::Sealed) {
        //         return ProcessChunkForSealedSeg<T>(
        //             func, skip_func, res, values...);
        //     }
        // }

        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;

            // if segment is chunked, type won't be growing
            int64_t size = segment_->chunk_size(field_id_, i) - data_pos;

            size = std::min(size, batch_size_ - processed_size);

            auto& skip_index = segment_->GetSkipIndex();
            if (!skip_func || !skip_func(skip_index, field_id_, i)) {
                bool is_seal = false;
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json> ||
                              std::is_same_v<T, ArrayView>) {
                    if (segment_->type() == SegmentType::Sealed) {
                        // first is the raw data, second is valid_data
                        // use valid_data to see if raw data is null
                        auto [data_vec, valid_data] =
                            segment_->get_batch_views<T>(
                                field_id_, i, data_pos, size);
                        func(data_vec.data(),
                             valid_data.data(),
                             nullptr,
                             size,
                             res + processed_size,
                             valid_res + processed_size,
                             values...);
                        is_seal = true;
                    }
                }
                if (!is_seal) {
                    auto chunk = segment_->chunk_data<T>(field_id_, i);
                    const T* data = chunk.data() + data_pos;
                    const bool* valid_data = chunk.valid_data();
                    if (valid_data != nullptr) {
                        valid_data += data_pos;
                    }
                    func(data,
                         valid_data,
                         nullptr,
                         size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                }
            } else {
                const bool* valid_data;
                if constexpr (std::is_same_v<T, std::string_view> ||
                              std::is_same_v<T, Json>) {
                    auto batch_views = segment_->get_batch_views<T>(
                        field_id_, i, data_pos, size);
                    valid_data = batch_views.second.data();
                    ApplyValidData(valid_data,
                                   res + processed_size,
                                   valid_res + processed_size,
                                   size);
                } else {
                    auto chunk = segment_->chunk_data<T>(field_id_, i);
                    valid_data = chunk.valid_data();
                    if (valid_data != nullptr) {
                        valid_data += data_pos;
                    }
                    ApplyValidData(valid_data,
                                   res + processed_size,
                                   valid_res + processed_size,
                                   size);
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

    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessDataChunks(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        ValTypes... values) {
        if (segment_->is_chunked()) {
            return ProcessDataChunksForMultipleChunk<T>(
                func, skip_func, res, valid_res, values...);
        } else {
            return ProcessDataChunksForSingleChunk<T>(
                func, skip_func, res, valid_res, values...);
        }
    }

    int
    ProcessIndexOneChunk(TargetBitmap& result,
                         TargetBitmap& valid_result,
                         size_t chunk_id,
                         const TargetBitmap& chunk_res,
                         const TargetBitmap& chunk_valid_res,
                         int processed_rows) {
        auto data_pos =
            chunk_id == current_index_chunk_ ? current_index_chunk_pos_ : 0;
        auto size = std::min(
            std::min(size_per_chunk_ - data_pos, batch_size_ - processed_rows),
            int64_t(chunk_res.size()));

        //        result.insert(result.end(),
        //                      chunk_res.begin() + data_pos,
        //                      chunk_res.begin() + data_pos + size);
        result.append(chunk_res, data_pos, size);
        valid_result.append(chunk_valid_res, data_pos, size);
        return size;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    VectorPtr
    ProcessIndexChunks(FUNC func, ValTypes... values) {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;
        TargetBitmap result;
        TargetBitmap valid_result;
        int processed_rows = 0;

        for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
            // This cache result help getting result for every batch loop.
            // It avoids indexing execute for every batch because indexing
            // executing costs quite much time.
            if (cached_index_chunk_id_ != i) {
                Index* index_ptr = nullptr;

                if (field_type_ == DataType::JSON) {
                    auto pointer = milvus::Json::pointer(nested_path_);

                    const Index& index =
                        segment_->chunk_scalar_index<IndexInnerType>(
                            field_id_, pointer, i);
                    index_ptr = const_cast<Index*>(&index);
                } else {
                    const Index& index =
                        segment_->chunk_scalar_index<IndexInnerType>(field_id_,
                                                                     i);
                    index_ptr = const_cast<Index*>(&index);
                }
                cached_index_chunk_res_ = std::move(func(index_ptr, values...));
                auto valid_result = index_ptr->IsNotNull();
                cached_index_chunk_valid_res_ = std::move(valid_result);
                cached_index_chunk_id_ = i;
            }

            auto size = ProcessIndexOneChunk(result,
                                             valid_result,
                                             i,
                                             cached_index_chunk_res_,
                                             cached_index_chunk_valid_res_,
                                             processed_rows);

            if (processed_rows + size >= batch_size_) {
                current_index_chunk_ = i;
                current_index_chunk_pos_ = i == current_index_chunk_
                                               ? current_index_chunk_pos_ + size
                                               : size;
                break;
            }
            processed_rows += size;
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
                    default:
                        PanicInfo(DataTypeInvalid,
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
                        PanicInfo(DataTypeInvalid,
                                  "unsupported element type: {}",
                                  element_type);
                }
            }
            const Index& index =
                segment_->chunk_scalar_index<IndexInnerType>(field_id_, 0);
            auto* index_ptr = const_cast<Index*>(&index);
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
                auto chunk = segment_->chunk_data<T>(field_id_, chunk_id);
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

            bool access_sealed_variable_column = false;
            if constexpr (std::is_same_v<T, std::string_view> ||
                          std::is_same_v<T, Json> ||
                          std::is_same_v<T, ArrayView>) {
                if (segment_->type() == SegmentType::Sealed) {
                    auto [data_vec, valid_data] = segment_->get_batch_views<T>(
                        field_id_, i, data_pos, size);
                    ApplyValidData(valid_data.data(),
                                   valid_result + processed_size,
                                   valid_result + processed_size,
                                   size);
                    access_sealed_variable_column = true;
                }
            }

            if (!access_sealed_variable_column) {
                auto chunk = segment_->chunk_data<T>(field_id_, i);
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

    int
    ProcessIndexOneChunkForValid(TargetBitmap& valid_result,
                                 size_t chunk_id,
                                 const TargetBitmap& chunk_valid_res,
                                 int processed_rows) {
        auto data_pos =
            chunk_id == current_index_chunk_ ? current_index_chunk_pos_ : 0;
        auto size = std::min(
            std::min(size_per_chunk_ - data_pos, batch_size_ - processed_rows),
            int64_t(chunk_valid_res.size()));

        valid_result.append(chunk_valid_res, data_pos, size);
        return size;
    }

    template <typename T>
    TargetBitmap
    ProcessIndexChunksForValid() {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;
        int processed_rows = 0;
        TargetBitmap valid_result;
        valid_result.set();

        for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
            // This cache result help getting result for every batch loop.
            // It avoids indexing execute for every batch because indexing
            // executing costs quite much time.
            if (cached_index_chunk_id_ != i) {
                const Index& index =
                    segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
                auto* index_ptr = const_cast<Index*>(&index);
                auto execute_sub_batch = [](Index* index_ptr) {
                    TargetBitmap res = index_ptr->IsNotNull();
                    return res;
                };
                cached_index_chunk_valid_res_ = execute_sub_batch(index_ptr);
                cached_index_chunk_id_ = i;
            }

            auto size = ProcessIndexOneChunkForValid(
                valid_result, i, cached_index_chunk_valid_res_, processed_rows);

            if (processed_rows + size >= batch_size_) {
                current_index_chunk_ = i;
                current_index_chunk_pos_ = i == current_index_chunk_
                                               ? current_index_chunk_pos_ + size
                                               : size;
                break;
            }
            processed_rows += size;
        }
        return valid_result;
    }

    template <typename FUNC, typename... ValTypes>
    VectorPtr
    ProcessTextMatchIndex(FUNC func, ValTypes... values) {
        TargetBitmap result;
        TargetBitmap valid_result;

        if (cached_match_res_ == nullptr) {
            auto index = segment_->GetTextIndex(field_id_);
            auto res = std::move(func(index, values...));
            auto valid_res = index->IsNotNull();
            cached_match_res_ = std::make_shared<TargetBitmap>(std::move(res));
            cached_index_chunk_valid_res_ = std::move(valid_res);
            if (cached_match_res_->size() < active_count_) {
                // some entities are not visible in inverted index.
                // only happend on growing segment.
                TargetBitmap tail(active_count_ - cached_match_res_->size());
                cached_match_res_->append(tail);
                cached_index_chunk_valid_res_.append(tail);
            }
        }

        // return batch size, not sure if we should use the data position.
        auto real_batch_size =
            current_data_chunk_pos_ + batch_size_ > active_count_
                ? active_count_ - current_data_chunk_pos_
                : batch_size_;
        result.append(
            *cached_match_res_, current_data_chunk_pos_, real_batch_size);
        valid_result.append(cached_index_chunk_valid_res_,
                            current_data_chunk_pos_,
                            real_batch_size);
        current_data_chunk_pos_ += real_batch_size;

        return std::make_shared<ColumnVector>(std::move(result),
                                              std::move(valid_result));
    }

    template <typename T, typename FUNC, typename... ValTypes>
    void
    ProcessIndexChunksV2(FUNC func, ValTypes... values) {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;

        for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
            const Index& index =
                segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
            auto* index_ptr = const_cast<Index*>(&index);
            func(index_ptr, values...);
        }
    }

    template <typename T>
    bool
    CanUseIndex(OpType op) const {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        if constexpr (!std::is_same_v<IndexInnerType, std::string>) {
            return true;
        }

        using Index = index::ScalarIndex<IndexInnerType>;
        if (op == OpType::Match) {
            for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
                const Index& index =
                    segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
                // 1, index support regex query, then index handles the query;
                // 2, index has raw data, then call index.Reverse_Lookup to handle the query;
                if (!index.SupportRegexQuery() && !index.HasRawData()) {
                    return false;
                }
                // all chunks have same index.
                return true;
            }
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
        for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
            const Index& index =
                segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
            if (!index.HasRawData()) {
                return false;
            }
        }

        return true;
    }

    void
    SetNotUseIndex() {
        use_index_ = false;
    }

    bool
    CanUseJsonKeyIndex(FieldId field_id) const {
        if (segment_->type() == SegmentType::Sealed) {
            auto sealed_seg =
                dynamic_cast<const segcore::SegmentSealed*>(segment_);
            Assert(sealed_seg != nullptr);
            if (sealed_seg->GetJsonKeyIndex(field_id) != nullptr) {
                return true;
            }
        } else if (segment_->type() == SegmentType ::Growing) {
            if (segment_->GetJsonKeyIndex(field_id) != nullptr) {
                return true;
            }
        }
        return false;
    }

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
    bool is_index_mode_{false};
    bool is_data_mode_{false};
    // sometimes need to skip index and using raw data
    // default true means use index as much as possible
    bool use_index_{true};

    int64_t active_count_{0};
    int64_t num_data_chunk_{0};
    int64_t num_index_chunk_{0};
    // State indicate position that expr computing at
    // because expr maybe called for every batch.
    int64_t current_data_chunk_{0};
    int64_t current_data_chunk_pos_{0};
    int64_t current_index_chunk_{0};
    int64_t current_index_chunk_pos_{0};
    int64_t size_per_chunk_{0};

    // Cache for index scan to avoid search index every batch
    int64_t cached_index_chunk_id_{-1};
    TargetBitmap cached_index_chunk_res_{};
    // Cache for chunk valid res.
    TargetBitmap cached_index_chunk_valid_res_{};

    // Cache for text match.
    std::shared_ptr<TargetBitmap> cached_match_res_{nullptr};
    int32_t consistency_level_{0};
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
