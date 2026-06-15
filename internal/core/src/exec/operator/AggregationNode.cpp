// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

//
// Created by hanchun on 24-10-18.
//

#include "AggregationNode.h"

#include <functional>
#include <string_view>
#include <utility>
#include <vector>

#include "cachinglayer/CacheSlot.h"

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Utils.h"
#include "exec/QueryContext.h"
#include "exec/VectorHasher.h"
#include "exec/expression/Utils.h"
#include "exec/operator/query-agg/AggChunkInput.h"
#include "exec/operator/query-agg/AggRawInput.h"
#include "exec/operator/query-agg/AggregateInfo.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {
namespace {

struct RawColumnPinHolderBase {
    virtual ~RawColumnPinHolderBase() = default;
};

template <typename T>
struct RawColumnPinHolder final : RawColumnPinHolderBase {
    explicit RawColumnPinHolder(cachinglayer::PinWrapper<T>&& pin)
        : pin_(std::move(pin)) {
    }

    cachinglayer::PinWrapper<T> pin_;
};

template <typename T>
struct MissingFixedRawColumnHolder final : RawColumnPinHolderBase {
    explicit MissingFixedRawColumnHolder(int64_t row_count)
        : values_(row_count),
          valid_(row_count, false),
          span_(values_.data(), valid_.data(), row_count, sizeof(T)) {
    }

    std::vector<T> values_;
    FixedVector<bool> valid_;
    SpanBase span_;
};

struct MissingStringRawColumnHolder final : RawColumnPinHolderBase {
    explicit MissingStringRawColumnHolder(int64_t row_count)
        : views_(row_count), valid_(row_count, false) {
    }

    std::vector<std::string_view> views_;
    FixedVector<bool> valid_;
};

struct MissingBoolRawColumnHolder final : RawColumnPinHolderBase {
    explicit MissingBoolRawColumnHolder(int64_t row_count)
        : values_(std::make_unique<bool[]>(row_count)),
          valid_(row_count, false),
          span_(values_.get(), valid_.data(), row_count, sizeof(bool)) {
    }

    std::unique_ptr<bool[]> values_;
    FixedVector<bool> valid_;
    SpanBase span_;
};

struct GrowingStringRawColumnPinHolder final : RawColumnPinHolderBase {
    GrowingStringRawColumnPinHolder(
        cachinglayer::PinWrapper<Span<std::string>>&& pin, int64_t row_count)
        : pin_(std::move(pin)) {
        auto span = pin_.get();
        views_.reserve(row_count);
        for (int64_t i = 0; i < row_count; ++i) {
            views_.emplace_back(span.data()[i]);
        }
        if (span.valid_data() != nullptr) {
            valid_.reserve(row_count);
            for (int64_t i = 0; i < row_count; ++i) {
                valid_.emplace_back(span.valid_data()[i]);
            }
        }
    }

    cachinglayer::PinWrapper<Span<std::string>> pin_;
    std::vector<std::string_view> views_;
    FixedVector<bool> valid_;
};

template <DataType Type>
void
AddMissingRawColumn(AggRawInput& input,
                    std::vector<std::unique_ptr<RawColumnPinHolderBase>>& pins,
                    int64_t row_count) {
    if constexpr (Type == DataType::VARCHAR || Type == DataType::STRING) {
        auto holder = std::make_unique<MissingStringRawColumnHolder>(row_count);
        input.AddStringColumn(Type, holder->views_, holder->valid_, false);
        pins.emplace_back(std::move(holder));
    } else if constexpr (Type == DataType::BOOL) {
        auto holder = std::make_unique<MissingBoolRawColumnHolder>(row_count);
        input.AddFixedWidthColumn(Type, holder->span_);
        pins.emplace_back(std::move(holder));
    } else if constexpr (Type == DataType::INT8 || Type == DataType::INT16 ||
                         Type == DataType::INT32 || Type == DataType::INT64 ||
                         Type == DataType::TIMESTAMPTZ ||
                         Type == DataType::FLOAT || Type == DataType::DOUBLE) {
        using NativeType = typename TypeTraits<Type>::NativeType;
        auto holder =
            std::make_unique<MissingFixedRawColumnHolder<NativeType>>(row_count);
        input.AddFixedWidthColumn(Type, holder->span_);
        pins.emplace_back(std::move(holder));
    } else {
        ThrowInfo(DataTypeInvalid,
                  "raw aggregation does not support input type {}",
                  Type);
    }
}

template <DataType Type>
void
AddRawColumn(AggRawInput& input,
             std::vector<std::unique_ptr<RawColumnPinHolderBase>>& pins,
             const segcore::SegmentInternalInterface& segment,
             OpContext* op_context,
             FieldId field_id,
             const AggSelectedChunk& chunk) {
    if (!segment.is_field_exist(field_id)) {
        AddMissingRawColumn<Type>(input, pins, chunk.row_count());
        return;
    }
    if constexpr (Type == DataType::VARCHAR || Type == DataType::STRING) {
        if (segment.type() == SegmentType::Growing) {
            auto pin = segment.chunk_data<std::string>(
                op_context, field_id, chunk.chunk_id());
            auto holder = std::make_unique<GrowingStringRawColumnPinHolder>(
                std::move(pin), chunk.row_count());
            input.AddStringColumn(Type, holder->views_, holder->valid_, false);
            pins.emplace_back(std::move(holder));
        } else {
            using PinValue =
                std::pair<std::vector<std::string_view>, FixedVector<bool>>;
            auto pin = AggSegmentChunkView(op_context, segment)
                           .PinViews<std::string_view>(field_id, chunk);
            auto holder =
                std::make_unique<RawColumnPinHolder<PinValue>>(std::move(pin));
            input.AddStringColumn(Type,
                                  holder->pin_.get().first,
                                  holder->pin_.get().second,
                                  !chunk.all_selected());
            pins.emplace_back(std::move(holder));
        }
    } else if constexpr (Type == DataType::BOOL) {
        auto pin =
            segment.chunk_data<bool>(op_context, field_id, chunk.chunk_id());
        auto holder =
            std::make_unique<RawColumnPinHolder<Span<bool>>>(std::move(pin));
        input.AddFixedWidthColumn(Type, holder->pin_.get());
        pins.emplace_back(std::move(holder));
    } else if constexpr (Type == DataType::INT8 || Type == DataType::INT16 ||
                         Type == DataType::INT32 || Type == DataType::INT64 ||
                         Type == DataType::TIMESTAMPTZ ||
                         Type == DataType::FLOAT || Type == DataType::DOUBLE) {
        using NativeType = typename TypeTraits<Type>::NativeType;
        auto pin = segment.chunk_data<NativeType>(
            op_context, field_id, chunk.chunk_id());
        auto holder =
            std::make_unique<RawColumnPinHolder<Span<NativeType>>>(
                std::move(pin));
        input.AddFixedWidthColumn(Type, holder->pin_.get());
        pins.emplace_back(std::move(holder));
    } else {
        ThrowInfo(DataTypeInvalid,
                  "raw aggregation does not support input type {}",
                  Type);
    }
}

void
AddRawColumn(AggRawInput& input,
             std::vector<std::unique_ptr<RawColumnPinHolderBase>>& pins,
             const segcore::SegmentInternalInterface& segment,
             OpContext* op_context,
             FieldId field_id,
             DataType type,
             const AggSelectedChunk& chunk) {
    MILVUS_DYNAMIC_TYPE_DISPATCH(
        AddRawColumn, type, input, pins, segment, op_context, field_id, chunk);
}

}  // namespace

PhyAggregationNode::PhyAggregationNode(
    int32_t operator_id,
    milvus::exec::DriverContext* ctx,
    const std::shared_ptr<const plan::AggregationNode>& node)
    : Operator(
          ctx, node->output_type(), operator_id, node->id(), "AggregationNode"),
      aggregationNode_(node),
      isGlobal_(node->GroupingKeys().empty()) {
}

void
PhyAggregationNode::initialize() {
    Operator::initialize();
    use_raw_input_ = aggregationNode_->UseRawInput();
    input_type_ = aggregationNode_->input_type() != nullptr
                      ? aggregationNode_->input_type()
                      : aggregationNode_->sources()[0]->output_type();
    raw_input_field_ids_ = aggregationNode_->RawInputFieldIds();
    if (use_raw_input_) {
        auto exec_context = operator_context_->get_exec_context();
        auto query_context = exec_context->get_query_context();
        segment_ = query_context->get_segment();
        op_context_ = query_context->get_op_context();
        AssertInfo(segment_, "segment cannot be nullptr for raw aggregation");
        AssertInfo(op_context_, "op context cannot be nullptr for raw aggregation");
        AssertInfo(input_type_ != nullptr,
                   "input type cannot be nullptr for raw aggregation");
        AssertInfo(input_type_->column_count() == raw_input_field_ids_.size(),
                   "raw aggregation input type and field id list must match");
    }
    auto hashers =
        createVectorHashers(input_type_, aggregationNode_->GroupingKeys());
    auto numHashers = hashers.size();
    std::vector<AggregateInfo> aggregateInfos =
        toAggregateInfo(*aggregationNode_, *operator_context_, numHashers);
    grouping_set_ = std::make_unique<GroupingSet>(
        input_type_, std::move(hashers), std::move(aggregateInfos));
    aggregationNode_.reset();
}

void
PhyAggregationNode::AddInput(RowVectorPtr& input) {
    if (use_raw_input_) {
        AddRawInput(input);
        return;
    }
    grouping_set_->addInput(input);
    numInputRows_ += input->size();
}

void
PhyAggregationNode::AddRawInput(RowVectorPtr& input) {
    auto filter_column = GetColumnVector(input);
    AssertInfo(filter_column->IsBitmap(),
               "raw aggregation expects bitmap input from upstream");
    TargetBitmapView filter_mask(filter_column->GetRawData(),
                                 filter_column->size());
    FieldId anchor_field;
    if (raw_input_field_ids_.empty()) {
        auto pk_field_id = segment_->get_schema().get_primary_field_id();
        AssertInfo(pk_field_id.has_value(),
                   "raw count aggregation requires a primary key anchor field");
        anchor_field = pk_field_id.value();
    } else {
        anchor_field = raw_input_field_ids_.front();
    }
    auto chunks = AggChunkInput::FromSegment(*segment_, anchor_field, filter_mask);
    for (const auto& chunk : chunks.chunks()) {
        AggRawInput raw_input(chunk);
        std::vector<std::unique_ptr<RawColumnPinHolderBase>> pins;
        pins.reserve(raw_input_field_ids_.size());
        for (size_t i = 0; i < raw_input_field_ids_.size(); ++i) {
            AddRawColumn(raw_input,
                         pins,
                         *segment_,
                         op_context_,
                         raw_input_field_ids_[i],
                         input_type_->column_type(i),
                         chunk);
        }
        grouping_set_->addRawInput(raw_input);
        numInputRows_ += raw_input.selected_count();
    }
}

RowVectorPtr
PhyAggregationNode::GetOutput() {
    if (finished_ || !no_more_input_) {
        input_ = nullptr;
        return nullptr;
    }
    DeferLambda([&]() { finished_ = true; });
    const auto outputRowCount = isGlobal_ ? 1 : grouping_set_->outputRowCount();
    output_ = std::make_shared<RowVector>(output_type_, outputRowCount);
    const bool hasData = grouping_set_->getOutput(output_);
    if (!hasData) {
        return nullptr;
    }
    numOutputRows_ += output_->size();
    return output_;
}

};  // namespace exec
};  // namespace milvus
