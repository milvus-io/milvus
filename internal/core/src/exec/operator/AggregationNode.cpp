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
#include <utility>

#include "common/Json.h"
#include "common/Utils.h"
#include "exec/JsonFieldUtils.h"
#include "exec/QueryContext.h"
#include "exec/VectorHasher.h"
#include "exec/operator/query-agg/AggregateInfo.h"
#include "index/json_stats/JsonKeyStats.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

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
    // aggregation operator will always have single one source
    const auto& input_type = aggregationNode_->sources()[0]->output_type();
    auto hashers =
        createVectorHashers(input_type, aggregationNode_->GroupingKeys());
    auto numHashers = hashers.size();
    std::vector<AggregateInfo> aggregateInfos =
        toAggregateInfo(*aggregationNode_, *operator_context_, numHashers);

    // Probe json_stats for shredding columns that can replace JSON parsing
    // in GROUP BY keys. Store (column_index, shred_field_name, json_stats)
    // for each hasher that has a nested_path and a matching shredding column.
    auto exec_ctx = operator_context_->get_exec_context();
    auto query_ctx = exec_ctx->get_query_context();
    auto* segment = query_ctx->get_segment();
    auto* op_ctx = query_ctx->get_op_context();
    for (size_t i = 0; i < hashers.size(); i++) {
        if (!hashers[i]->has_nested_path()) {
            continue;
        }
        // The hasher's channel index points to the JSON column in the input
        auto col_idx = hashers[i]->ChannelIndex();
        auto& nested_path = hashers[i]->nested_path();
        // Resolve field_id from the plan node's grouping keys
        auto& group_keys = aggregationNode_->GroupingKeys();
        if (i < group_keys.size()) {
            auto field_id = FieldId(group_keys[i]->field_id());
            auto stats = segment->GetJsonStats(op_ctx, field_id);
            if (stats) {
                auto pointer = Json::pointer(nested_path);
                auto shred_field = stats->GetShreddingField(
                    pointer, index::JSONType::STRING);
                if (!shred_field.empty()) {
                    shredding_info_.push_back(
                        {col_idx, shred_field, stats, op_ctx});
                }
            }
        }
    }

    grouping_set_ = std::make_unique<GroupingSet>(
        input_type, std::move(hashers), std::move(aggregateInfos));
    aggregationNode_.reset();
}

void
PhyAggregationNode::AddInput(RowVectorPtr& input) {
    // If we have shredding columns for GROUP BY keys, replace the JSON column
    // in the input with pre-extracted VARCHAR data before passing to GroupingSet.
    // This avoids per-row JSON parsing inside HashTable::prepareForGroupProbe.
    if (!shredding_info_.empty()) {
        for (auto& info : shredding_info_) {
            auto json_col = std::dynamic_pointer_cast<ColumnVector>(
                input->child(info.column_idx));
            if (!json_col ||
                json_col->type() != DataType::JSON) {
                continue;
            }
            // Read pre-extracted string values from shredding column
            auto shred_col =
                info.stats->GetShreddingColumnByName(info.shred_field);
            if (!shred_col) {
                continue;
            }
            auto num_rows = json_col->size();
            auto result =
                std::make_shared<ColumnVector>(DataType::VARCHAR, num_rows);
            // The input batch contains raw segment offsets encoded in the
            // bitmap; here we just need the first num_rows values from
            // the shredding column sequentially (same order as input).
            // NOTE: This works because the pipeline processes rows in
            // segment offset order within each batch.
            size_t chunk_idx = 0;
            size_t chunk_offset = 0;
            size_t num_chunks = shred_col->num_chunks();
            size_t cur_chunk_size =
                num_chunks > 0 ? shred_col->chunk_row_nums(0) : 0;

            // Advance to the correct position based on numInputRows_
            // (rows already consumed in previous batches)
            int64_t skip = numInputRows_;
            while (skip > 0 && chunk_idx < num_chunks) {
                auto remaining_in_chunk = cur_chunk_size - chunk_offset;
                if (skip >= static_cast<int64_t>(remaining_in_chunk)) {
                    skip -= remaining_in_chunk;
                    chunk_idx++;
                    chunk_offset = 0;
                    cur_chunk_size = chunk_idx < num_chunks
                                        ? shred_col->chunk_row_nums(chunk_idx)
                                        : 0;
                } else {
                    chunk_offset += skip;
                    skip = 0;
                }
            }

            // Now read num_rows sequentially
            size_t written = 0;
            while (written < static_cast<size_t>(num_rows) &&
                   chunk_idx < num_chunks) {
                auto pw = shred_col->StringViews(info.op_ctx, chunk_idx);
                auto [data_vec, valid_data] = pw.get();
                while (chunk_offset < cur_chunk_size &&
                       written < static_cast<size_t>(num_rows)) {
                    if (valid_data[chunk_offset]) {
                        result->SetValueAt<std::string>(
                            written,
                            std::string(data_vec[chunk_offset]));
                    } else {
                        result->SetValueAt<std::string>(written, "");
                    }
                    chunk_offset++;
                    written++;
                }
                chunk_idx++;
                chunk_offset = 0;
                cur_chunk_size = chunk_idx < num_chunks
                                     ? shred_col->chunk_row_nums(chunk_idx)
                                     : 0;
            }

            // Replace JSON column with pre-extracted VARCHAR column
            input->setChild(info.column_idx, result);
        }
    }
    grouping_set_->addInput(input);
    numInputRows_ += input->size();
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
