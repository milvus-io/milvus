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

#include "MvccNode.h"

#include <utility>
#include <vector>

#include "common/Tracer.h"
#include "exec/QueryContext.h"
#include "exec/expression/Utils.h"
#include "fmt/core.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

PhyMvccNode::PhyMvccNode(int32_t operator_id,
                         DriverContext* driverctx,
                         const std::shared_ptr<const plan::MvccNode>& mvcc_node)
    : Operator(driverctx,
               mvcc_node->output_type(),
               operator_id,
               mvcc_node->id(),
               "PhyIterativeFilterNode") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    QueryContext* query_context = exec_context->get_query_context();
    segment_ = query_context->get_segment();
    query_timestamp_ = query_context->get_query_timestamp();
    active_count_ = query_context->get_active_count();
    is_source_node_ = mvcc_node->sources().size() == 0;
    collection_ttl_timestamp_ = query_context->get_collection_ttl();
}

void
PhyMvccNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyMvccNode::GetOutput() {
    auto* query_context =
        operator_context_->get_exec_context()->get_query_context();
    milvus::exec::checkCancellation(query_context);

    if (is_finished_) {
        return nullptr;
    }

    tracer::AutoSpan span("PhyMvccNode::Execute", tracer::GetRootSpan(), true);

    if (active_count_ == 0) {
        is_finished_ = true;
        return nullptr;
    }

    if (!is_source_node_ && input_ == nullptr) {
        return nullptr;
    }

    // ── Sealed + no-filter fast paths ──────────────────────────────
    // Split into two independent optimizations so each applies on its own:
    //   Level 1: no deletes → skip everything (QueryContext flag + thread-local bitmap)
    //   Level 2: has deletes → skip timestamp mask, only apply delete mask
    // Both require: source node (no scalar filter), sealed segment, no TTL.
    // mask_with_timestamps is redundant on sealed segments without TTL
    // because all rows are committed before the query timestamp.
    if (is_source_node_ &&
        segment_->type() == SegmentType::Sealed &&
        collection_ttl_timestamp_ == 0) {

        // Level 1: no deletes — every row is visible.
        // Return a thread-local full-size zero bitmap (satisfies Driver +
        // ExecPlanNodeVisitor assertions) and set skip_filter flag so
        // VectorSearchNode passes empty BitsetView to Knowhere.
        if (segment_->get_deleted_count() == 0) {
            is_finished_ = true;
            auto* qctx =
                operator_context_->get_exec_context()->get_query_context();
            qctx->set_skip_filter(true);

            thread_local int64_t cached_size = 0;
            thread_local RowVectorPtr cached_output;
            if (cached_size != active_count_) {
                auto col = std::make_shared<ColumnVector>(
                    TargetBitmap(active_count_),
                    TargetBitmap(active_count_));
                cached_output = std::make_shared<RowVector>(
                    std::vector<VectorPtr>{col});
                cached_size = active_count_;
            }
            return cached_output;
        }

        // Level 2: has deletes — allocate bitmap, apply delete mask only
        auto col_input = std::make_shared<ColumnVector>(
            TargetBitmap(active_count_), TargetBitmap(active_count_));
        TargetBitmapView data(col_input->GetRawData(), col_input->size());
        segment_->mask_with_delete(data, active_count_, query_timestamp_);
        is_finished_ = true;

        auto output_rows = active_count_ - data.count();
        tracer::AddEvent(fmt::format(
            "output_rows: {}, filtered: {}", output_rows, data.count()));
        return std::make_shared<RowVector>(
            std::vector<VectorPtr>{col_input});
    }

    // ── Level 3: default path (has filter / growing / TTL) ────────
    tracer::AddEvent(fmt::format("input_rows: {}", active_count_));
    // the first vector is filtering result and second bitset is a valid bitset
    // if valid_bitset[i]==false, means result[i] is null
    auto col_input = is_source_node_ ? std::make_shared<ColumnVector>(
                                           TargetBitmap(active_count_),
                                           TargetBitmap(active_count_))
                                     : GetColumnVector(input_);

    TargetBitmapView data(col_input->GetRawData(), col_input->size());
    // need to expose null?
    segment_->mask_with_timestamps(
        data, query_timestamp_, collection_ttl_timestamp_);
    segment_->mask_with_delete(data, active_count_, query_timestamp_);
    is_finished_ = true;

    auto output_rows = active_count_ - data.count();
    tracer::AddEvent(fmt::format(
        "output_rows: {}, filtered: {}", output_rows, data.count()));

    // input_ have already been updated
    return std::make_shared<RowVector>(std::vector<VectorPtr>{col_input});
}

bool
PhyMvccNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus