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

    tracer::AddEvent(fmt::format("input_rows: {}", active_count_));

    // ── Three-level fast path for sealed segments without filters ──
    // When MvccNode is the source (no scalar filter upstream) on a sealed
    // segment with no collection-level TTL, and the query timestamp covers
    // all data in the segment (query_ts >= max_insert_ts), we can skip
    // expensive bitmap ops. The timestamp guard ensures correctness under
    // Bounded/Eventually consistency where query_ts may lag behind.
    if (is_source_node_ && segment_->type() == SegmentType::Sealed &&
        collection_ttl_timestamp_ == 0 &&
        query_timestamp_ >= segment_->get_max_timestamp()) {
        // Level 1: Sealed + no deletes → all rows visible, skip everything
        if (segment_->get_deleted_count() == 0) {
            is_finished_ = true;
            query_context->set_all_rows_visible(true);

            // Reuse thread-local zero bitmap to avoid per-query allocation.
            // INVARIANT: This cached bitmap is never read by downstream operators.
            // all_rows_visible=true causes VectorSearchNode to use empty BitsetView
            // directly, bypassing this bitmap entirely. The cache exists only to
            // satisfy the Operator interface (GetOutput must return non-null
            // RowVectorPtr with size>0).
            thread_local int64_t cached_size = 0;
            thread_local RowVectorPtr cached_output;
            if (cached_size != active_count_) {
                auto col = std::make_shared<ColumnVector>(
                    TargetBitmap(active_count_), TargetBitmap(active_count_));
                cached_output =
                    std::make_shared<RowVector>(std::vector<VectorPtr>{col});
                cached_size = active_count_;
            }
            return cached_output;
        }

        // Level 2: Sealed + has deletes → only apply delete mask, skip timestamps
        auto col_input = std::make_shared<ColumnVector>(
            TargetBitmap(active_count_), TargetBitmap(active_count_));
        TargetBitmapView data(col_input->GetRawData(), col_input->size());
        segment_->mask_with_delete(data, active_count_, query_timestamp_);
        is_finished_ = true;
        return std::make_shared<RowVector>(std::vector<VectorPtr>{col_input});
    }

    // Level 3: Default path (has filter / growing / TTL)
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

    // input_ have already been updated
    return std::make_shared<RowVector>(std::vector<VectorPtr>{col_input});
}

bool
PhyMvccNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus