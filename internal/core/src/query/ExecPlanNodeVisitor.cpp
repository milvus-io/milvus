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

#include "query/ExecPlanNodeVisitor.h"

#include <memory>
#include <utility>

#include "expr/ITypeExpr.h"
#include "query/PlanImpl.h"
#include "query/SubSearchResult.h"
#include "query/Utils.h"
#include "segcore/SegmentGrowing.h"
#include "common/Json.h"
#include "log/Log.h"
#include "plan/PlanNode.h"
#include "exec/Task.h"
#include "segcore/SegmentInterface.h"
#include "common/Tracer.h"
namespace milvus::query {

static SearchResult
empty_search_result(int64_t num_queries) {
    SearchResult final_result;
    final_result.total_nq_ = num_queries;
    final_result.unity_topK_ = 0;  // no result
    final_result.total_data_cnt_ = 0;
    return final_result;
}

BitsetType
ExecPlanNodeVisitor::ExecuteTask(
    plan::PlanFragment& plan,
    std::shared_ptr<milvus::exec::QueryContext> query_context,
    bool collect_bitset) {
    tracer::AutoSpan span("ExecuteTask", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("active_count",
                                 query_context->get_active_count());

    LOG_DEBUG("plannode: {}, active_count: {}, timestamp: {}",
              plan.plan_node_->ToString(),
              query_context->get_active_count(),
              query_context->get_query_timestamp());
    auto task =
        milvus::exec::Task::Create(DEFAULT_TASK_ID, plan, 0, query_context);
    int64_t processed_num = 0;
    BitsetType bitset_holder;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            Assert(processed_num == query_context->get_active_count());
            break;
        }
        auto childrens = result->childrens();
        AssertInfo(childrens.size() == 1,
                   "plannode result vector's children size not equal one");
        LOG_DEBUG("output result length:{}", childrens[0]->size());
        if (auto vec = std::dynamic_pointer_cast<ColumnVector>(childrens[0])) {
            processed_num += vec->size();
            if (collect_bitset) {
                BitsetTypeView view(vec->GetRawData(), vec->size());
                bitset_holder.append(view);
            }
        } else {
            ThrowInfo(UnexpectedError, "expr return type not matched");
        }
    }

    span.GetSpan()->SetAttribute("total_rows", processed_num);

    return bitset_holder;
}

std::unique_ptr<RetrieveResult>
wrap_num_entities(int64_t cnt) {
    auto retrieve_result = std::make_unique<RetrieveResult>();
    DataArray arr;
    arr.set_type(milvus::proto::schema::Int64);
    auto scalar = arr.mutable_scalars();
    scalar->mutable_long_data()->mutable_data()->Add(cnt);
    retrieve_result->field_data_ = {arr};
    retrieve_result->total_data_cnt_ = 0;
    return retrieve_result;
}

void
ExecPlanNodeVisitor::visit(RetrievePlanNode& node) {
    assert(!retrieve_result_opt_.has_value());
    auto segment =
        dynamic_cast<const segcore::SegmentInternalInterface*>(&segment_);
    AssertInfo(segment, "Support SegmentSmallIndex Only");
    RetrieveResult retrieve_result;
    retrieve_result.total_data_cnt_ = 0;

    auto active_count = segment->get_active_count(timestamp_);

    // PreExecute: skip all calculation
    if (active_count == 0 && !node.is_count_) {
        retrieve_result_opt_ = std::move(retrieve_result);
        return;
    }

    if (active_count == 0 && node.is_count_) {
        retrieve_result = *(wrap_num_entities(0));
        retrieve_result_opt_ = std::move(retrieve_result);
        return;
    }

    // Get plan
    auto plan = plan::PlanFragment(node.plannodes_);

    // Set query context
    auto query_context =
        std::make_shared<milvus::exec::QueryContext>(DEAFULT_QUERY_ID,
                                                     segment,
                                                     active_count,
                                                     timestamp_,
                                                     collection_ttl_timestamp_,
                                                     consistency_level_,
                                                     node.plan_options_);

    // Set op context to query context
    auto op_context = milvus::OpContext(cancel_token_);
    query_context->set_op_context(&op_context);

    // Do task execution
    auto bitset_holder = ExecuteTask(plan, query_context);

    // Store result
    if (node.is_count_) {
        retrieve_result_opt_ = std::move(query_context->get_retrieve_result());
        retrieve_result_opt_->retrieve_storage_cost_.scanned_remote_bytes =
            op_context.storage_usage.scanned_cold_bytes.load();
        retrieve_result_opt_->retrieve_storage_cost_.scanned_total_bytes =
            op_context.storage_usage.scanned_total_bytes.load();
    } else {
        retrieve_result.total_data_cnt_ = bitset_holder.size();
        tracer::AutoSpan _("Find Limit Pk", tracer::GetRootSpan(), true);
        auto results_pair = segment->find_first(node.limit_, bitset_holder);
        retrieve_result.result_offsets_ = std::move(results_pair.first);
        retrieve_result.has_more_result = results_pair.second;
        retrieve_result_opt_ = std::move(retrieve_result);
        retrieve_result_opt_->retrieve_storage_cost_.scanned_remote_bytes =
            op_context.storage_usage.scanned_cold_bytes.load();
        retrieve_result_opt_->retrieve_storage_cost_.scanned_total_bytes =
            op_context.storage_usage.scanned_total_bytes.load();
    }
}

void
ExecPlanNodeVisitor::visit(VectorPlanNode& node) {
    assert(!search_result_opt_.has_value());
    auto segment =
        dynamic_cast<const segcore::SegmentInternalInterface*>(&segment_);
    AssertInfo(segment, "support SegmentSmallIndex Only");

    auto active_count = segment->get_active_count(timestamp_);

    // PreExecute: skip all calculation
    if (active_count == 0) {
        search_result_opt_ = std::move(
            empty_search_result(placeholder_group_->at(0).num_of_queries_));
        return;
    }

    // Construct plan fragment
    auto plan = plan::PlanFragment(node.plannodes_);

    // Set query context
    auto query_context =
        std::make_shared<milvus::exec::QueryContext>(DEAFULT_QUERY_ID,
                                                     segment,
                                                     active_count,
                                                     timestamp_,
                                                     collection_ttl_timestamp_,
                                                     consistency_level_,
                                                     node.plan_options_);

    query_context->set_search_info(node.search_info_);
    query_context->set_placeholder_group(placeholder_group_);

    // Set op context to query context
    auto op_context = milvus::OpContext(cancel_token_);
    query_context->set_op_context(&op_context);

    // Do plan fragment task work (search doesn't use bitset return value)
    ExecuteTask(plan, query_context, false);

    // Store result
    search_result_opt_ = std::move(query_context->get_search_result());
    search_result_opt_->search_storage_cost_.scanned_remote_bytes =
        op_context.storage_usage.scanned_cold_bytes.load();
    search_result_opt_->search_storage_cost_.scanned_total_bytes =
        op_context.storage_usage.scanned_total_bytes.load();
}

}  // namespace milvus::query
