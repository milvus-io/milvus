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
#include "query/PlanProto.h"
#include "query/SubSearchResult.h"
#include "query/Utils.h"
#include "segcore/SegmentGrowing.h"
#include "common/Json.h"
#include "log/Log.h"
#include "plan/PlanNode.h"
#include "exec/Task.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Utils.h"
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

RowVectorPtr
ExecPlanNodeVisitor::ExecuteTask(
    plan::PlanFragment& plan,
    std::shared_ptr<milvus::exec::QueryContext> query_context) {
    tracer::AutoSpan span("ExecuteTask", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("active_count",
                                 query_context->get_active_count());

    LOG_DEBUG("plannode: {}, active_count: {}, timestamp: {}",
              plan.plan_node_->ToString(),
              query_context->get_active_count(),
              query_context->get_query_timestamp());

    auto task =
        milvus::exec::Task::Create(DEFAULT_TASK_ID, plan, 0, query_context);
    RowVectorPtr ret = nullptr;
    int64_t processed_num = 0;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            if (ret && !ret->childrens().empty()) {
                auto first_column =
                    std::dynamic_pointer_cast<ColumnVector>(ret->child(0));
                AssertInfo(first_column,
                           "first column must be a column vector");
                if (first_column->IsBitmap()) {
                    if (query_context->get_active_element_count() > 0) {
                        Assert(processed_num ==
                               query_context->get_active_element_count());
                    } else {
                        Assert(processed_num ==
                               query_context->get_active_count());
                    }
                }
            }
            break;
        }
        processed_num += result->size();
        if (ret) {
            auto childrens = result->childrens();
            AssertInfo(childrens.size() == ret->childrens().size(),
                       "column count of row vectors in different rounds"
                       "should be consistent, ret_column_count:{}, "
                       "new_result_column_count:{}",
                       childrens.size(),
                       ret->childrens().size());
            for (auto i = 0; i < childrens.size(); i++) {
                if (auto column_vec =
                        std::dynamic_pointer_cast<ColumnVector>(childrens[i])) {
                    auto ret_column_vector =
                        std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
                    ret_column_vector->append(*column_vec);
                } else {
                    ThrowInfo(UnexpectedError, "expr return type not matched");
                }
            }
        } else {
            ret = result;
        }
    }
    span.GetSpan()->SetAttribute("total_rows", processed_num);
    span.GetSpan()->SetAttribute("matched_rows",
                                 ret ? processed_num - ret->nullCount() : 0);
    return ret;
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

template <typename S, typename T>
void
fillTypedDataArray(void* src_raw_data, int64_t count, T* dst) {
    static_assert(IsScalar<T>);
    const S* src_data = static_cast<const S*>(src_raw_data);
    for (auto i = 0; i < count; i++) {
        dst[i] = src_data[i];
    }
}

template <typename S, typename T>
void
fillTypedDataPtrArray(void* src_raw_data,
                      int64_t count,
                      google::protobuf::RepeatedPtrField<T>* dst) {
    static_assert(IsScalar<T>);
    const S* src_data = static_cast<const S*>(src_raw_data);
    for (int i = 0; i < count; i++) {
        dst->at(i) = T(src_data[i]);
    }
}

void
fillDataArrayFromColumnVector(const ColumnVectorPtr& column_vector,
                              DataArray& data_array) {
    auto column_raw_data = column_vector->GetRawData();
    auto column_data_size = column_vector->size();
    switch (column_vector->type()) {
        case DataType::BOOL: {
            auto bool_data = data_array.mutable_scalars()->mutable_bool_data();
            fillTypedDataArray<bool>(column_raw_data,
                                     column_data_size,
                                     bool_data->mutable_data()->mutable_data());
            break;
        }
        case DataType::INT8: {
            auto int_data = data_array.mutable_scalars()->mutable_int_data();
            fillTypedDataArray<int8_t, int32_t>(
                column_raw_data,
                column_data_size,
                int_data->mutable_data()->mutable_data());
            break;
        }
        case DataType::INT16: {
            auto int_data = data_array.mutable_scalars()->mutable_int_data();
            fillTypedDataArray<int16_t, int32_t>(
                column_raw_data,
                column_data_size,
                int_data->mutable_data()->mutable_data());
            break;
        }
        case DataType::INT32: {
            auto int_data = data_array.mutable_scalars()->mutable_int_data();
            fillTypedDataArray<int32_t>(
                column_raw_data,
                column_data_size,
                int_data->mutable_data()->mutable_data());
            break;
        }
        case DataType::INT64: {
            auto longData = data_array.mutable_scalars()->mutable_long_data();
            fillTypedDataArray<int64_t>(
                column_raw_data,
                column_data_size,
                longData->mutable_data()->mutable_data());
            break;
        }
        case DataType::TIMESTAMPTZ: {
            auto timestamptzData =
                data_array.mutable_scalars()->mutable_timestamptz_data();
            fillTypedDataArray<int64_t>(
                column_raw_data,
                column_data_size,
                timestamptzData->mutable_data()->mutable_data());
            break;
        }
        case DataType::FLOAT: {
            auto float_data =
                data_array.mutable_scalars()->mutable_float_data();
            fillTypedDataArray<float>(
                column_raw_data,
                column_data_size,
                float_data->mutable_data()->mutable_data());
            break;
        }
        case DataType::DOUBLE: {
            auto double_data =
                data_array.mutable_scalars()->mutable_double_data();
            fillTypedDataArray<double>(
                column_raw_data,
                column_data_size,
                double_data->mutable_data()->mutable_data());
            break;
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            auto string_data =
                data_array.mutable_scalars()->mutable_string_data();
            fillTypedDataPtrArray<std::string>(
                column_raw_data, column_data_size, string_data->mutable_data());
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported data type {}", column_vector->type()));
        }
    }
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
    auto result = ExecuteTask(plan, query_context);
    setupRetrieveResult(result, op_context, node, retrieve_result, segment);
}

void
ExecPlanNodeVisitor::setupRetrieveResult(
    const milvus::RowVectorPtr& result,
    const OpContext& op_context,
    const RetrievePlanNode& node,
    RetrieveResult& tmp_retrieve_result,
    const segcore::SegmentInternalInterface* segment) {
    if (result == nullptr) {
        retrieve_result_opt_ = std::move(tmp_retrieve_result);
        return;
    }
    AssertInfo(!result->childrens().empty(),
               "Result row vector must have at least one column");
    auto first_column =
        std::dynamic_pointer_cast<ColumnVector>(result->child(0));
    AssertInfo(first_column,
               "children inside row vector must be of column vector for now");
    tmp_retrieve_result.total_data_cnt_ = first_column->size();
    if (first_column->IsBitmap()) {
        tracer::AutoSpan _("Find Limit Pk", tracer::GetRootSpan());
        BitsetTypeView view(first_column->GetRawData(), first_column->size());
        auto results_pair = segment->find_first(node.limit_, view);
        tmp_retrieve_result.result_offsets_ = std::move(results_pair.first);
        tmp_retrieve_result.has_more_result = results_pair.second;
        retrieve_result_opt_ = std::move(tmp_retrieve_result);
    } else {
        // load data in the result vector into retrieve_result
        tracer::AutoSpan _("Load Column Data", tracer::GetRootSpan());
        auto column_count = result->childrens().size();
        tmp_retrieve_result.field_data_.resize(column_count);
        for (auto i = 0; i < column_count; i++) {
            auto column_vec =
                std::dynamic_pointer_cast<ColumnVector>(result->child(i));
            AssertInfo(
                column_vec,
                "children inside row vector must be of column vector for now");
            DataArray data_array;
            milvus::segcore::CreateScalarDataArray(data_array,
                                                   column_vec->size(),
                                                   column_vec->type(),
                                                   column_vec->type(),
                                                   column_vec->nullCount() > 0);
            fillDataArrayFromColumnVector(column_vec, data_array);
            tmp_retrieve_result.field_data_[i] = std::move(data_array);
        }
        retrieve_result_opt_ = std::move(tmp_retrieve_result);
    }
    retrieve_result_opt_->retrieve_storage_cost_.scanned_remote_bytes =
        op_context.storage_usage.scanned_cold_bytes.load();
    retrieve_result_opt_->retrieve_storage_cost_.scanned_total_bytes =
        op_context.storage_usage.scanned_total_bytes.load();
}

void
ExecPlanNodeVisitor::visit(VectorPlanNode& node) {
    assert(!search_result_opt_.has_value());
    auto segment =
        dynamic_cast<const segcore::SegmentInternalInterface*>(&segment_);
    AssertInfo(segment, "support SegmentSmallIndex Only");

    auto active_count = segment->get_active_count(timestamp_);

    // Handle filter-only mode: execute only the filter and return valid_count
    if (filter_only_) {
        SearchResult filter_only_result;
        filter_only_result.total_nq_ = 0;
        filter_only_result.unity_topK_ = 0;
        filter_only_result.total_data_cnt_ = 0;

        if (active_count == 0) {
            filter_only_result.valid_count_ = 0;
            search_result_opt_ = std::move(filter_only_result);
            return;
        }

        auto filter_only_plan =
            ProtoParser::ExtractFilterOnlyPlan(node.plannodes_);

        AssertInfo(filter_only_plan != nullptr,
                   "filter only plan is nullptr, please check the plan");

        auto plan_fragment = plan::PlanFragment(filter_only_plan);
        auto query_context =
            std::make_shared<exec::QueryContext>(DEAFULT_QUERY_ID,
                                                 segment,
                                                 active_count,
                                                 timestamp_,
                                                 collection_ttl_timestamp_,
                                                 consistency_level_,
                                                 node.plan_options_);

        auto result = ExecuteTask(plan_fragment, query_context);

        if (result == nullptr || result->childrens().empty()) {
            filter_only_result.valid_count_ = active_count;
            search_result_opt_ = std::move(filter_only_result);
            return;
        }

        auto col_vec =
            std::dynamic_pointer_cast<ColumnVector>(result->childrens()[0]);
        AssertInfo(col_vec != nullptr,
                   "filter result must be a column vector with bitset");

        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        auto valid_count = active_count - view.count();
        LOG_DEBUG("filter only result validCount: {}, activeCount: {}",
                  valid_count,
                  active_count);
        filter_only_result.valid_count_ = valid_count;
        search_result_opt_ = std::move(filter_only_result);
        return;
    }

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

    // Do plan fragment task work
    auto result = ExecuteTask(plan, query_context);

    // Store result
    search_result_opt_ = std::move(query_context->get_search_result());
    search_result_opt_->search_storage_cost_.scanned_remote_bytes =
        op_context.storage_usage.scanned_cold_bytes.load();
    search_result_opt_->search_storage_cost_.scanned_total_bytes =
        op_context.storage_usage.scanned_total_bytes.load();
}

}  // namespace milvus::query
