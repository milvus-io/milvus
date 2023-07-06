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

#include "common/CGoHelper.h"
#include "pb/segcore.pb.h"
#include "query/Plan.h"
#include "segcore/Collection.h"
#include "segcore/plan_c.h"

// Note: serialized_expr_plan is of binary format
CStatus
CreateSearchPlanByExpr(CCollection c_col,
                       const void* serialized_expr_plan,
                       const int64_t size,
                       CSearchPlan* res_plan) {
    auto col = (milvus::segcore::Collection*)c_col;

    try {
        auto res = milvus::query::CreateSearchPlanByExpr(
            *col->get_schema(), serialized_expr_plan, size);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto plan = (CSearchPlan)res.release();
        *res_plan = plan;
        return status;
    } catch (milvus::SegcoreError& e) {
        auto status = CStatus();
        status.error_code = e.get_error_code();
        status.error_msg = strdup(e.what());
        *res_plan = nullptr;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *res_plan = nullptr;
        return status;
    }
}

CStatus
ParsePlaceholderGroup(CSearchPlan c_plan,
                      const void* placeholder_group_blob,
                      const int64_t blob_size,
                      CPlaceholderGroup* res_placeholder_group) {
    auto plan = (milvus::query::Plan*)c_plan;

    try {
        auto res = milvus::query::ParsePlaceholderGroup(
            plan, (const uint8_t*)(placeholder_group_blob), blob_size);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto group = (CPlaceholderGroup)res.release();
        *res_placeholder_group = group;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *res_placeholder_group = nullptr;
        return status;
    }
}

int64_t
GetNumOfQueries(CPlaceholderGroup placeholder_group) {
    auto res = milvus::query::GetNumOfQueries(
        (milvus::query::PlaceholderGroup*)placeholder_group);
    return res;
}

int64_t
GetTopK(CSearchPlan plan) {
    auto res = milvus::query::GetTopK((milvus::query::Plan*)plan);
    return res;
}

CStatus
GetFieldID(CSearchPlan plan, int64_t* field_id) {
    try {
        auto p = static_cast<const milvus::query::Plan*>(plan);
        *field_id = milvus::query::GetFieldID(p);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, strdup(e.what()));
    }
}

const char*
GetMetricType(CSearchPlan plan) {
    auto search_plan = static_cast<milvus::query::Plan*>(plan);
    auto& metric_str = search_plan->plan_node_->search_info_.metric_type_;
    return strdup(metric_str.c_str());
}

void
SetMetricType(CSearchPlan plan, const char* metric_type) {
    auto search_plan = static_cast<milvus::query::Plan*>(plan);
    if (search_plan->plan_node_->search_info_.metric_type_ == "") {
        search_plan->plan_node_->search_info_.metric_type_ =
            std::string(metric_type);
    }
}

void
DeleteSearchPlan(CSearchPlan cPlan) {
    auto plan = (milvus::query::Plan*)cPlan;
    delete plan;
}

void
DeletePlaceholderGroup(CPlaceholderGroup cPlaceholder_group) {
    auto placeHolder_group =
        (milvus::query::PlaceholderGroup*)cPlaceholder_group;
    delete placeHolder_group;
}

CStatus
CreateRetrievePlanByExpr(CCollection c_col,
                         const void* serialized_expr_plan,
                         const int64_t size,
                         CRetrievePlan* res_plan) {
    auto col = (milvus::segcore::Collection*)c_col;

    try {
        auto res = milvus::query::CreateRetrievePlanByExpr(
            *col->get_schema(), serialized_expr_plan, size);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto plan = (CRetrievePlan)res.release();
        *res_plan = plan;
        return status;
    } catch (milvus::SegcoreError& e) {
        auto status = CStatus();
        status.error_code = e.get_error_code();
        status.error_msg = strdup(e.what());
        *res_plan = nullptr;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *res_plan = nullptr;
        return status;
    }
}

void
DeleteRetrievePlan(CRetrievePlan c_plan) {
    auto plan = (milvus::query::RetrievePlan*)c_plan;
    delete plan;
}
