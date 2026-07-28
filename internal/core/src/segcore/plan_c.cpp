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

#include <algorithm>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string.h>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "NamedType/underlying_functionalities.hpp"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/SearchBruteForce.h"
#include "segcore/Collection.h"
#include "segcore/plan_c.h"

// Note: serialized_expr_plan is of binary format
CStatus
CreateSearchPlanByExpr(CCollection c_col,
                       const void* serialized_expr_plan,
                       const int64_t size,
                       CSearchPlan* res_plan) {
    auto col = static_cast<milvus::segcore::Collection*>(c_col);
    auto schema = col->get_schema();

    try {
        auto res = milvus::query::CreateSearchPlanByExpr(
            schema, serialized_expr_plan, size);
        // Plan creation no longer consults a collection-wide index meta.
        //
        // It used to reject the whole shard with FieldNotLoaded when the field was
        // missing from that snapshot, and to source the metric type and brute-force
        // params from it. But the snapshot is refreshed out of band -- QueryCoord
        // RPCs each carrying a whole ListIndexes result, with no ordering authority
        // -- so a late older one could reject a search the segments were perfectly
        // able to serve. It also never guaranteed what it appeared to: a field
        // present in it may still be missing from any given segment.
        //
        // Whether a segment can answer, and with what metric, is a property of that
        // segment. Both are now resolved there, from its loaded index or its own
        // load info, and travel back up on SearchResult for reduce. The metric the
        // request named, if any, still reaches the plan through PlanProto and takes
        // precedence.

        auto status = CStatus();
        status.error_code = milvus::Success;
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
        status.error_code = milvus::UnexpectedError;
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
        status.error_code = milvus::Success;
        status.error_msg = "";
        auto group = (CPlaceholderGroup)res.release();
        *res_placeholder_group = group;
        return status;
    } catch (milvus::SegcoreError& e) {
        *res_placeholder_group = nullptr;
        return milvus::FailureCStatus(e.get_error_code(), e.what());
    } catch (std::exception& e) {
        *res_placeholder_group = nullptr;
        return milvus::FailureCStatus(&e);
    }
}

int64_t
GetNumOfQueries(CPlaceholderGroup placeholder_group) {
    auto res = milvus::query::GetNumOfQueries(
        static_cast<milvus::query::PlaceholderGroup*>(placeholder_group));
    return res;
}

int64_t
GetTopK(CSearchPlan plan) {
    auto res = milvus::query::GetTopK(static_cast<milvus::query::Plan*>(plan));
    return res;
}

CStatus
GetFieldID(CSearchPlan plan, int64_t* field_id) {
    try {
        auto p = static_cast<const milvus::query::Plan*>(plan);
        *field_id = milvus::query::GetFieldID(p);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

const char*
GetMetricType(CSearchPlan plan) {
    auto search_plan = static_cast<milvus::query::Plan*>(plan);
    auto& metric_str = search_plan->plan_node_->search_info_.metric_type_;
    return strdup(metric_str.c_str());
}

bool
HasTargetEntries(CSearchPlan plan) {
    auto search_plan = static_cast<milvus::query::Plan*>(plan);
    return !search_plan->target_entries_.empty();
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
    auto plan = static_cast<milvus::query::Plan*>(cPlan);
    delete plan;
}

void
DeletePlaceholderGroup(CPlaceholderGroup cPlaceholder_group) {
    auto placeHolder_group =
        static_cast<milvus::query::PlaceholderGroup*>(cPlaceholder_group);
    delete placeHolder_group;
}

CStatus
CreateRetrievePlanByExpr(CCollection c_col,
                         const void* serialized_expr_plan,
                         const int64_t size,
                         CRetrievePlan* res_plan) {
    auto col = static_cast<milvus::segcore::Collection*>(c_col);

    try {
        auto res = milvus::query::CreateRetrievePlanByExpr(
            col->get_schema(), serialized_expr_plan, size);

        auto status = CStatus();
        status.error_code = milvus::Success;
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
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        *res_plan = nullptr;
        return status;
    }
}

void
DeleteRetrievePlan(CRetrievePlan c_plan) {
    auto plan = static_cast<milvus::query::RetrievePlan*>(c_plan);
    delete plan;
}

bool
ShouldIgnoreNonPk(CRetrievePlan c_plan) {
    auto plan = static_cast<milvus::query::RetrievePlan*>(c_plan);
    // ORDER BY queries must not use two-phase retrieval: the pipeline
    // returns data in a positional layout [pk, orderby, remaining] that
    // the Go-side Remap depends on.  RetrieveByOffsets would re-fetch
    // via FillTargetEntry in field_ids_ order, breaking that layout.
    if (plan->plan_node_ && plan->plan_node_->has_order_by_) {
        return false;
    }
    auto pk_field = plan->schema_->get_primary_field_id();
    auto only_contain_pk = pk_field.has_value() &&
                           plan->field_ids_.size() == 1 &&
                           pk_field.value() == plan->field_ids_[0];
    return !only_contain_pk;
}
