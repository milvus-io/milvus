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

#include "pb/segcore.pb.h"
#include "query/Plan.h"
#include "segcore/Collection.h"
#include "segcore/plan_c.h"
#include "common/CGoHelper.h"

CStatus
CreateSearchPlan(CCollection c_col, const char* dsl, CSearchPlan* res_plan) {
    try {
        auto col = reinterpret_cast<milvus::segcore::Collection*>(c_col);
        auto res = milvus::query::CreatePlan(*col->get_schema(), dsl);
        *res_plan = res.release();
        return milvus::SuccessCStatus();
    } catch (milvus::SegcoreError& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    } catch (std::exception& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

// Note: serialized_expr_plan is of binary format
CStatus
CreateSearchPlanByExpr(CCollection c_col, const void* serialized_expr_plan, const int64_t size, CSearchPlan* res_plan) {
    try {
        auto col = reinterpret_cast<milvus::segcore::Collection*>(c_col);
        auto res = milvus::query::CreateSearchPlanByExpr(*col->get_schema(), serialized_expr_plan, size);
        *res_plan = res.release();
        return milvus::SuccessCStatus();
    } catch (milvus::SegcoreError& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    } catch (std::exception& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
ParsePlaceholderGroup(CSearchPlan c_plan,
                      const void* placeholder_group_blob,
                      const int64_t blob_size,
                      CPlaceholderGroup* res_placeholder_group) {
    try {
        std::string blob_str(reinterpret_cast<char*>(const_cast<void*>(placeholder_group_blob)), blob_size);
        auto plan = reinterpret_cast<milvus::query::Plan*>(c_plan);
        auto res = milvus::query::ParsePlaceholderGroup(plan, blob_str);
        *res_placeholder_group = res.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetNumOfQueries(CPlaceholderGroup placeholder_group, int64_t* nq) {
    try {
        *nq = milvus::query::GetNumOfQueries(reinterpret_cast<milvus::query::PlaceholderGroup*>(placeholder_group));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetTopK(CSearchPlan plan, int64_t* topK) {
    try {
        *topK = milvus::query::GetTopK(reinterpret_cast<milvus::query::Plan*>(plan));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeleteSearchPlan(CSearchPlan cPlan) {
    try {
        auto plan = reinterpret_cast<milvus::query::Plan*>(cPlan);
        delete plan;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeletePlaceholderGroup(CPlaceholderGroup cPlaceholder_group) {
    try {
        auto placeHolder_group = reinterpret_cast<milvus::query::PlaceholderGroup*>(cPlaceholder_group);
        delete placeHolder_group;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
CreateRetrievePlanByExpr(CCollection c_col,
                         const void* serialized_expr_plan,
                         const int64_t size,
                         CRetrievePlan* res_plan) {
    try {
        auto col = reinterpret_cast<milvus::segcore::Collection*>(c_col);
        auto res = milvus::query::CreateRetrievePlanByExpr(*col->get_schema(), serialized_expr_plan, size);
        *res_plan = res.release();
        return milvus::SuccessCStatus();
    } catch (milvus::SegcoreError& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    } catch (std::exception& e) {
        *res_plan = nullptr;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeleteRetrievePlan(CRetrievePlan c_plan) {
    try {
        auto plan = reinterpret_cast<milvus::query::RetrievePlan*>(c_plan);
        delete plan;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
