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

#pragma once
#include "Reduce.h"
#include "common/QueryResult.h"
#include "query/PlanImpl.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"

namespace milvus::segcore {
class OrderByReduceHelper : public ReduceHelper {
 public:
    explicit OrderByReduceHelper(std::vector<SearchResult*>& search_results,
                                 milvus::query::Plan* plan,
                                 int64_t* slice_nqs,
                                 int64_t* slice_topKs,
                                 int64_t slice_num,
                                 tracer::TraceContext* trace_ctx)
        : ReduceHelper(search_results,
                       plan,
                       slice_nqs,
                       slice_topKs,
                       slice_num,
                       trace_ctx) {
        // Extract order_by_fields from plan
        if (plan->plan_node_->search_info_.order_by_fields_.has_value()) {
            order_by_fields_ = plan->plan_node_->search_info_.order_by_fields_.value();
        }
    }

 protected:
    int64_t
    ReduceSearchResultForOneNQ(int64_t qi,
                               int64_t topk,
                               int64_t& result_offset) override;

    void
    FillOtherData(int result_count,
                  int64_t nq_begin,
                  int64_t nq_end,
                  std::unique_ptr<milvus::proto::schema::SearchResultData>&
                      search_res_data) override;

 private:
    std::vector<plan::OrderByField> order_by_fields_;

    // Helper to read order_by field values for a SearchResultPair
    void
    ReadOrderByValues(SearchResultPair* pair);

    // Helper to create DataGetters for order_by fields
    void
    InitializeDataGetters(SearchResult* search_result);
};

}  // namespace milvus::segcore
